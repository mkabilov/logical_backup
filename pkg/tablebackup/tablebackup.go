package tablebackup

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"

	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/queue"
)

const (
	dirPerms       = os.ModePerm
	archiverBuffer = 100
	deltasDir      = "deltas"
)

type TableBackuper interface {
	SaveRawMessage([]byte, uint64) (uint64, error)
	Basebackup() error
	Files() int
	Truncate() error
	String() string
	CloseOldFiles() error
}

type TableBackup struct {
	message.Identifier

	ctx context.Context

	// Table info
	oid uint32

	// Basebackup
	tx    *pgx.Tx
	conn  *pgx.Conn
	cfg   *config.Config
	dbCfg pgx.ConnConfig

	// Files
	tableDir           string
	archiveDir         string
	basebackupFilename string
	infoFilename       string

	// Deltas
	deltaCnt             int
	deltaFilesCnt        int
	deltasSinceBackupCnt int
	filenamePostfix      uint32
	lastLSN              uint64
	currentDeltaFp       *os.File
	currentDeltaFilename string

	// Basebackup
	basebackupLSN       uint64
	lastBasebackupTime  time.Time
	sleepBetweenBackups time.Duration
	lastBackupDuration  time.Duration
	lastWrittenMessage  time.Time

	locker uint32

	basebackupQueue *queue.Queue
	msgLen          []byte

	archiveFiles chan string // path relative to table dir
}

func New(ctx context.Context, cfg *config.Config, tbl message.Identifier, dbCfg pgx.ConnConfig, basebackupsQueue *queue.Queue) (*TableBackup, error) { //TODO: maybe use oid instead of schema-name pair?
	tblHash := hash(tbl)
	tableDir := fmt.Sprintf("%s/%s/%s/%s/%s.%s", tblHash[0:2], tblHash[2:4], tblHash[4:6], tblHash, tbl.Namespace, tbl.Name)

	tb := TableBackup{
		Identifier:          tbl,
		ctx:                 ctx,
		sleepBetweenBackups: time.Second * 3,
		cfg:                 cfg,
		dbCfg:               dbCfg,
		tableDir:            path.Join(cfg.TempDir, tableDir),
		archiveDir:          path.Join(cfg.ArchiveDir, tableDir),
		basebackupFilename:  "basebackup.copy",
		infoFilename:        "info.yaml",
		msgLen:              make([]byte, 8),
		archiveFiles:        make(chan string, archiverBuffer),
	}

	if err := tb.createDirs(); err != nil {
		return nil, fmt.Errorf("could not create dirs: %v", err)
	}

	tb.basebackupQueue = basebackupsQueue

	go tb.archiver()
	go tb.periodicBackup()

	return &tb, nil
}

func (t *TableBackup) SaveRawMessage(msg []byte, lsn uint64) (uint64, error) {
	if t.currentDeltaFp == nil {
		if err := t.rotateFile(lsn); err != nil {
			return 0, fmt.Errorf("could not create file: %v", err)
		}
	}

	if t.deltaCnt >= t.cfg.DeltasPerFile {
		if err := t.rotateFile(lsn); err != nil {
			return 0, fmt.Errorf("could not rotate file: %v", err)
		}
	}

	if t.deltaFilesCnt%t.cfg.BackupThreshold == 0 && t.deltaCnt == 0 {
		log.Printf("queueing base backup because we reached backupDeltaThreshold %s", t)
		t.basebackupQueue.Put(t)
	}

	t.deltaCnt++
	t.deltasSinceBackupCnt++

	ln := uint64(len(msg) + 8)

	binary.BigEndian.PutUint64(t.msgLen, ln)

	_, err := t.currentDeltaFp.Write(append(t.msgLen, msg...))
	if err != nil {
		return 0, fmt.Errorf("could not save delta: %v", err)
	}

	if t.cfg.Fsync {
		if err := t.currentDeltaFp.Sync(); err != nil {
			return 0, fmt.Errorf("could not fsync: %v", err)
		}
	}

	t.lastWrittenMessage = time.Now()

	return ln, nil
}

func (t *TableBackup) archiver() {
	for {
		select {
		case file := <-t.archiveFiles:
			sourceFile := path.Join(t.tableDir, file)
			destFile := path.Join(t.archiveDir, file)

			if _, err := os.Stat(sourceFile); os.IsNotExist(err) {
				log.Printf("source file doesn't exist: %q; skipping", sourceFile)
				break
			}

			if st, err := os.Stat(destFile); os.IsExist(err) {
				if st.Size() == 0 {
					os.Remove(destFile)
				} else {
					log.Printf("destination file is not empty %q; skipping", destFile)
				}
			}

			if _, err := copyFile(sourceFile, destFile); err != nil {
				os.Remove(destFile)
				log.Printf("could not move %s -> %s file: %v", sourceFile, destFile, err)
				break
			}

			if err := os.Remove(sourceFile); err != nil {
				log.Printf("could not delete old file: %v", err)
			} else {
				log.Printf("file moved: %s -> %s", sourceFile, destFile)
			}
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *TableBackup) Files() int {
	return t.deltaFilesCnt
}

func (t *TableBackup) rotateFile(newLSN uint64) error {
	if t.currentDeltaFp != nil {
		if err := t.currentDeltaFp.Close(); err != nil {
			return fmt.Errorf("could not close old file: %v", err)
		}

		t.archiveFiles <- t.currentDeltaFilename //TODO: potential lock
	}

	deltaFilename := fmt.Sprintf("%016x", newLSN)
	filename := path.Join(t.tableDir, deltasDir, deltaFilename)
	if _, err := os.Stat(filename); t.lastLSN == newLSN || os.IsExist(err) {
		t.filenamePostfix++
	} else {
		t.filenamePostfix = 0
	}

	if t.filenamePostfix > 0 {
		filename = fmt.Sprintf("%s.%x", filename, t.filenamePostfix)
	}
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}

	t.currentDeltaFilename = path.Join(deltasDir, deltaFilename)
	t.currentDeltaFp = fp
	t.lastLSN = newLSN
	t.deltaFilesCnt++
	t.deltaCnt = 0

	return nil
}

func (t *TableBackup) periodicBackup() {
	periodicBackup := time.NewTicker(t.cfg.PeriodBetweenBackups)
	heartbeat := time.NewTicker(time.Minute)

	for {
		select {
		case <-t.ctx.Done():
			periodicBackup.Stop()
			heartbeat.Stop()
			return
		case <-periodicBackup.C:
			log.Printf("queuing backup of %s", t)
			//t.basebackupQueue.Put(t)
		case <-heartbeat.C:
			if t.lastWrittenMessage.IsZero() || t.cfg.OldDeltaBackupTrigger.Seconds() < 1 {
				break
			}

			if t.deltasSinceBackupCnt == 0 {
				break
			}

			if time.Since(t.lastWrittenMessage) > t.cfg.OldDeltaBackupTrigger {
				log.Printf("queuing backup of %s due to old delta message, lastwritten: %v", t, t.lastWrittenMessage)
				t.basebackupQueue.Put(t)
			}
		}
	}
}

func (t *TableBackup) String() string {
	return t.Identifier.String()
}

func (t *TableBackup) createDirs() error {
	deltasPath := path.Join(t.tableDir, deltasDir)
	archiveDeltasPath := path.Join(t.archiveDir, deltasDir)

	if _, err := os.Stat(deltasPath); os.IsNotExist(err) {
		if err := os.MkdirAll(deltasPath, dirPerms); err != nil {
			return fmt.Errorf("could not create delta dir: %v", err)
		}
	}

	if _, err := os.Stat(archiveDeltasPath); os.IsNotExist(err) {
		if err := os.MkdirAll(archiveDeltasPath, dirPerms); err != nil {
			return fmt.Errorf("could not create archive delta dir: %v", err)
		}
	}

	log.Printf("%s: deltasPath: %s", t, deltasPath)
	log.Printf("%s: archiveDeltasPath: %s", t, archiveDeltasPath)

	return nil
}

func (t *TableBackup) hasRows() (bool, error) {
	var hasRows bool

	row := t.conn.QueryRow(fmt.Sprintf("select exists(select 1 from %s)", t.Identifier.Sanitize()))
	err := row.Scan(&hasRows)

	return hasRows, err
}

func (t *TableBackup) Truncate() error {
	if err := os.RemoveAll(t.tableDir); err != nil {
		return fmt.Errorf("could not recreate table dir: %v", err)
	}

	if err := t.createDirs(); err != nil {
		return err
	}

	t.currentDeltaFp = nil
	t.deltaCnt = 0
	t.deltaFilesCnt = 0
	t.filenamePostfix = 0

	return nil
}

func (t *TableBackup) CloseOldFiles() error {
	if t.lastWrittenMessage.IsZero() {
		return nil
	}

	if !t.lastWrittenMessage.IsZero() && time.Since(t.lastWrittenMessage) <= time.Hour*3 {
		return nil
	}

	return t.currentDeltaFp.Close()
}

func FetchRelationInfo(tx *pgx.Tx, tbl message.Identifier) (message.Relation, error) {
	var rel message.Relation
	row := tx.QueryRow(fmt.Sprintf(`SELECT c.oid, c.relreplident 
		FROM pg_catalog.pg_class c
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = %s AND c.relname = %s`,
		dbutils.QuoteLiteral(tbl.Namespace),
		dbutils.QuoteLiteral(tbl.Name)))

	var relOid uint32
	var relRepIdentity pgtype.BPChar

	if err := row.Scan(&relOid, &relRepIdentity); err != nil {
		return rel, fmt.Errorf("could not fetch table info: %v", err)
	}

	rows, err := tx.Query(fmt.Sprintf(`SELECT
    a.attname,
    t.oid,
	a.atttypmod,
	format_type(t.oid, a.atttypmod)
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid 
WHERE 
    n.nspname = %s AND c.relname = %s
    AND a.attnum > 0::pg_catalog.int2
    AND a.attisdropped = false
ORDER BY
    a.attnum`,
		dbutils.QuoteLiteral(tbl.Namespace),
		dbutils.QuoteLiteral(tbl.Name)))
	if err != nil {
		return rel, fmt.Errorf("could not exec query: %v", err)
	}
	columns := make([]message.Column, 0)

	for rows.Next() {
		var (
			name       string
			attType    uint32
			typMod     int32
			formatType string
		)
		if err := rows.Scan(&name, &attType, &typMod, &formatType); err != nil {
			return rel, fmt.Errorf("could not scan row: %v", err)
		}
		columns = append(columns, message.Column{
			IsKey:         false,
			Name:          name,
			TypeOID:       attType,
			Mode:          typMod,
			FormattedType: formatType,
		})
	}

	rel.Namespace = tbl.Namespace
	rel.Name = tbl.Name
	rel.Columns = columns
	rel.ReplicaIdentity = message.ReplicaIdentity(uint8(relRepIdentity.String[0]))
	rel.OID = relOid

	return rel, nil
}

func hash(tbl message.Identifier) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(tbl.Sanitize())))
}

func copyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)

	return nBytes, err
}

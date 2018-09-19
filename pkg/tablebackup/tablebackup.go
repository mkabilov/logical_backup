package tablebackup

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"gopkg.in/yaml.v2"

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
	fsync bool

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
	deltasDir          string
	basebackupFilename string
	infoFilename       string

	// Deltas
	deltaCnt             int
	deltaFilesCnt        int
	filenamePostfix      uint32
	lastLSN              uint64
	currentDeltaFp       *os.File
	currentDeltaFilename string
	deltasPerFile        int
	backupThreshold      int

	// Basebackup
	basebackupLSN        uint64
	lastBasebackupTime   time.Time
	sleepBetweenBackups  time.Duration
	periodBetweenBackups time.Duration
	lastBackupDuration   time.Duration
	lastWrittenMessage   time.Time

	locker uint32

	basebackupQueue *queue.Queue
	msgLen          []byte

	archiveFiles chan string // path relative to table dir
}

func New(ctx context.Context, cfg *config.Config, tbl message.Identifier, dbCfg pgx.ConnConfig, basebackupsQueue *queue.Queue) (*TableBackup, error) { //TODO: maybe use oid instead of schema-name pair?
	// tb, err := tablebackup.New(b.ctx, b.cfg, t, b.connCfg, b.basebackupQueue)
	tblHash := hash(tbl)
	tableDir := path.Join(cfg.TempDir, fmt.Sprintf("%s/%s/%s/%s/%s.%s", tblHash[0:2], tblHash[2:4], tblHash[4:6], tblHash, tbl.Namespace, tbl.Name))
	tb := TableBackup{
		Identifier:           tbl,
		ctx:                  ctx,
		deltasPerFile:        cfg.DeltasPerFile,
		sleepBetweenBackups:  time.Second * 3,
		cfg:                  cfg,
		dbCfg:                dbCfg,
		tableDir:             tableDir,
		deltasDir:            path.Join(tableDir, deltasDir),
		basebackupFilename:   path.Join(tableDir, "basebackup.copy"),
		infoFilename:         path.Join(tableDir, "info.yaml"),
		periodBetweenBackups: cfg.PeriodBetweenBackups,
		backupThreshold:      cfg.BackupThreshold,
		fsync:                cfg.Fsync,
		msgLen:               make([]byte, 8),
		archiveFiles:         make(chan string, archiverBuffer),
	}

	if err := tb.createDirs(); err != nil {
		return nil, fmt.Errorf("could not create dirs: %v", err)
	}

	tb.basebackupQueue = basebackupsQueue

	go tb.Archiver()

	return &tb, nil
}

func (t *TableBackup) SaveRawMessage(msg []byte, lsn uint64) (uint64, error) {
	if t.currentDeltaFp == nil {
		if err := t.rotateFile(lsn); err != nil {
			return 0, fmt.Errorf("could not create file: %v", err)
		}
	}

	if t.deltaCnt >= t.deltasPerFile {
		if err := t.rotateFile(lsn); err != nil {
			return 0, fmt.Errorf("could not rotate file: %v", err)
		}
	}

	if t.deltaFilesCnt%t.backupThreshold == 0 && t.deltaCnt == 0 {
		log.Printf("queueing base backup because we reached backupDeltaThreshold %s", t)
		t.basebackupQueue.Put(t)
	}

	t.deltaCnt++

	ln := uint64(len(msg) + 8)

	binary.BigEndian.PutUint64(t.msgLen, ln)

	_, err := t.currentDeltaFp.Write(append(t.msgLen, msg...))
	if err != nil {
		return 0, fmt.Errorf("could not save delta: %v", err)
	}

	if t.fsync {
		if err := t.currentDeltaFp.Sync(); err != nil {
			return 0, fmt.Errorf("could not fsync: %v", err)
		}
	}

	t.lastWrittenMessage = time.Now()

	return ln, nil
}

func (t *TableBackup) Archiver() {
	select {
	case file := <-t.archiveFiles:
		log.Printf("file to move to archive: %v", file)
	case <-t.ctx.Done():
		return
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
	filename := path.Join(t.deltasDir, deltaFilename)
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
	for {
		ticker := time.NewTicker(t.periodBetweenBackups)
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			log.Printf("queuing backup of %s", t)
			t.basebackupQueue.Put(t)
		}
	}
}

func (t *TableBackup) Basebackup() error {
	if !atomic.CompareAndSwapUint32(&t.locker, 0, 1) {
		log.Printf("Already locked %s; skipping", t)
		return nil
	}
	defer func() {
		log.Printf("Finished backup of %s", t)
		atomic.StoreUint32(&t.locker, 0)
	}()

	log.Printf("Starting base backup of %s", t)
	tempFilename := fmt.Sprintf("%s.new", t.infoFilename)
	if _, err := os.Stat(tempFilename); os.IsExist(err) {
		os.Remove(tempFilename)
	}

	infoFp, err := os.OpenFile(tempFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not create info file: %v", err)
	}
	defer infoFp.Close()
	defer func() {
		if _, err := os.Stat(tempFilename); os.IsExist(err) {
			os.Remove(tempFilename)
		}
	}()

	if !t.lastBasebackupTime.IsZero() && time.Since(t.lastBasebackupTime) <= t.sleepBetweenBackups {
		log.Printf("base backups happening too often; skipping")
		return nil
	}

	if err := t.connect(); err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}
	defer t.disconnect()

	if err := t.txBegin(); err != nil {
		return fmt.Errorf("could not start transaction: %v", err)
	}

	startTime := time.Now()

	if err := t.createTempReplicationSlot(); err != nil { // slot will be dropped on tx finish
		return fmt.Errorf("could not create replication slot: %v", err)
	}

	if err := t.lockTable(); err != nil {
		return fmt.Errorf("could not lock table: %v", err)
	}

	//TODO: check if table is empty before creating logical replication slot
	if hasRows, err := t.hasRows(); err != nil {
		return fmt.Errorf("could not check if table has rows: %v", err)
	} else if !hasRows {
		log.Printf("table %s seems to have no rows; skipping", t.Identifier)
		return nil
	}

	relationInfo, err := FetchRelationInfo(t.tx, t.Identifier)
	if err != nil {
		return fmt.Errorf("could not fetch table struct: %v", err)
	}

	if err := t.copyDump(); err != nil {
		return fmt.Errorf("could not dump table: %v", err)
	}

	if err := t.txCommit(); err != nil {
		return fmt.Errorf("could not commit: %v", err)
	}

	t.lastBackupDuration = time.Since(startTime)
	err = yaml.NewEncoder(infoFp).Encode(message.DumpInfo{
		StartLSN:       pgx.FormatLSN(t.basebackupLSN),
		CreateDate:     time.Now(),
		Relation:       relationInfo,
		BackupDuration: t.lastBackupDuration.Seconds(),
	})
	if err != nil {
		return fmt.Errorf("could not save info file: %v", err)
	}

	if err := os.Rename(tempFilename, t.infoFilename); err != nil {
		log.Printf("could not rename: %v", err)
	}

	log.Printf("it took %v to base backup for %s table; start lsn: %s ",
		t.lastBackupDuration, t.String(), pgx.FormatLSN(t.basebackupLSN))

	if err := t.RotateOldDeltas(t.deltasDir, t.lastLSN); err != nil {
		return fmt.Errorf("could not archive old deltas: %v", err)
	}

	t.lastBasebackupTime = time.Now()

	return nil
}

func (t *TableBackup) String() string {
	return t.Identifier.String()
}

func (t *TableBackup) createDirs() error {
	if _, err := os.Stat(t.deltasDir); os.IsNotExist(err) {
		if err := os.MkdirAll(t.deltasDir, dirPerms); err != nil {
			return fmt.Errorf("could not create delta dir: %v", err)
		}
	}

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

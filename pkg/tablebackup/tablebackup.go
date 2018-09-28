package tablebackup

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"

	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/queue"
	"github.com/ikitiki/logical_backup/pkg/utils"
)

const (
	dirPerms                 = os.ModePerm
	archiverBuffer           = 100
	deltasDirName            = "deltas"
	maxArchiverRetryTimeout  = 5 * time.Second
	maxArchiverRetryAttempts = 3
	archiverWorkerNapTime    = 500 * time.Millisecond
	archiverCloseNapTime     = 10 * time.Minute
)

type TableBackuper interface {
	SaveRawMessage([]byte, uint64) (uint64, error)
	TableBaseBackup
	Files() int
	Truncate() error
	String() string
}

type TableBaseBackup interface {
	SetBasebackupPending()
	IsBasebackupPending() bool
	ClearBasebackupPending()
	RunBasebackup() error
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
	tempDir            string
	finalDir           string
	basebackupFilename string
	infoFilename       string

	// Deltas
	deltaCnt             int
	deltaFilesCnt        int
	deltasSinceBackupCnt int
	filenamePostfix      uint32
	lastLSN              uint64

	currentDeltaFp       *os.File
	deltaFileAccessMutex *sync.Mutex
	currentDeltaFilename string

	// Basebackup
	firstDeltaLSNToKeep uint64
	lastBasebackupTime  time.Time
	sleepBetweenBackups time.Duration
	lastBackupDuration  time.Duration
	lastWrittenMessage  time.Time
	basebackupIsPending bool

	locker uint32

	basebackupQueue *queue.Queue
	msgLen          []byte

	archiveFiles chan string // path relative to table dir
}

func New(ctx context.Context, cfg *config.Config, tbl message.Identifier, dbCfg pgx.ConnConfig, basebackupsQueue *queue.Queue) (*TableBackup, error) { //TODO: maybe use oid instead of schema-name pair?
	tableDir := utils.TableDir(tbl)

	tb := TableBackup{
		Identifier:           tbl,
		ctx:                  ctx,
		sleepBetweenBackups:  time.Second * 3,
		cfg:                  cfg,
		dbCfg:                dbCfg,
		tempDir:              path.Join(cfg.TempDir, tableDir),
		finalDir:             path.Join(cfg.ArchiveDir, tableDir),
		basebackupFilename:   "basebackup.copy",
		infoFilename:         "info.yaml",
		msgLen:               make([]byte, 8),
		archiveFiles:         make(chan string, archiverBuffer),
		deltaFileAccessMutex: &sync.Mutex{},
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
	t.deltaFileAccessMutex.Lock()
	defer t.deltaFileAccessMutex.Unlock()

	if t.deltaCnt >= t.cfg.DeltasPerFile || t.currentDeltaFp == nil {
		if err := t.createNewDeltaFile(lsn); err != nil {
			return 0, fmt.Errorf("could not rotate file: %v", err)
		}
	}

	if t.deltaFilesCnt%t.cfg.BackupThreshold == 0 && t.deltaCnt == 0 && !t.IsBasebackupPending() {
		log.Printf("queueing base backup because we reached backupDeltaThreshold %s", t)
		t.SetBasebackupPending()
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

// the archiver goroutine is responsible for moving complete files to the final backup directory, as well as for closing
// files in the temporary directory that didn't receive any writes for the specific amount of time (currently 3h)
func (t *TableBackup) archiver() {
	closeCall := time.NewTicker(archiverCloseNapTime)
	q := queue.New(t.ctx)

	// worker routine to do the heavy-lifting
	go func() {
		for {
			select {
			case <-time.After(archiverWorkerNapTime):
				for {
					obj, err := q.Get()
					if err != nil {
						if err == context.Canceled {
							return
						}
						// the only other error we expect is that the queue is empty, in which case we go to sleep
						break
					}
					fname := obj.(string)
					lsn := uint64(0)
					if isDelta, filename := t.isDeltaFileName(fname); isDelta {
						var err error
						lsn, err = utils.GetLSNFromDeltaFilename(filename)
						if err != nil {
							log.Printf("could not decode lsn from the file name %s: %v", filename)
						}
					}

					archiveAction := func() (bool, error) {
						if lsn > 0 && lsn < t.firstDeltaLSNToKeep {
							log.Printf("archiving of segment %s skipped, because the changes predate the latest basebackup lsn %s",
								fname, pgx.FormatLSN(t.firstDeltaLSNToKeep))
							// a  code in RunBasebackup will take care of removing actual files from the temp directory
							return false, nil
						}
						err := archiveOneFile(path.Join(t.tempDir, fname), path.Join(t.finalDir, fname))
						return err != nil, err
					}

					err = utils.Retry(archiveAction, maxArchiverRetryAttempts, archiverWorkerNapTime, maxArchiverRetryTimeout)
					if err != nil {
						log.Printf("could not archive %s: %v", fname, err)
					}
				}
			}
		}
	}()
	for {
		select {
		case file := <-t.archiveFiles:
			q.Put(file)
		case <-t.ctx.Done():
			closeCall.Stop()
			return
		case <-closeCall.C:
			if !t.triggerArchiveTimeoutOnTable() {
				continue
			}
			t.deltaFileAccessMutex.Lock()
			if err := t.currentDeltaFp.Close(); err != nil {
				log.Printf("could not close %s due to inactivity: %v", t.currentDeltaFilename, err)
				t.deltaFileAccessMutex.Unlock()
				continue
			}
			t.currentDeltaFp = nil
			// unlock as soon as possible, but no sooner than clearing up current delta file pointer and name
			fname := t.currentDeltaFilename
			t.currentDeltaFilename = ""

			t.deltaFileAccessMutex.Unlock()

			q.Put(fname)
			log.Printf("archiving %s due to archiver timeout", fname)

		}
	}
}

// if we should archive the active non-empty delta due to the lack of activity on the table for a certain amount of time
// TODO: make sure we don't have empty delta files opened in the first place and remove lastWrittenMessage.IsZero check.
func (t *TableBackup) triggerArchiveTimeoutOnTable() bool {
	return t.currentDeltaFp != nil &&
		!t.lastWrittenMessage.IsZero() &&
		t.cfg.ArchiverTimeout > 0 &&
		time.Since(t.lastWrittenMessage) > t.cfg.ArchiverTimeout
}

// archiveOneFile returns error only if the actual copy failed, so that we could retry it. In all other "unusual" cases
// it just displays a cause and bails out.
func archiveOneFile(sourceFile, destFile string) (err error) {
	if _, err := os.Stat(sourceFile); os.IsNotExist(err) {
		fmt.Printf("source file doesn't exist: %q; skipping", sourceFile)
		return nil
	}

	if st, err := os.Stat(destFile); os.IsExist(err) {
		if st.Size() == 0 {
			os.Remove(destFile)
		} else {
			fmt.Printf("destination file is not empty %q; skipping", destFile)
			return nil

		}
	}

	if _, err := copyFile(sourceFile, destFile); err != nil {
		os.Remove(destFile)
		return fmt.Errorf("could not move %s -> %s file: %v", sourceFile, destFile, err)
	}

	if err := os.Remove(sourceFile); err != nil {
		fmt.Printf("could not delete old file: %v", err)
	}
	return nil
}

func (t *TableBackup) Files() int {
	return t.deltaFilesCnt
}

func (t *TableBackup) createNewDeltaFile(newLSN uint64) error {
	if t.currentDeltaFp != nil {
		if err := t.currentDeltaFp.Close(); err != nil {
			return fmt.Errorf("could not close old file: %v", err)
		}

		t.archiveFiles <- t.currentDeltaFilename
	}

	filename := path.Join(deltasDirName, fmt.Sprintf("%016x", newLSN))
	if _, err := os.Stat(filename); t.lastLSN == newLSN || os.IsExist(err) {
		t.filenamePostfix++
	} else {
		t.filenamePostfix = 0
	}

	if t.filenamePostfix > 0 {
		filename = fmt.Sprintf("%s.%x", filename, t.filenamePostfix)
	}

	fp, err := os.OpenFile(path.Join(t.tempDir, filename), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	t.currentDeltaFp = fp

	t.currentDeltaFilename = filename
	t.lastLSN = newLSN
	t.deltaFilesCnt++
	t.deltaCnt = 0

	return nil
}

func (t *TableBackup) periodicBackup() {
	heartbeat := time.NewTicker(time.Minute)

	for {
		select {
		case <-t.ctx.Done():
			heartbeat.Stop()
			return
		case <-heartbeat.C:
			// there must be some activity on the table and the backup threshold should be more than 0
			if t.lastWrittenMessage.IsZero() ||
				t.cfg.ForceBasebackupAfterInactivityInterval.Seconds() < 1 ||
				t.deltasSinceBackupCnt == 0 ||
				t.IsBasebackupPending() {
				break
			}

			// do we need to create a new backup?
			if time.Since(t.lastWrittenMessage) > t.cfg.ForceBasebackupAfterInactivityInterval {
				log.Printf("last write to the table %s happened %v ago, new backup is queued",
					t, time.Since(t.lastWrittenMessage).Truncate(1*time.Second))
				t.SetBasebackupPending()
				t.basebackupQueue.Put(t)
			}
		}
	}
}

func (t *TableBackup) String() string {
	return t.Identifier.String()
}

func (t *TableBackup) createDirs() error {
	deltasPath := path.Join(t.tempDir, deltasDirName)
	archiveDeltasPath := path.Join(t.finalDir, deltasDirName)

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

	return nil
}

func (t *TableBackup) hasRows() (bool, error) {
	var hasRows bool

	row := t.conn.QueryRow(fmt.Sprintf("select exists(select 1 from %s)", t.Identifier.Sanitize()))
	err := row.Scan(&hasRows)

	return hasRows, err
}

func (t *TableBackup) Truncate() error {
	if err := os.RemoveAll(t.tempDir); err != nil {
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

func (t *TableBackup) isDeltaFileName(name string) (bool, string) {
	if name == t.basebackupFilename || name == t.infoFilename {
		return false, name
	}
	dir, file := path.Split(name)
	if dir == deltasDirName {
		return true, file
	}

	fields := strings.Split(file, ".")
	finalPos := 0
	for pos, ch := range fields[0] {
		if pos >= 16 && !(ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'F') {
			return false, name
		}
		finalPos++
	}
	if finalPos != 16 {
		return false, name
	}
	if len(fields) < 2 {
		return true, file
	}
	if len(fields) == 2 {
		for _, ch := range fields[1] {
			if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
				return false, name
			}
		}
		return true, file
	}
	return false, name

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

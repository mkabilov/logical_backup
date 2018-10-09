package tablebackup

import (
	"bytes"
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
	deltasDirName            = "deltas"
	basebackupFilename       = "basebackup.copy"
	tableInfoFilename        = "info.yaml"
	maxArchiverRetryTimeout  = 5 * time.Second
	maxArchiverRetryAttempts = 3
	archiverWorkerNapTime    = 500 * time.Millisecond
	archiverCloseNapTime     = 1 * time.Minute
)

type TableBackuper interface {
	TableBaseBackup

	SaveRawMessage([]byte, dbutils.Lsn) (uint64, error)
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
	message.NamespacedName

	ctx  context.Context
	wait *sync.WaitGroup

	// Table info
	oid dbutils.Oid

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
	segmentsCnt          int
	deltasSinceBackupCnt int
	lastLSN              dbutils.Lsn

	segmentBufferMutex *sync.Mutex
	segmentFilename    string
	filenamePostfix    uint32

	// buffer for the current delta segment
	segmentBuffer bytes.Buffer

	// Basebackup
	firstDeltaLSNToKeep dbutils.Lsn
	lastBasebackupTime  time.Time
	sleepBetweenBackups time.Duration
	lastBackupDuration  time.Duration
	lastWrittenMessage  time.Time
	basebackupIsPending bool

	locker uint32

	basebackupQueue *queue.Queue
	msgLen          []byte

	archiveFilesQueue *queue.Queue
}

func New(ctx context.Context, group *sync.WaitGroup, cfg *config.Config, tbl message.NamespacedName, oid dbutils.Oid,
	dbCfg pgx.ConnConfig, basebackupsQueue *queue.Queue) (*TableBackup, error) {
	tableDir := utils.TableDir(tbl, oid)

	tb := TableBackup{
		NamespacedName:      tbl,
		oid:                 oid,
		ctx:                 ctx,
		wait:                group,
		sleepBetweenBackups: time.Second * 3,
		cfg:                 cfg,
		dbCfg:               dbCfg,
		tempDir:             path.Join(cfg.TempDir, tableDir),
		finalDir:            path.Join(cfg.ArchiveDir, tableDir),
		basebackupFilename:  basebackupFilename,
		infoFilename:        tableInfoFilename,
		msgLen:              make([]byte, 8),
		archiveFilesQueue:   queue.New(ctx),
		segmentBufferMutex:  &sync.Mutex{},
	}

	if err := tb.createDirs(); err != nil {
		return nil, fmt.Errorf("could not create dirs: %v", err)
	}

	tb.basebackupQueue = basebackupsQueue

	tb.wait.Add(3)

	go tb.janitor()
	go tb.archiver()
	go tb.periodicBackup()

	return &tb, nil
}

func (t *TableBackup) SaveRawMessage(msg []byte, lsn dbutils.Lsn) (uint64, error) {
	t.segmentBufferMutex.Lock()
	defer t.segmentBufferMutex.Unlock()

	// should we switch to the new segment already?
	if t.deltaCnt >= t.cfg.DeltasPerFile || t.segmentFilename == "" {
		if t.segmentFilename != "" {
			// flush current buffer to the delta filename and archive it.
			if err := t.writeSegmentToFile(); err != nil {
				return 0, fmt.Errorf("could not save current message; %v", err)
			}
		}
		t.startNewSegment(lsn)
	}

	if t.segmentsCnt%t.cfg.BackupThreshold == 0 && t.deltaCnt == 0 && !t.IsBasebackupPending() {
		log.Printf("queueing base backup because we reached backupDeltaThreshold %s", t)
		t.SetBasebackupPending()
		t.basebackupQueue.Put(t)
	}

	length := uint64(len(msg) + 8)
	binary.BigEndian.PutUint64(t.msgLen, length)

	if err := t.appendDeltaToSegment(append(t.msgLen, msg...)); err != nil {
		log.Printf("could not append delta to the current segment buffer: %v", err)
	}

	return length, nil
}

func (t *TableBackup) appendDeltaToSegment(currentDelta []byte) error {
	var err error
	if _, err = t.segmentBuffer.Write(currentDelta); err != nil {
		return fmt.Errorf("could not write current delta: %v", err)
	}

	t.deltaCnt++
	t.deltasSinceBackupCnt++
	t.lastWrittenMessage = time.Now()

	return err
}

func (t *TableBackup) writeSegmentToFile() error {
	deltaPath := path.Join(t.tempDir, t.segmentFilename)
	fp, err := os.OpenFile(deltaPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file %s to write delta segment: %v", deltaPath, err)
	}
	defer fp.Close()

	if _, err = t.segmentBuffer.WriteTo(fp); err != nil {
		return fmt.Errorf("could not write delta segment to a file %s: %v", deltaPath, err)
	}

	if t.cfg.Fsync {
		// sync the file data itself
		if err := utils.SyncFileAndDirectory(fp, deltaPath, t.tempDir); err != nil {
			return err
		}
	}
	log.Printf("flushed current segment to disk file %s", deltaPath)
	t.archiveFilesQueue.Put(t.segmentFilename)

	return err
}

func (t *TableBackup) startNewSegment(startLSN dbutils.Lsn) {
	t.segmentBuffer.Reset()
	if startLSN != 0 {
		t.setSegmentFilename(startLSN)

		t.lastLSN = startLSN
		t.segmentsCnt++
	} else {
		t.segmentFilename = ""
	}
	t.deltaCnt = 0
}

func (t *TableBackup) isCurrentSegmentEmpty() bool {
	return t.segmentBuffer.Len() == 0
}

func (t *TableBackup) periodicBackup() {
	defer t.wait.Done()
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

// the archiver goroutine is responsible for moving complete files to the final backup directory, as well as for closing
// files in the temporary directory that didn't receive any writes for the specific amount of time (currently 3h)
func (t *TableBackup) archiver() {
	defer t.wait.Done()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(archiverWorkerNapTime):
			for {
				obj, err := t.archiveFilesQueue.Get()
				if err != nil {
					if err == context.Canceled {
						return
					}
					// the only other error we expect is that the queue is empty, in which case we go to sleep
					break
				}
				fname := obj.(string)
				lsn := dbutils.InvalidLsn
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
							fname, t.firstDeltaLSNToKeep)
						// a  code in RunBasebackup will take care of removing actual files from the temp directory
						return false, nil
					}
					err := archiveOneFile(path.Join(t.tempDir, fname), path.Join(t.finalDir, fname), t.cfg.Fsync)
					return err != nil, err
				}

				err = utils.Retry(archiveAction, maxArchiverRetryAttempts, archiverWorkerNapTime, maxArchiverRetryTimeout)
				if err != nil {
					log.Printf("could not archive %s: %v", fname, err)
				}
			}
		}
	}
}

// the janitor goroutine is responsible for closing files in the temporary directory
// that didn't receive any writes for the specific amount of time (currently 3h)
func (t *TableBackup) janitor() {
	defer t.wait.Done()

	closeCall := time.NewTicker(archiverCloseNapTime)
	for {
		select {
		case <-t.ctx.Done():
			closeCall.Stop()

			if err := t.archiveCurrentSegment("shutdown"); err != nil {
				log.Printf("could not write active changes to on shutdown: %v", err)
			}
			return
		case <-closeCall.C:
			if !t.triggerArchiveTimeoutOnTable() {
				continue
			}
			if err := t.archiveCurrentSegment("timeout"); err != nil {
				log.Printf("could not write changes to %s due to inactivity", t.segmentFilename, err)
			}
		}
	}
}

func (t *TableBackup) archiveCurrentSegment(reason string) error {
	t.segmentBufferMutex.Lock()
	defer t.segmentBufferMutex.Unlock()

	if t.segmentFilename == "" || t.deltaCnt == 0 {
		return nil
	}

	log.Printf("writing and archiving current segment to %s due to the %s", t.segmentFilename, reason)
	if err := t.writeSegmentToFile(); err != nil {
		return err
	}
	t.startNewSegment(dbutils.InvalidLsn)

	return nil
}

// if we should archive the active non-empty delta due to the lack of activity on the table for a certain amount of time
// TODO: make sure we don't have empty delta files opened in the first place and remove lastWrittenMessage.IsZero check.
func (t *TableBackup) triggerArchiveTimeoutOnTable() bool {
	return !t.isCurrentSegmentEmpty() &&
		!t.lastWrittenMessage.IsZero() &&
		t.cfg.ArchiverTimeout > 0 &&
		time.Since(t.lastWrittenMessage) > t.cfg.ArchiverTimeout
}

// archiveOneFile returns error only if the actual copy failed, so that we could retry it. In all other "unusual" cases
// it just displays a cause and bails out.
func archiveOneFile(sourceFile, destFile string, fsync bool) error {
	if _, err := os.Stat(sourceFile); os.IsNotExist(err) {
		log.Printf("source file doesn't exist: %q; skipping", sourceFile)
		return nil
	} else if err != nil {
		return fmt.Errorf("could not stat source file %q: %v", sourceFile, err)
	}

	if st, err := os.Stat(destFile); !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("could not stat destination file %q: %s", destFile, err)
		}
		// for the basebackup we always come up with the same path, and therefore, needs to remove the previous backup
		// if we fail, it is not an issue, since we have both basebackup and info file in the staging directory.
		// TODO: make sure we properly archive the leftovers of.copy and info file on restart
		if basename := path.Base(destFile); basename == basebackupFilename ||
			basename == tableInfoFilename ||
			st.Size() == 0 {
			os.Remove(destFile)
		} else {
			log.Printf("destination file is not empty %q; skipping", destFile)
			return nil
		}
	}

	if _, err := copyFile(sourceFile, destFile, fsync); err != nil {
		os.Remove(destFile)
		return fmt.Errorf("could not move %s -> %s file: %v", sourceFile, destFile, err)
	}

	if err := os.Remove(sourceFile); err != nil {
		log.Printf("could not delete old file: %v", err)
	}

	log.Printf("successfully archived %s", destFile)
	return nil
}

func (t *TableBackup) Files() int {
	return t.segmentsCnt
}

func (t *TableBackup) setSegmentFilename(newLSN dbutils.Lsn) {
	filename := path.Join(deltasDirName, fmt.Sprintf("%016x", uint64(newLSN)))
	// XXX: ignoring os.stat errors outside of 'file already exists'
	if _, err := os.Stat(filename); t.lastLSN == newLSN || !os.IsNotExist(err) {
		if err != nil && !os.IsNotExist(err) {
			fmt.Printf("could not stat %q: %s", filename, err)
		}
		t.filenamePostfix++
	} else {
		t.filenamePostfix = 0
	}

	if t.filenamePostfix > 0 {
		filename = fmt.Sprintf("%s.%x", filename, t.filenamePostfix)
	}

	t.segmentFilename = filename
}

func (t *TableBackup) String() string {
	return t.NamespacedName.String()
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

	row := t.conn.QueryRow(fmt.Sprintf("select exists(select 1 from %s)", t.NamespacedName.Sanitize()))
	err := row.Scan(&hasRows)

	return hasRows, err
}

func (t *TableBackup) Truncate() error {
	t.segmentBufferMutex.Lock()
	defer t.segmentBufferMutex.Unlock()
	if err := os.RemoveAll(t.tempDir); err != nil {
		return fmt.Errorf("could not recreate table dir: %v", err)
	}

	if err := t.createDirs(); err != nil {
		return err
	}

	t.startNewSegment(dbutils.InvalidLsn)
	t.segmentsCnt = 0
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

func FetchRelationInfo(tx *pgx.Tx, tbl message.NamespacedName) (message.Relation, error) {
	var (
		rel            message.Relation
		relOid         dbutils.Oid
		relRepIdentity pgtype.BPChar
	)

	row := tx.QueryRow(`
		SELECT c.oid, c.relreplident 
		FROM pg_catalog.pg_class c
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'`,
		tbl.Namespace,
		tbl.Name)

	if err := row.Scan(&relOid, &relRepIdentity); err != nil {
		return rel, fmt.Errorf("could not fetch table info: %v", err)
	}

	rows, err := tx.Query(`
	SELECT a.attname, a.atttypid, a.atttypmod, format_type(a.atttypid, a.atttypmod)
	FROM pg_catalog.pg_attribute a  
	WHERE a.attrelid = $1 AND a.attnum > 0 AND a.attisdropped = false
	ORDER BY a.attnum`, relOid)
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
			TypeOID:       dbutils.Oid(attType),
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

func copyFile(src, dst string, fsync bool) (int64, error) {
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
	if err != nil {
		return nBytes, fmt.Errorf("could not copy %s to %s: %v", err)
	}

	if fsync {
		if err = utils.SyncFileAndDirectory(destination, dst, path.Dir(dst)); err != nil {
			return nBytes, fmt.Errorf("could not sync %s: %v", dst, err)
		}
	}

	return nBytes, nil
}

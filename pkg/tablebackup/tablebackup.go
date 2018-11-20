package tablebackup

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/prometheus"
	"github.com/ikitiki/logical_backup/pkg/queue"
	"github.com/ikitiki/logical_backup/pkg/utils"
)

const (
	// TODO: move those constants somewhere else
	DeltasDirName              = "deltas"
	BasebackupFilename         = "basebackup.copy"
	BaseBackupStateFileName    = "info.yaml"
	BackupStateFileNamePattern = "backup_state_%d.yaml"

	dirPerms                 = os.ModePerm
	maxArchiverRetryTimeout  = 5 * time.Second
	maxArchiverRetryAttempts = 3
	archiverWorkerNapTime    = 500 * time.Millisecond
	archiverCloseNapTime     = 1 * time.Minute
	sleepBetweenBackups      = time.Second * 3
)

type tableBackupState struct {
	DeltasSinceBackupCount   uint32    // trigger next backup based on the deltas
	SegmentsSinceBackupCount uint32    // trigger next backup based on the segments
	LastWrittenMessage       time.Time // trigger next backup based on time
	LatestCommitLSN          string    // assign correct name to the next segment
	LatestFlushLSN           string    // do not write anything before or equal this point
}

type TableBackuper interface {
	TableBaseBackup

	WriteDelta([]byte, dbutils.Lsn, dbutils.Lsn) (uint64, error)
	Files() int
	String() string
	ID() dbutils.Oid
	TextID() string
	SetTextID(name message.NamespacedName)
	GetFlushLSN() (flushLSN dbutils.Lsn, changedSinceLastFlush bool)
	Stop()
}

type TableBaseBackup interface {
	SetBasebackupPending()
	IsBasebackupPending() bool
	ClearBasebackupPending()
	RunBasebackup() error
}

type TableBackup struct {
	message.NamespacedName

	ctx    context.Context
	wait   *sync.WaitGroup
	cancel context.CancelFunc

	// Table info
	oid         dbutils.Oid
	currentName message.NamespacedName

	// Basebackup
	tx    *pgx.Tx
	conn  *pgx.Conn
	cfg   *config.Config
	dbCfg pgx.ConnConfig

	// Files
	stagingDir string
	archiveDir string

	// Deltas
	// TODO: should we get rid of those altogether in favor of Prometheus?
	deltaCnt             int
	segmentsCnt          int
	deltasSinceBackupCnt int
	segmentStartLSN      dbutils.Lsn
	// Data for LSNs below and including this one for a given table are guaranteed to be flushed.
	flushLSN        dbutils.Lsn
	currentLSN      dbutils.Lsn
	latestCommitLSN dbutils.Lsn // we need to store it on disk at shutdown to name the first segment after the restart.

	segmentBufferMutex *sync.Mutex
	segmentFilename    string
	filenamePostfix    uint32

	// buffer for the current delta segment
	segmentBuffer bytes.Buffer

	// Basebackup
	firstDeltaLSNToKeep dbutils.Lsn
	lastBasebackupTime  time.Time
	lastBackupDuration  time.Duration
	lastWrittenMessage  time.Time
	basebackupIsPending bool

	locker uint32

	basebackupQueue *queue.Queue
	msgLen          []byte

	archiveFilesQueue *queue.Queue
	prom              promexporter.PrometheusExporterInterface
}

func New(ctx context.Context, group *sync.WaitGroup, cfg *config.Config, tbl message.NamespacedName, oid dbutils.Oid,
	dbCfg pgx.ConnConfig, basebackupsQueue *queue.Queue, prom promexporter.PrometheusExporterInterface) (*TableBackup, error) {
	tableDir := utils.TableDir(oid)

	perTableContext, cancel := context.WithCancel(ctx)

	tb := TableBackup{
		NamespacedName:     tbl,
		oid:                oid,
		ctx:                perTableContext,
		cancel:             cancel,
		wait:               group,
		cfg:                cfg,
		dbCfg:              dbCfg,
		archiveDir:         path.Join(cfg.ArchiveDir, tableDir),
		msgLen:             make([]byte, 8),
		archiveFilesQueue:  queue.New(ctx),
		segmentBufferMutex: &sync.Mutex{},
		prom:               prom,
	}
	if cfg.StagingDir != "" {
		tb.stagingDir = path.Join(cfg.StagingDir, tableDir)
	}

	if err := tb.createDirs(); err != nil {
		return nil, fmt.Errorf("could not create dirs: %v", err)
	}

	// Read our previous state if any
	if err := tb.LoadState(); err != nil {
		return nil, fmt.Errorf("could not load previous backup state from file: %v", err)
	}

	tb.basebackupQueue = basebackupsQueue

	tb.wait.Add(2)
	go tb.janitor()
	go tb.periodicBackup()

	if cfg.StagingDir != "" {
		tb.wait.Add(1)
		go tb.archiver()
	}

	return &tb, nil
}

func (t *TableBackup) Stop() {
	log.Printf("terminating processing of table %v", t)
	t.cancel()
}

func (t *TableBackup) queueArchiveFile(filename string) {
	if t.cfg.StagingDir != "" {
		t.archiveFilesQueue.Put(filename)
	}
}

// WriteDelta writes the delta into the current table segment. It is responsible for triggering the switch to the
// new segment once the current one receives more than DeltasPerFile changes. For that sake, we pass a currentLSN
// to it, so that it will set a flushLSN to it once the segment is switched and flushed.
func (t *TableBackup) WriteDelta(msg []byte, commitLSN dbutils.Lsn, currentLSN dbutils.Lsn) (uint64, error) {
	t.segmentBufferMutex.Lock()
	defer t.segmentBufferMutex.Unlock()

	if !currentLSN.IsValid() {
		panic(fmt.Sprintf("Trying to write a message %v with InvalidLSN for table %s commitLSN %s", msg, t, commitLSN))
	}

	// TODO: factor out switch into a separate routine.
	// should we switch to the new segment already?
	if t.deltaCnt >= t.cfg.DeltasPerFile || t.segmentFilename == "" {
		if t.segmentFilename != "" {
			// flush current buffer to the delta filename and archive it.
			if err := t.writeSegmentToFile(); err != nil {
				return 0, fmt.Errorf("could not save current message; %v", err)
			}
		}
		// TODO: why use commitLSN and not a current one?
		t.startNewSegment(commitLSN)
	}

	if t.segmentsCnt%t.cfg.BackupThreshold == 0 && t.deltaCnt == 0 && !t.IsBasebackupPending() {
		log.Printf("queueing base backup because we reached backupDeltaThreshold %s", t)
		t.SetBasebackupPending()
		t.basebackupQueue.Put(t)
	}

	length := uint64(len(msg) + 8)
	binary.BigEndian.PutUint64(t.msgLen, length)

	if err := t.appendDeltaToSegment(append(t.msgLen, msg...), currentLSN); err != nil {
		log.Printf("could not append delta to the current segment buffer: %v", err)
	}
	// it's tempting to do this only during the segment switch, but that would give us a wrong
	// LSN on restart if the segment we wrote just before the shutdown consisted of multiple transactions.
	t.latestCommitLSN = commitLSN

	return length, nil
}

func (t *TableBackup) appendDeltaToSegment(currentDelta []byte, currentLSN dbutils.Lsn) error {
	var err error

	if _, err = t.segmentBuffer.Write(currentDelta); err != nil {
		return fmt.Errorf("could not write current delta: %v", err)
	}

	t.deltaCnt++
	t.deltasSinceBackupCnt++
	t.lastWrittenMessage = time.Now()
	t.currentLSN = currentLSN

	return err
}

func (t *TableBackup) getBackupStateFilename() string {
	return fmt.Sprintf(BackupStateFileNamePattern, t.oid)
}

func (t *TableBackup) writeSegmentToFile() error {
	var baseDir string

	if t.stagingDir != "" {
		baseDir = t.stagingDir
	} else {
		baseDir = t.archiveDir
	}

	deltaFilepath := path.Join(baseDir, DeltasDirName, t.segmentFilename)

	fp, err := os.OpenFile(deltaFilepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file %s to write delta segment: %v", deltaFilepath, err)
	}
	defer fp.Close()

	if _, err = t.segmentBuffer.WriteTo(fp); err != nil {
		return fmt.Errorf("could not write delta segment to a file %s: %v", deltaFilepath, err)
	}

	if t.cfg.Fsync {
		// sync the file data itself
		if err := utils.SyncFileAndDirectory(fp); err != nil {
			return err
		}
	}
	t.flushLSN = t.currentLSN
	log.Printf("flushed current segment to disk file %s and set flush LSN to %s", deltaFilepath, t.flushLSN)

	if err = t.StoreState(); err != nil {
		return fmt.Errorf("could not write table backup state to file: %v", err)
	}
	t.queueArchiveFile(path.Join(DeltasDirName, t.segmentFilename))
	t.queueArchiveFile(path.Join(DeltasDirName, t.getBackupStateFilename()))

	return err
}

func (t *TableBackup) startNewSegment(startLSN dbutils.Lsn) {
	t.segmentBuffer.Reset()

	if startLSN != 0 {
		t.setSegmentFilename(startLSN)

		t.segmentStartLSN = startLSN
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
						// RunBasebackup() will take care of removing actual files from the temp directory
						return false, nil
					}
					err := archiveOneFile(path.Join(t.stagingDir, fname), path.Join(t.archiveDir, fname), t.cfg.Fsync)

					return err != nil, err
				}

				err = utils.Retry(archiveAction, maxArchiverRetryAttempts, archiverWorkerNapTime, maxArchiverRetryTimeout)
				if err != nil {
					log.Printf("could not archive %s: %v", fname, err)
					continue
				}

				t.updateMetricsForArchiver(false)
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
				continue
			}

			t.updateMetricsForArchiver(true)
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
// TODO: make sure we don't have empty delta files opened in the first place and remove LastWrittenMessage.IsZero check.
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
		if basename := path.Base(destFile); basename == BasebackupFilename ||
			basename == BaseBackupStateFileName ||
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

func (t *TableBackup) updateMetricsForArchiver(isTimeout bool) {
	var filesCounter, perTableFilesCounter string

	if isTimeout {
		filesCounter = promexporter.FilesArchivedTimeoutCounter
		perTableFilesCounter = promexporter.PerTableFilesArchivedTimeoutCounter
	} else {
		filesCounter = promexporter.FilesArchivedCounter
		perTableFilesCounter = promexporter.PerTablesFilesArchivedCounter
	}

	t.prom.Inc(filesCounter, nil)
	t.prom.Inc(perTableFilesCounter, []string{t.ID().String(), t.TextID()})
}

func (t *TableBackup) Files() int {
	return t.segmentsCnt
}

func (t *TableBackup) setSegmentFilename(newLSN dbutils.Lsn) {
	filename := fmt.Sprintf("%016x", uint64(newLSN))
	// XXX: ignoring os.stat errors outside of 'file already exists'
	if _, err := os.Stat(filename); t.segmentStartLSN == newLSN || !os.IsNotExist(err) {
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
	return t.NamespacedName.Sanitize()
}

func (t *TableBackup) createDirs() error {
	if t.stagingDir != "" {
		stagingDeltasPath := path.Join(t.stagingDir, DeltasDirName)
		if _, err := os.Stat(stagingDeltasPath); os.IsNotExist(err) {
			if err := os.MkdirAll(stagingDeltasPath, dirPerms); err != nil {
				return fmt.Errorf("could not create staging delta dir: %v", err)
			}
		}
	}

	archiveDeltasPath := path.Join(t.archiveDir, DeltasDirName)
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

func (t *TableBackup) isDeltaFileName(name string) (bool, string) {
	if name == BasebackupFilename || name == BaseBackupStateFileName || name == t.getBackupStateFilename() {
		return false, name
	}
	dir, file := path.Split(name)
	if dir == DeltasDirName {
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

// TODO: consider providing a pair of values identifying the table for prometheus in a single function call
func (tb *TableBackup) ID() dbutils.Oid {
	return tb.oid
}

func (tb *TableBackup) TextID() string {
	return tb.currentName.String()
}

func (tb *TableBackup) SetTextID(name message.NamespacedName) {
	tb.currentName = name
}

func (tb *TableBackup) GetFlushLSN() (lsn dbutils.Lsn, changedSinceLastFlush bool) {
	return tb.flushLSN, tb.flushLSN != tb.currentLSN
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
		if err = utils.SyncFileAndDirectory(destination); err != nil {
			return nBytes, fmt.Errorf("could not sync %s: %v", dst, err)
		}
	}

	return nBytes, nil
}

// StoreState writes state variables that helps decide on base backups and writing of new deltas
func (t *TableBackup) StoreState() error {
	tableDirectory := t.getDirectory()
	backupStateFileName := t.getBackupStateFilename()
	fp, err := ioutil.TempFile(tableDirectory, backupStateFileName)
	if err != nil {
		log.Printf("could not create temporary state file: %v", err)
	}

	// close the file before returning and remove it if any errors occurred (otherwsie, it should be renamed already)
	defer func() {
		fp.Close()
		if err != nil {
			if err := os.Remove(fp.Name()); err != nil {
				log.Printf("could not remove a temporary state file %q: %v", fp.Name(), err)
			}
		}
	}()

	state := tableBackupState{
		DeltasSinceBackupCount:   uint32(t.deltasSinceBackupCnt),
		SegmentsSinceBackupCount: uint32(t.segmentsCnt),
		LastWrittenMessage:       t.lastWrittenMessage,
		LatestCommitLSN:          t.latestCommitLSN.String(),
		LatestFlushLSN:           t.flushLSN.String(),
	}

	if err = yaml.NewEncoder(fp).Encode(state); err != nil {
		return fmt.Errorf("could not encode backup state %#v: %v", state, err)
	}
	finalName := path.Join(tableDirectory, backupStateFileName)
	if err := os.Rename(fp.Name(), finalName); err != nil {
		return fmt.Errorf("could not rename temporary state file %q to %q: %v", fp.Name(), finalName)
	}

	return err
}

// LoadState reads the data written by the StoreState in the previous session
func (t *TableBackup) LoadState() error {
	var state tableBackupState
	tableDirectory := t.getDirectory()
	backupStateFileName := t.getBackupStateFilename()
	stateFile := path.Join(tableDirectory, backupStateFileName)

	// if the file doesn't exist, this is not an error
	fp, err := os.Open(stateFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("could not open backup state file %q: %v", stateFile, err)
		}
		return nil
	}

	if err := yaml.NewDecoder(fp).Decode(&state); err != nil {
		return fmt.Errorf("could not unmarshal yaml from file %q: %v", stateFile, err)
	}

	lsn, err := pgx.ParseLSN(state.LatestFlushLSN)
	if err != nil {
		return fmt.Errorf("could not parse latest flush LSN %s: %v", state.LatestFlushLSN, err)
	}
	t.flushLSN = dbutils.Lsn(lsn)
	lsn, err = pgx.ParseLSN(state.LatestCommitLSN)
	if err != nil {
		return fmt.Errorf("could not parse latest commit LSN %s: %v", state.LatestCommitLSN, err)
	}
	t.latestCommitLSN = dbutils.Lsn(lsn)

	t.lastWrittenMessage = state.LastWrittenMessage
	t.segmentsCnt = int(state.SegmentsSinceBackupCount)
	t.deltasSinceBackupCnt = int(state.DeltasSinceBackupCount)

	return nil
}

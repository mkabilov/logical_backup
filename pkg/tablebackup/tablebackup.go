package tablebackup

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/logger"
	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/prometheus"
	"github.com/ikitiki/logical_backup/pkg/queue"
	"github.com/ikitiki/logical_backup/pkg/utils"
)

const (
	// TODO: move those constants somewhere else
	DeltasDirName           = "deltas"
	BasebackupFilename      = "basebackup.copy"
	BaseBackupStateFileName = "info.yaml"
	DeltasState             = "deltas_state.yaml"

	dirPerms                 = os.ModePerm
	maxArchiverRetryTimeout  = 5 * time.Second
	maxArchiverRetryAttempts = 3
	archiverWorkerNapTime    = 500 * time.Millisecond
	archiverCloseNapTime     = 1 * time.Minute
	sleepBetweenBackups      = time.Second * 3
)

type tableBackupState struct {
	DeltasSinceBackupCount   uint32    `yaml:"DeltasSinceBackupCount"`   // trigger next backup based on the deltas
	SegmentsSinceBackupCount uint32    `yaml:"SegmentsSinceBackupCount"` // trigger next backup based on the segments
	LastWrittenMessage       time.Time `yaml:"LastWrittenMessage"`       // trigger next backup based on time
	LatestCommitLSN          string    `yaml:"LatestCommitLSN"`          // assign correct name to the next segment
	LatestFlushLSN           string    `yaml:"LatestFlushLSN"`           // do not write anything before or equal this point
}

type TableBackuper interface {
	TableBaseBackup

	WriteDelta([]byte, dbutils.LSN, dbutils.LSN) (uint64, error)
	Files() int
	String() string
	OID() dbutils.OID
	TextID() string
	SetTextID(name message.NamespacedName)
	GetFlushLSN() (flushLSN dbutils.LSN, changedSinceLastFlush bool)
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
	log    *logger.Log

	// Table info
	oid         dbutils.OID
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
	segmentStartLSN      dbutils.LSN
	// Data for LSNs below and including this one for a given table are guaranteed to be flushed.
	flushLSN        dbutils.LSN
	currentLSN      dbutils.LSN
	latestCommitLSN dbutils.LSN

	segmentBufferMutex *sync.Mutex
	segmentFilename    string
	filenamePostfix    uint32

	// buffer for the current delta segment
	segmentBuffer bytes.Buffer

	// Basebackup
	firstDeltaLSNToKeep dbutils.LSN
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

func New(ctx context.Context, group *sync.WaitGroup,
	cfg *config.Config, tbl message.NamespacedName,
	oid dbutils.OID, dbCfg pgx.ConnConfig,
	basebackupsQueue *queue.Queue, prom promexporter.PrometheusExporterInterface,
	parentLogger *logger.Log) (*TableBackup, error) {

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
		log:                logger.NewLoggerFrom(parentLogger, fmt.Sprintf("table backup for %s", tbl), "OID", oid),
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
	t.log.Warnf("terminating")
	t.cancel()
	t.log.Sync()
}

func (t *TableBackup) queueArchiveFile(filename string) {
	if t.cfg.StagingDir != "" {
		t.archiveFilesQueue.Put(filename)
	}
}

// WriteDelta writes the delta into the current table segment. It is an entry point for writes of a TableBackup class.
func (t *TableBackup) WriteDelta(msg []byte, commitLSN dbutils.LSN, currentLSN dbutils.LSN) (uint64, error) {
	t.segmentBufferMutex.Lock()
	defer t.segmentBufferMutex.Unlock()

	if !currentLSN.IsValid() {
		t.log.WithReplicationMessage(msg).WithLSN(dbutils.InvalidLSN).WithCustomNamedLSN("Commit LSN", commitLSN).
			DPanic("asked to write a bogus message")
		return 0, nil
	}

	// check if we have already flushed past this message to disk
	if currentLSN <= t.flushLSN {
		t.log.WithLSN(currentLSN).WithCustomNamedLSN("flush LSN", t.flushLSN).
			Info("skip write of an already written delta with LSN preceeding the flushed one")
		return 0, nil
	}

	// TODO: use currentLSN instead of a commit one
	if err := t.maybeStartNewSegment(commitLSN); err != nil {
		return 0, err
	}

	length := uint64(len(msg) + 8)
	binary.BigEndian.PutUint64(t.msgLen, length)

	if err := t.appendDeltaToSegment(append(t.msgLen, msg...), currentLSN); err != nil {
		t.log.WithError(err).Error("could not append delta to the current segment buffer")
	}
	// it's tempting to do this only during the segment switch, but that would give us a wrong
	// LSN on restart if the segment we wrote just before the shutdown consisted of multiple transactions.
	t.latestCommitLSN = commitLSN

	return length, nil
}

// maybeStartNewSegment is responsible for triggering the switch to the new segment once the current one receives more
// than DeltasPerFile changes.
func (t *TableBackup) maybeStartNewSegment(segmentLSN dbutils.LSN) error {
	if t.deltaCnt >= t.cfg.DeltasPerFile || t.segmentFilename == "" {
		if t.segmentFilename != "" {
			// flush current buffer to the delta filename and archive it.
			if err := t.writeSegmentToFile(); err != nil {
				return fmt.Errorf("could not save current message; %v", err)
			}
		}
		t.startNewSegment(segmentLSN)
	}
	if t.segmentsCnt%t.cfg.BackupThreshold == 0 && t.deltaCnt == 0 && !t.IsBasebackupPending() {
		t.log.WithDetail("backupThreshold = %d", t.cfg.BackupThreshold).
			Info("queueing base backup due to reaching backupThreshold segments")
		t.SetBasebackupPending()
		t.basebackupQueue.Put(t)
	}

	return nil
}

func (t *TableBackup) appendDeltaToSegment(currentDelta []byte, currentLSN dbutils.LSN) error {
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

// writeSegentToFile flushes the in-memory buffer to a segment file. The current LSN of the latest written segment is
// taken as a flushLSN after the segment file has been fsynced.
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
	t.log.WithCustomNamedLSN("Flush LSN", t.flushLSN).Debugf("flushed current segment to disk file %s", deltaFilepath)

	if err = t.StoreState(); err != nil {
		return fmt.Errorf("could not write table backup state to file: %v", err)
	}
	t.queueArchiveFile(path.Join(DeltasDirName, t.segmentFilename))
	t.queueArchiveFile(path.Join(DeltasDirName, DeltasState))

	return err
}

func (t *TableBackup) startNewSegment(startLSN dbutils.LSN) {
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
				t.log.Infof("last write to the table happened %v ago, new backup is queued",
					time.Since(t.lastWrittenMessage).Truncate(1*time.Second))
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
				lsn := dbutils.InvalidLSN
				if isDelta, filename := t.isDeltaFileName(fname); isDelta {
					var err error

					lsn, err = utils.GetLSNFromDeltaFilename(filename)
					if err != nil {
						t.log.WithError(err).Errorf("could not decode lsn from the file name %s", filename)
					}
				}

				archiveAction := func() (bool, error) {
					if lsn > 0 && lsn < t.firstDeltaLSNToKeep {
						t.log.WithLSN(lsn).
							WithCustomNamedLSN("Backup LSN", t.firstDeltaLSNToKeep).
							WithFilename(fname).
							Infof("archiving skipped, changes predate the latest basebackup")
						// RunBasebackup() will take care of removing actual files from the temp directory
						return false, nil
					}
					err := archiveOneFile(path.Join(t.stagingDir, fname),
						path.Join(t.archiveDir, fname),
						t.cfg.Fsync, t.log)

					return err != nil, err
				}

				err = utils.Retry(archiveAction, maxArchiverRetryAttempts, archiverWorkerNapTime, maxArchiverRetryTimeout)
				if err != nil {
					t.log.WithError(err).Errorf("could not archive %s", fname)
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
				t.log.WithError(err).WithFilename(t.segmentFilename).
					Error("could not write changes to on shutdown")
			}
			return
		case <-closeCall.C:
			if !t.triggerArchiveTimeoutOnTable() {
				continue
			}
			if err := t.archiveCurrentSegment("timeout"); err != nil {
				t.log.WithError(err).WithFilename(t.segmentFilename).
					Errorf("could not write changes after inactivity timeout")
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

	t.log.WithFilename(t.segmentFilename).Debugf("writing and archiving current segment due to the %s", reason)
	if err := t.writeSegmentToFile(); err != nil {
		return err
	}
	t.startNewSegment(dbutils.InvalidLSN)

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
func archiveOneFile(sourceFile, destFile string, fsync bool, log *logger.Log) error {
	if _, err := os.Stat(sourceFile); os.IsNotExist(err) {
		log.WithFilename(sourceFile).Warn("couldn't archive: source file doesn't exist")
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
			log.WithFilename(destFile).Warnf("could not archive: destination file is not empty")
			return nil
		}
	}

	if _, err := copyFile(sourceFile, destFile, fsync); err != nil {
		os.Remove(destFile)
		return fmt.Errorf("could not move %s -> %s file: %v", sourceFile, destFile, err)
	}

	if err := os.Remove(sourceFile); err != nil {
		log.WithError(err).Errorf("could not delete already archived file")
	}

	log.WithFilename(destFile).Infof("successfully archived")

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
	t.prom.Inc(perTableFilesCounter, []string{t.OID().String(), t.TextID()})
}

func (t *TableBackup) Files() int {
	return t.segmentsCnt
}

func (t *TableBackup) setSegmentFilename(newLSN dbutils.LSN) {
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
	if name == BasebackupFilename || name == BaseBackupStateFileName || name == DeltasState {
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
func (t *TableBackup) OID() dbutils.OID {
	return t.oid
}

func (t *TableBackup) TextID() string {
	return t.currentName.String()
}

func (t *TableBackup) SetTextID(name message.NamespacedName) {
	t.currentName = name
}

func (t *TableBackup) GetFlushLSN() (lsn dbutils.LSN, changedSinceLastFlush bool) {
	return t.flushLSN, t.flushLSN != t.currentLSN
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
		return nBytes, fmt.Errorf("could not copy %s to %s: %v", src, dst, err)
	}

	if fsync {
		if err := utils.SyncFileAndDirectory(destination); err != nil {
			return nBytes, fmt.Errorf("could not sync %s: %v", dst, err)
		}
	}

	return nBytes, nil
}

// StoreState writes state variables that helps decide on base backups and writing of new deltas
func (t *TableBackup) StoreState() error {
	tableDirectory := t.getDirectory()
	fp, err := ioutil.TempFile(tableDirectory, DeltasState)
	if err != nil {
		t.log.WithError(err).WithFilename(fp.Name()).Errorf("could not create temporary state file")
	}

	// close the file before returning and remove it if any errors occurred (otherwsie, it should be renamed already)
	defer func() {
		fp.Close()
		if err != nil {
			if err := os.Remove(fp.Name()); err != nil {
				t.log.WithError(err).WithFilename(fp.Name()).Error("could not remove a temporary state file")
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
	finalName := path.Join(tableDirectory, DeltasState)
	if err := os.Rename(fp.Name(), finalName); err != nil {
		return fmt.Errorf("could not rename temporary state file %q to %q: %v", fp.Name(), finalName, err)
	}

	return err
}

// LoadState reads the data written by the StoreState in the previous session
func (t *TableBackup) LoadState() error {
	var state tableBackupState
	tableDirectory := t.getDirectory()
	stateFile := path.Join(tableDirectory, DeltasState)

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
	t.flushLSN = dbutils.LSN(lsn)
	lsn, err = pgx.ParseLSN(state.LatestCommitLSN)
	if err != nil {
		return fmt.Errorf("could not parse latest commit LSN %s: %v", state.LatestCommitLSN, err)
	}
	t.latestCommitLSN = dbutils.LSN(lsn)

	t.lastWrittenMessage = state.LastWrittenMessage
	t.segmentsCnt = int(state.SegmentsSinceBackupCount)
	t.deltasSinceBackupCnt = int(state.DeltasSinceBackupCount)

	return nil
}

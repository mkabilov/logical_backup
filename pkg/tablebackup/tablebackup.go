package tablebackup

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/mkabilov/logical_backup/pkg/archiver"
	"github.com/mkabilov/logical_backup/pkg/config"
	"github.com/mkabilov/logical_backup/pkg/deltas"
	"github.com/mkabilov/logical_backup/pkg/message"
	"github.com/mkabilov/logical_backup/pkg/prometheus"
	"github.com/mkabilov/logical_backup/pkg/utils"
	"github.com/mkabilov/logical_backup/pkg/utils/dbutils"
)

const (
	tableStateFilename   = "state.yaml"
	dirPerms             = os.ModePerm
	archiverCloseNapTime = 1 * time.Minute

	//maxArchiverRetryTimeout  = 5 * time.Second
	//maxArchiverRetryAttempts = 3
	//archiverWorkerNapTime    = 500 * time.Millisecond
	//sleepBetweenBackups      = time.Second * 3
)

type state struct {
	MessagesSinceBackupCount uint        `yaml:"messagesSinceBackupCount"`
	DeltasSinceBackupCount   uint        `yaml:"deltasSinceBackupCount"`
	LastProcessedMessage     time.Time   `yaml:"lastProcessedMessageTime"`
	LatestFinalLSN           dbutils.LSN `yaml:"latestFinalLSN"`
	LatestFlushLSN           dbutils.LSN `yaml:"latestFlushLSN"`
}

type controllerInterface interface {
	AdvanceLSN()
	QueueTable(TableBackuper)
}

//TableBackuper ...
type TableBackuper interface {
	OID() dbutils.OID
	Name() message.NamespacedName
	SetName(message.NamespacedName)
	RelationMessage() message.Relation
	String() string
	FlushLSN() dbutils.LSN
	TableDirectory() string
	ProcessDMLMessage(message.Message) error
	ProcessBegin(message.Begin) error
	ProcessCommit(message.Commit) error
	ProcessRelationMessage(message.Relation) error
	LatestCommitLSN() dbutils.LSN
	Wait()
	LastProcessedMessageTime() time.Time
	MessagesProcessed() uint
	QueueBasebackup()
	LastBasebackupTime() time.Time
	BasebackupDone(dump, info string, lsn dbutils.LSN) error
	LoadTableInfo() error
}

//tableBackup ...
type tableBackup struct {
	message.NamespacedName

	oid             dbutils.OID
	relationMessage message.Relation
	pendingBackup   bool

	wg  *sync.WaitGroup
	ctx context.Context
	cfg *config.Config

	// Files
	stagingDir string // table level staging dir
	archiveDir string // table level archive dir

	// Stats
	// TODO: should we get rid of those altogether in favor of Prometheus?
	deltaFilesWrittenCnt     uint // since last backup
	messagesProcessed        uint // since last backup
	lastProcessedMessageTime time.Time

	// Data for LSNs below and including this one for a given table are guaranteed to be flushed.
	flushLSN        dbutils.LSN // messages prior to that LSN position are safe to delete
	latestCommitLSN dbutils.LSN // LSN of the last commit
	latestFinalLSN  dbutils.LSN

	// Aux
	messageCollector deltas.MessageCollector
	archiver         archiver.Archiver
	ctrl             controllerInterface
	prom             promexporter.PromInterface

	// Basebackup
	basebackupLSN      dbutils.LSN
	lastBasebackupTime time.Time
}

// New instantiates tableBackup
func New(ctx context.Context,
	name message.NamespacedName,
	oid dbutils.OID,
	ctrl controllerInterface,
	cfg *config.Config,
	prom promexporter.PromInterface,
) (*tableBackup, error) {
	tableDir := utils.TableDir(oid)

	tb := tableBackup{
		NamespacedName:    name,
		oid:               oid,
		ctx:               ctx,
		archiveDir:        path.Join(cfg.ArchiveDir, tableDir),
		prom:              prom,
		cfg:               cfg,
		ctrl:              ctrl,
		messagesProcessed: 0,

		wg: &sync.WaitGroup{},
	}

	if cfg.StagingDir != "" {
		tb.stagingDir = path.Join(cfg.StagingDir, tableDir)
		tb.messageCollector = deltas.New(tb.stagingDir, cfg.Fsync)
		tb.archiver = archiver.New(ctx, tb.stagingDir, tb.archiveDir, cfg.Fsync)
	} else {
		tb.messageCollector = deltas.New(tb.archiveDir, cfg.Fsync)
	}

	if err := tb.createDirs(); err != nil {
		return nil, fmt.Errorf("could not create dirs: %v", err)
	}

	if tb.archiver != nil {
		log.Printf("Starting archiver for %s table", tb.NamespacedName)
		tb.archiver.Run()
	}

	tb.wg.Add(1)
	go tb.run()

	return &tb, nil
}

//LatestCommitLSN returns latest commit lsn
func (t *tableBackup) LatestCommitLSN() dbutils.LSN {
	return t.latestCommitLSN
}

// LastProcessedMessageTime returns time of the last processed message
func (t *tableBackup) LastProcessedMessageTime() time.Time {
	return t.lastProcessedMessageTime
}

// MessagesProcessed returns number of processed messages since last basebackup
func (t *tableBackup) MessagesProcessed() uint {
	return t.messagesProcessed
}

// RelationMessage returns relation replication message of the table
func (t *tableBackup) RelationMessage() message.Relation {
	return t.relationMessage
}

// OID returns oid of the table
func (t *tableBackup) OID() dbutils.OID {
	return t.oid
}

// Name returns namespaced name
func (t *tableBackup) Name() message.NamespacedName {
	return t.NamespacedName
}

// SetName renames table
func (t *tableBackup) SetName(name message.NamespacedName) {
	if t.NamespacedName != name {
		t.NamespacedName = name
	}
}

// LastBasebackupTime returns last base backup time of the table
func (t *tableBackup) LastBasebackupTime() time.Time {
	return t.lastBasebackupTime
}

// FlushLSN ...
func (t *tableBackup) FlushLSN() dbutils.LSN {
	return t.flushLSN
}

// QueueBasebackup queues table for basebackup
func (t *tableBackup) QueueBasebackup() {
	if t.pendingBackup {
		return
	}

	t.ctrl.QueueTable(t)
	t.pendingBackup = true
}

func (t *tableBackup) stop() {
	defer t.wg.Done()
	t.wg.Add(1)

	filename, minLSN, maxLSN, err := t.messageCollector.Save()
	if err == deltas.EmptyBuffer {
		return
	} else if err != nil {
		log.Printf("could not save messages: %v", err)
		return
	}
	t.flushLSN = maxLSN

	t.queueDeltaFile(filename, minLSN)
}

// Wait waits for goroutines
func (t *tableBackup) Wait() {
	if t.archiver != nil {
		t.archiver.Wait()
	}

	t.wg.Wait()
}

func (t *tableBackup) saveState() error {
	tempStateFilepath := path.Join(t.TableDirectory(), tableStateFilename+".new")
	if _, err := os.Stat(tempStateFilepath); !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("could not stat %q: %v", tempStateFilepath, err)
		}

		if err := os.Remove(tempStateFilepath); err != nil {
			log.Printf("could not delete old temp state file: %v", err)
		}
	}

	fp, err := os.OpenFile(tempStateFilepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open %q file: %v", tempStateFilepath, err)
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

	s := state{
		MessagesSinceBackupCount: t.messagesProcessed,
		DeltasSinceBackupCount:   t.deltaFilesWrittenCnt,
		LastProcessedMessage:     t.lastProcessedMessageTime,
		LatestFinalLSN:           t.latestFinalLSN,
		LatestFlushLSN:           t.flushLSN,
	}

	if err = yaml.NewEncoder(fp).Encode(s); err != nil {
		return fmt.Errorf("could not encode backup state: %v", err)
	}

	finalFilepath := path.Join(t.TableDirectory(), tableStateFilename)
	if err := os.Rename(tempStateFilepath, finalFilepath); err != nil {
		return fmt.Errorf("could not rename temporary state file %q to %q: %v", fp.Name(), finalFilepath, err)
	}

	t.queueFile(tableStateFilename)

	return nil
}

func (t *tableBackup) loadState() error {
	var s state

	stateFilepath := utils.FirstExistingFile(
		path.Join(t.cfg.StagingDir, tableStateFilename),
		path.Join(t.cfg.ArchiveDir, tableStateFilename),
	)

	if stateFilepath == "" {
		log.Printf("could not find state file for %v table", t) // DEBUG
		return nil
	}

	log.Printf("state loaded for %s table", t)

	fp, err := os.OpenFile(stateFilepath, os.O_RDONLY, os.ModePerm)
	//TODO: file might be corrupted/empty in this case we need to switch to the state file in the archive directory
	if err != nil {
		return fmt.Errorf("could not open state file: %v", err)
	}
	defer fp.Close()

	if err := yaml.NewDecoder(fp).Decode(&s); err != nil {
		return fmt.Errorf("could not decode state yaml: %v", err)
	}

	t.messagesProcessed = s.MessagesSinceBackupCount
	t.deltaFilesWrittenCnt = s.DeltasSinceBackupCount
	t.lastProcessedMessageTime = s.LastProcessedMessage
	t.latestFinalLSN = s.LatestFinalLSN
	t.flushLSN = s.LatestFlushLSN

	return nil
}

func (t *tableBackup) maybeSaveMessages() error {
	saveNeeded := false
	msgCnt := t.messageCollector.MessageCnt()
	if msgCnt >= t.cfg.MessagesPerDelta {
		log.Printf("archiving due to MessagesPerDelta")
		saveNeeded = true
	}

	if msgAge := time.Since(t.messageCollector.LastMessageTime()); msgAge > t.cfg.ArchiverTimeout && msgCnt > 0 {
		log.Printf("archiving due to age of the last msg (%v)", msgAge)
		saveNeeded = true
	}

	if !saveNeeded {
		return nil
	}

	deltaFilename, minLSN, maxLSN, err := t.messageCollector.Save()
	if err != nil {
		return fmt.Errorf("could not save delta file: %v", err)
	}

	t.queueDeltaFile(deltaFilename, minLSN)

	if err := t.saveState(); err != nil {
		return fmt.Errorf("could not save state: %v", err)
	}

	t.flushLSN = maxLSN
	t.ctrl.AdvanceLSN()

	log.Printf("%s: messages are saved to the %q delta file (lsn: %v - %v)", t.String(), deltaFilename, minLSN, maxLSN)

	return nil
}

func (t *tableBackup) run() {
	defer t.wg.Done()

	for {
		select {
		case <-t.ctx.Done():
			t.stop()
			return
		case <-time.Tick(archiverCloseNapTime):
			if err := t.maybeSaveMessages(); err != nil {
				log.Printf("could not save: %v", err)
			}
		case <-time.Tick(t.cfg.ForceBasebackupAfterInactivityInterval):
			lpt := t.LastProcessedMessageTime()
			if lpt.IsZero() || t.MessagesProcessed() == 0 {
				continue
			}

			if time.Since(lpt) > t.cfg.ForceBasebackupAfterInactivityInterval {
				log.Printf("queueing table %s for basebackup due to inactivity(%v)",
					t.String(), t.cfg.ForceBasebackupAfterInactivityInterval)
				t.QueueBasebackup()
			}
		}
	}
}

//ProcessBegin ...
func (t *tableBackup) ProcessBegin(msg message.Begin) error {
	t.latestFinalLSN = msg.FinalLSN

	return t.saveMessage(msg)
}

//ProcessCommit ...
func (t *tableBackup) ProcessCommit(msg message.Commit) error {
	t.latestFinalLSN = msg.LSN
	t.latestCommitLSN = msg.TransactionLSN

	return t.saveMessage(msg)
}

//ProcessRelationMessage ...
func (t *tableBackup) ProcessRelationMessage(msg message.Relation) error {
	t.relationMessage = msg

	return t.saveMessage(msg)
}

// ProcessDMLMessage processes incoming dml message
func (t *tableBackup) ProcessDMLMessage(msg message.Message) error {
	return t.saveMessage(msg)
}

func (t *tableBackup) saveMessage(msg message.Message) error {
	if t.latestFinalLSN < t.basebackupLSN {
		return nil // skipping message
	}

	t.messageCollector.AddMessage(msg)

	if err := t.maybeSaveMessages(); err != nil {
		return fmt.Errorf("could not save delta file: %v", err)
	}

	t.messagesProcessed++
	t.lastProcessedMessageTime = time.Now()

	return nil
}

func (t *tableBackup) createDirs() error {
	if t.stagingDir != "" {
		stagingDeltasPath := path.Join(t.stagingDir, deltas.DirName)
		if _, err := os.Stat(stagingDeltasPath); os.IsNotExist(err) {
			if err := os.MkdirAll(stagingDeltasPath, dirPerms); err != nil {
				return fmt.Errorf("could not create staging delta dir: %v", err)
			}
		}
	}

	archiveDeltasPath := path.Join(t.archiveDir, deltas.DirName)
	if _, err := os.Stat(archiveDeltasPath); os.IsNotExist(err) {
		if err := os.MkdirAll(archiveDeltasPath, dirPerms); err != nil {
			return fmt.Errorf("could not create archive delta dir: %v", err)
		}
	}

	return nil
}

func (t *tableBackup) purgeObsoleteDeltaFiles(deltasDir string) error {
	log.Printf("Purging segments in %s before the LSN %s", deltasDir, t.basebackupLSN)
	fileList, err := ioutil.ReadDir(deltasDir)
	if err != nil {
		return fmt.Errorf("could not list directory: %v", err)
	}

	for _, v := range fileList {
		filename := v.Name()
		if lsn, err := utils.GetLSNFromDeltaFilename(filename); err == nil {
			if lsn < t.basebackupLSN {
				filename = path.Join(deltasDir, filename)
				if err := os.Remove(filename); err != nil {
					return fmt.Errorf("could not remove %q file: %v", filename, err)
				}
			}
		} else {
			return fmt.Errorf("could not parse filename: %v", err)
		}
	}

	return nil
}

func (t *tableBackup) queueFile(filename string) {
	if t.archiver != nil {
		t.archiver.QueueFile(filename)
	}
}

// we need to know the min lsn position of the delta file because by the time we process file the
// basebackup lsn might be different
func (t *tableBackup) queueDeltaFile(filename string, minLSN dbutils.LSN) {
	if t.archiver != nil {
		t.archiver.QueueDeltaFile(path.Join(deltas.DirName, filename), minLSN)
	}

	t.deltaFilesWrittenCnt++
	if t.deltaFilesWrittenCnt >= t.cfg.BackupThreshold {
		log.Printf("queueing table due to backup threshold")
		t.QueueBasebackup()
	}
}

// BasebackupDone does post base backup operations
func (t *tableBackup) BasebackupDone(basebackupFilename, infoFilename string, lsn dbutils.LSN) error {
	if t.archiver != nil {
		t.archiver.SetBasebackupLSN(lsn)
	}

	t.flushLSN = lsn
	t.basebackupLSN = lsn
	t.queueFile(basebackupFilename)
	t.queueFile(infoFilename)

	for _, baseDir := range []string{t.stagingDir, t.archiveDir} {
		if baseDir == "" {
			continue
		}

		deltasDir := path.Join(baseDir, deltas.DirName)
		if err := t.purgeObsoleteDeltaFiles(deltasDir); err != nil {
			return fmt.Errorf("could not purge delta files from %q: %v", deltasDir, err)
		}
	}
	t.updateMetricsAfterBaseBackup()

	t.lastBasebackupTime = time.Now()
	t.messagesProcessed = 0
	t.pendingBackup = false
	t.deltaFilesWrittenCnt = 0

	return nil
}

// TableDirectory returns full path to the table
func (t *tableBackup) TableDirectory() string {
	if t.stagingDir != "" {
		return t.stagingDir
	}

	return t.archiveDir
}

func (t *tableBackup) loadStagingFiles() error {
	if t.stagingDir == "" {
		return nil
	}

	fileList, err := ioutil.ReadDir(t.stagingDir)
	if err != nil {
		return fmt.Errorf("could not read directory: %v", err)
	}

	for _, file := range fileList {
		if file.IsDir() {
			continue
		}

		t.archiver.QueueFile(file.Name())
	}

	fileList, err = ioutil.ReadDir(path.Join(t.stagingDir, deltas.DirName))
	if err != nil {
		return fmt.Errorf("could not read directory: %v", err)
	}

	for _, file := range fileList {
		var minLSN dbutils.LSN

		if file.IsDir() {
			log.Printf("unexpected %v directory in the deltas dir", file.Name())
			continue
		}

		minLSNstr := strings.Split(file.Name(), ".")[0]
		if minLSNstr == "" {
			return fmt.Errorf("no lsn found in the filename")
		}

		if err := minLSN.ParseHex(minLSNstr); err != nil {
			return fmt.Errorf("could not extract lsn from the delta filename: %v", err)
		}

		t.archiver.QueueDeltaFile(path.Join(deltas.DirName, file.Name()), minLSN)
	}

	return nil
}

// LoadTableInfo loads info about the table from the filesystem
func (t *tableBackup) LoadTableInfo() error {
	if err := t.loadState(); err != nil {
		return fmt.Errorf("could not load state: %v", err)
	}

	if err := t.loadStagingFiles(); err != nil {
		return fmt.Errorf("could not load staging file: %v", err)
	}

	return nil
}

package logicalbackup

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/decoder"
	"github.com/ikitiki/logical_backup/pkg/logger"
	"github.com/ikitiki/logical_backup/pkg/message"
	prom "github.com/ikitiki/logical_backup/pkg/prometheus"
	"github.com/ikitiki/logical_backup/pkg/queue"
	"github.com/ikitiki/logical_backup/pkg/tablebackup"
	"github.com/ikitiki/logical_backup/pkg/utils"
)

type msgType int

const (
	applicationName = "logical_backup"
	OidNameMapFile  = "oid2name.yaml"
	stateFile       = "state.yaml"
	httpSrvPort     = 8080
	httpSrvTimeout  = 20 * time.Second

	statusTimeout   = time.Second * 10
	replWaitTimeout = time.Second * 10

	mInsert msgType = iota
	mUpdate
	mDelete
	mBegin
	mCommit
	mRelation
	mType
)

type NameAtLSN struct {
	Name message.NamespacedName
	Lsn  dbutils.LSN
}

type oidToName struct {
	isChanged         bool
	nameChangeHistory map[dbutils.OID][]NameAtLSN
}

type StateInfo struct {
	Timestamp  time.Time
	CurrentLSN string
}

type LogicalBackuper interface {
	Run()
	Wait()
}

type LogicalBackup struct {
	ctx    context.Context
	cancel context.CancelFunc

	log *logger.Log

	cfg *config.Config

	pluginArgs []string

	backupTables *BackupTables

	tableNameChanges oidToName

	dbCfg    pgx.ConnConfig
	replConn *pgx.ReplicationConn // replication connection for fetching deltas
	tx       *pgx.Tx

	currentLSN           dbutils.LSN // latest received LSN
	transactionCommitLSN dbutils.LSN // commit LSN of the latest observed transaction (obtained when reading BEGIN)
	latestFlushLSN       dbutils.LSN // latest LSN flushed to disk
	lastTxId             int32       // transaction ID of the latest observed transaction (obtained when reading BEGIN)

	basebackupQueue *queue.Queue
	waitGr          *sync.WaitGroup
	stopCh          chan struct{}
	closeOnce       sync.Once

	// TODO: should we get rid of those altogether given that we have prometheus?
	msgCnt       map[msgType]int
	bytesWritten uint64

	txBeginRelMsg    map[dbutils.OID]struct{} // list of the relations with begin pending message
	relationMessages map[dbutils.OID][]byte
	beginMsg         []byte
	typeMsg          []byte

	srv  http.Server
	prom prom.PrometheusExporterInterface
}

func New(cfg *config.Config) (*LogicalBackup, error) {
	ctx, cancel := context.WithCancel(context.Background())
	lb := &LogicalBackup{
		ctx:    ctx,
		cancel: cancel,
		stopCh: make(chan struct{}),

		backupTables:     NewBackupTables(),
		relationMessages: make(map[dbutils.OID][]byte),
		tableNameChanges: oidToName{nameChangeHistory: make(map[dbutils.OID][]NameAtLSN)},
		pluginArgs:       []string{`"proto_version" '1'`, fmt.Sprintf(`"publication_names" '%s'`, cfg.PublicationName)},
		basebackupQueue:  queue.New(ctx),
		waitGr:           &sync.WaitGroup{},
		cfg:              cfg,
		msgCnt:           make(map[msgType]int),
		prom:             prom.New(cfg.PrometheusPort),
	}

	if l, err := logger.NewLogger("logical backup", cfg.Debug); err != nil {
		return nil, fmt.Errorf("could not create backup logger: %v", err)
	} else {
		lb.log = l
	}

	lb.initHttpSrv(httpSrvPort)

	if err := createDirs(cfg.StagingDir, cfg.ArchiveDir); err != nil {
		return nil, err
	}

	if err := lb.initReplConn(); err != nil {
		return nil, err
	}

	if err := lb.prepareDB(); err != nil {
		return nil, err
	}

	if err := lb.registerMetrics(); err != nil {
		return nil, err
	}

	return lb, nil
}

func createDirs(dirs ...string) error {
	for _, dir := range dirs {
		if dir == "" {
			continue
		}

		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.Mkdir(dir, os.ModePerm); err != nil {
				return fmt.Errorf("could not create %q dir: %v", dir, err)
			}
		} else if err != nil {
			return fmt.Errorf("%q stat error: %v", dir, err)
		}
	}

	return nil
}

func (b *LogicalBackup) prepareDB() error {
	b.dbCfg = b.cfg.DB
	b.dbCfg.RuntimeParams = map[string]string{"application_name": applicationName}

	conn, err := pgx.Connect(b.dbCfg) // non replication protocol db connection
	if err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}
	defer conn.Close()

	b.log.Debugf("Pg backend session PID: %d", conn.PID())

	if err := dbutils.CreateMissingPublication(conn, b.cfg.PublicationName); err != nil {
		return err
	}
	b.log.WithDetail("PublicationName: %s", b.cfg.PublicationName).Debugf("created missing publication")

	b.latestFlushLSN, err = dbutils.GetSlotFlushLSN(conn, b.cfg.SlotName, b.cfg.DB.Database)
	if err != nil {
		return fmt.Errorf("could not init replication slot; %v", err)
	}

	// invalid start LSN and no error from initSlot denotes a non-existing slot.
	slotExists := b.latestFlushLSN.IsValid()
	if !slotExists {
		// TODO: this will discard all existing backup data, we should probably bail out if existing backup is there
		b.log.Infof("Creating logical replication slot %s", b.cfg.SlotName)

		initialLSN, err := dbutils.CreateSlot(conn, b.ctx, b.cfg.SlotName)
		if err != nil {
			return fmt.Errorf("could not create replication slot: %v", err)
		}
		b.log.WithCustomNamedLSN("Consistent point LSN", initialLSN).
			Infof("Created missing replication slot %q", b.cfg.SlotName)

		// solve impedance mismatch between the flush LSN (the LSN we confirmed and flushed) and slot initial LSN
		// (next, but not yet received LSN).
		b.latestFlushLSN = initialLSN - 1

		if err := b.writeRestartLSN(); err != nil {
			b.log.WithError(err).WithLSN(b.latestFlushLSN).Errorw("could not store initial LSN")
		}
	} else {
		restartLSN, err := b.readRestartLSN()
		if err != nil {
			return fmt.Errorf("could not read previous flush lsn: %v", err)
		}
		if restartLSN.IsValid() {
			b.latestFlushLSN = restartLSN
		}
		// nothing to call home about, we may have flushed the final segment at shutdown
		// without bothering to advance the slot LSN.
		if err := b.sendStatus(); err != nil {
			b.log.WithError(err).Errorf("could not send replay progress")
		}
	}

	// run per-table backups after we ensure there is a replication slot to retain changes.
	if err = b.prepareTablesForPublication(conn); err != nil {
		return fmt.Errorf("could not prepare tables for backup: %v", err)
	}

	return nil
}

func (b *LogicalBackup) initHttpSrv(port int) {
	mux := http.NewServeMux()

	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

	b.srv = http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.TimeoutHandler(mux, httpSrvTimeout, ""),
	}
}

func (b *LogicalBackup) initReplConn() error {
	rc, err := pgx.ReplicationConnect(b.cfg.DB)
	if err != nil {
		return fmt.Errorf("could not connect using replication protocol: %v", err)
	}
	b.replConn = rc

	return nil
}

func (b *LogicalBackup) baseDir() string {
	if b.cfg.StagingDir != "" {
		return b.cfg.StagingDir
	}

	return b.cfg.ArchiveDir
}

func (b *LogicalBackup) processDMLMessage(tableOID dbutils.OID, typ msgType, msg []byte) error {
	bt, ok := b.backupTables.Get(tableOID)
	if !ok {
		b.log.With("OID", tableOID).Warnf("table is not tracked")
		return nil
	}

	// We need to write the transaction begin message to every table being collected,
	// therefore, it cannot be written right away when we receive it. Instead, wait for
	// the actual relation data to arrive and then write it here.
	// TODO: avoid multiple writes
	if _, ok := b.txBeginRelMsg[tableOID]; !ok {
		if err := b.WriteTableMessage(bt, mBegin, b.beginMsg); err != nil {
			return err
		}
		b.txBeginRelMsg[bt.OID()] = struct{}{}
	}

	if b.typeMsg != nil {
		if err := b.WriteTableMessage(bt, mType, b.typeMsg); err != nil {
			return fmt.Errorf("could not write type message: %v", err)
		}
		b.typeMsg = nil
	}

	if relationMsg, ok := b.relationMessages[tableOID]; ok {
		if err := b.WriteTableMessage(bt, mRelation, relationMsg); err != nil {
			return fmt.Errorf("could not write a relation message: %v", err)
		}
		delete(b.relationMessages, tableOID)
	}

	if err := b.WriteTableMessage(bt, typ, msg); err != nil {
		return fmt.Errorf("could not write message: %v", err)
	}

	return nil
}

func (b *LogicalBackup) WriteTableMessage(t tablebackup.TableBackuper, typ msgType, msg []byte) error {
	ln, err := t.WriteDelta(msg, b.transactionCommitLSN, b.currentLSN)
	if err != nil {
		return err
	}

	b.bytesWritten += ln
	b.msgCnt[typ]++

	b.updateMetricsAfterWriteDelta(t, typ, ln)

	return nil
}

func (b *LogicalBackup) handler(m message.Message, walStart dbutils.LSN) error {
	var err error

	b.currentLSN = walStart

	switch v := m.(type) {
	case message.Relation:
		err = b.processRelationMessage(v)
	case message.Insert:
		err = b.processDMLMessage(v.RelationOID, mInsert, v.Raw)
	case message.Update:
		err = b.processDMLMessage(v.RelationOID, mUpdate, v.Raw)
	case message.Delete:
		err = b.processDMLMessage(v.RelationOID, mDelete, v.Raw)
	case message.Begin:
		b.lastTxId = v.XID
		b.transactionCommitLSN = v.FinalLSN

		b.txBeginRelMsg = make(map[dbutils.OID]struct{})
		b.beginMsg = v.Raw
	case message.Commit:
		// commit is special, because the LSN of the CopyData message points past the commit message.
		// for consistency we set the currentLSN here to the commit message LSN inside the commit itself.
		b.currentLSN = v.LSN
		for relOID := range b.txBeginRelMsg {
			tb := b.backupTables.GetIfExists(relOID)
			if err = b.WriteTableMessage(tb, mCommit, v.Raw); err != nil {
				return err
			}
		}

		// if there were any changes in the table names, flush the map file
		if err := b.flushOidNameMap(); err != nil {
			b.log.WithError(err).Errorf("could not flush the oid to map file")
		}

		candidateFlushLSN := b.getNextFlushLSN()

		if candidateFlushLSN > b.latestFlushLSN {
			b.latestFlushLSN = candidateFlushLSN
			b.log.WithCustomNamedLSN("Flush LSN", b.latestFlushLSN).
				Info("advanced flush LSN")

			if err := b.writeRestartLSN(); err != nil {
				b.log.WithCustomNamedLSN("Flush LSN", b.latestFlushLSN).WithError(err).
					Errorf("could not store flush LSN")
			}

			if err = b.sendStatus(); err != nil {
				b.log.WithError(err).Errorf("could not send replay progress")
			}
		}

		b.updateMetricsOnCommit(v.Timestamp.Unix())

	case message.Origin:
		//TODO:
	case message.Truncate:
		//TODO:
	case message.Type:
		//TODO: consider writing this message to all tables in a transaction as a safety measure during restore.
	}

	return err
}

// getNextFlushLSN computes a minimum among flush LSNs of all tables that are part of the logical backup.
// As we flush data by reaching deltasPerFile changes since the last flush and each table is written into
// its own file, the LSNs that are guaranteed to be flushed may vary from one table to another.
func (b *LogicalBackup) getNextFlushLSN() dbutils.LSN {
	result := b.transactionCommitLSN

	b.backupTables.Map(func(table tablebackup.TableBackuper) {
		flushLSN, isFlushRequired := table.GetFlushLSN()
		// ignore tables that didn't change since the previous flush
		if isFlushRequired && flushLSN < result {
			result = flushLSN
		}
	})

	return result
}

// act on a new relation message. We act on table renames, drops and recreations and new tables
func (b *LogicalBackup) processRelationMessage(m message.Relation) error {
	if _, isRegistered := b.backupTables.Get(m.OID); !isRegistered {
		if track, err := b.registerNewTable(m); !track || err != nil {
			if err != nil {
				return fmt.Errorf("could not add a backup process for the new table %s: %v", m.NamespacedName, err)
			}
			// instructed not to track this table
			return nil
		}
	}
	b.maybeRegisterNewName(m.OID, m.NamespacedName)
	b.relationMessages[m.OID] = m.Raw
	// we will write this message later, when we receive the actual DML for this table

	return nil
}

func (b *LogicalBackup) registerNewTable(m message.Relation) (bool, error) {
	if !b.cfg.TrackNewTables {
		b.log.WithOID(m.OID).WithTableName(m.NamespacedName).
			Infow("ignoring relation message because we are configured not to track new tables")
		return false, nil
	}

	tb, err := tablebackup.New(b.ctx, b.waitGr, b.cfg, m.NamespacedName, m.OID, b.dbCfg, b.basebackupQueue, b.prom, b.log)
	if err != nil {
		return false, err
	}

	b.backupTables.Set(m.OID, tb)
	b.log.WithOID(m.OID).WithTableName(m.NamespacedName).Infow("registered new table")

	return true, nil
}

func (b *LogicalBackup) sendStatus() error {
	b.log.With("LSN", b.latestFlushLSN).Debugf("sending new status (i:%d u:%d d:%d b:%0.2fMb) ",
		b.msgCnt[mInsert], b.msgCnt[mUpdate], b.msgCnt[mDelete], float64(b.bytesWritten)/1048576)

	b.msgCnt = make(map[msgType]int)
	b.bytesWritten = 0

	status, err := pgx.NewStandbyStatus(uint64(b.latestFlushLSN))

	if err != nil {
		return fmt.Errorf("error creating standby status: %s", err)
	}

	if err := b.replConn.SendStandbyStatus(status); err != nil {
		return fmt.Errorf("failed to send standy status: %s", err)
	}

	return nil
}

func (b *LogicalBackup) logicalDecoding() {
	defer b.waitGr.Done()

	// TODO: move out the initialization routines
	// TODO: rename all messages mentioning 'replication' to 'logical decoding'
	b.log.WithLSN(b.latestFlushLSN).Infof("Starting logical decoding")

	err := b.replConn.StartReplication(b.cfg.SlotName, uint64(b.latestFlushLSN), -1, b.pluginArgs...)
	if err != nil {
		b.log.WithError(err).Error("failed to start replication")
		b.Close()
		return
	}

	ticker := time.NewTicker(statusTimeout)
	for {
		select {
		case <-b.ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			if err := b.sendStatus(); err != nil {
				b.log.WithError(err).Error("could not send replay progress")
				b.Close()
				return
			}
		default:
			wctx, cancel := context.WithTimeout(b.ctx, replWaitTimeout)
			repMsg, err := b.replConn.WaitForReplicationMessage(wctx)
			cancel()
			if err == context.DeadlineExceeded {
				continue
			}
			if err == context.Canceled {
				b.log.Warn("received shutdown request: replication terminated")
				return
			}
			// TODO: make sure we retry and cleanup after ourselves afterwards
			if err != nil {
				b.log.WithError(err).Error("replication failed")
				b.Close()
				return
			}

			if repMsg == nil {
				b.log.Warn("received null replication message")
				continue
			}

			if repMsg.WalMessage != nil {
				walStart := dbutils.LSN(repMsg.WalMessage.WalStart)
				// We may have flushed this LSN to all tables, but the slot's restart LSN did not advance
				// and it is sent to us again after the restart of the backup tool. Skip it, unless it is a non-data
				// message that doesn't have any LSN assigned.
				if walStart.IsValid() && walStart <= b.latestFlushLSN {
					b.log.WithLSN(b.currentLSN).WithCustomNamedLSN("Flush LSN", b.latestFlushLSN).
						Info("skip WAL message with LSN that is lower or equal to the flush LSN")
					continue
				}
				logmsg, err := decoder.Parse(repMsg.WalMessage.WalData)
				if err != nil {
					b.log.WithError(err).Errorf("invalid pgoutput message")
					b.Close()
					return
				}

				if err := b.handler(logmsg, walStart); err != nil {
					b.log.WithError(err).Errorf("error handling waldata")
					b.Close()
					return
				}
			}

			if repMsg.ServerHeartbeat != nil && repMsg.ServerHeartbeat.ReplyRequested == 1 {
				b.log.Infof("server wants a reply")
				if err := b.sendStatus(); err != nil {
					b.log.WithError(err).Errorf("could not send replay progress")
					b.Close()
					return
				}
			}
		}
	}
}

// Wait for the goroutines to finish
func (b *LogicalBackup) Wait() {
	b.waitGr.Wait()
}

// register tables for the backup; add replica identity when necessary
func (b *LogicalBackup) prepareTablesForPublication(conn *pgx.Conn) error {
	// fetch all tables from the current publication, together with the information on whether we need to create
	// replica identity full for them
	type tableInfo struct {
		oid             dbutils.OID
		name            message.NamespacedName
		hasPK           bool
		replicaIdentity message.ReplicaIdentity
	}
	rows, err := conn.Query(`
			select c.oid,
				   n.nspname,
				   c.relname,
			       csr.oid is not null as has_pk,
                   c.relreplident as replica_identity
			from pg_class c
				   join pg_namespace n on n.oid = c.relnamespace
       			   join pg_publication_tables pub on (c.relname = pub.tablename and n.nspname = pub.schemaname)
       			   left join pg_constraint csr on (csr.conrelid = c.oid and csr.contype = 'p')
			where c.relkind = 'r'
  			  and pub.pubname = $1`, b.cfg.PublicationName)
	if err != nil {
		return fmt.Errorf("could not execute query: %v", err)
	}

	tables := make([]tableInfo, 0)
	func() {
		defer rows.Close()

		for rows.Next() {
			tab := tableInfo{}
			err = rows.Scan(&tab.oid, &tab.name.Namespace, &tab.name.Name, &tab.hasPK, &tab.replicaIdentity)
			if err != nil {
				return
			}

			tables = append(tables, tab)
		}
	}()

	if err != nil {
		return fmt.Errorf("could not fetch row values from the driver: %v", err)
	}

	if len(tables) == 0 && !b.cfg.TrackNewTables {
		return fmt.Errorf("no tables found")
	}

	for _, t := range tables {
		targetReplicaIdentity := t.replicaIdentity

		if t.hasPK {
			targetReplicaIdentity = message.ReplicaIdentityDefault
		} else if t.replicaIdentity != message.ReplicaIdentityIndex {
			targetReplicaIdentity = message.ReplicaIdentityFull
		}

		if targetReplicaIdentity != t.replicaIdentity {
			fqtn := t.name.Sanitize()

			if _, err := conn.Exec(fmt.Sprintf("alter table only %s replica identity %s", fqtn, targetReplicaIdentity)); err != nil {
				return fmt.Errorf("could not set replica identity to %s for table %s: %v", targetReplicaIdentity, fqtn, err)
			}

			b.log.WithTableName(t.name).Infof("set replica identity to %s", targetReplicaIdentity)
		}

		tb, err := tablebackup.New(b.ctx, b.waitGr, b.cfg, t.name, t.oid, b.dbCfg, b.basebackupQueue, b.prom, b.log)
		if err != nil {
			return fmt.Errorf("could not create tablebackup instance: %v", err)
		}

		b.backupTables.Set(t.oid, tb)

		// register the new table OID to name mapping
		b.maybeRegisterNewName(t.oid, tb.NamespacedName)

	}
	// flush the OID to name mapping
	if err := b.flushOidNameMap(); err != nil {
		b.log.WithError(err).Errorf("could not flush oid name map")
	}

	return nil
}

// returns the LSN from where we should restart reads from the slot,
// InvalidLSN if no state file exists and error otherwise.
func (b *LogicalBackup) readRestartLSN() (dbutils.LSN, error) {
	var stateInfo StateInfo
	stateFilename := path.Join(b.baseDir(), stateFile)

	if _, err := os.Stat(stateFilename); os.IsNotExist(err) {
		return dbutils.InvalidLSN, nil
	}

	fp, err := os.OpenFile(stateFilename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return dbutils.InvalidLSN, fmt.Errorf("could not create current lsn file: %v", err)
	}
	defer fp.Close()

	if err := yaml.NewDecoder(fp).Decode(&stateInfo); err != nil {
		return dbutils.InvalidLSN, fmt.Errorf("could not decode state info yaml: %v", err)
	}

	currentLSN, err := pgx.ParseLSN(stateInfo.CurrentLSN)
	if err != nil {
		return dbutils.InvalidLSN, fmt.Errorf("could not parse %q LSN string: %v", stateInfo.CurrentLSN, err)
	}

	return dbutils.LSN(currentLSN), nil
}

func (b *LogicalBackup) writeRestartLSN() error {
	stateInfo := StateInfo{
		Timestamp:  time.Now(),
		CurrentLSN: b.latestFlushLSN.String(),
	}

	if b.cfg.StagingDir != "" {
		fp, err := os.OpenFile(path.Join(b.cfg.StagingDir, stateFile), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
		if err != nil {
			return fmt.Errorf("could not create current lsn file: %v", err)
		}
		defer fp.Close()

		if err := yaml.NewEncoder(fp).Encode(stateInfo); err != nil {
			return fmt.Errorf("could not save current lsn: %v", err)
		}

		if err := utils.SyncFileAndDirectory(fp); err != nil {
			b.log.WithError(err).Errorf("could not sync file and dir")
		}
	}

	fpArchive, err := os.OpenFile(path.Join(b.cfg.ArchiveDir, stateFile), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not create archive lsn file: %v", err)
	}
	defer fpArchive.Close()

	if err := yaml.NewEncoder(fpArchive).Encode(stateInfo); err != nil {
		return fmt.Errorf("could not save current lsn: %v", err)
	}

	if err := utils.SyncFileAndDirectory(fpArchive); err != nil {
		b.log.WithError(err).Errorf("could not sync file and dir")
	}

	return nil
}

func (b *LogicalBackup) BackgroundBasebackuper(i int) {
	defer b.waitGr.Done()

	baseBackupLog := logger.NewLoggerFrom(b.log, fmt.Sprintf("background base backuper %d", i))

	for {
		obj, err := b.basebackupQueue.Get()
		if err == context.Canceled {
			baseBackupLog.Warn("quiting")
			return
		}

		t := obj.(tablebackup.TableBackuper)
		baseBackupLog.WithTableNameString(t.String()).Debugf("backing up table")
		if err := t.RunBasebackup(); err != nil {
			if err == tablebackup.ErrTableNotFound {
				// Remove the table from the list of those to backup.
				// Hold the mutex to protect against concurrent access in QueueBasebackupTables
				t.Stop()
				b.backupTables.Delete(t.OID())
			} else if err != context.Canceled {
				baseBackupLog.WithError(err).WithTableNameString(t.String()).Errorf("could not basebackup")
			}
		}
		// from now on we can schedule new basebackups on that table
		t.ClearBasebackupPending()
	}
}

func (b *LogicalBackup) QueueBasebackupTables() {
	// TODO: make it a responsibility of periodicBackup on a table itself

	// need to hold the mutex here to prevent concurrent deletion of entries in the map.
	b.backupTables.Map(func(t tablebackup.TableBackuper) {
		b.basebackupQueue.Put(t)
		t.SetBasebackupPending()
	})
}

func (b *LogicalBackup) Close() error {
	b.closeOnce.Do(func() {
		close(b.stopCh)
	})

	return nil
}

func (b *LogicalBackup) Run() error {
	if b.cfg.InitialBasebackup {
		b.log.Infof("Queueing tables for the initial backup")
		b.QueueBasebackupTables()
	}

	b.waitGr.Add(1)
	go b.logicalDecoding()

	b.log.WithDetail("ConcurrentBaseBackupers = %d", b.cfg.ConcurrentBasebackups).
		Debugf("Starting background basebackupers backupers")
	for i := 0; i < b.cfg.ConcurrentBasebackups; i++ {
		b.waitGr.Add(1)
		go b.BackgroundBasebackuper(i)
	}

	b.waitGr.Add(1)
	go func() {
		defer b.waitGr.Done()
		if err := b.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			b.log.WithError(err).Errorf("could not start http server")
		}
		b.Close()
		return
	}()

	// XXX: hack to make sure the http server is aware of the context being closed.
	b.waitGr.Add(1)
	go func() {
		defer b.waitGr.Done()

		<-b.ctx.Done()
		if err := b.srv.Close(); err != nil {
			b.log.WithError(err).Errorf("could not close http server")
		}

		b.log.Infof("debug http server closed")
	}()

	b.waitGr.Add(1)
	go b.prom.Run(b.ctx, b.waitGr)

	b.WaitForShutdown()

	b.cancel()
	b.Wait()
	b.log.Sync()
	b.backupTables.Map(func(t tablebackup.TableBackuper) { t.Stop() })

	return nil
}

func (b *LogicalBackup) WaitForShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)

loop:
	for {
		select {
		case sig := <-sigs:
			switch sig {
			case syscall.SIGABRT:
				fallthrough
			case syscall.SIGINT:
				fallthrough
			case syscall.SIGQUIT:
				fallthrough
			case syscall.SIGTERM:
				break loop
			case syscall.SIGHUP:
				//TODO: reload the config?
			default:
				b.log.With("signal", sig).Error("unhandled signal")
			}
		case <-b.stopCh:
			b.log.Warn("received termination request")
			break loop
		}
	}
}

func (b *LogicalBackup) flushOidNameMap() error {
	if !b.tableNameChanges.isChanged {
		return nil
	}

	fp, err := os.OpenFile(path.Join(b.baseDir(), OidNameMapFile), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer fp.Close()

	err = yaml.NewEncoder(fp).Encode(b.tableNameChanges.nameChangeHistory)
	if err == nil {
		b.tableNameChanges.isChanged = false
	}

	if err := utils.SyncFileAndDirectory(fp); err != nil {
		return fmt.Errorf("could not sync oid to name map file: %v", err)
	}

	return err
}

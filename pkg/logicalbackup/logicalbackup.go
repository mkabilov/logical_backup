package logicalbackup

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx"

	"github.com/ikitiki/logical_backup/pkg/basebackup"
	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/consumer"
	"github.com/ikitiki/logical_backup/pkg/message"
	prom "github.com/ikitiki/logical_backup/pkg/prometheus"
	"github.com/ikitiki/logical_backup/pkg/tablebackup"
	"github.com/ikitiki/logical_backup/pkg/utils"
	"github.com/ikitiki/logical_backup/pkg/utils/dbutils"
	"github.com/ikitiki/logical_backup/pkg/utils/namehistory"
	"github.com/ikitiki/logical_backup/pkg/utils/tablesmap"
)

const (
	//OidNameMapFile represents file name of the oid-to-name map file
	OidNameMapFile = "oid2name.yaml"

	pgApplicationName = "logical_backup"
	httpSrvPort       = 8080
	httpSrvTimeout    = 20 * time.Second
)

type logicalBackup struct {
	ctx    context.Context
	cancel context.CancelFunc
	waitGr *sync.WaitGroup
	errCh  chan error

	// configs
	cfg   *config.Config
	dbCfg pgx.ConnConfig

	// auxiliary background workers
	srv          http.Server
	baseBackuper basebackup.Basebackuper
	consumer     consumer.Interface

	tables               tablesmap.TablesMapInterface // list of tables to take care
	nameHistory          namehistory.Interface        // table name history
	transactionCommitLSN dbutils.LSN                  // commit LSN of the latest observed transaction
	latestFlushLSN       dbutils.LSN                  // latest LSN flushed to disk
	beginTxLSN           dbutils.LSN
	relationsPendingTx   map[dbutils.OID]struct{} // list of the relations with begin pending message
	beginMsg             message.Begin
	typeMsg              message.Type

	prom prom.PromInterface
}

// New instantiates logical backup tool
func New(cfg *config.Config) (*logicalBackup, error) {
	ctx, cancel := context.WithCancel(context.Background())
	lb := &logicalBackup{
		ctx:                ctx,
		cancel:             cancel,
		errCh:              make(chan error),
		tables:             tablesmap.New(),
		relationsPendingTx: make(map[dbutils.OID]struct{}),
		waitGr:             &sync.WaitGroup{},
		cfg:                cfg,
		prom:               prom.New(cfg.PrometheusPort),
	}
	lb.baseBackuper = basebackup.New(ctx, lb.tables, cfg)

	lb.initHttpSrv(httpSrvPort)
	lb.nameHistory = namehistory.New(lb.filePath(OidNameMapFile))

	if err := utils.CreateDirs(cfg.StagingDir, cfg.ArchiveDir); err != nil {
		return nil, err
	}

	if err := lb.prepareDB(); err != nil {
		return nil, err
	}

	lb.consumer = consumer.New(ctx, lb.errCh, cfg.DB, cfg.SlotName, cfg.PublicationName, lb.latestFlushLSN)

	if err := lb.registerMetrics(); err != nil {
		return nil, err
	}

	return lb, nil
}

// Run runs logical backup
func (b *logicalBackup) Run() error {
	if b.cfg.InitialBasebackup {
		log.Printf("Queueing all the tables for the initial backup")
		b.tables.Map(func(t tablebackup.TableBackuper) {
			t.QueueBasebackup()
		})
	}

	if err := b.consumer.Run(b); err != nil {
		return fmt.Errorf("could not run consumer: %v", err)
	}

	b.baseBackuper.Run(b.cfg.ConcurrentBasebackups)
	b.runHttpSrv()

	b.waitGr.Add(1)
	go b.prom.Run(b.ctx, b.waitGr)

	b.waitForShutdown()
	b.cancel()

	b.consumer.Wait()
	b.baseBackuper.Wait()
	b.tables.Map(func(t tablebackup.TableBackuper) {
		t.Wait()
	})
	b.waitGr.Wait()

	return nil
}

// prepareDB creates replication slot if necessary
func (b *logicalBackup) prepareDB() error {
	b.dbCfg = b.cfg.DB
	b.dbCfg.RuntimeParams = map[string]string{"application_name": pgApplicationName}

	conn, err := pgx.Connect(b.dbCfg) // non replication protocol db connection
	if err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("could not close db connection: %v", err)
		}
	}()

	//TODO: switch to more sophisticated logger and display pid only if in debug mode
	log.Printf("Pg backend session PID: %d", conn.PID())

	if err := dbutils.CreateMissingPublication(conn, b.cfg.PublicationName); err != nil {
		return err
	}

	b.latestFlushLSN, err = dbutils.GetSlotFlushLSN(conn, b.cfg.SlotName, b.cfg.DB.Database)
	if err != nil {
		return fmt.Errorf("could not init replication slot; %v", err)
	}

	if !b.latestFlushLSN.IsValid() { // invalid start LSN and no error from initSlot denotes a non-existing slot.
		// TODO: this will discard all existing backup data, we should probably bail out if existing backup is there
		log.Printf("Creating logical replication slot %s", b.cfg.SlotName)

		initialLSN, err := dbutils.CreateSlot(conn, b.ctx, b.cfg.SlotName)
		if err != nil {
			return fmt.Errorf("could not create replication slot: %v", err)
		}
		log.Printf("Created missing replication slot %q, consistent point %s", b.cfg.SlotName, initialLSN)

		// solve impedance mismatch between the flush LSN (the LSN we confirmed and flushed) and slot initial LSN
		// (next, but not yet received LSN).
		b.latestFlushLSN = initialLSN - 1
	} else {
		if restartLSN, err := b.readRestartLSN(); err != nil {
			return fmt.Errorf("could not read previous flush lsn: %v", err)
		} else if restartLSN.IsValid() {
			b.latestFlushLSN = restartLSN
		}
	}

	// run per-table backups after we ensure there is a replication slot to retain changes.
	if err = b.prepareTablesForPublication(conn); err != nil {
		return fmt.Errorf("could not prepare tables for backup: %v", err)
	}

	return nil
}

func (b *logicalBackup) readRestartLSN() (dbutils.LSN, error) {
	//TODO:

	return dbutils.InvalidLSN, nil
}

// flushLSN gets the minimum flush lsn among all the tables
func (b *logicalBackup) flushLSN() dbutils.LSN {
	lsn := dbutils.InvalidLSN

	b.tables.Map(func(t tablebackup.TableBackuper) {
		if flushLSN := t.FlushLSN(); flushLSN.IsValid() {
			if !lsn.IsValid() || flushLSN < lsn {
				lsn = flushLSN
			}
		}
	})

	return lsn
}

func (b *logicalBackup) filePath(parts ...string) string {
	var path string

	if b.cfg.StagingDir != "" {
		path = b.cfg.StagingDir
	}
	path = b.cfg.ArchiveDir

	for _, part := range parts {
		path = filepath.Join(path, part)
	}

	return path
}

//HandleMessage processes the incoming logical replication message
func (b *logicalBackup) HandleMessage(msg message.Message, walStart dbutils.LSN) error {
	switch v := msg.(type) {
	case message.Relation:
		return b.processRelationMessage(v)
	case message.Insert:
		return b.processInsertMessage(v)
	case message.Update:
		return b.processUpdateMessage(v)
	case message.Delete:
		return b.processDeleteMessage(v)
	case message.Begin:
		b.beginTxLSN = walStart
		return b.processBeginMessage(v)
	case message.Commit:
		b.transactionCommitLSN = v.LSN
		return b.processCommitMessage(v)
	case message.Origin:
		return b.processOriginMessage(v)
	case message.Truncate:
		return b.processTruncateMessage(v)
	case message.Type:
		return b.processTypeMessage(v)
	default:
		return fmt.Errorf("unknown message type")
	}
}

func (b *logicalBackup) writeTableDMLMessage(relOID dbutils.OID, msg message.Message) error {
	tb, ok := b.tables.Get(relOID)
	if !ok {
		return fmt.Errorf("could not find relation with oid %v", relOID)
	}

	// write pending begin message
	if _, ok := b.relationsPendingTx[relOID]; !ok {
		if err := tb.ProcessBegin(b.beginMsg); err != nil {
			return err
		}

		b.relationsPendingTx[relOID] = struct{}{}
	}

	if err := tb.ProcessDMLMessage(msg); err != nil {
		return fmt.Errorf("could not save message: %v", err)
	}

	if err := b.updateMetricsAfterWriteDelta(tb, msg.MsgType(), uint(len(msg.RawData()))); err != nil {
		log.Printf("could not update metrics for %s table: %v", tb, err)
	}

	return nil
}

func (b *logicalBackup) processInsertMessage(msg message.Insert) error {
	return b.writeTableDMLMessage(msg.RelationOID, msg)
}

func (b *logicalBackup) processUpdateMessage(msg message.Update) error {
	return b.writeTableDMLMessage(msg.RelationOID, msg)
}

func (b *logicalBackup) processDeleteMessage(msg message.Delete) error {
	return b.writeTableDMLMessage(msg.RelationOID, msg)
}

func (b *logicalBackup) processRelationMessage(msg message.Relation) error {
	if tb, isRegistered := b.tables.Get(msg.OID); isRegistered {
		b.nameHistory.SetName(tb.OID(), b.beginTxLSN, msg.NamespacedName)
		tb.SetName(msg.NamespacedName)
		return nil
	}

	if track, err := b.registerNewTable(msg); err != nil {
		return fmt.Errorf("could not add a backup process for the new table %s: %v", msg.NamespacedName, err)
	} else if !track { // instructed not to track this table
		return nil
	}

	return nil
}

func (b *logicalBackup) processTruncateMessage(msg message.Truncate) error {
	var err error

	for _, oid := range msg.RelationOIDs {
		err = b.writeTableDMLMessage(oid, msg)
		if err != nil {
			break
		}
	}

	return err
}

func (b *logicalBackup) processOriginMessage(msg message.Origin) error {
	//TODO:
	return nil
}

func (b *logicalBackup) processTypeMessage(msg message.Type) error {
	//TODO: consider writing this message to all tables in a transaction as a safety measure during restore.
	return nil
}

func (b *logicalBackup) processBeginMessage(msg message.Begin) error {
	b.relationsPendingTx = make(map[dbutils.OID]struct{})
	b.beginMsg = msg

	return nil
}

// AdvanceLSN checks if we need to advance lsn
func (b *logicalBackup) AdvanceLSN() {
	if candidateFlushLSN := b.flushLSN(); candidateFlushLSN > b.latestFlushLSN {
		b.latestFlushLSN = candidateFlushLSN
		b.consumer.AdvanceLSN(b.latestFlushLSN)

		if err := b.consumer.SendStatus(); err != nil {
			log.Printf("could not send replay progress: %v", err)
		}
	}
}

// QueueTable queues the basebackup of the table
func (b *logicalBackup) QueueTable(t tablebackup.TableBackuper) {
	b.baseBackuper.QueueTable(t)
}

func (b *logicalBackup) processCommitMessage(msg message.Commit) error {
	// commit is special, because the LSN of the CopyData message points past the commit message.
	// for consistency we set the currentLSN here to the commit message LSN inside the commit itself.

	for relOID := range b.relationsPendingTx {
		tb, ok := b.tables.Get(relOID)
		if !ok {
			return fmt.Errorf("could not find relation with oid %v", relOID)
		}

		if err := tb.ProcessCommit(msg); err != nil {
			return fmt.Errorf("could not write commit message for table with %v oid: %v", relOID, err)
		}
	}

	// if there were any changes in the table names, flush the map file
	if err := b.nameHistory.Save(); err != nil {
		log.Printf("could not flush the oid to map file: %v", err)
	}

	b.AdvanceLSN()

	if err := b.updateMetricsCommit(b.transactionCommitLSN, msg.Timestamp); err != nil {
		log.Printf("could not update metrics: %v", err)
	}

	return nil
}

func (b *logicalBackup) registerNewTable(msg message.Relation) (bool, error) {
	var (
		tb  tablebackup.TableBackuper
		err error
	)

	if !b.cfg.TrackNewTables {
		log.Printf("skip the table with oid %d and name %v because we are configured not to track new tables",
			msg.OID, msg.NamespacedName)
		return false, nil
	}

	tb, err = tablebackup.New(b.ctx, msg.NamespacedName, msg.OID, b, b.cfg, b.prom)
	if err != nil {
		return false, err
	}

	if err := tb.ProcessRelationMessage(msg); err != nil {
		return false, err
	}

	b.tables.Set(msg.OID, tb)
	b.nameHistory.SetName(msg.OID, b.beginTxLSN, msg.NamespacedName)
	log.Printf("registered new table with oid %d and name %s", msg.OID, msg.NamespacedName.Sanitize())

	return true, nil
}

// register tables for the backup; add replica identity when necessary
func (b *logicalBackup) prepareTablesForPublication(conn *pgx.Conn) error {
	// fetch all tables from the current publication, together with the information on whether we need to create
	// replica identity "full" for them
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
	for rows.Next() {
		var tab tableInfo

		err = rows.Scan(&tab.oid, &tab.name.Namespace, &tab.name.Name, &tab.hasPK, &tab.replicaIdentity)
		if err != nil {
			break
		}

		tables = append(tables, tab)
	}
	rows.Close()
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
				return fmt.Errorf("could not set replica identity to %s for %s table: %v", targetReplicaIdentity, fqtn, err)
			}

			log.Printf("set replica identity to %s for %s table", targetReplicaIdentity, fqtn)
		}
	}

	for _, t := range tables {
		tb, err := tablebackup.New(b.ctx, t.name, t.oid, b, b.cfg, b.prom)
		if err != nil {
			return fmt.Errorf("could not create tablebackup instance: %v", err)
		}

		if err := tb.LoadTableInfo(); err != nil {
			return fmt.Errorf("could not load table info: %v", err)
		}

		b.tables.Set(t.oid, tb)
		b.nameHistory.SetName(t.oid, b.latestFlushLSN, tb.NamespacedName)
	}

	// flush the OID to name mapping
	if err := b.nameHistory.Save(); err != nil {
		log.Printf("could not flush oid name map: %v", err)
	}

	return nil
}

func (b *logicalBackup) waitForShutdown() {
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
				log.Printf("unhandled signal: %v", sig)
			}
		case err := <-b.errCh:
			log.Printf("failed: %v", err)
			break loop
		}
	}
}

func (b *logicalBackup) initHttpSrv(port int) {
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

func (b *logicalBackup) runHttpSrv() {
	b.waitGr.Add(1)
	go func() {
		defer b.waitGr.Done()

		if err := b.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("could not start http server %v", err)
		}
		return
	}()

	// XXX: hack to make sure the http server is aware of the context being closed.
	b.waitGr.Add(1)
	go func() {
		defer b.waitGr.Done()

		<-b.ctx.Done()
		if err := b.srv.Close(); err != nil {
			log.Printf("could not close http server: %v", err)
		}

		log.Printf("debug http server shut down")
	}()
}

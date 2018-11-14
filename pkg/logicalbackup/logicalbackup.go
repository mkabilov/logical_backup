package logicalbackup

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"path"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/decoder"
	"github.com/ikitiki/logical_backup/pkg/message"
	prom "github.com/ikitiki/logical_backup/pkg/prometheus"
	"github.com/ikitiki/logical_backup/pkg/queue"
	"github.com/ikitiki/logical_backup/pkg/tablebackup"
	"github.com/ikitiki/logical_backup/pkg/utils"
)

type cmdType int

const (
	applicationName = "logical_backup"
	outputPlugin    = "pgoutput"
	logicalSlotType = "logical"
	OidNameMapFile  = "oid2name.yaml"
	stateFile       = "state.yaml"

	statusTimeout = time.Second * 10
	waitTimeout   = time.Second * 10

	cInsert cmdType = iota
	cUpdate
	cDelete
	cBegin
	cCommit
	cRelation
	cType
)

type NameAtLsn struct {
	Name message.NamespacedName
	Lsn  dbutils.Lsn
}

type oidToName struct {
	isChanged         bool
	nameChangeHistory map[dbutils.Oid][]NameAtLsn
}

type StateInfo struct {
	Timestamp  time.Time
	CurrentLSN string
}

type LogicalBackuper interface {
	Run()
	Wait()
}

type BackupTables struct {
	sync.RWMutex
	data map[dbutils.Oid]tablebackup.TableBackuper
}

func NewBackupTables() *BackupTables {
	return &BackupTables{sync.RWMutex{}, make(map[dbutils.Oid]tablebackup.TableBackuper)}
}

func (bt *BackupTables) GetIfExists(oid dbutils.Oid) tablebackup.TableBackuper {
	bt.RLock()
	defer bt.RUnlock()

	return bt.data[oid]
}

func (bt *BackupTables) Get(oid dbutils.Oid) (result tablebackup.TableBackuper, ok bool) {
	bt.RLock()
	defer bt.RUnlock()

	result, ok = bt.data[oid]
	return
}

func (bt *BackupTables) Set(oid dbutils.Oid, t tablebackup.TableBackuper) {
	bt.Lock()
	defer bt.Unlock()

	bt.data[oid] = t
}

func (bt *BackupTables) Delete(oid dbutils.Oid) {
	bt.Lock()
	defer bt.Unlock()

	delete(bt.data, oid)
}

func (bt *BackupTables) Map(fn func(t tablebackup.TableBackuper)) {
	bt.Lock()
	defer bt.Unlock()

	for _, t := range bt.data {
		fn(t)
	}
}

type LogicalBackup struct {
	ctx context.Context
	cfg *config.Config

	pluginArgs []string

	backupTables *BackupTables

	tableNameChanges oidToName

	dbCfg    pgx.ConnConfig
	replConn *pgx.ReplicationConn // connection for logical replication
	tx       *pgx.Tx

	replMessageWaitTimeout time.Duration
	statusTimeout          time.Duration

	currentLSN           dbutils.Lsn // latest received LSN
	transactionCommitLSN dbutils.Lsn // commit LSN of the latest observed transaction (obtained when reading BEGIN)
	latestFlushLSN       dbutils.Lsn // latest LSN flushed to disk
	lastTxId             int32       // transaction ID of the latest observed transaction (obtained when reading BEGIN)

	basebackupQueue *queue.Queue
	waitGr          *sync.WaitGroup
	stopCh          chan struct{}

	// TODO: should we get rid of those altogether given that we have prometheus?
	msgCnt       map[cmdType]int
	bytesWritten uint64

	txBeginRelMsg    map[dbutils.Oid]struct{}
	relationMessages map[dbutils.Oid][]byte
	beginMsg         []byte
	typeMsg          []byte

	srv  http.Server
	prom prom.PrometheusExporterInterface
}

func New(ctx context.Context, stopCh chan struct{}, cfg *config.Config) (*LogicalBackup, error) {
	var (
		slotExists bool
		err        error
	)

	pgxConn := cfg.DB
	pgxConn.RuntimeParams = map[string]string{"application_name": applicationName}

	mux := http.NewServeMux()

	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

	lb := &LogicalBackup{
		ctx:    ctx,
		stopCh: stopCh,
		dbCfg:  pgxConn,

		replMessageWaitTimeout: waitTimeout,
		statusTimeout:          statusTimeout,
		backupTables:           NewBackupTables(),
		relationMessages:       make(map[dbutils.Oid][]byte),
		tableNameChanges:       oidToName{nameChangeHistory: make(map[dbutils.Oid][]NameAtLsn)},
		pluginArgs:             []string{`"proto_version" '1'`, fmt.Sprintf(`"publication_names" '%s'`, cfg.PublicationName)},
		basebackupQueue:        queue.New(ctx),
		waitGr:                 &sync.WaitGroup{},
		cfg:                    cfg,
		msgCnt:                 make(map[cmdType]int),
		srv: http.Server{
			Addr:    fmt.Sprintf(":%d", 8080),                     // TODO: get rid of the hardcoded value
			Handler: http.TimeoutHandler(mux, time.Second*20, ""), // TODO: get rid of the hardcoded value
		},
		prom: prom.New(cfg.PrometheusPort),
	}

	if cfg.StagingDir != "" {
		if _, err := os.Stat(cfg.StagingDir); os.IsNotExist(err) {
			if err := os.Mkdir(cfg.StagingDir, os.ModePerm); err != nil {
				return nil, fmt.Errorf("could not create staging dir: %v", err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("%q stat error: %v", cfg.StagingDir, err)
		}
	}

	if _, err := os.Stat(cfg.ArchiveDir); os.IsNotExist(err) {
		if err := os.Mkdir(cfg.ArchiveDir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("could not create archive dir: %v", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("%q stat error: %v", cfg.ArchiveDir, err)
	}

	conn, err := pgx.Connect(pgxConn)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %v", err)
	}
	defer conn.Close()

	//TODO: switch to more sophisticated logger and display pid only if in debug mode
	log.Printf("Pg backend session PID: %d", conn.PID())

	if err := lb.initPublication(conn); err != nil {
		return nil, err
	}

	lb.latestFlushLSN, err = lb.initSlot(conn)
	if err != nil {
		return nil, fmt.Errorf("could not init replication slot; %v", err)
	}
	// invalid start LSN and no error from initSlot denotes a non-existing slot.
	slotExists = lb.latestFlushLSN.IsValid()

	//TODO: have a separate "init" command which will set replica identity and create replication slots/publications
	if rc, err := pgx.ReplicationConnect(cfg.DB); err != nil {
		return nil, fmt.Errorf("could not connect using replication protocol: %v", err)
	} else {
		lb.replConn = rc
	}

	if !slotExists {
		// TODO: this will discard all existing backup data, we should probably bail out if existing backup is there
		log.Printf("Creating logical replication slot %s", lb.cfg.Slotname)

		initialLSN, err := lb.createSlot(conn)
		if err != nil {
			return nil, fmt.Errorf("could not create replication slot: %v", err)
		}
		log.Printf("Created missing replication slot %q, consistent point %s", lb.cfg.Slotname, initialLSN)

		lb.latestFlushLSN = initialLSN

		if err := lb.writeRestartLSN(); err != nil {
			log.Printf("could not store initial LSN: %v", err)
		}
	} else {
		restartLSN, err := lb.readRestartLSN()
		if err != nil {
			return nil, fmt.Errorf("could not read previous flush lsn: %v", err)
		}
		if restartLSN.IsValid() {
			lb.latestFlushLSN = restartLSN
		}
		// we may have flushed the final segment at shutdown without bothering to advance the slot LSN.
		if err := lb.sendStatus(); err != nil {
			log.Printf("could not send replay progress: %v", err)
		}
	}

	// run per-table backups after we ensure there is a replication slot to retain changes.
	if err = lb.prepareTablesForPublication(conn); err != nil {
		return nil, fmt.Errorf("could not prepare tables for backup: %v", err)
	}

	if err := lb.registerMetrics(); err != nil {
		return nil, err
	}

	return lb, nil
}

func (b *LogicalBackup) baseDir() string {
	if b.cfg.StagingDir != "" {
		return b.cfg.StagingDir
	} else {
		return b.cfg.ArchiveDir
	}
}

func (b *LogicalBackup) processDMLMessage(tableOID dbutils.Oid, cmd cmdType, msg []byte) error {
	bt, ok := b.backupTables.Get(tableOID)
	if !ok {
		log.Printf("table with OID %d is not tracked", tableOID)
		return nil
	}

	// We need to write the transaction begin message to every table being collected,
	// therefore, it cannot be written right away when we receive it. Instead, wait for
	// the actual relation data to arrive and then write it here.
	// TODO: avoid multiple writes
	if _, ok := b.txBeginRelMsg[tableOID]; !ok {
		if err := b.WriteCommandDataForTable(bt, b.beginMsg, cBegin); err != nil {
			return err
		}
		b.txBeginRelMsg[bt.ID()] = struct{}{}
	}

	if b.typeMsg != nil {
		if err := b.WriteCommandDataForTable(bt, b.typeMsg, cType); err != nil {
			return fmt.Errorf("could not write type message: %v", err)
		}
		b.typeMsg = nil
	}

	if relationMsg, ok := b.relationMessages[tableOID]; ok {
		if err := b.WriteCommandDataForTable(bt, relationMsg, cRelation); err != nil {
			return fmt.Errorf("could not write a relation message: %v", err)
		}
		delete(b.relationMessages, tableOID)
	}

	if err := b.WriteCommandDataForTable(bt, msg, cmd); err != nil {
		return fmt.Errorf("could not write message: %v", err)
	}

	return nil
}

func (b *LogicalBackup) WriteCommandDataForTable(t tablebackup.TableBackuper, msg []byte, cmd cmdType) error {
	ln, err := t.WriteDelta(msg, b.transactionCommitLSN, b.currentLSN)
	if err != nil {
		return err
	}

	b.bytesWritten += ln
	b.msgCnt[cmd]++

	b.updateMetricsAfterWriteDelta(t, cmd, ln)

	return nil
}

func (b *LogicalBackup) handler(m message.Message) error {
	var err error

	switch v := m.(type) {

	case message.Relation:
		err = b.processRelationMessage(v)

	case message.Insert:
		err = b.processDMLMessage(v.RelationOID, cInsert, v.Raw)

	case message.Update:
		err = b.processDMLMessage(v.RelationOID, cUpdate, v.Raw)

	case message.Delete:
		err = b.processDMLMessage(v.RelationOID, cDelete, v.Raw)

	case message.Begin:
		b.lastTxId = v.XID
		b.transactionCommitLSN = v.FinalLSN

		b.txBeginRelMsg = make(map[dbutils.Oid]struct{})
		b.beginMsg = v.Raw

	case message.Commit:
		// commit is special, because the LSN of the CopyData message points past the commit message.
		// for consistency we set the currentLSN here to the commit message LSN inside the commit itself.
		b.currentLSN = v.LSN
		for relOID := range b.txBeginRelMsg {
			tb := b.backupTables.GetIfExists(relOID)
			if err = b.WriteCommandDataForTable(tb, v.Raw, cCommit); err != nil {
				return err
			}
		}

		// if there were any changes in the table names, flush the map file
		if err := b.flushOidNameMap(); err != nil {
			log.Printf("could not flush the oid to map file: %v", err)
		}

		candidateFlushLSN := b.getNextFlushLSN()

		if candidateFlushLSN > b.latestFlushLSN {
			b.latestFlushLSN = candidateFlushLSN
			log.Printf("advanced flush LSN to %s", b.latestFlushLSN)

			if err := b.writeRestartLSN(); err != nil {
				log.Printf("could not store flush LSN: %v", err)
			}

			if err = b.sendStatus(); err != nil {
				log.Printf("could not send replay progress: %v", err)
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
func (b *LogicalBackup) getNextFlushLSN() dbutils.Lsn {
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

func printMessage(m message.Message, currentLSN dbutils.Lsn) {
	printer := func(messageType string) {
		log.Printf("received %s message with LSN %s", messageType, currentLSN)
	}
	switch m.(type) {
	case message.Relation:
		printer("relation")
	case message.Begin:
		printer("begin")
	case message.Commit:
		printer("commit")
	case message.Insert:
		printer("insert")
	case message.Update:
		printer("update")
	case message.Delete:
		printer("delete")
	case message.Truncate:
		printer("truncate")
	}
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
		log.Printf("skip the table with oid %d and name %v because we are configured not to track new tables",
			m.OID, m.NamespacedName)
		return false, nil
	}

	tb, err := tablebackup.New(b.ctx, b.waitGr, b.cfg, m.NamespacedName, m.OID, b.dbCfg, b.basebackupQueue, b.prom)
	if err != nil {
		return false, err
	}

	b.backupTables.Set(m.OID, tb)
	log.Printf("registered new table with oid %d and name %s", m.OID, m.NamespacedName.Sanitize())

	return true, nil
}

func (b *LogicalBackup) sendStatus() error {
	log.Printf("sending new status with %s flush lsn (i:%d u:%d d:%d b:%0.2fMb) ",
		b.latestFlushLSN, b.msgCnt[cInsert], b.msgCnt[cUpdate], b.msgCnt[cDelete], float64(b.bytesWritten)/1048576)

	b.msgCnt = make(map[cmdType]int)
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
	log.Printf("Starting from %s lsn", b.latestFlushLSN)

	err := b.replConn.StartReplication(b.cfg.Slotname, uint64(b.latestFlushLSN), -1, b.pluginArgs...)
	if err != nil {
		log.Printf("failed to start replication: %s", err)
		b.stopCh <- struct{}{}
		return
	}

	ticker := time.NewTicker(b.statusTimeout)
	for {
		select {
		case <-b.ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			if err := b.sendStatus(); err != nil {
				log.Printf("could not send replay progress: %v", err)
				b.stopCh <- struct{}{}
				return
			}
		default:
			wctx, cancel := context.WithTimeout(b.ctx, b.replMessageWaitTimeout)
			repMsg, err := b.replConn.WaitForReplicationMessage(wctx)
			cancel()
			if err == context.DeadlineExceeded {
				continue
			}
			if err == context.Canceled {
				log.Printf("received shutdown request: replication terminated")
				return
			}
			// TODO: make sure we retry and cleanup after ourselves afterwards
			if err != nil {
				log.Printf("replication failed: %v", err)
				b.stopCh <- struct{}{}
				return
			}

			if repMsg == nil {
				log.Printf("received null replication message")
				continue
			}

			if repMsg.WalMessage != nil {
				b.currentLSN = dbutils.Lsn(repMsg.WalMessage.WalStart)
				logmsg, err := decoder.Parse(repMsg.WalMessage.WalData)
				if err != nil {
					log.Printf("invalid pgoutput message: %s", err)
					b.stopCh <- struct{}{}
					return
				}
				if err := b.handler(logmsg); err != nil {
					log.Printf("error handling waldata: %s", err)
					b.stopCh <- struct{}{}
					return
				}
			}

			if repMsg.ServerHeartbeat != nil && repMsg.ServerHeartbeat.ReplyRequested == 1 {
				log.Println("server wants a reply")
				if err := b.sendStatus(); err != nil {
					log.Printf("could not send replay progress: %v", err)
					b.stopCh <- struct{}{}
					return
				}
			}
		}
	}
}

func (b *LogicalBackup) initSlot(conn *pgx.Conn) (dbutils.Lsn, error) {
	var lsn dbutils.Lsn

	rows, err := conn.Query("select confirmed_flush_lsn, slot_type, database from pg_replication_slots where slot_name = $1;", b.cfg.Slotname)
	if err != nil {
		return dbutils.InvalidLsn, fmt.Errorf("could not execute query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var lsnString, slotType, database string

		if err := rows.Err(); err != nil {
			return dbutils.InvalidLsn, fmt.Errorf("could not execute query: %v", err)
		}

		if err := rows.Scan(&lsnString, &slotType, &database); err != nil {
			return dbutils.InvalidLsn, fmt.Errorf("could not scan lsn: %v", err)
		}

		if slotType != logicalSlotType {
			return dbutils.InvalidLsn, fmt.Errorf("slot %q is not a logical slot", b.cfg.Slotname)
		}

		if database != b.dbCfg.Database {
			return dbutils.InvalidLsn, fmt.Errorf("replication slot %q belongs to %q database", b.cfg.Slotname, database)
		}
		if err := lsn.Parse(lsnString); err != nil {
			return dbutils.InvalidLsn, fmt.Errorf("could not parse lsn: %v", err)
		}
	}

	return lsn, nil
}

func (b *LogicalBackup) createSlot(conn *pgx.Conn) (dbutils.Lsn, error) {
	var strLSN sql.NullString
	row := conn.QueryRowEx(b.ctx, "select lsn from pg_create_logical_replication_slot($1, $2)",
		nil, b.cfg.Slotname, outputPlugin)

	if err := row.Scan(&strLSN); err != nil {
		return 0, fmt.Errorf("could not scan: %v", err)
	}
	if !strLSN.Valid {
		return 0, fmt.Errorf("null lsn returned")
	}

	lsn, err := pgx.ParseLSN(strLSN.String)
	if err != nil {
		return 0, fmt.Errorf("could not parse lsn: %v", err)
	}

	return dbutils.Lsn(lsn), nil
}

// Wait for the goroutines to finish
func (b *LogicalBackup) Wait() {
	b.waitGr.Wait()
}

func (b *LogicalBackup) initPublication(conn *pgx.Conn) error {
	rows, err := conn.Query("select 1 from pg_publication where pubname = $1;", b.cfg.PublicationName)
	if err != nil {
		return fmt.Errorf("could not execute query: %v", err)
	}

	for rows.Next() {
		rows.Close()
		return nil
	}
	rows.Close()

	query := fmt.Sprintf("create publication %s for all tables",
		pgx.Identifier{b.cfg.PublicationName}.Sanitize())

	if _, err := conn.Exec(query); err != nil {
		return fmt.Errorf("could not create publication: %v", err)
	}
	rows.Close()
	log.Printf("created missing publication: %q", query)

	return nil
}

// register tables for the backup; add replica identity when necessary
func (b *LogicalBackup) prepareTablesForPublication(conn *pgx.Conn) error {
	// fetch all tables from the current publication, together with the information on whether we need to create
	// replica identity full for them
	type tableInfo struct {
		oid             dbutils.Oid
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

			log.Printf("set replica identity to %s for table %s", targetReplicaIdentity, fqtn)
		}

		tb, err := tablebackup.New(b.ctx, b.waitGr, b.cfg, t.name, t.oid, b.dbCfg, b.basebackupQueue, b.prom)
		if err != nil {
			return fmt.Errorf("could not create tablebackup instance: %v", err)
		}

		b.backupTables.Set(t.oid, tb)

		// register the new table OID to name mapping
		b.maybeRegisterNewName(t.oid, tb.NamespacedName)

	}
	// flush the OID to name mapping
	if err := b.flushOidNameMap(); err != nil {
		log.Printf("could not flush oid name map: %v", err)
	}

	return nil
}

// returns the LSN from where we should restart reads from the slot,
// InvalidLSN if no state file exists and error otherwise.
func (b *LogicalBackup) readRestartLSN() (dbutils.Lsn, error) {
	var stateInfo StateInfo
	stateFilename := path.Join(b.baseDir(), stateFile)

	if _, err := os.Stat(stateFilename); os.IsNotExist(err) {
		return dbutils.InvalidLsn, nil
	}

	fp, err := os.OpenFile(stateFilename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return dbutils.InvalidLsn, fmt.Errorf("could not create current lsn file: %v", err)
	}
	defer fp.Close()

	if err := yaml.NewDecoder(fp).Decode(&stateInfo); err != nil {
		return dbutils.InvalidLsn, fmt.Errorf("could not decode state info yaml: %v", err)
	}

	currentLSN, err := pgx.ParseLSN(stateInfo.CurrentLSN)
	if err != nil {
		return dbutils.InvalidLsn, fmt.Errorf("could not parse %q Lsn string: %v", stateInfo.CurrentLSN, err)
	}

	return dbutils.Lsn(currentLSN), nil
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
			log.Printf("could not sync file and dir: %v", err)
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
		log.Printf("could not sync file and dir: %v", err)
	}

	return nil
}

func (b *LogicalBackup) BackgroundBasebackuper(i int) {
	defer b.waitGr.Done()

	for {
		obj, err := b.basebackupQueue.Get()
		if err == context.Canceled {
			log.Printf("quiting background base backuper %d", i)
			return
		}

		t := obj.(tablebackup.TableBackuper)
		log.Printf("background base backuper %d: backing up table %s", i, t)
		if err := t.RunBasebackup(); err != nil {
			if err == tablebackup.ErrTableNotFound {
				// Remove the table from the list of those to backup.
				// Hold the mutex to protect against concurrent access in QueueBasebackupTables
				t.Stop()
				b.backupTables.Delete(t.ID())
			} else if err != context.Canceled {
				log.Printf("could not basebackup %s: %v", t, err)
			}
		}
		// from now on we can schedule new basebackups on that table
		t.ClearBasebackupPending()
	}
}

// TODO: make it a responsibility of periodicBackup on a table itself
func (b *LogicalBackup) QueueBasebackupTables() {
	// need to hold the mutex here to prevent concurrent deletion of entries in the map.
	b.backupTables.Map(func(t tablebackup.TableBackuper) {
		b.basebackupQueue.Put(t)
		t.SetBasebackupPending()
	})
}

func (b *LogicalBackup) Run() {
	b.waitGr.Add(1)
	go b.logicalDecoding()

	log.Printf("Starting %d background backupers", b.cfg.ConcurrentBasebackups)
	for i := 0; i < b.cfg.ConcurrentBasebackups; i++ {
		b.waitGr.Add(1)
		go b.BackgroundBasebackuper(i)
	}

	b.waitGr.Add(1)
	go func() {
		defer b.waitGr.Done()
		if err := b.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("could not start http server %v", err)
		}
		b.stopCh <- struct{}{}
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

		log.Printf("debug http server closed")
	}()

	b.waitGr.Add(1)
	go b.prom.Run(b.ctx, b.waitGr, b.stopCh)
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

func (b *LogicalBackup) maybeRegisterNewName(oid dbutils.Oid, name message.NamespacedName) {
	var lastEntry NameAtLsn

	if b.tableNameChanges.nameChangeHistory[oid] != nil {
		lastEntry = b.tableNameChanges.nameChangeHistory[oid][len(b.tableNameChanges.nameChangeHistory[oid])-1]
	}
	if b.tableNameChanges.nameChangeHistory[oid] == nil || lastEntry.Name != name {
		b.tableNameChanges.nameChangeHistory[oid] = append(b.tableNameChanges.nameChangeHistory[oid],
			NameAtLsn{Name: name, Lsn: b.transactionCommitLSN})
		b.tableNameChanges.isChanged = true

		// inform the tableBackuper about the new name
		b.backupTables.GetIfExists(oid).SetTextID(name)
	}
}

func (lb *LogicalBackup) registerMetrics() error {
	registerMetrics := []prom.MetricsToRegister{
		{
			prom.MessageCounter,
			"total number of messages received",
			[]string{prom.MessageTypeLabel},
			prom.MetricsCounter,
		},
		{
			prom.TotalBytesWrittenCounter,
			"total bytes written",
			nil,
			prom.MetricsCounter,
		},
		{
			prom.TransactionCounter,
			"total number of transactions",
			nil,
			prom.MetricsCounter,
		},
		{
			prom.FlushLSNCGauge,
			"last LSN to flush",
			nil,
			prom.MetricsGauge,
		},
		{
			prom.LastCommitTimestampGauge,
			"last commit timestamp",
			nil,
			prom.MetricsGauge,
		},
		{
			prom.LastWrittenMessageTimestampGauge,
			"last written message timestamp",
			nil,
			prom.MetricsGauge,
		},
		{
			prom.FilesArchivedCounter,
			"total files archived",
			nil,
			prom.MetricsCounter,
		},
		{
			prom.FilesArchivedTimeoutCounter,
			"total number of files archived due to a timeout",
			nil,
			prom.MetricsCounter,
		},
		{
			prom.PerTableMessageCounter,
			"per table number of messages written",
			[]string{prom.TableOIDLabel, prom.TableNameLabel, prom.MessageTypeLabel},
			prom.MetricsCounterVector,
		},
		{
			prom.PerTableBytesCounter,
			"per table number of bytes written",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsCounterVector,
		},
		{
			prom.PerTablesFilesArchivedCounter,
			"per table number of segments archived",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsCounterVector,
		},
		{
			prom.PerTableFilesArchivedTimeoutCounter,
			"per table number of segments archived due to a timeout",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsCounterVector,
		},
		{
			prom.PerTableLastCommitTimestampGauge,
			"per table last commit message timestamp",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsGaugeVector,
		},
		{
			prom.PerTableLastBackupEndTimestamp,
			"per table last backup end timestamp",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsGaugeVector,
		},
		{
			prom.PerTableMessageSinceLastBackupGauge,
			"per table number of messages since the last basebackup",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsGaugeVector,
		},
	}
	for _, m := range registerMetrics {
		if err := lb.prom.RegisterMetricsItem(&m); err != nil {
			return fmt.Errorf("could not register prometheus metrics: %v", err)
		}
	}

	return nil
}

func (b *LogicalBackup) updateMetricsAfterWriteDelta(t tablebackup.TableBackuper, cmd cmdType, ln uint64) {
	var promType string

	switch cmd {
	case cInsert:
		promType = prom.MessageTypeInsert
	case cUpdate:
		promType = prom.MessageTypeUpdate
	case cDelete:
		promType = prom.MessageTypeDelete
	case cBegin:
		promType = prom.MessageTypeBegin
	case cCommit:
		promType = prom.MessageTypeCommit
	case cRelation:
		promType = prom.MessageTypeRelation
	case cType:
		promType = prom.MessageTypeTypeInfo
	default:
		promType = prom.MessageTypeUnknown
	}

	b.prom.Inc(prom.MessageCounter, []string{promType})
	b.prom.Inc(prom.PerTableMessageCounter, []string{t.ID().String(), t.TextID(), promType})
	b.prom.SetToCurrentTime(prom.LastWrittenMessageTimestampGauge, nil)

	b.prom.Add(prom.TotalBytesWrittenCounter, float64(ln), nil)
	b.prom.Add(prom.PerTableBytesCounter, float64(ln), []string{t.ID().String(), t.TextID()})
	b.prom.Inc(prom.PerTableMessageSinceLastBackupGauge, []string{t.ID().String(), t.TextID()})
}

func (b *LogicalBackup) updateMetricsOnCommit(commitTimestamp int64) {

	for relOID := range b.txBeginRelMsg {
		tb := b.backupTables.GetIfExists(relOID)
		b.prom.Set(prom.PerTableLastCommitTimestampGauge, float64(commitTimestamp), []string{tb.ID().String(), tb.TextID()})
	}

	b.prom.Inc(prom.TransactionCounter, nil)
	b.prom.Set(prom.FlushLSNCGauge, float64(b.transactionCommitLSN), nil)
	b.prom.Set(prom.LastCommitTimestampGauge, float64(commitTimestamp), nil)
}

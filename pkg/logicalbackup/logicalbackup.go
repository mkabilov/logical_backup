package logicalbackup

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/decoder"
	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/queue"
	"github.com/ikitiki/logical_backup/pkg/tablebackup"
)

const (
	applicationName = "logical_backup"
	outputPlugin    = "pgoutput"
	logicalSlotType = "logical"

	statusTimeout = time.Second * 10
	waitTimeout   = time.Second * 10
	backupsPeriod = time.Minute * 10
)

type LogicalBackuper interface {
	Run()
	Wait()
}

type LogicalBackup struct {
	ctx           context.Context
	stateFilename string
	cfg           *config.Config

	pluginArgs []string

	backupTables map[uint32]tablebackup.TableBackuper

	connCfg  pgx.ConnConfig
	replConn *pgx.ReplicationConn // connection for logical replication
	tx       *pgx.Tx

	replMessageWaitTimeout time.Duration
	statusTimeout          time.Duration

	relations     map[message.Identifier]message.Relation
	relationNames map[uint32]message.Identifier
	types         map[uint32]message.Type

	storedRestartLSN uint64
	startLSN         uint64
	flushLSN         uint64
	lastOrigin       string
	lastTxId         int32

	bbQueue    *queue.Queue
	bbInterval time.Duration
	waitGr     *sync.WaitGroup
}

func New(ctx context.Context, cfg *config.Config) (*LogicalBackup, error) {
	var (
		startLSN   uint64
		slotExists bool
		err        error
	)

	pgxConn := cfg.DB
	pgxConn.RuntimeParams = map[string]string{"application_name": applicationName}

	lb := &LogicalBackup{
		ctx:                    ctx,
		connCfg:                pgxConn,
		replMessageWaitTimeout: waitTimeout,
		statusTimeout:          statusTimeout,
		relations:              make(map[message.Identifier]message.Relation),
		relationNames:          make(map[uint32]message.Identifier),
		types:                  make(map[uint32]message.Type),
		backupTables:           make(map[uint32]tablebackup.TableBackuper),
		pluginArgs:             []string{`"proto_version" '1'`, fmt.Sprintf(`"publication_names" '%s'`, cfg.PublicationName)},
		bbQueue:                queue.New(ctx),
		waitGr:                 &sync.WaitGroup{},
		stateFilename:          path.Join(cfg.Dir, "state.yaml"),
		bbInterval:             backupsPeriod,
		cfg:                    cfg,
	}

	conn, err := pgx.Connect(pgxConn)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %v", err)
	}
	defer conn.Close()

	//TODO: have a separate command "init" which will set replica identity and create replication slots/publications
	if err := lb.checkPgParams(conn); err != nil {
		return nil, err
	}

	if err := lb.initPublication(conn); err != nil {
		return nil, err
	}

	slotExists, err = lb.initSlot(conn)
	if err != nil {
		return nil, err
	}

	if err := lb.initTables(conn, cfg.Tables); err != nil {
		return nil, err
	}

	if len(lb.backupTables) == 0 {
		if !lb.cfg.TrackNewTables {
			log.Fatalf("no tables to backup")
		}
	} else {
		tables := make([]string, 0)
		for _, t := range lb.backupTables {
			tables = append(tables, t.String())
		}

		log.Printf("backup tables: %v", strings.Join(tables, ", "))
	}

	if rc, err := pgx.ReplicationConnect(cfg.DB); err != nil {
		return nil, fmt.Errorf("could not replication connect: %v", err)
	} else {
		lb.replConn = rc
	}

	if !slotExists {
		log.Printf("Creating logical replication slot %s", lb.cfg.Slotname)

		startLSN, err := lb.createSlot(conn)
		if err != nil {
			return nil, fmt.Errorf("could not create replication slot: %v", err)
		}
		log.Printf("Created missing replication slot %q, consistent point %s", lb.cfg.Slotname, pgx.FormatLSN(startLSN))

		lb.startLSN = startLSN
		if err := lb.storeRestartLSN(); err != nil {
			log.Printf("could not store current LSN: %v", err)
		}
	} else {
		startLSN, err = lb.readRestartLSN()
		if err != nil {
			return nil, fmt.Errorf("could not read last lsn: %v", err)
		}
		if startLSN != 0 {
			lb.startLSN = startLSN
			lb.storedRestartLSN = startLSN
		}
	}

	if len(cfg.Tables) > 0 {
		//TODO: if list of tables provided, backup only them
		log.Printf("Tables to backup: %s", strings.Join(cfg.Tables, ", "))
	} else {
		log.Printf("Backing up all the publication tables")
	}

	return lb, nil
}

func (b *LogicalBackup) handler(m message.Message) error {
	var err error

	switch v := m.(type) {
	case message.Relation:
		tblName := message.Identifier{Namespace: v.Namespace, Name: v.Name}
		if oldRel, ok := b.relations[tblName]; !ok { // new table or renamed
			if oldTblName, ok := b.relationNames[v.OID]; ok { // renamed table
				log.Printf("table was renamed %s -> %s", oldTblName, tblName)
				delete(b.relations, oldTblName)
				delete(b.relationNames, v.OID)

				bt, ok := b.backupTables[v.OID]
				if !ok {
					log.Printf("table is not tracked %s", tblName)
					// table is not tracked — skip it
					break
				}

				err = bt.SaveDelta(message.DDLMessage{ // alter table %s rename to %s
					TxId:        b.lastTxId,
					LastLSN:     b.flushLSN,
					RelationOID: v.OID,
					Origin:      b.lastOrigin,
					Query:       v.SQL(oldRel),
				})
			} else { // new table
				if _, ok := b.backupTables[v.OID]; !ok { // not tracking
					if b.cfg.TrackNewTables {
						log.Printf("new table %s", tblName)
						if tb, tErr := tablebackup.New(b.ctx, b.cfg.Dir, tblName, b.connCfg, time.Minute, b.bbQueue, b.cfg.DeltasPerFile, b.cfg.BackupThreshold, b.cfg.Fsync); tErr != nil {
							err = fmt.Errorf("could not init tablebackup: %v", tErr)
						} else {
							b.backupTables[v.OID] = tb
						}
					} else {
						log.Printf("skipping new table %s due to trackNewTables = false", tblName)
					}
				}
			}
		} else { // existing table
			if oldRel.OID != v.OID { // dropped and created again
				log.Printf("table was dropped and created again %s", tblName)
				bt, ok := b.backupTables[oldRel.OID]
				if !ok {
					// table is not tracked — skip it
					break
				}
				err = bt.Truncate()
			} else {
				bt, ok := b.backupTables[v.OID]
				if !ok {
					// table is not tracked — skip it
					break
				}

				err = bt.SaveDelta(message.DDLMessage{
					TxId:        b.lastTxId,
					LastLSN:     b.flushLSN,
					RelationOID: v.OID,
					Origin:      b.lastOrigin,
					Query:       v.SQL(oldRel),
				})
			}
		}

		b.relations[tblName] = v
		b.relationNames[v.OID] = tblName
	case message.Insert:
		tbl := b.relationNames[v.RelationOID]

		rel, ok := b.relations[tbl] //TODO: check if key exists
		if !ok {
			return fmt.Errorf("could not find table %v", v.RelationOID)
		}
		if _, ok := b.backupTables[v.RelationOID]; !ok {
			break
		}

		err := b.backupTables[v.RelationOID].SaveDelta(message.DMLMessage{
			TxId:        b.lastTxId,
			LastLSN:     b.flushLSN,
			RelationOID: v.RelationOID,
			Origin:      b.lastOrigin,
			Query:       v.SQL(rel),
		})
		if err != nil {
			return fmt.Errorf("could not save delta: %v", err)
		}
	case message.Update:
		tbl := b.relationNames[v.RelationOID]
		rel, ok := b.relations[tbl] //TODO: check if key exists

		if !ok {
			return fmt.Errorf("could not find table %v", v.RelationOID)
		}
		if _, ok := b.backupTables[v.RelationOID]; !ok {
			break
		}

		err := b.backupTables[v.RelationOID].SaveDelta(message.DMLMessage{
			TxId:        b.lastTxId,
			LastLSN:     b.flushLSN,
			RelationOID: v.RelationOID,
			Origin:      b.lastOrigin,
			Query:       v.SQL(rel),
		})
		if err != nil {
			return fmt.Errorf("could not save delta: %v", err)
		}
	case message.Delete:
		tbl := b.relationNames[v.RelationOID]
		rel, ok := b.relations[tbl] //TODO: check if key exists

		if !ok {
			return fmt.Errorf("could not find table %v", v.RelationOID)
		}
		if _, ok := b.backupTables[v.RelationOID]; !ok {
			break
		}

		err := b.backupTables[v.RelationOID].SaveDelta(message.DMLMessage{
			TxId:        b.lastTxId,
			LastLSN:     b.flushLSN,
			RelationOID: v.RelationOID,
			Origin:      b.lastOrigin,
			Query:       v.SQL(rel),
		})
		if err != nil {
			return fmt.Errorf("could not save delta: %v", err)
		}
	case message.Begin:
		b.lastTxId = v.XID
		b.flushLSN = v.FinalLSN
		b.lastOrigin = ""
	case message.Commit:
		if b.cfg.SendStatusOnCommit {
			if err := b.sendStatus(); err != nil {
				return err
			}
		}
	case message.Origin:
		b.lastOrigin = v.Name
	case message.Truncate:
		//TODO:
	case message.Type:
		if _, ok := b.types[v.ID]; !ok {
			b.types[v.ID] = v
		}
	}

	return err
}

func (b *LogicalBackup) sendStatus() error {
	log.Printf("sending new status with %s flush lsn", pgx.FormatLSN(b.flushLSN))
	status, err := pgx.NewStandbyStatus(b.flushLSN)

	if err != nil {
		return fmt.Errorf("error creating standby status: %s", err)
	}

	if err := b.replConn.SendStandbyStatus(status); err != nil {
		return fmt.Errorf("failed to send standy status: %s", err)
	}

	if b.storedRestartLSN != b.flushLSN {
		if err := b.storeRestartLSN(); err != nil {
			return err
		}
	}

	b.storedRestartLSN = b.flushLSN

	return nil
}

func (b *LogicalBackup) startReplication() error {
	defer b.waitGr.Done()

	log.Printf("Starting from %s lsn", pgx.FormatLSN(b.startLSN))

	err := b.replConn.StartReplication(b.cfg.Slotname, b.startLSN, -1, b.pluginArgs...)
	if err != nil {
		return fmt.Errorf("failed to start replication: %s", err)
	}

	ticker := time.NewTicker(b.statusTimeout)
	for {
		select {
		case <-b.ctx.Done():
			return b.ctx.Err()
		case <-ticker.C:
			if err := b.sendStatus(); err != nil {
				return err
			}
		default:
			wctx, cancel := context.WithTimeout(b.ctx, b.replMessageWaitTimeout)
			repMsg, err := b.replConn.WaitForReplicationMessage(wctx)
			cancel()
			if err == context.DeadlineExceeded {
				continue
			}
			if err != nil {
				return fmt.Errorf("replication failed: %s", err)
			}

			if repMsg == nil {
				log.Printf("receieved null replication message")
				return fmt.Errorf("null replication message")
			}

			if repMsg.WalMessage != nil {
				logmsg, err := decoder.Parse(repMsg.WalMessage.WalData)
				if err != nil {
					return fmt.Errorf("invalid pgoutput message: %s", err)
				}
				if err := b.handler(logmsg); err != nil {
					return fmt.Errorf("error handling waldata: %s", err)
				}
			}
			if repMsg.ServerHeartbeat != nil && repMsg.ServerHeartbeat.ReplyRequested == 1 {
				log.Println("server wants a reply")
				if err := b.sendStatus(); err != nil {
					return err
				}
			}
		}
	}
}

func (b *LogicalBackup) initSlot(conn *pgx.Conn) (bool, error) {
	slotExists := false

	rows, err := conn.Query("select confirmed_flush_lsn, slot_type, database from pg_replication_slots where slot_name = $1;", b.cfg.Slotname)
	if err != nil {
		return false, fmt.Errorf("could not execute query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var lsnString, slotType, database string

		slotExists = true
		if err := rows.Scan(&lsnString, &slotType, &database); err != nil {
			return false, fmt.Errorf("could not scan lsn: %v", err)
		}

		if slotType != logicalSlotType {
			return false, fmt.Errorf("slot %q is not a logical slot", b.cfg.Slotname)
		}

		if database != b.connCfg.Database {
			return false, fmt.Errorf("replication slot %q belongs to %q database", b.cfg.Slotname, database)
		}

		if lsn, err := pgx.ParseLSN(lsnString); err != nil {
			return false, fmt.Errorf("could not parse lsn: %v", err)
		} else {
			b.startLSN = lsn
		}
	}

	return slotExists, nil
}

func (b *LogicalBackup) createSlot(conn *pgx.Conn) (uint64, error) {
	var strLSN sql.NullString
	row := conn.QueryRowEx(b.ctx, "select lsn from pg_create_logical_replication_slot($1, $2)", nil, b.cfg.Slotname, outputPlugin)

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

	return lsn, nil
}

// Wait for the goroutines to finish
func (b *LogicalBackup) Wait() {
	b.waitGr.Wait()
}

func (b *LogicalBackup) initTables(conn *pgx.Conn, tables []string) error {
	query := `select c.oid, n.nspname, c.relname
     from pg_class c
     inner join pg_namespace n on (n.oid = c.relnamespace)
     inner join pg_get_publication_tables('%s') x on x.relid = c.oid;`

	if len(tables) > 0 {
		tbls := make([]string, 0)
		for _, t := range tables {
			tbls = append(tbls, fmt.Sprintf("'%s'", t))
		}

		query += " where n.nspname || '.' || c.relname in (" + strings.Join(tbls, ", ") + ")"
	}
	query = fmt.Sprintf(query, b.cfg.PublicationName)

	rows, err := conn.Query(query)
	if err != nil {
		return fmt.Errorf("could not execute query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			t   message.Identifier
			oid uint32
		)

		if err := rows.Scan(&oid, &t.Namespace, &t.Name); err != nil {
			return fmt.Errorf("could not scan: %v", err)
		}

		tb, err := tablebackup.New(b.ctx,
			b.cfg.Dir,
			t,
			b.connCfg,
			time.Minute,
			b.bbQueue,
			b.cfg.DeltasPerFile,
			b.cfg.BackupThreshold,
			b.cfg.Fsync)

		if err != nil {
			return fmt.Errorf("could not create tablebackup instance: %v", err)
		}

		b.backupTables[oid] = tb
	}

	return nil
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

func (b *LogicalBackup) readRestartLSN() (uint64, error) {
	var (
		ts     time.Time
		LSNstr string
	)

	if _, err := os.Stat(b.stateFilename); os.IsNotExist(err) {
		return 0, nil
	}

	fp, err := os.OpenFile(b.stateFilename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return 0, fmt.Errorf("could not create current lsn file: %v", err)
	}
	defer fp.Close()

	yaml.NewDecoder(fp).Decode(&struct {
		Timestamp  *time.Time
		CurrentLSN *string
	}{&ts, &LSNstr})

	currentLSN, err := pgx.ParseLSN(LSNstr)
	if err != nil {
		return 0, fmt.Errorf("could not parse %q LSN string: %v", LSNstr, err)
	}

	return currentLSN, nil
}

func (b *LogicalBackup) storeRestartLSN() error {
	fp, err := os.OpenFile(b.stateFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not create current lsn file: %v", err)
	}
	defer fp.Close()

	err = yaml.NewEncoder(fp).Encode(struct {
		Timestamp  time.Time
		CurrentLSN string
	}{time.Now(), pgx.FormatLSN(b.flushLSN)})
	if err != nil {
		return fmt.Errorf("could not save current lsn: %v", err)
	}
	fp.Sync()

	return nil
}

func (b *LogicalBackup) checkPgParams(conn *pgx.Conn) error {
	tables := make([]string, 0)

	rows, err := conn.Query(`select quote_ident(t.nspname) || '.' || quote_ident(t.relname)
		from (
			select c.oid, c.relname, n.nspname 
			from pg_class c 
			inner join pg_namespace n on n.oid = c.relnamespace
			where c.relkind = 'r' 
				and n.nspname not in ('pg_catalog', 'information_schema')
				and c.relreplident in ('d', 'n')
		) as t left outer join pg_constraint c on c.contype = 'p' and c.conrelid = t.oid
		where c.conname is null 
		order by t.oid;`)
	if err != nil {
		return fmt.Errorf("could not execute query: %v", err)
	}

	for rows.Next() {
		var quotedTable string

		if err := rows.Scan(&quotedTable); err != nil {
			rows.Close()
			return fmt.Errorf("could not scan: %v", err)
		}

		tables = append(tables, quotedTable)
	}
	rows.Close()

	for _, t := range tables {
		if _, err := conn.Exec(fmt.Sprintf("alter table only %s replica identity full", t)); err != nil {
			return fmt.Errorf("could not set replica identity for table %s: %v", t, err)
		}

		log.Printf("set replica identity for table %s to full", t)
	}

	//TODO: check stuff like wal_level = logical, max_replication_slots, max_wal_senders
	//TODO: for the tables without PK alter table to use replica identity full
	return nil
}

func (b *LogicalBackup) BackgroundBasebackuper() {
	defer b.waitGr.Done()

	for {
		obj, err := b.bbQueue.Get()
		if err == context.Canceled {
			return
		}

		if err := obj.(tablebackup.TableBackuper).Basebackup(); err != nil && err != context.Canceled {
			log.Printf("could not basebackup %s: %v", obj.(tablebackup.TableBackuper), err)
		}
	}
}

func (b *LogicalBackup) queueBbTables() {
	for _, t := range b.backupTables {
		b.bbQueue.Put(t)
	}
}

func (b *LogicalBackup) Run() {
	b.waitGr.Add(1)
	go b.startReplication()

	log.Printf("Starting %d background backupers", b.cfg.ConcurrentBasebackups)
	for i := 0; i < b.cfg.ConcurrentBasebackups; i++ {
		b.waitGr.Add(1)
		go b.BackgroundBasebackuper()
	}

	if b.cfg.InitialBasebackup {
		log.Printf("Queueing tables for the initial backup")
		b.queueBbTables()
	}
}

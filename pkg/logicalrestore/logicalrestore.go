package logicalrestore

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/basebackup/bbtable"
	"github.com/ikitiki/logical_backup/pkg/deltas"
	"github.com/ikitiki/logical_backup/pkg/logicalbackup"
	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/utils"
	"github.com/ikitiki/logical_backup/pkg/utils/dbutils"
	"github.com/ikitiki/logical_backup/pkg/utils/deltafiles"
	"github.com/ikitiki/logical_backup/pkg/utils/namehistory"
)

type logicalRestore struct {
	message.NamespacedName

	curLSN   dbutils.LSN
	startLSN dbutils.LSN
	relInfo  message.Relation

	conn *pgx.Conn
	tx   *pgx.Tx
	cfg  pgx.ConnConfig
	ctx  context.Context

	baseDir  string
	tableOID dbutils.OID

	truncate bool
}

// New instantiates logical restore
func New(ctx context.Context, tbl message.NamespacedName, dir string, truncate bool, cfg pgx.ConnConfig) *logicalRestore {
	return &logicalRestore{
		ctx:            ctx,
		baseDir:        dir,
		cfg:            cfg,
		NamespacedName: tbl,
		truncate:       truncate,
	}
}

func (r *logicalRestore) connect() error {
	conn, err := pgx.Connect(r.cfg)
	if err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}

	r.conn = conn

	return nil
}

func (r *logicalRestore) disconnect() error {
	return r.conn.Close()
}

func (r *logicalRestore) begin() error {
	if r.tx != nil {
		return fmt.Errorf("there is already a transaction in progress")
	}
	if r.conn == nil {
		return fmt.Errorf("no postgresql connection")
	}

	tx, err := r.conn.Begin()
	if err != nil {
		return fmt.Errorf("could not begin tx: %v", err)
	}

	r.tx = tx

	return nil
}

func (r *logicalRestore) commit() error {
	if r.tx == nil {
		return fmt.Errorf("no running transaction")
	}

	if err := r.tx.Commit(); err != nil {
		return err
	}

	r.tx = nil
	return nil
}

func (r *logicalRestore) rollback() error {
	if r.tx == nil {
		return fmt.Errorf("no running transaction")
	}

	if err := r.tx.Rollback(); err != nil {
		return err
	}

	r.tx = nil
	return nil
}

func (r *logicalRestore) loadInfo() error {
	var info message.DumpInfo

	infoFilename := path.Join(r.baseDir, utils.TableDir(r.tableOID), bbtable.BasebackupInfoFilename)

	fp, err := os.OpenFile(infoFilename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	if err := yaml.NewDecoder(fp).Decode(&info); err != nil {
		return fmt.Errorf("could not load dump info: %v", err)
	}

	r.relInfo = info.Relation
	r.startLSN = info.StartLSN

	return nil
}

func (r *logicalRestore) loadDump() error {
	dumpFilename := path.Join(r.baseDir, utils.TableDir(r.tableOID), bbtable.BasebackupFilename)

	if _, err := os.Stat(dumpFilename); os.IsNotExist(err) {
		log.Printf("dump file doesn't exist, skipping")

		return nil
	}

	if err := r.begin(); err != nil {
		return err
	}

	fp, err := os.OpenFile(dumpFilename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	if err := r.conn.CopyFromReader(fp, fmt.Sprintf("copy %s from stdin", r.Sanitize())); err != nil {
		return fmt.Errorf("could not copy: %v", err)
	} else {
		log.Printf("initial dump loaded")
	}

	if err := r.commit(); err != nil {
		return err
	}

	return nil
}

func (r *logicalRestore) applySegmentFile(filename string) error {
	deltaCollector := deltas.New(path.Join(r.baseDir, utils.TableDir(r.tableOID)), false)
	if err := deltaCollector.Load(filename); err != nil {
		return fmt.Errorf("could not load file: %v", err)
	}

	log.Printf("messages in the file: %v", deltaCollector.MessageCnt())
	for {
		msg, err := deltaCollector.GetMessage()
		if err == io.EOF {
			break
		}

		if _, err := r.applyMessage(msg); err != nil {
			return fmt.Errorf("could not apply message: %v", err)
		}
	}

	return nil
}

func (r *logicalRestore) execSQL(sql string) error {
	if sql == "" {
		return nil
	}

	if _, err := r.tx.ExecEx(r.ctx, sql, &pgx.QueryExOptions{SimpleProtocol: true}); err != nil {
		return fmt.Errorf("%v (%q)", err, sql)
	}

	return nil
}

func (r *logicalRestore) skipLSN() bool {
	return r.curLSN.IsValid() && r.curLSN <= r.startLSN
}

func (r *logicalRestore) applyMessage(msg message.Message) (sql string, err error) {
	if _, ok := msg.(message.Begin); !ok && r.skipLSN() {
		return
	}

	switch v := msg.(type) {
	case message.Begin:
		r.curLSN = v.FinalLSN
		if r.skipLSN() {
			return
		}

		r.tx, err = r.conn.Begin()
		if err != nil {
			err = fmt.Errorf("could not begin: %v", err)
			return
		}
	case message.Commit:
		if r.tx == nil {
			break
		}

		if err = r.tx.Commit(); err != nil {
			err = fmt.Errorf("could not commit: %v", err)
			return
		}
		r.tx = nil
	case message.Origin:
	case message.Type:
	case message.Insert:
		sql = v.SQL(r.relInfo)
	case message.Update:
		sql = v.SQL(r.relInfo)
	case message.Delete:
		sql = v.SQL(r.relInfo)
	default:
		panic(fmt.Sprintf("unknown message type: %T", msg))
	}

	err = r.execSQL(sql)

	return
}

func (r *logicalRestore) applyDeltas() error {
	deltaDir := path.Join(r.baseDir, utils.TableDir(r.tableOID), deltas.DirName)

	fileList, err := ioutil.ReadDir(deltaDir)
	if err != nil {
		return fmt.Errorf("could not read directory: %v", err)
	}

	deltaFiles := make(deltafiles.DeltaFiles, 0)
	for _, v := range fileList {
		deltaFiles = append(deltaFiles, v.Name())
	}

	if len(deltaFiles) == 0 {
		log.Printf("no delta files")
		return nil
	}

	sort.Sort(deltaFiles)

	for _, filename := range deltaFiles {
		log.Printf("applying delta: %v", filename)
		if err := r.applySegmentFile(filename); err != nil {
			return fmt.Errorf("could not apply deltas from %q file: %v", filename, err)
		}
	}

	return nil
}

func (r *logicalRestore) setTableOID() error {
	tableNames := namehistory.New(path.Join(r.baseDir, logicalbackup.OidNameMapFile))
	if err := tableNames.Load(); err != nil {
		return err
	}

	oid, _ := tableNames.GetOID(r.NamespacedName, dbutils.InvalidLSN)
	if oid == dbutils.InvalidOID {
		return fmt.Errorf("could not find table")
	}
	r.tableOID = oid

	return nil
}

func (r *logicalRestore) disableConstraints() error {
	//TODO
	return nil
}

func (r *logicalRestore) enableConstraints() error {
	//TODO
	return nil
}

func (r *logicalRestore) checkTableStruct() error {
	var rel message.Relation

	if err := rel.FetchByName(r.conn, r.NamespacedName); err != nil {
		return fmt.Errorf("could not fetch table info from the db: %v", err)
	}

	rel.OID = r.tableOID // fake that we have same oid (in fact we might not)

	if !r.relInfo.Equals(&rel) {
		log.Printf("src table: %#v", r.relInfo)
		log.Printf("dst table: %#v", rel)
		return fmt.Errorf("tables are different")
	}

	return nil
}

func (r *logicalRestore) truncateTable() error {
	if _, err := r.conn.Exec(fmt.Sprintf("truncate %s", r.Sanitize())); err != nil {
		return err
	}

	return nil
}

// Restore restores the table
func (r *logicalRestore) Restore() error {
	if err := r.setTableOID(); err != nil {
		return fmt.Errorf("could not load table names: %v", err)
	}

	if err := r.loadInfo(); err != nil {
		return fmt.Errorf("could not load dump info: %v", err)
	}

	if err := r.connect(); err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}
	defer func() {
		if err := r.disconnect(); err != nil {
			log.Printf("could not disconnect: %v", err)
		}
	}()

	if err := r.checkTableStruct(); err != nil {
		return fmt.Errorf("table struct error: %v", err)
	}

	if err := r.disableConstraints(); err != nil {
		return fmt.Errorf("could not disable constraints: %v", err)
	}

	if r.truncate {
		if err := r.truncateTable(); err != nil {
			return fmt.Errorf("could not truncate table: %v", err)
		}

		log.Printf("table %s truncated", r)
	}

	log.Printf("start lsn: %v", r.startLSN)

	if err := r.loadDump(); err != nil {
		return fmt.Errorf("could not load dump: %v", err)
	}

	if err := r.applyDeltas(); err != nil {
		return fmt.Errorf("could not apply deltas: %v", err)
	}

	if err := r.enableConstraints(); err != nil {
		return fmt.Errorf("could not enable constraints: %v", err)
	}

	return nil
}

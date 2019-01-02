package logicalrestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"reflect"
	"sort"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/decoder"
	"github.com/ikitiki/logical_backup/pkg/logicalbackup"
	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/tablebackup"
	"github.com/ikitiki/logical_backup/pkg/utils"
)

type LogicalRestorer interface {
	Restore(schemaName, tableName string) error
}

type LogicalRestore struct {
	message.NamespacedName

	startLSN dbutils.LSN
	relInfo  message.Relation

	conn *pgx.Conn
	tx   *pgx.Tx
	cfg  pgx.ConnConfig
	ctx  context.Context

	baseDir string
	tblOid  dbutils.OID
	curTx   int32
	curLsn  dbutils.LSN
}

func New(tbl message.NamespacedName, dir string, cfg pgx.ConnConfig) *LogicalRestore {
	return &LogicalRestore{
		ctx:            context.Background(),
		baseDir:        dir,
		cfg:            cfg,
		NamespacedName: tbl,
	}
}

func (r *LogicalRestore) connect() error {
	conn, err := pgx.Connect(r.cfg)
	if err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}

	r.conn = conn

	return nil
}

func (r *LogicalRestore) disconnect() error {
	return r.conn.Close()
}

func (r *LogicalRestore) begin() error {
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

func (r *LogicalRestore) commit() error {
	if r.tx == nil {
		return fmt.Errorf("no running transaction")
	}

	if err := r.tx.Commit(); err != nil {
		return err
	}

	r.tx = nil
	return nil
}

func (r *LogicalRestore) rollback() error {
	if r.tx == nil {
		return fmt.Errorf("no running transaction")
	}

	if err := r.tx.Rollback(); err != nil {
		return err
	}

	r.tx = nil
	return nil
}

func (r *LogicalRestore) loadInfo() error {
	var info message.DumpInfo

	infoFilename := path.Join(r.baseDir, utils.TableDir(r.tblOid), tablebackup.BaseBackupStateFileName)

	fp, err := os.OpenFile(infoFilename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	if err := yaml.NewDecoder(fp).Decode(&info); err != nil {
		return fmt.Errorf("could not load dump info: %v", err)
	}

	if lsn, err := pgx.ParseLSN(info.StartLSN); err != nil {
		return fmt.Errorf("could not parse lsn: %v", err)
	} else {
		r.startLSN = dbutils.LSN(lsn) //TODO: move to pgx.LSN
	}

	r.relInfo = info.Relation

	return nil
}

func (r *LogicalRestore) loadDump() error {
	dumpFilename := path.Join(r.baseDir, utils.TableDir(r.tblOid), tablebackup.BasebackupFilename)

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

func (r *LogicalRestore) applySegmentFile(filePath string) error {
	log.Printf("reading %q segment file", filePath)

	fp, err := os.OpenFile(filePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	lnBuf := make([]byte, 8)
	for {
		if n, err := fp.Read(lnBuf); err == io.EOF && n == 0 {
			break
		} else if err != nil || n != 8 {
			return fmt.Errorf("could not read: %v", err)
		}

		ln := binary.BigEndian.Uint64(lnBuf) - 8
		dataBuf := make([]byte, ln)

		if n, err := fp.Read(dataBuf); err == io.EOF && n == 0 {
			break
		} else if err != nil || uint64(n) != ln {
			return fmt.Errorf("could not read %d bytes: %v", ln, err)
		}

		msg, err := decoder.Parse(dataBuf)
		if err != nil {
			return fmt.Errorf("could not parse message: %v", err)
		}

		printMessage(msg, r.curLsn)

		switch v := msg.(type) {
		case message.Relation:
			r.relInfo = v
		case message.Begin:
			if r.tx != nil {
				panic("there is tx already open")
			}

			if v.FinalLSN < r.startLSN {
				log.Printf("reading transaction begin for commit LSN %s smaller than start LSN %s", v.FinalLSN, r.startLSN)
				r.curLsn = dbutils.InvalidLSN
				r.curTx = 0
				break
			}

			r.curTx = v.XID
			r.curLsn = v.FinalLSN

			if err := r.begin(); err != nil {
				return fmt.Errorf("could not begin transaction: %v", err)
			}
		case message.Commit:
			if v.LSN < r.startLSN {
				r.curLsn = dbutils.InvalidLSN
				r.curTx = 0
				break
			}

			if r.curLsn != dbutils.InvalidLSN && r.curLsn != v.LSN {
				panic(fmt.Sprintf("final LSN(%s) does not match begin LSN(%s)", v.LSN, r.curLsn))
			}

			if err := r.commit(); err != nil {
				return fmt.Errorf("could not commit transaction: %v", err)
			}
		case message.Origin:
			//TODO
		case message.Truncate:
			//TODO
		case message.Type:
			//TODO
		default:
			if r.tx == nil {
				break
			}

			if err := r.applyDMLMessage(msg); err == nil {
				break
			} else {
				log.Printf("could not apply delta message: %v", err)
			}

			if r.tx != nil {
				if err2 := r.rollback(); err2 != nil {
					return fmt.Errorf("could not rollback transaction: %v", err2)
				}
			}

			return fmt.Errorf("could not process message: %v", err)
		}
	}

	return nil
}

func printMessage(msg message.Message, currentLSN dbutils.LSN) {
	log.Printf("restored %T with LSN %s", msg, currentLSN)
}

func (r *LogicalRestore) execSQL(sql string) error {
	if sql == "" {
		return nil
	}

	_, err := r.tx.ExecEx(r.ctx, sql, &pgx.QueryExOptions{SimpleProtocol: true})

	return err
}

func (r *LogicalRestore) applyDMLMessage(msg message.Message) (err error) {
	switch v := msg.(type) {
	case message.Insert:
		err = r.execSQL(v.SQL(r.relInfo))
	case message.Update:
		err = r.execSQL(v.SQL(r.relInfo))
	case message.Delete:
		err = r.execSQL(v.SQL(r.relInfo))
	default:
		panic("unreachable")
	}

	return
}

func (r *LogicalRestore) applyDeltas() error {
	deltaDir := path.Join(r.baseDir, utils.TableDir(r.tblOid), tablebackup.DeltasDirName)

	fileList, err := ioutil.ReadDir(deltaDir)
	if err != nil {
		return fmt.Errorf("could not read directory: %v", err)
	}

	segmentFiles := make(segments, 0)
	for _, v := range fileList {
		segmentFiles = append(segmentFiles, v.Name())
	}

	if len(segmentFiles) == 0 {
		log.Printf("no delta files")
		return nil
	}

	sort.Sort(segmentFiles)

	for _, filename := range segmentFiles {
		if err := r.applySegmentFile(path.Join(deltaDir, filename)); err != nil {
			return fmt.Errorf("could not apply deltas from %q file: %v", filename, err)
		}
	}

	return nil
}

func (r *LogicalRestore) checkTableStruct() error {
	if err := r.begin(); err != nil {
		return err
	}

	relationInfo, err := tablebackup.FetchRelationInfo(r.tx, r.NamespacedName)
	if err != nil {
		return fmt.Errorf("could not fetch table info: %v", err)
	}

	if !reflect.DeepEqual(relationInfo.Columns, r.relInfo.Columns) {
		return fmt.Errorf("table structs do not match: \n%#v\n%#v", relationInfo.Structure(), r.relInfo.Structure())
	}

	if err := r.commit(); err != nil {
		return err
	}

	return nil
}

func (r *LogicalRestore) getOID(tblName message.NamespacedName) (dbutils.OID, error) {
	history := make(map[dbutils.OID][]logicalbackup.NameAtLSN)

	oidMapFilename := path.Join(r.baseDir, logicalbackup.OidNameMapFile)

	fp, err := os.OpenFile(oidMapFilename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return dbutils.InvalidOID, fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	if err = yaml.NewDecoder(fp).Decode(&history); err != nil {
		return dbutils.InvalidOID, fmt.Errorf("could not decode yaml: %v", err)
	}

	maxLsn := dbutils.InvalidLSN
	lastOid := dbutils.InvalidOID
	for oid, names := range history {
		for _, n := range names {
			if n.Name != tblName {
				continue
			}

			if n.Lsn > r.startLSN {
				continue
			}

			if n.Lsn > maxLsn || maxLsn == 0 {
				maxLsn = n.Lsn
				lastOid = oid
			}
		}
	}

	return lastOid, nil
}

func (r *LogicalRestore) disableConstraints() error {
	//TODO
	return nil
}

func (r *LogicalRestore) enableConstraints() error {
	//TODO
	return nil
}

func (r *LogicalRestore) Restore() error {
	tblOid, err := r.getOID(r.NamespacedName)
	if err != nil {
		return fmt.Errorf("could not get oid of the table: %v", err)
	}
	if tblOid == dbutils.InvalidOID {
		return fmt.Errorf("could not find oid for the table, table does not exist?")
	}

	r.tblOid = tblOid

	if err := r.loadInfo(); err != nil {
		return fmt.Errorf("could not load dump info: %v", err)
	}

	if err := r.connect(); err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}
	defer r.disconnect()

	if err := r.checkTableStruct(); err != nil {
		return fmt.Errorf("table struct error: %v", err)
	}

	if err := r.disableConstraints(); err != nil {
		return fmt.Errorf("could not disable constraints: %v", err)
	}

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

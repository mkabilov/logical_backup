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
	"strconv"
	"strings"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/decoder"
	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/tablebackup"
	"github.com/ikitiki/logical_backup/pkg/utils"
)

type deltas []string

func (d deltas) Len() int {
	return len(d)
}

func (d deltas) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d deltas) Less(i, j int) bool {
	var parts1, parts2 []string

	if strings.Contains(d[i], ".") {
		parts1 = strings.Split(d[i], ".")
	} else {
		parts1 = []string{d[i], "0"}
	}

	if strings.Contains(d[j], ".") {
		parts2 = strings.Split(d[j], ".")
	} else {
		parts2 = []string{d[j], "0"}
	}

	if parts1[0] != parts2[0] {
		return parts1[0] < parts2[0]
	}

	i1, err := strconv.ParseInt(parts1[1], 16, 32)
	if err != nil {
		panic(err)
	}

	i2, err := strconv.ParseInt(parts2[1], 16, 32)
	if err != nil {
		panic(err)
	}

	return i1 < i2
}

type LogicalRestorer interface {
	Restore(schemaName, tableName string) error
}

type LogicalRestore struct {
	message.NamespacedName

	startLSN    uint64
	columnNames []string
	relInfo     message.Relation

	conn *pgx.Conn
	tx   *pgx.Tx
	cfg  pgx.ConnConfig
	ctx  context.Context

	baseDir string
}

func New(schemaName, tableName, dir string, cfg pgx.ConnConfig) *LogicalRestore {
	return &LogicalRestore{
		ctx:            context.Background(),
		baseDir:        dir,
		cfg:            cfg,
		NamespacedName: message.NamespacedName{Namespace: schemaName, Name: tableName},
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

func (r *LogicalRestore) infoFilepath() string {
	return path.Join(r.baseDir, utils.TableDir(r.NamespacedName), "info.yaml")
}

func (r *LogicalRestore) dumpFilepath() string {
	return path.Join(r.baseDir, utils.TableDir(r.NamespacedName), "basebackup.copy")
}

func (r *LogicalRestore) deltaDir() string {
	return path.Join(r.baseDir, utils.TableDir(r.NamespacedName), "deltas")
}

func (r *LogicalRestore) loadInfo() error {
	var info message.DumpInfo
	fp, err := os.OpenFile(r.infoFilepath(), os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	if err := yaml.NewDecoder(fp).Decode(&info); err != nil {
		return fmt.Errorf("could not load dump info: %v", err)
	}

	lsn, err := pgx.ParseLSN(info.StartLSN)
	if err != nil {
		return fmt.Errorf("could not parse lsn: %v", err)
	}
	r.startLSN = lsn

	r.columnNames = make([]string, 0)
	for _, c := range info.Relation.Columns {
		r.columnNames = append(r.columnNames, c.Name)
	}
	r.relInfo = info.Relation

	return nil
}

func (r *LogicalRestore) loadDump() error {
	fp, err := os.OpenFile(r.dumpFilepath(), os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	if err := r.conn.CopyFromReader(fp, fmt.Sprintf("copy %s from stdin", r.NamespacedName.Sanitize())); err != nil {
		return fmt.Errorf("could not copy: %v", err)
	}

	return nil
}

func (r *LogicalRestore) applyDelta(filePath string) error {
	var (
		sql   string
		curTx int32
	)
	log.Printf("reading %q delta file", filePath)

	fp, err := os.OpenFile(filePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	lnBuf := make([]byte, 8)
	for {
		if n, err := fp.Read(lnBuf); err == io.EOF {
			break
		} else if err != nil || n != 8 {
			return fmt.Errorf("could not read: %v", err)
		}

		ln := binary.BigEndian.Uint64(lnBuf) - 8
		dataBuf := make([]byte, ln)

		if n, err := fp.Read(dataBuf); err == io.EOF {
			break
		} else if err != nil || uint64(n) != ln {
			return fmt.Errorf("could not read %d bytes: %v", ln, err)
		}

		msg, err := decoder.Parse(dataBuf)
		if err != nil {
			return fmt.Errorf("could not parse message: %v", err)
			break
		}

		switch v := msg.(type) {
		case message.Relation:
			//TODO: mind rename table case
			if r.relInfo.Name != v.Name || r.relInfo.Namespace != v.Namespace {
				log.Printf("message does not belong to current relation")
			}
			sql = v.SQL(r.relInfo)
			r.relInfo.Columns = v.Columns
			r.relInfo.ReplicaIdentity = v.ReplicaIdentity
		case message.Insert:
			sql = v.SQL(r.relInfo)
		case message.Update:
			sql = v.SQL(r.relInfo)
		case message.Delete:
			sql = v.SQL(r.relInfo)
		case message.Begin:
			curTx = v.XID
			sql = fmt.Sprintf("BEGIN -- %d", curTx)
		case message.Commit:
			sql = fmt.Sprintf("COMMIT -- %d", curTx)
		case message.Origin:
			//TODO
		case message.Truncate:
			//TODO
		case message.Type:
			//TODO
		}

		log.Println(sql)
	}

	return nil
}

func (r *LogicalRestore) applyDeltas() error {
	deltaFiles := make(deltas, 0)
	fileList, err := ioutil.ReadDir(r.deltaDir())
	if err != nil {
		return fmt.Errorf("could not read directory: %v", err)
	}
	for _, v := range fileList {
		deltaFiles = append(deltaFiles, v.Name())
	}

	sort.Sort(deltaFiles)

	for _, deltaFile := range deltaFiles {
		if err := r.applyDelta(path.Join(r.deltaDir(), deltaFile)); err != nil {
			return fmt.Errorf("could not apply %q delta file: %v", deltaFile, err)
		}
	}

	return nil
}

func (r *LogicalRestore) checkTableStruct() error {
	relationInfo, err := tablebackup.FetchRelationInfo(r.tx, r.NamespacedName)
	if err != nil {
		return fmt.Errorf("could not fetch table info: %v", err)
	}

	if !reflect.DeepEqual(relationInfo.Columns, r.relInfo.Columns) {
		return fmt.Errorf("table structs do not match: \n%#v\n%#v", relationInfo.Columns, r.relInfo.Columns)
	}

	return nil
}

func (r *LogicalRestore) Restore() error {
	if err := r.connect(); err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}

	if err := r.loadInfo(); err != nil {
		return fmt.Errorf("could not load dump info: %v", err)
	}

	if err := r.begin(); err != nil {
		return fmt.Errorf("could not start transaction: %v", err)
	}

	if err := r.checkTableStruct(); err != nil {
		return fmt.Errorf("table struct error: %v", err)
	}

	if err := r.loadDump(); err != nil {
		return fmt.Errorf("could not load dump: %v", err)
	}

	if err := r.applyDeltas(); err != nil {
		return fmt.Errorf("could not apply deltas: %v", err)
	}

	if err := r.commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %v", err)
	}

	return nil
}

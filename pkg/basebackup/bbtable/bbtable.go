package bbtable

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

	"github.com/mkabilov/logical_backup/pkg/message"
	"github.com/mkabilov/logical_backup/pkg/tablebackup"
	"github.com/mkabilov/logical_backup/pkg/utils/dbutils"
)

const (
	errCodeTableNotFound = "42P01"

	//BasebackupFilename represents file name of the copy dump file
	BasebackupFilename = "basebackup.copy"

	//BasebackupInfoFilename represents file name of the table info file
	BasebackupInfoFilename = "basebackup_info.yaml"
)

// TableBasebackuper represents interface for basebackup table
type TableBasebackuper interface {
	Basebackup() error
	DumpFilename() string
	InfoFilename() string
	Lsn() dbutils.LSN
}

type tableBasebackup struct {
	message.DumpInfo

	conn *pgx.Conn

	table tablebackup.TableBackuper
	tx    *pgx.Tx
	dbCfg pgx.ConnConfig
	dir   string

	infoFilename string
	dumpFilename string
}

var (
	// ErrTableNotFound represents table not found error
	ErrTableNotFound = errors.New("table not found")
)

// New instantiates basebackup table
func New(dbCfg pgx.ConnConfig, table tablebackup.TableBackuper) *tableBasebackup {
	return &tableBasebackup{
		table: table,
		dbCfg: dbCfg.Merge(pgx.ConnConfig{
			RuntimeParams:        map[string]string{"replication": "database"},
			PreferSimpleProtocol: true}),
		dir: table.TableDirectory(),
	}
}

// InfoFilename return path to the info file
func (t *tableBasebackup) InfoFilename() string {
	return t.infoFilename
}

// DumpFilename returns path to the copy dump file
func (t *tableBasebackup) DumpFilename() string {
	return t.dumpFilename
}

// Lsn returns basebackup LSN
func (t *tableBasebackup) Lsn() dbutils.LSN {
	return t.StartLSN
}

func (t *tableBasebackup) hasRows() (bool, error) {
	var hasRows bool

	row := t.tx.QueryRow(fmt.Sprintf("select exists(select 1 from %s)", t.table.Name().Sanitize()))
	err := row.Scan(&hasRows)

	return hasRows, err
}

func (t *tableBasebackup) fetchRelationInfo() error {
	t.Relation = t.table.RelationMessage()
	if t.Relation.OID != dbutils.InvalidOID {
		return nil
	}

	if err := t.Relation.FetchByOID(t.conn, t.table.OID()); err != nil {
		return fmt.Errorf("could not fetch relation from db: %v", err)
	}

	return nil
}

func (t *tableBasebackup) copyDump(filename string) error {
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	if err := t.tx.CopyToWriter(fp, fmt.Sprintf("copy %s to stdout", t.table.Name().Sanitize())); err != nil {
		return fmt.Errorf("could not copy: %v", err)
	}

	return nil
}

func (t *tableBasebackup) createTempReplicationSlot(slotName string) error {
	var (
		createdSlotName, basebackupLSN, snapshotName, plugin sql.NullString

		lsn dbutils.LSN
	)

	if t.tx == nil {
		return fmt.Errorf("no running transaction")
	}

	// we don't need to accumulate WAL on the server throughout the whole COPY running time. We need the slot
	// exclusively to figure out our transaction LSN, why not drop it once we have got it?
	row := t.tx.QueryRow(fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s USE_SNAPSHOT",
		slotName, dbutils.OutputPlugin))

	if err := row.Scan(&createdSlotName, &basebackupLSN, &snapshotName, &plugin); err != nil {
		return fmt.Errorf("could not scan: %v", err)
	}

	defer func() {
		if _, err := t.tx.Exec(fmt.Sprintf("DROP_REPLICATION_SLOT %s", slotName)); err != nil {
			log.Printf("could not drop replication slot %s right away: %v, it will be dropped at the end of the backup",
				slotName, err)
		}
	}()

	if !basebackupLSN.Valid {
		return fmt.Errorf("null consistent point")
	}

	if err := lsn.Parse(basebackupLSN.String); err != nil {
		return fmt.Errorf("could not parse LSN: %v", err)
	}
	if lsn == 0 {
		return fmt.Errorf("no consistent starting point for a dump")
	}

	t.StartLSN = lsn

	return nil
}

func (t *tableBasebackup) lockTable() error {
	query := fmt.Sprintf("LOCK TABLE %s IN ACCESS SHARE MODE", t.table.Name().Sanitize())
	if _, err := t.tx.Exec(query); err != nil {
		// check if the error comes from the server
		if e, ok := err.(pgx.PgError); ok && e.Code == errCodeTableNotFound {
			return ErrTableNotFound
		}

		return err
	}

	return nil
}

func (t *tableBasebackup) rollback() {
	if err := t.tx.Rollback(); err != nil {
		log.Printf("could not rollback: %v", err)
	}
}

func (t *tableBasebackup) connect() error {
	var err error

	t.conn, err = pgx.Connect(t.dbCfg)
	if err != nil {
		return fmt.Errorf("could not connect to db: %v", err)
	}

	connInfo, err := initPostgresql(t.conn)
	if err != nil {
		return fmt.Errorf("could not fetch conn info: %v", err)
	}
	t.conn.ConnInfo = connInfo

	return nil
}

// Basebackup performs base backup of the table
func (t *tableBasebackup) Basebackup() error {
	if err := t.connect(); err != nil {
		return fmt.Errorf("could not connect to db: %v", err)
	}

	tempInfoFilepath := path.Join(t.dir, BasebackupInfoFilename+".new")
	if _, err := os.Stat(tempInfoFilepath); !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("could not stat %q: %v", tempInfoFilepath, err)
		}

		if err := os.Remove(tempInfoFilepath); err != nil {
			log.Printf("could not delete old temp info file: %v", err)
		}
	}

	infoFp, err := os.OpenFile(tempInfoFilepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not create info file: %v", err)
	}
	defer func() {
		infoFp.Close()
		os.Remove(tempInfoFilepath)
	}()

	tempDumpFilepath := path.Join(t.dir, BasebackupFilename+".new")
	if _, err := os.Stat(tempDumpFilepath); !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("could not stat %q: %v", tempDumpFilepath, err)
		}

		if err := os.Remove(tempDumpFilepath); err != nil {
			log.Printf("could not delete old temp info file: %v", err)
		}
	}
	defer os.Remove(tempDumpFilepath)

	tx, err := t.conn.BeginEx(context.Background(), &pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("could not start transaction: %v", err)
	}
	t.tx = tx

	if err := t.createTempReplicationSlot(fmt.Sprintf("tempslot_%d", t.conn.PID())); err != nil {
		return fmt.Errorf("could not create temp replication slot: %v", err)
	}
	if _, err := t.hasRows(); err != nil {
		return fmt.Errorf("could not check if table has rows: %v", err)
	}

	t.CreateDate = time.Now()

	if err := t.lockTable(); err != nil {
		if err == ErrTableNotFound {
			return err
		}

		t.rollback()

		return fmt.Errorf("could not lock table: %v", err)
	}

	if err := t.fetchRelationInfo(); err != nil {
		t.rollback()
		return fmt.Errorf("could not fetch relation info: %v", err)
	}

	if err := t.copyDump(tempDumpFilepath); err != nil {
		t.rollback()
		return fmt.Errorf("could not dump table: %v", err)
	}

	if err := t.tx.Commit(); err != nil {
		return fmt.Errorf("could not commit: %v", err)
	}

	t.dumpFilename = BasebackupFilename
	if err := os.Rename(tempDumpFilepath, path.Join(t.dir, t.dumpFilename)); err != nil {
		return fmt.Errorf("could not move copy dump file: %v", err)
	}

	t.BackupDuration = time.Since(t.CreateDate)
	if err := yaml.NewEncoder(infoFp).Encode(t.DumpInfo); err != nil {
		return fmt.Errorf("could not save info file: %v", err)
	}

	t.infoFilename = BasebackupInfoFilename
	if err := os.Rename(tempInfoFilepath, path.Join(t.dir, t.infoFilename)); err != nil {
		return fmt.Errorf("could not move dump info file: %v", err)
	}

	log.Printf("snapshotLSN: %v", t.StartLSN)
	log.Printf("It took %v to basebackup", t.BackupDuration)

	return nil
}

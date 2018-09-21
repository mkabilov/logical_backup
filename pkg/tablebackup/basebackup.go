package tablebackup

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/message"
)

func (t *TableBackup) Basebackup() error {
	if !atomic.CompareAndSwapUint32(&t.locker, 0, 1) {
		log.Printf("Already locked %s; skipping", t)
		return nil
	}
	defer func() {
		atomic.StoreUint32(&t.locker, 0)
	}()

	log.Printf("Starting base backup of %s", t)
	tempFilepath := path.Join(t.tableDir, t.infoFilename+".new")
	if _, err := os.Stat(tempFilepath); os.IsExist(err) {
		os.Remove(tempFilepath)
	}

	infoFp, err := os.OpenFile(tempFilepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not create info file: %v", err)
	}

	defer func() {
		infoFp.Close()
		os.Remove(tempFilepath)
	}()

	if !t.lastBasebackupTime.IsZero() && time.Since(t.lastBasebackupTime) <= t.sleepBetweenBackups {
		log.Printf("base backups happening too often; skipping")
		return nil
	}

	if err := t.connect(); err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}
	defer t.disconnect()

	if err := t.txBegin(); err != nil {
		return fmt.Errorf("could not start transaction: %v", err)
	}

	startTime := time.Now()

	if err := t.createTempReplicationSlot(); err != nil { // slot will be dropped on tx finish
		return fmt.Errorf("could not create replication slot: %v", err)
	}

	if err := t.lockTable(); err != nil {
		return fmt.Errorf("could not lock table: %v", err)
	}

	//TODO: check if table is empty before creating logical replication slot
	if hasRows, err := t.hasRows(); err != nil {
		return fmt.Errorf("could not check if table has rows: %v", err)
	} else if !hasRows {
		log.Printf("table %s seems to have no rows; skipping", t.Identifier)
		return nil
	}

	relationInfo, err := FetchRelationInfo(t.tx, t.Identifier)
	if err != nil {
		return fmt.Errorf("could not fetch table struct: %v", err)
	}

	if err := t.copyDump(); err != nil {
		return fmt.Errorf("could not dump table: %v", err)
	}

	if err := t.txCommit(); err != nil {
		return fmt.Errorf("could not commit: %v", err)
	}

	t.lastBackupDuration = time.Since(startTime)
	err = yaml.NewEncoder(infoFp).Encode(message.DumpInfo{
		StartLSN:       pgx.FormatLSN(t.basebackupLSN),
		CreateDate:     time.Now(),
		Relation:       relationInfo,
		BackupDuration: t.lastBackupDuration.Seconds(),
	})
	if err != nil {
		return fmt.Errorf("could not save info file: %v", err)
	}

	if err := os.Rename(tempFilepath, path.Join(t.tableDir, t.infoFilename)); err != nil {
		log.Printf("could not rename: %v", err)
	}

	t.archiveFiles <- t.infoFilename

	log.Printf("%s backed up in %v; start lsn: %s",
		t.String(), t.lastBackupDuration.Truncate(1*time.Second), pgx.FormatLSN(t.basebackupLSN))

	if err := t.RotateOldDeltas(path.Join(t.tableDir, deltasDir), t.lastLSN); err != nil {
		return fmt.Errorf("could not archive old deltas: %v", err)
	}

	t.lastBasebackupTime = time.Now()
	t.deltasSinceBackupCnt = 0

	return nil
}

// connects to the postgresql instance using replication protocol
func (t *TableBackup) connect() error {
	cfg := t.dbCfg.Merge(pgx.ConnConfig{
		RuntimeParams:        map[string]string{"replication": "database"},
		PreferSimpleProtocol: true,
	})
	conn, err := pgx.Connect(cfg)

	if err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}

	connInfo, err := t.initPostgresql(conn)
	if err != nil {
		return fmt.Errorf("could not fetch conn info: %v", err)
	}
	conn.ConnInfo = connInfo
	t.conn = conn

	return nil
}

func (t *TableBackup) disconnect() error {
	if t.conn == nil {
		return fmt.Errorf("no open connections")
	}

	return t.conn.Close()
}

func (t *TableBackup) tempSlotName() string {
	return fmt.Sprintf("tempslot_%d", t.conn.PID())
}

func (t *TableBackup) RotateOldDeltas(deltasDir string, lastLSN uint64) error {
	fileList, err := ioutil.ReadDir(deltasDir)
	if err != nil {
		return fmt.Errorf("could not list directory: %v", err)
	}
	for _, v := range fileList {
		filename := v.Name()
		lsnStr := filename
		if strings.Contains(filename, ".") {
			parts := strings.Split(filename, ".")
			lsnStr = parts[0]
		}

		lsn, err := strconv.ParseUint(lsnStr, 16, 64)
		if err != nil {
			return fmt.Errorf("could not parse filename: %v", err)
		}
		if lastLSN == lsn {
			continue // skip current file
		}

		if lsn < t.basebackupLSN {
			filename = fmt.Sprintf("%s/%s", deltasDir, filename)
			if err := os.Remove(filename); err != nil {
				return fmt.Errorf("could not remove %q file: %v", filename, err)
			}
		}
	}

	return nil
}

func (t *TableBackup) lockTable() error {
	if _, err := t.tx.Exec(fmt.Sprintf("LOCK TABLE %s IN ACCESS SHARE MODE", t.Identifier.Sanitize())); err != nil {
		return fmt.Errorf("could not lock the table: %v", err)
	}

	return nil
}

func (t *TableBackup) txBegin() error {
	if t.tx != nil {
		return fmt.Errorf("there is already a transaction in progress")
	}
	if t.conn == nil {
		return fmt.Errorf("no postgresql connection")
	}

	tx, err := t.conn.BeginEx(t.ctx, &pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("could not begin tx: %v", err)
	}

	t.tx = tx

	return nil
}

func (t *TableBackup) txCommit() error {
	if t.tx == nil {
		return fmt.Errorf("no running transaction")
	}

	if t.conn == nil {
		return fmt.Errorf("no open connections")
	}

	if err := t.tx.Commit(); err != nil {
		return err
	}

	t.tx = nil
	return nil
}

func (t *TableBackup) txRollback() error {
	if t.tx == nil {
		return fmt.Errorf("no running transaction")
	}

	if t.conn == nil {
		return fmt.Errorf("no open connections")
	}

	if err := t.tx.Rollback(); err != nil {
		return err
	}

	t.tx = nil
	return nil
}

func (t *TableBackup) copyDump() error {
	if t.tx == nil {
		return fmt.Errorf("no running transaction")
	}
	if t.basebackupLSN == 0 {
		return fmt.Errorf("no consistent point")
	}

	tempFilename := path.Join(t.tableDir, t.basebackupFilename+".new")
	if _, err := os.Stat(tempFilename); os.IsExist(err) {
		os.Remove(tempFilename)
	}

	fp, err := os.OpenFile(tempFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	if err := t.tx.CopyToWriter(fp, fmt.Sprintf("copy %s to stdout", t.Identifier.Sanitize())); err != nil {
		if err2 := t.txRollback(); err2 != nil {
			os.Remove(tempFilename)
			return fmt.Errorf("could not copy and rollback tx: %v, %v", err2, err)
		}
		os.Remove(tempFilename)
		return fmt.Errorf("could not copy: %v", err)
	}
	if err := os.Rename(tempFilename, path.Join(t.tableDir, t.basebackupFilename)); err != nil {
		return fmt.Errorf("could not move file: %v", err)
	}

	t.archiveFiles <- t.basebackupFilename

	return nil
}

func (t *TableBackup) createTempReplicationSlot() error {
	var createdSlotName, basebackupLSN, snapshotName, plugin sql.NullString

	if t.tx == nil {
		return fmt.Errorf("no running transaction")
	}

	row := t.tx.QueryRow(fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s USE_SNAPSHOT",
		t.tempSlotName(), "pgoutput"))

	if err := row.Scan(&createdSlotName, &basebackupLSN, &snapshotName, &plugin); err != nil {
		return fmt.Errorf("could not scan: %v", err)
	}

	if !basebackupLSN.Valid {
		return fmt.Errorf("null consistent point")
	}

	lsn, err := pgx.ParseLSN(basebackupLSN.String)
	if err != nil {
		return fmt.Errorf("could not parse LSN: %v", err)
	}

	t.basebackupLSN = lsn

	return nil
}

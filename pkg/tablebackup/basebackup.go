package tablebackup

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/prometheus"
	"github.com/ikitiki/logical_backup/pkg/utils"
)

const (
	ErrCodeTableNotFound = "42P01"
)

var (
	ErrTableNotFound = errors.New("table not found")
)

// RunBasebackup produces a 'snapshot' of the table (using COPY).
// It returns false if the table doesn't exist, otherwise true alongside the error if any
func (t *TableBackup) RunBasebackup() error {
	//TODO: split me into several methods
	var snapshotLSN, preBackupLSN, postBackupLSN dbutils.LSN

	if !atomic.CompareAndSwapUint32(&t.locker, 0, 1) {
		log.Printf("Already locked %s; skipping", t) // info
		return nil
	}
	defer atomic.StoreUint32(&t.locker, 0)
	log.Printf("Starting base backup of %s", t) // info

	if !t.lastBasebackupTime.IsZero() && time.Since(t.lastBasebackupTime) <= sleepBetweenBackups {
		log.Printf("base backups happening too often; skipping") // info
		return nil
	}

	if err := t.replConnect(); err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}

	startTime := time.Now()

	snapshotID, snapshotLSN, repSlotDuration, err := t.createTempRepSlot() // uses repl connect
	if err != nil {
		t.replDisconnect()
		return fmt.Errorf("could not get snapshot: %v", err)
	}
	log.Printf("It took %v to create replication slot", repSlotDuration) // debug

	postBackupLSN = t.segmentStartLSN

	if err := t.connect(); err != nil {
		t.replDisconnect()
		return fmt.Errorf("could not connect: %v", err)
	}

	if err := t.txBegin(snapshotID); err != nil {
		t.replDisconnect()
		return fmt.Errorf("could not begin tx: %v", err)
	}
	defer t.txRollback()

	if err := t.replDisconnect(); err != nil {
		return fmt.Errorf("could not close replication connection: %v", err)
	}

	if err := t.lockTable(); err == ErrTableNotFound {
		return err
	} else if err != nil {
		return fmt.Errorf("could not lock table: %v", err)
	}

	//TODO: check if table is empty before creating logical replication slot
	if hasRows, err := t.hasRows(); err != nil {
		return fmt.Errorf("could not check if table has rows: %v", err)
	} else if !hasRows {
		log.Printf("table %s seems to have no rows; skipping", t.NamespacedName)
		return nil
	}

	if err := t.copyDump(); err != nil {
		return fmt.Errorf("could not dump table: %v", err)
	}

	t.lastBackupDuration = time.Since(startTime)

	if err := t.StoreDumpInfo(snapshotLSN); err != nil {
		return fmt.Errorf("could not save state: %v", err)
	}

	// remove all deltas before this point, but keep the latest delta started before the backup, because it may
	// have changes happening after the backup has started. There is a slight race of a race condition, if a delta
	// file gets rotated after creation of the slot but before the LSN to keep is recorded; to avoid that, we keep
	// the LSN before and after the logical slot creation and, in case of their mismatch, take the latest delta
	// file that has been created before the slot. Since changes to firstDeltaLSNToKeep are effective right away on the
	// archiver side, we go extra mile not to assign intermediate results to it.

	candidateLSN := postBackupLSN

	if candidateLSN > snapshotLSN {
		log.Printf("first delta lsn to keep %s is higher than the backup lsn %s, attempting the previous delta lsn %s",
			postBackupLSN, snapshotLSN, preBackupLSN)
		// looks like the slot that has been created after the delta segment got an LSN that is lower than that segment!
		if preBackupLSN > snapshotLSN {
			log.Panic(fmt.Sprintf("table %s: logical backup lsn %s points to an earlier location than the lsn of the latest delta created before it %s",
				t, snapshotLSN, t.firstDeltaLSNToKeep))
		}
		candidateLSN = preBackupLSN
	}

	// Make sure we have a cutoff point
	if candidateLSN == 0 {
		log.Printf("first delta to keep lsn is not defined, reverting to the backup lsn %s", snapshotLSN)
		candidateLSN = snapshotLSN
	}

	t.firstDeltaLSNToKeep = candidateLSN
	t.queueArchiveFile(BaseBackupStateFileName)

	log.Printf("%s backed up in %v; start lsn: %s, first delta lsn to keep: %s",
		t.String(),
		t.lastBackupDuration.Truncate(1*time.Second),
		snapshotLSN,
		t.firstDeltaLSNToKeep)

	// note that the archiver has stopped archiving old deltas, we can purge them from both staging and final directories
	for _, baseDir := range []string{t.stagingDir, t.archiveDir} {
		if baseDir == "" {
			continue
		}

		if err := t.purgeObsoleteDeltaFiles(path.Join(baseDir, DeltasDirName)); err != nil {
			return fmt.Errorf("could not purge delta files from %q: %v", path.Join(baseDir, DeltasDirName), err)
		}
	}

	t.lastBasebackupTime = time.Now()
	t.deltasSinceBackupCnt = 0

	if err := t.updateMetricsAfterBaseBackup(); err != nil {
		log.Printf("could not update metrics: %v", err)
	}

	return nil
}

func (t *TableBackup) SetBasebackupPending() {
	t.basebackupIsPending = true
}

func (t *TableBackup) ClearBasebackupPending() {
	t.basebackupIsPending = false
}

func (t *TableBackup) IsBasebackupPending() bool {
	return t.basebackupIsPending
}

func (t *TableBackup) StoreDumpInfo(backupLSN dbutils.LSN) error {
	tableInfoDir := t.archiveDir
	if t.stagingDir != "" {
		tableInfoDir = t.stagingDir
	}

	relationInfo, err := FetchRelationInfo(t.tx, t.NamespacedName)
	if err != nil {
		return fmt.Errorf("could not fetch table struct: %v", err)
	}

	tempInfoFilepath := path.Join(tableInfoDir, BaseBackupStateFileName+".new")
	if _, err := os.Stat(tempInfoFilepath); !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("could not stat %q: %v", tempInfoFilepath, err)
		}

		os.Remove(tempInfoFilepath)
	}

	infoFp, err := os.OpenFile(tempInfoFilepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open info file %q: %v", tempInfoFilepath, err)
	}
	defer func() {
		infoFp.Close()
		os.Remove(tempInfoFilepath)
	}()

	err = yaml.NewEncoder(infoFp).Encode(message.DumpInfo{
		StartLSN:       backupLSN.String(),
		CreateDate:     time.Now(),
		Relation:       relationInfo,
		BackupDuration: t.lastBackupDuration.Seconds(),
	})
	if err != nil {
		return fmt.Errorf("could not save info file: %v", err)
	}

	if err := os.Rename(tempInfoFilepath, path.Join(tableInfoDir, BaseBackupStateFileName)); err != nil {
		log.Printf("could not rename: %v", err)
	}

	return nil
}

func (t *TableBackup) updateMetricsAfterBaseBackup() error {
	err := t.prom.Set(
		promexporter.PerTableLastBackupEndTimestamp,
		float64(t.lastBasebackupTime.Unix()), []string{t.OID().String(), t.TextID()})
	if err != nil {
		return fmt.Errorf("could not set %s: %v", promexporter.PerTableLastBackupEndTimestamp, err)
	}

	err = t.prom.Reset(promexporter.PerTableMessageSinceLastBackupGauge, []string{t.OID().String(), t.TextID()})
	if err != nil {
		return fmt.Errorf("could not reset %s: %v", promexporter.PerTableMessageSinceLastBackupGauge, err)
	}

	return nil
}

func (t *TableBackup) connect() error {
	cfg := t.dbCfg.Merge(pgx.ConnConfig{
		PreferSimpleProtocol: true,
	})

	conn, err := pgx.Connect(cfg)
	if err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}

	connInfo, err := initPostgresql(conn)
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

// connects to the postgresql instance using replication protocol
func (t *TableBackup) replConnect() error {
	cfg := t.dbCfg.Merge(pgx.ConnConfig{
		RuntimeParams:        map[string]string{"replication": "database"},
		PreferSimpleProtocol: true,
	})

	replConn, err := pgx.Connect(cfg)
	if err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}

	connInfo, err := initPostgresql(replConn)
	if err != nil {
		return fmt.Errorf("could not fetch conn info: %v", err)
	}

	replConn.ConnInfo = connInfo
	t.replConn = replConn

	return nil
}

func (t *TableBackup) replDisconnect() error {
	if t.replConn == nil {
		return fmt.Errorf("no open connections")
	}

	return t.replConn.Close()
}

func (t *TableBackup) tempSlotName() string {
	return fmt.Sprintf("tempslot_%d", t.replConn.PID())
}

func (t *TableBackup) purgeObsoleteDeltaFiles(deltasDir string) error {
	log.Printf("Purging segments in %s before the LSN %s", deltasDir, t.firstDeltaLSNToKeep)
	fileList, err := ioutil.ReadDir(deltasDir)
	if err != nil {
		return fmt.Errorf("could not list directory: %v", err)
	}
	for _, v := range fileList {
		filename := v.Name()
		if lsn, err := utils.GetLSNFromDeltaFilename(filename); err == nil {
			if lsn < t.firstDeltaLSNToKeep {
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

// lockTable tries to lock the table, if the table does not exist, an specific error will be returned
func (t *TableBackup) lockTable() error {
	sql := fmt.Sprintf("LOCK TABLE %s IN ACCESS SHARE MODE", t.NamespacedName.Sanitize())
	if _, err := t.tx.Exec(sql); err != nil {
		// check if the error comes from the server
		if e, ok := err.(pgx.PgError); ok && e.Code == ErrCodeTableNotFound {
			return ErrTableNotFound
		}

		return err
	}

	return nil
}

func (t *TableBackup) txBegin(snapshot dbutils.SnapshotID) error {
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

	if _, err := t.tx.Exec(fmt.Sprintf("set transaction snapshot '%s'", snapshot)); err != nil {
		return fmt.Errorf("could not set transaction snapshot")
	}

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

	dumpDir := t.archiveDir
	if t.cfg.StagingDir != "" {
		dumpDir = t.stagingDir
	}

	tempFilename := path.Join(dumpDir, BasebackupFilename+".new")
	if _, err := os.Stat(tempFilename); !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("could not stat %q: %v", tempFilename, err)
		}
		os.Remove(tempFilename)
	}

	fp, err := os.OpenFile(tempFilename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer fp.Close()

	if err := t.tx.CopyToWriter(fp, fmt.Sprintf("copy %s to stdout", t.NamespacedName.Sanitize())); err != nil {
		if err2 := t.txRollback(); err2 != nil {
			os.Remove(tempFilename)
			return fmt.Errorf("could not copy and rollback tx: %v, %v", err2, err)
		}

		os.Remove(tempFilename)
		return fmt.Errorf("could not copy: %v", err)
	}
	if err := os.Rename(tempFilename, path.Join(dumpDir, BasebackupFilename)); err != nil {
		return fmt.Errorf("could not move file: %v", err)
	}
	t.queueArchiveFile(BasebackupFilename)

	return nil
}

// createTempRepSlot creates temporary replication slot with EXPORT_SNAPSHOT keyword
func (t *TableBackup) createTempRepSlot() (dbutils.SnapshotID, dbutils.LSN, time.Duration, error) {
	var (
		slotName        string
		consistentPoint string
		outputPlugin    string
		snapshotName    dbutils.SnapshotID
		lsn             dbutils.LSN
		duration        time.Duration
	)

	startTime := time.Now()
	row := t.replConn.QueryRow(fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s EXPORT_SNAPSHOT",
		t.tempSlotName(), dbutils.OutputPlugin))

	if err := row.Scan(&slotName, &consistentPoint, &snapshotName, &outputPlugin); err != nil {
		return snapshotName, lsn, duration, fmt.Errorf("could not scan: %v", err)
	}

	if err := lsn.Parse(consistentPoint); err != nil {
		return snapshotName, lsn, duration, fmt.Errorf("could not parse lsn: %v", err)
	}

	return snapshotName, lsn, time.Since(startTime), nil
}

func (t *TableBackup) getDirectory() string {
	if t.stagingDir != "" {
		return t.stagingDir
	}
	return t.archiveDir
}

package tablebackup

import (
	"database/sql"
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

func (t *TableBackup) RunBasebackup() error {
	//TODO: split me into several methods
	var (
		backupLSN, preBackupLSN, postBackupLSN dbutils.Lsn
		tableInfoDir                           string
	)

	if !atomic.CompareAndSwapUint32(&t.locker, 0, 1) {
		log.Printf("Already locked %s; skipping", t)
		return nil
	}
	defer atomic.StoreUint32(&t.locker, 0)
	log.Printf("Starting base backup of %s", t)

	if t.stagingDir != "" {
		tableInfoDir = t.stagingDir
	} else {
		tableInfoDir = t.archiveDir
	}

	tempInfoFilepath := path.Join(tableInfoDir, TableInfoFilename+".new")
	if _, err := os.Stat(tempInfoFilepath); !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("could not stat %q: %v", tempInfoFilepath, err)
		}

		os.Remove(tempInfoFilepath)
	}

	infoFp, err := os.OpenFile(tempInfoFilepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not create info file: %v", err)
	}

	defer func() {
		infoFp.Close()
		os.Remove(tempInfoFilepath)
	}()

	if !t.lastBasebackupTime.IsZero() && time.Since(t.lastBasebackupTime) <= t.sleepBetweenBackups {
		log.Printf("base backups happening too often; skipping")
		return nil
	}

	if err := t.connect(); err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}
	defer t.disconnect()

	startTime := time.Now()
	var relationInfo message.Relation

	stop, err := func() (stop bool, err error) {
		// do rollback if the code has thrown an error or simply found no rows, otherwise commit the changes.
		// if the error happens during rollback or commit we either return it, if it is the first error in the
		// transaction, or show it to the user to if there was a prior one, to avoid making that prior one.
		if err := t.txBegin(); err != nil {
			return true, fmt.Errorf("could not start transaction: %v", err)
		}

		defer func() {
			if stop || err != nil {
				if err2 := t.txRollback(); err2 != nil {
					log.Printf("could not rollback: %v", err2)
					if err == nil {
						err = err2
					}
				}
			} else {
				// in this case we also want to return this error
				if err := t.txCommit(); err != nil {
					stop = true
					err = fmt.Errorf("could not commit: %v", err)

				}
			}
		}()
		preBackupLSN = t.lastLSN
		backupLSN, err = t.createTempReplicationSlot()
		if err != nil { // slot will be dropped on tx finish
			return true, fmt.Errorf("could not create replication slot: %v", err)
		}
		postBackupLSN = t.lastLSN

		if err := t.lockTable(); err != nil {
			return true, fmt.Errorf("could not lock table: %v", err)
		}

		relationInfo, err = FetchRelationInfo(t.tx, t.NamespacedName)
		if err != nil {
			return true, fmt.Errorf("could not fetch table struct: %v", err)
		}

		//TODO: check if table is empty before creating logical replication slot
		if hasRows, err := t.hasRows(); err != nil {
			return true, fmt.Errorf("could not check if table has rows: %v", err)
		} else if !hasRows {
			log.Printf("table %s seems to have no rows; skipping", t.NamespacedName)
			return false, nil
		}

		if err := t.copyDump(); err != nil {
			return true, fmt.Errorf("could not dump table: %v", err)
		}

		return false, nil
	}()
	if stop {
		return err
	}

	t.lastBackupDuration = time.Since(startTime)
	err = yaml.NewEncoder(infoFp).Encode(message.DumpInfo{
		StartLSN:       backupLSN.String(),
		CreateDate:     time.Now(),
		Relation:       relationInfo,
		BackupDuration: t.lastBackupDuration.Seconds(),
	})
	if err != nil {
		return fmt.Errorf("could not save info file: %v", err)
	}

	if err := os.Rename(tempInfoFilepath, path.Join(tableInfoDir, TableInfoFilename)); err != nil {
		log.Printf("could not rename: %v", err)
	}

	// remove all deltas before this point, but keep the latest delta started before the backup, because it may
	// have changes happening after the backup has started. There is a slight race of a race condition, if a delta
	// file gets rotated after creation of the slot but before the LSN to keep is recorded; to avoid that, we keep
	// the LSN before and after the logical slot creation and, in case of their mismatch, take the latest delta
	// file that has been created before the slot. Since changes to firstDeltaLSNToKeep are effective right away on the
	// archiver side, we go extra mile not to assign intermediate results to it.

	candidateLSN := postBackupLSN

	if candidateLSN > backupLSN {
		log.Printf("first delta lsn to keep %s is higher than the backup lsn %s, attempting the previous delta lsn %s",
			postBackupLSN, backupLSN, preBackupLSN)
		// looks like the slot that has been created after the delta segment got an LSN that is lower than that segment!
		if preBackupLSN > backupLSN {
			log.Panic("table %s: logical backup lsn %s points to an earlier location than the lsn of the latest delta created before it %s",
				t, backupLSN, t.firstDeltaLSNToKeep)
		}
		candidateLSN = preBackupLSN
	}

	// Make sure we have a cutoff point
	if candidateLSN == 0 {
		log.Printf("first delta to keep lsn is not defined, reverting to the backup lsn %s", backupLSN)
		candidateLSN = backupLSN
	}

	t.firstDeltaLSNToKeep = candidateLSN
	t.queueArchiveFile(TableInfoFilename)

	log.Printf("%s backed up in %v; start lsn: %s, first delta lsn to keep: %s",
		t.String(),
		t.lastBackupDuration.Truncate(1*time.Second),
		backupLSN,
		t.firstDeltaLSNToKeep)

	// note that the archiver has stopped archiving old deltas, we can purge them from both staging and final directories
	for _, baseDir := range []string{t.stagingDir, t.archiveDir} {
		if baseDir == "" {
			continue
		}

		if err := t.PurgeObsoleteDeltaFiles(path.Join(baseDir, DeltasDirName)); err != nil {
			return fmt.Errorf("could not purge delta files from %q: %v", path.Join(baseDir, DeltasDirName), err)
		}
	}

	t.lastBasebackupTime = time.Now()
	t.deltasSinceBackupCnt = 0

	t.updateMetricsAfterBaseBackup()

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

func (t *TableBackup) updateMetricsAfterBaseBackup() {
	err := t.prom.Set(
		promexporter.PerTableLastBackupEndTimestamp,
		float64(t.lastBasebackupTime.Unix()), []string{t.ID().String(), t.TextID()})
	if err != nil {
		log.Printf("could not set %s: %v", promexporter.PerTableLastBackupEndTimestamp, err)
	}

	err = t.prom.Reset(promexporter.PerTableMessageSinceLastBackupGauge, []string{t.ID().String(), t.TextID()})
	if err != nil {
		log.Printf("could not reset %s: %v", promexporter.PerTableMessageSinceLastBackupGauge, err)
	}
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

func (t *TableBackup) PurgeObsoleteDeltaFiles(deltasDir string) error {
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

func (t *TableBackup) lockTable() error {
	if _, err := t.tx.Exec(fmt.Sprintf("LOCK TABLE %s IN ACCESS SHARE MODE", t.NamespacedName.Sanitize())); err != nil {
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
	var dumpDir string

	if t.tx == nil {
		return fmt.Errorf("no running transaction")
	}

	if t.cfg.StagingDir != "" {
		dumpDir = t.stagingDir
	} else {
		dumpDir = t.archiveDir
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

func (t *TableBackup) createTempReplicationSlot() (dbutils.Lsn, error) {
	var createdSlotName, basebackupLSN, snapshotName, plugin sql.NullString

	var lsn dbutils.Lsn

	if t.tx == nil {
		return lsn, fmt.Errorf("no running transaction")
	}

	// we don't need to accumulate WAL on the server throughout the whole COPY running time. We need the slot
	// exclusively to figure out our transaction LSN, why not drop it once we have got it?

	row := t.tx.QueryRow(fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s USE_SNAPSHOT",
		t.tempSlotName(), "pgoutput"))

	if err := row.Scan(&createdSlotName, &basebackupLSN, &snapshotName, &plugin); err != nil {
		return lsn, fmt.Errorf("could not scan: %v", err)
	}

	defer func() {
		if _, err := t.tx.Exec(fmt.Sprintf("DROP_REPLICATION_SLOT %s", t.tempSlotName())); err != nil {
			log.Printf("could not drop replication slot %s right away: %v, it will be dropped at the end of the backup",
				t.tempSlotName(), err)
		}
	}()

	if !basebackupLSN.Valid {
		return lsn, fmt.Errorf("null consistent point")
	}

	if err := lsn.Parse(basebackupLSN.String); err != nil {
		return lsn, fmt.Errorf("could not parse LSN: %v", err)
	}
	if lsn == 0 {
		return lsn, fmt.Errorf("no consistent starting point for a dump")
	}

	return lsn, nil
}

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

	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/utils"
)

func (t *TableBackup) RunBasebackup() error {
	var (
		backupLSN, preBackupLSN, postBackupLSN uint64
	)

	if !atomic.CompareAndSwapUint32(&t.locker, 0, 1) {
		log.Printf("Already locked %s; skipping", t)
		return nil
	}
	defer func() {
		atomic.StoreUint32(&t.locker, 0)
	}()

	log.Printf("Starting base backup of %s", t)
	tempFilepath := path.Join(t.tempDir, t.infoFilename+".new")
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

	startTime := time.Now()
	var relationInfo message.Relation

	stop, err := func() (stop bool, err error) {
		if err := t.txBegin(); err != nil {
			return true, fmt.Errorf("could not start transaction: %v", err)
		}

		defer func() {
			if stop || err != nil {
				if err := t.txRollback(); err != nil {
					fmt.Printf("could not rollback: %v", err)
				}
			} else {
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

		//TODO: check if table is empty before creating logical replication slot
		if hasRows, err := t.hasRows(); err != nil {
			return true, fmt.Errorf("could not check if table has rows: %v", err)
		} else if !hasRows {
			log.Printf("table %s seems to have no rows; skipping", t.Identifier)
			return true, nil
		}

		relationInfo, err = FetchRelationInfo(t.tx, t.Identifier)
		if err != nil {
			return true, fmt.Errorf("could not fetch table struct: %v", err)
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
		StartLSN:       pgx.FormatLSN(backupLSN),
		CreateDate:     time.Now(),
		Relation:       relationInfo,
		BackupDuration: t.lastBackupDuration.Seconds(),
	})
	if err != nil {
		return fmt.Errorf("could not save info file: %v", err)
	}

	if err := os.Rename(tempFilepath, path.Join(t.tempDir, t.infoFilename)); err != nil {
		log.Printf("could not rename: %v", err)
	}

	// remove all deltas before this point, but keep the latest delta started before the backup, because it may
	// have changes happening after the backup has started. There is a slight race of a race condition, if a delta
	// file gets rotated after creation of the slot but before the LSN to keep is recorded; to avoid that, we keep
	// the LSN before and after the logical slot creation and, in case of their mismatch, take the latest delta
	// file that has been created before the slot. Since changes to firstDeltaLSNToKeep are effective right away on the
	// archiver side we go extra mile not to assign intermediate results to it.

	candidateLSN := postBackupLSN

	if candidateLSN > backupLSN {
		log.Printf("first delta lsn to keep %s is higher than the backup lsn %s, attempting the previous delta lsn %s",
			pgx.FormatLSN(postBackupLSN), pgx.FormatLSN(backupLSN), pgx.FormatLSN(preBackupLSN))
		// lookd like the slot that has been created after the delta segment got an LSN that is lower than that segment!
		if preBackupLSN > backupLSN {
			log.Fatalf("logical slot lsn %s points to an earlier location than the lsn of the latest delta created before the slot %s",
				pgx.FormatLSN(backupLSN), pgx.FormatLSN(t.firstDeltaLSNToKeep))
		}
		candidateLSN = preBackupLSN
	}

	// Make sure we have a cutoff point
	if candidateLSN == 0 {
		log.Printf("first delta to keep lsn is not defined, reverting to the backup lsn %s",
			pgx.FormatLSN(backupLSN))
		candidateLSN = backupLSN
	}

	t.firstDeltaLSNToKeep = candidateLSN

	t.archiveFiles <- t.infoFilename

	log.Printf("%s backed up in %v; start lsn: %s",
		t.String(), t.lastBackupDuration.Truncate(1*time.Second), pgx.FormatLSN(backupLSN))

	// not that the archiver has stopped archiving old deltas, we can purge them from both staging and final directories
	for _, basedir := range []string{t.tempDir, t.finalDir} {
		if err := t.PurgeObsoleteDeltaFiles(path.Join(basedir, deltasDirName)); err != nil {
			return fmt.Errorf("could not purge delta files from %q: %v", path.Join(basedir, deltasDirName), err)
		}
	}

	t.lastBasebackupTime = time.Now()
	t.deltasSinceBackupCnt = 0

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
	log.Printf("Purging deltas in %s before the lsn %s",
		deltasDir, pgx.FormatLSN(t.firstDeltaLSNToKeep))
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

	tempFilename := path.Join(t.tempDir, t.basebackupFilename+".new")
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
	if err := os.Rename(tempFilename, path.Join(t.tempDir, t.basebackupFilename)); err != nil {
		return fmt.Errorf("could not move file: %v", err)
	}

	t.archiveFiles <- t.basebackupFilename

	return nil
}

func (t *TableBackup) createTempReplicationSlot() (uint64, error) {
	var createdSlotName, basebackupLSN, snapshotName, plugin sql.NullString

	var lsn uint64

	if t.tx == nil {
		return lsn, fmt.Errorf("no running transaction")
	}

	// XXX: we don't need to accumulate WAL on the server throughout the whole COPY running time. We need the slot
	// exclusively to figure out our transaction LSN, why not drop it once we have got it?

	row := t.tx.QueryRow(fmt.Sprintf("CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s USE_SNAPSHOT",
		t.tempSlotName(), "pgoutput"))

	if err := row.Scan(&createdSlotName, &basebackupLSN, &snapshotName, &plugin); err != nil {
		return lsn, fmt.Errorf("could not scan: %v", err)
	}

	if !basebackupLSN.Valid {
		return lsn, fmt.Errorf("null consistent point")
	}

	lsn, err := pgx.ParseLSN(basebackupLSN.String)
	if err != nil {
		return lsn, fmt.Errorf("could not parse LSN: %v", err)
	}
	if lsn == 0 {
		return lsn, fmt.Errorf("no consistent starting point for a dump")
	}

	return lsn, nil
}

package tablebackup

import (
	"log"

	"github.com/ikitiki/logical_backup/pkg/prometheus"
)

func (t *tableBackup) updateMetricsAfterBaseBackup() {
	err := t.prom.Set(
		promexporter.PerTableLastBackupEndTimestamp,
		float64(t.lastBasebackupTime.Unix()), []string{t.OID().String(), t.String()})
	if err != nil {
		log.Printf("could not set %s: %v", promexporter.PerTableLastBackupEndTimestamp, err)
	}

	err = t.prom.Reset(promexporter.PerTableMessageSinceLastBackupGauge, []string{t.OID().String(), t.String()})
	if err != nil {
		log.Printf("could not reset %s: %v", promexporter.PerTableMessageSinceLastBackupGauge, err)
	}
}

func (t *tableBackup) updateMetricsForArchiver(isTimeout bool) error {
	var filesCounter, perTableFilesCounter string

	if isTimeout {
		filesCounter = promexporter.FilesArchivedTimeoutCounter
		perTableFilesCounter = promexporter.PerTableFilesArchivedTimeoutCounter
	} else {
		filesCounter = promexporter.FilesArchivedCounter
		perTableFilesCounter = promexporter.PerTablesFilesArchivedCounter
	}

	if err := t.prom.Inc(filesCounter, nil); err != nil {
		return err
	}

	if err := t.prom.Inc(perTableFilesCounter, []string{t.OID().String(), t.String()}); err != nil {
		return err
	}

	return nil
}

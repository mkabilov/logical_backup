package logicalbackup

import (
	"fmt"
	"time"

	"github.com/ikitiki/logical_backup/pkg/message"
	prom "github.com/ikitiki/logical_backup/pkg/prometheus"
	"github.com/ikitiki/logical_backup/pkg/tablebackup"
	"github.com/ikitiki/logical_backup/pkg/utils/dbutils"
)

func (b *logicalBackup) registerMetrics() error {
	registerMetrics := []prom.MetricsToRegister{
		{
			prom.MessageCounter,
			"total number of messages received",
			[]string{prom.MessageTypeLabel},
			prom.MetricsCounter,
		},
		{
			prom.TotalBytesWrittenCounter,
			"total bytes written",
			nil,
			prom.MetricsCounter,
		},
		{
			prom.TransactionCounter,
			"total number of transactions",
			nil,
			prom.MetricsCounter,
		},
		{
			prom.FlushLSNCGauge,
			"last LSN to flush",
			nil,
			prom.MetricsGauge,
		},
		{
			prom.LastCommitTimestampGauge,
			"last commit timestamp",
			nil,
			prom.MetricsGauge,
		},
		{
			prom.LastWrittenMessageTimestampGauge,
			"last written message timestamp",
			nil,
			prom.MetricsGauge,
		},
		{
			prom.FilesArchivedCounter,
			"total files archived",
			nil,
			prom.MetricsCounter,
		},
		{
			prom.FilesArchivedTimeoutCounter,
			"total number of files archived due to a timeout",
			nil,
			prom.MetricsCounter,
		},
		{
			prom.PerTableMessageCounter,
			"per table number of messages written",
			[]string{prom.TableOIDLabel, prom.TableNameLabel, prom.MessageTypeLabel},
			prom.MetricsCounterVector,
		},
		{
			prom.PerTableBytesCounter,
			"per table number of bytes written",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsCounterVector,
		},
		{
			prom.PerTablesFilesArchivedCounter,
			"per table number of segments archived",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsCounterVector,
		},
		{
			prom.PerTableFilesArchivedTimeoutCounter,
			"per table number of segments archived due to a timeout",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsCounterVector,
		},
		{
			prom.PerTableLastCommitTimestampGauge,
			"per table last commit message timestamp",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsGaugeVector,
		},
		{
			prom.PerTableLastBackupEndTimestamp,
			"per table last backup end timestamp",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsGaugeVector,
		},
		{
			prom.PerTableMessageSinceLastBackupGauge,
			"per table number of messages since the last basebackup",
			[]string{prom.TableOIDLabel, prom.TableNameLabel},
			prom.MetricsGaugeVector,
		},
	}
	for _, m := range registerMetrics {
		if err := b.prom.RegisterMetricsItem(&m); err != nil {
			return fmt.Errorf("could not register prometheus metrics: %v", err)
		}
	}

	return nil
}

func (b *logicalBackup) updateMetricsAfterWriteDelta(t tablebackup.TableBackuper, typ message.MType, ln uint) error {
	promType := typ.String()

	if err := b.prom.Inc(prom.MessageCounter, []string{promType}); err != nil {
		return err
	}

	if err := b.prom.Inc(prom.PerTableMessageCounter, []string{t.OID().String(), t.String(), promType}); err != nil {
		return err
	}

	if err := b.prom.SetToCurrentTime(prom.LastWrittenMessageTimestampGauge, nil); err != nil {
		return err
	}

	if err := b.prom.Add(prom.TotalBytesWrittenCounter, float64(ln), nil); err != nil {
		return err
	}

	if err := b.prom.Add(prom.PerTableBytesCounter, float64(ln), []string{t.OID().String(), t.String()}); err != nil {
		return err
	}

	if err := b.prom.Inc(prom.PerTableMessageSinceLastBackupGauge, []string{t.OID().String(), t.String()}); err != nil {
		return err
	}

	return nil
}

func (b *logicalBackup) updateMetricsCommit(lsn dbutils.LSN, commitTime time.Time) error {
	if err := b.prom.Inc(prom.TransactionCounter, nil); err != nil {
		return err
	}

	if err := b.prom.Set(prom.FlushLSNCGauge, float64(lsn), nil); err != nil {
		return err
	}

	if err := b.prom.Set(prom.LastCommitTimestampGauge, float64(commitTime.Unix()), nil); err != nil {
		return err
	}

	return nil
}

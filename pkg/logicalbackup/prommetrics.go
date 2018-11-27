package logicalbackup

import (
	"fmt"

	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/message"
	prom "github.com/ikitiki/logical_backup/pkg/prometheus"
	"github.com/ikitiki/logical_backup/pkg/tablebackup"
)

func (b *LogicalBackup) maybeRegisterNewName(oid dbutils.OID, name message.NamespacedName) {
	var lastEntry NameAtLSN

	if b.tableNameChanges.nameChangeHistory[oid] != nil {
		lastEntry = b.tableNameChanges.nameChangeHistory[oid][len(b.tableNameChanges.nameChangeHistory[oid])-1]
	}
	if b.tableNameChanges.nameChangeHistory[oid] == nil || lastEntry.Name != name {
		b.tableNameChanges.nameChangeHistory[oid] = append(b.tableNameChanges.nameChangeHistory[oid],
			NameAtLSN{Name: name, Lsn: b.transactionCommitLSN})
		b.tableNameChanges.isChanged = true

		// inform the tableBackuper about the new name
		b.backupTables.GetIfExists(oid).SetTextID(name)
	}
}

func (b *LogicalBackup) registerMetrics() error {
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

func (b *LogicalBackup) updateMetricsAfterWriteDelta(t tablebackup.TableBackuper, cmd msgType, ln uint64) {
	var promType string

	switch cmd {
	case mInsert:
		promType = prom.MessageTypeInsert
	case mUpdate:
		promType = prom.MessageTypeUpdate
	case mDelete:
		promType = prom.MessageTypeDelete
	case mBegin:
		promType = prom.MessageTypeBegin
	case mCommit:
		promType = prom.MessageTypeCommit
	case mRelation:
		promType = prom.MessageTypeRelation
	case mType:
		promType = prom.MessageTypeTypeInfo
	default:
		promType = prom.MessageTypeUnknown
	}

	b.prom.Inc(prom.MessageCounter, []string{promType})
	b.prom.Inc(prom.PerTableMessageCounter, []string{t.OID().String(), t.TextID(), promType})
	b.prom.SetToCurrentTime(prom.LastWrittenMessageTimestampGauge, nil)

	b.prom.Add(prom.TotalBytesWrittenCounter, float64(ln), nil)
	b.prom.Add(prom.PerTableBytesCounter, float64(ln), []string{t.OID().String(), t.TextID()})
	b.prom.Inc(prom.PerTableMessageSinceLastBackupGauge, []string{t.OID().String(), t.TextID()})
}

func (b *LogicalBackup) updateMetricsOnCommit(commitTimestamp int64) {
	for relOID := range b.txBeginRelMsg {
		tb := b.backupTables.GetIfExists(relOID)
		b.prom.Set(prom.PerTableLastCommitTimestampGauge, float64(commitTimestamp), []string{tb.OID().String(), tb.TextID()})
	}

	b.prom.Inc(prom.TransactionCounter, nil)
	b.prom.Set(prom.FlushLSNCGauge, float64(b.transactionCommitLSN), nil)
	b.prom.Set(prom.LastCommitTimestampGauge, float64(commitTimestamp), nil)
}

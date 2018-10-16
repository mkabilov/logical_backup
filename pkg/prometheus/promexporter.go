package promexporter

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	prometheusNamespace = "lbt"

	MessageCounter                   = "backup_messages_total"
	FilesArchivedCounter             = "backup_files_archived_total"
	FilesArchivedTimeoutCounter      = "backup_files_timeout_archived_total"
	TransactionCounter               = "backup_transactions_total"
	TotalBytesWrittenCounter         = "backup_bytes_written_total"
	FlushLSNCGauge                   = "backup_flush_lsn_counter"
	LastCommitTimestampGauge         = "backup_last_commit_timestamp"
	LastWrittenMessageTimestampGauge = "backup_last_written_message_timestamp"

	PerTableMessageCounter              = "backup_messages_per_table"
	PerTableBytesCounter                = "backup_bytes_per_table"
	PerTablesFilesArchivedCounter       = "backup_files_archived_per_table"
	PerTableFilesArchivedTimeoutCounter = "backup_files_timeout_archived_per_table"
	PerTableLastCommitTimestampGauge    = "backup_last_commit_timestamp_per_table"
	PerTableLastBackupEndTimestamp      = "backup_last_backup_end_timestamp_per_table"
	PerTableMessageSinceLastBackupGauge = "backup_messages_since_last_basebackup_per_table"

	MessageTypeLabel = "message_type"
	TableNameLabel   = "table_name"
	TableOIDLabel    = "table_oid"

	MessageTypeInsert   = "insert"
	MessageTypeUpdate   = "update"
	MessageTypeDelete   = "delete"
	MessageTypeRelation = "relation"
	MessageTypeBegin    = "begin"
	MessageTypeCommit   = "commit"
	MessageTypeTypeInfo = "type"
	MessageTypeUnknown  = "unknown"
)

type PrometheusExporter struct {
	metrics map[string]interface{}
	port    int
}

func New(port int) *PrometheusExporter {
	metrics := make(map[string]interface{})
	return &PrometheusExporter{metrics: metrics, port: port}
}

func (pe *PrometheusExporter) errorIfExists(name, typeName string) error {
	_, ok := pe.metrics[name]
	if ok {
		return fmt.Errorf("%s with the name %q is already registered", typeName, name)
	}
	return nil
}

func (pe *PrometheusExporter) RegisterCounterVector(name, help string, labelNames []string) error {
	if err := pe.errorIfExists(name, "counter vector"); err != nil {
		return err
	}

	pe.metrics[name] = promauto.NewCounterVec(prom.CounterOpts{Name: name, Namespace: prometheusNamespace, Help: help},
		labelNames)

	return nil
}

func (pe *PrometheusExporter) RegisterCounter(name, help string) error {
	if err := pe.errorIfExists(name, "counter"); err != nil {
		return err
	}

	pe.metrics[name] = promauto.NewCounter(prom.CounterOpts{Name: name, Namespace: prometheusNamespace, Help: help})

	return nil
}

func (pe *PrometheusExporter) RegisterGauge(name, help string) error {
	if err := pe.errorIfExists(name, "counter"); err != nil {
		return err
	}

	pe.metrics[name] = promauto.NewGauge(prom.GaugeOpts{Name: name, Namespace: prometheusNamespace, Help: help})

	return nil
}

func (pe *PrometheusExporter) RegisterGaugeVector(name, help string, labelNames []string) error {
	if err := pe.errorIfExists(name, "gauge vector"); err != nil {
		return err
	}

	pe.metrics[name] = promauto.NewGaugeVec(prom.GaugeOpts{Name: name, Namespace: prometheusNamespace, Help: help},
		labelNames)

	return nil
}

func (pe *PrometheusExporter) Inc(name string, labelValues []string) error {
	switch t := pe.metrics[name].(type) {
	case prom.Counter:
		t.Inc()
	case prom.Gauge:
		t.Inc()
	case *prom.CounterVec:
		t.WithLabelValues(labelValues...).Inc()
	case *prom.GaugeVec:
		t.WithLabelValues(labelValues...).Inc()
	default:
		return fmt.Errorf("type %T doesn't support Inc", t)
	}

	return nil
}

func (pe *PrometheusExporter) Add(name string, addition float64, labelValues []string) error {
	switch t := pe.metrics[name].(type) {
	case prom.Counter:
		t.Add(addition)
	case *prom.CounterVec:
		t.WithLabelValues(labelValues...).Add(addition)
	default:
		return fmt.Errorf("type %T doesn't support Add", t)
	}

	return nil
}

func (pe *PrometheusExporter) Set(name string, value float64, labelValues []string) error {
	switch t := pe.metrics[name].(type) {
	case prom.Gauge:
		t.Set(value)
	case *prom.GaugeVec:
		t.WithLabelValues(labelValues...).Set(value)
	default:
		return fmt.Errorf("type %T doesn't support Set", t)
	}
	return nil
}

func (pe *PrometheusExporter) Reset(name string, labelValues []string) error {
	return pe.Set(name, 0, labelValues)
}

func (pe *PrometheusExporter) SetToCurrentTime(name string, labelValues []string) error {
	switch t := pe.metrics[name].(type) {
	case prom.Gauge:
		t.SetToCurrentTime()
	case prom.GaugeVec:
		t.WithLabelValues(labelValues...).SetToCurrentTime()
	default:
		return fmt.Errorf("type %T doesn't support SetToCurrentTime", t)
	}
	return nil
}

func (pe *PrometheusExporter) Run(ctx context.Context, wait *sync.WaitGroup, serverStopChan chan struct{}) {
	defer wait.Done()

	var srv *http.Server

	wait.Add(1)
	go func() {
		defer wait.Done()
		for {
			select {
			case <-ctx.Done():
				if err := srv.Close(); err != nil {
					log.Printf("error while closing prometheus connections: %v", err)
				}
				log.Printf("prometheus connection closed")
				return
			}
		}
	}()

	// TODO: avoid exposting noisy metrics about the prometheus itself
	http.Handle("/", http.RedirectHandler("/metrics", http.StatusMovedPermanently))
	http.Handle("/metrics", promhttp.Handler())
	srv = &http.Server{Addr: fmt.Sprintf(":%d", pe.port)}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("prometheus exporter routine closed with error %v", err)
	}
	serverStopChan <- struct{}{}
}

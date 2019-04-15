package basebackup

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mkabilov/logical_backup/pkg/basebackup/bbtable"
	"github.com/mkabilov/logical_backup/pkg/config"
	"github.com/mkabilov/logical_backup/pkg/tablebackup"
	"github.com/mkabilov/logical_backup/pkg/utils/queue"
	"github.com/mkabilov/logical_backup/pkg/utils/tablesmap"
)

const (
	sleepBetweenBackups = time.Second * 3
)

// Basebackuper represents interface for creating base backups,
// opens db connection for each base backup session
type Basebackuper interface {
	Run(int)
	Wait()
	QueueTable(tablebackup.TableBackuper)
}

type basebackup struct {
	ctx context.Context
	wg  *sync.WaitGroup

	cfg          *config.Config
	queue        *queue.Queue
	backupTables tablesmap.TablesMapInterface
	inProgress   sync.Map
}

// New instantiates basebackup,
// we need to pass backupTables so that we can remove deleted tables from the backupTables
func New(ctx context.Context, backupTables tablesmap.TablesMapInterface, cfg *config.Config) *basebackup {
	b := basebackup{
		ctx:          ctx,
		cfg:          cfg,
		wg:           &sync.WaitGroup{},
		backupTables: backupTables,
		queue:        queue.New(ctx),
	}

	return &b
}

// Run starts background processes of the basebackuper
func (b *basebackup) Run(cnt int) {
	log.Printf("Starting %d background backupers", cnt)
	for i := 0; i < cnt; i++ {
		b.wg.Add(1)
		go b.worker(i)
	}
}

func (b *basebackup) basebackupTable(table tablebackup.TableBackuper) error {
	var bbTable bbtable.TableBasebackuper

	if _, loaded := b.inProgress.LoadOrStore(table.OID(), struct{}{}); loaded {
		log.Printf("Basebackup of the %s table is in progress by another process; skipping", table)
		return nil
	}
	defer b.inProgress.Delete(table.OID())

	if lbTime := table.LastBasebackupTime(); !lbTime.IsZero() && time.Since(lbTime) <= sleepBetweenBackups {
		log.Printf("Base backups happening too often; skipping this one")
		return nil
	}

	if table.MessagesProcessed() == 0 && !table.LastBasebackupTime().IsZero() {
		log.Printf("No backup needed. no processed messages since last basebackup")
		return nil
	}

	log.Printf("Starting base backup of %s", table)
	bbTable = bbtable.New(b.cfg.DB, table)
	if err := bbTable.Basebackup(); err != nil {
		if err == bbtable.ErrTableNotFound {
			log.Printf("Table %s not found, skipping basebackup and removing from the list", table)
			b.backupTables.Delete(table.OID())
			return nil
		} else {
			return fmt.Errorf("could not basebackup %s table: %v", table, err)
		}
	}

	if err := table.BasebackupDone(bbTable.DumpFilename(), bbTable.InfoFilename(), bbTable.Lsn()); err != nil {
		return fmt.Errorf("could not process post basebackup operations: %v", err)
	}

	return nil
}

// Wait waits for all the basebackup workers to finish
func (b *basebackup) Wait() {
	b.wg.Wait()
}

// QueueTable queues the table for the base backups
func (b *basebackup) QueueTable(t tablebackup.TableBackuper) {
	b.queue.Put(t)
}

func (b *basebackup) worker(id int) {
	defer b.wg.Done()

	for {
		obj, err := b.queue.Get()
		if err == context.Canceled {
			log.Printf("Quiting background base backuper %d", id)
			return
		}

		t := obj.(tablebackup.TableBackuper)
		if err := b.basebackupTable(t); err != nil {
			log.Printf("Could not basebackup %s: %v", t, err)
		}
	}
}

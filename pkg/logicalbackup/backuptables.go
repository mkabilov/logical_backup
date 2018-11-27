package logicalbackup

import (
	"sync"

	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/tablebackup"
)

type BackupTables struct {
	sync.RWMutex
	data map[dbutils.OID]tablebackup.TableBackuper
}

func NewBackupTables() *BackupTables {
	return &BackupTables{
		RWMutex: sync.RWMutex{},
		data:    make(map[dbutils.OID]tablebackup.TableBackuper),
	}
}

func (bt *BackupTables) GetIfExists(oid dbutils.OID) tablebackup.TableBackuper {
	bt.RLock()
	defer bt.RUnlock()

	return bt.data[oid]
}

func (bt *BackupTables) Get(oid dbutils.OID) (result tablebackup.TableBackuper, ok bool) {
	bt.RLock()
	defer bt.RUnlock()

	result, ok = bt.data[oid]
	return
}

func (bt *BackupTables) Set(oid dbutils.OID, t tablebackup.TableBackuper) {
	bt.Lock()
	defer bt.Unlock()

	bt.data[oid] = t
}

func (bt *BackupTables) Delete(oid dbutils.OID) {
	bt.Lock()
	defer bt.Unlock()

	delete(bt.data, oid)
}

func (bt *BackupTables) Map(fn func(t tablebackup.TableBackuper)) {
	bt.Lock()
	defer bt.Unlock()

	for _, t := range bt.data {
		fn(t)
	}
}

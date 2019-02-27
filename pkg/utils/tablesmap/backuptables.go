package tablesmap

import (
	"sync"

	"github.com/mkabilov/logical_backup/pkg/tablebackup"
	"github.com/mkabilov/logical_backup/pkg/utils/dbutils"
)

// TablesMapInterface represents interface for the backup tablesMap map
type TablesMapInterface interface {
	Get(dbutils.OID) (tablebackup.TableBackuper, bool)
	Set(dbutils.OID, tablebackup.TableBackuper)
	Delete(dbutils.OID)
	Map(func(tablebackup.TableBackuper))
}

type tablesMap struct {
	sync.RWMutex
	data map[dbutils.OID]tablebackup.TableBackuper
}

// New instantiates backup table
func New() *tablesMap {
	return &tablesMap{
		RWMutex: sync.RWMutex{},
		data:    make(map[dbutils.OID]tablebackup.TableBackuper),
	}
}

// Get gets the table using OID
func (bt *tablesMap) Get(oid dbutils.OID) (result tablebackup.TableBackuper, ok bool) {
	bt.RLock()
	defer bt.RUnlock()

	result, ok = bt.data[oid]
	return
}

// Set stores table
func (bt *tablesMap) Set(oid dbutils.OID, t tablebackup.TableBackuper) {
	bt.Lock()
	defer bt.Unlock()

	bt.data[oid] = t
}

// Delete removes table with OID
func (bt *tablesMap) Delete(oid dbutils.OID) {
	bt.Lock()
	defer bt.Unlock()

	delete(bt.data, oid)
}

// Map maps function for stored table
func (bt *tablesMap) Map(fn func(t tablebackup.TableBackuper)) {
	bt.Lock()
	defer bt.Unlock()

	for _, t := range bt.data {
		fn(t)
	}
}

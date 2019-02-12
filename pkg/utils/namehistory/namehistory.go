package namehistory

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/utils"
	"github.com/ikitiki/logical_backup/pkg/utils/dbutils"
)

// Interface represents interface for the name history
type Interface interface {
	Save() error
	Load() error
	SetName(dbutils.OID, dbutils.LSN, message.NamespacedName)
}

type nameAtLSN struct {
	Name message.NamespacedName `yaml:"name"`
	LSN  dbutils.LSN            `yaml:"lsn"`
}

type nameHistory struct {
	isChanged bool
	entries   map[dbutils.OID][]nameAtLSN
	filepath  string
}

// New instantiates name history
func New(filepath string) *nameHistory {
	return &nameHistory{
		entries:  make(map[dbutils.OID][]nameAtLSN),
		filepath: filepath,
	}
}

// SetName sets name for the table with specified oid
func (n *nameHistory) SetName(oid dbutils.OID, lsn dbutils.LSN, name message.NamespacedName) {
	if tableHistory, ok := n.entries[oid]; !ok {
		n.entries[oid] = []nameAtLSN{{Name: name, LSN: lsn}}
		n.isChanged = true
	} else {
		if tableHistory[len(tableHistory)-1].Name != name {
			n.entries[oid] = append(n.entries[oid], nameAtLSN{Name: name, LSN: lsn})
			n.isChanged = true
		}
	}
}

//GetOID returns oid of the table at atLSN point
func (n *nameHistory) GetOID(name message.NamespacedName, atLSN dbutils.LSN) (dbutils.OID, dbutils.LSN) {
	var (
		recentName nameAtLSN
		recentOID  dbutils.OID
	)

	for tblOID, values := range n.entries {
		for _, v := range values {
			if v.Name != name || (v.LSN > atLSN && atLSN.IsValid()) {
				continue
			}

			if recentName.LSN < v.LSN || recentOID == dbutils.InvalidOID {
				recentName = v
				recentOID = tblOID
			}
		}
	}

	return recentOID, recentName.LSN
}

// Save saves history to the file
func (n *nameHistory) Save() error {
	if !n.isChanged {
		return nil
	}

	fp, err := os.OpenFile(n.filepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open %q file: %v", n.filepath, err)
	}
	defer fp.Close()

	if err := yaml.NewEncoder(fp).Encode(n.entries); err != nil {
		return fmt.Errorf("could not save table name history: %v", err)
	}

	if err := utils.SyncFileAndDirectory(fp); err != nil {
		return fmt.Errorf("could not sync oid to name map file: %v", err)
	}

	n.isChanged = false

	return nil
}

// Load loads history from file
func (n *nameHistory) Load() error {
	fp, err := os.OpenFile(n.filepath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open %q file: %v", n.filepath, err)
	}
	defer fp.Close()

	if err := yaml.NewDecoder(fp).Decode(&n.entries); err != nil {
		return fmt.Errorf("could not decode file: %v", err)
	}

	n.isChanged = false

	return nil
}

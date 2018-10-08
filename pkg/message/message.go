package message

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx"

	"github.com/ikitiki/logical_backup/pkg/dbutils"
)

const (
	ReplicaIdentityDefault ReplicaIdentity = 'd'
	ReplicaIdentityNothing                 = 'n'
	ReplicaIdentityIndex                   = 'i'
	ReplicaIdentityFull                    = 'f'

	NullValue    TupleKind = 'n' // null
	ToastedValue           = 'u' // unchanged column
	TextValue              = 't' // text formatted value
)

var replicaIdentities = map[ReplicaIdentity]string{
	ReplicaIdentityDefault: "default",
	ReplicaIdentityIndex:   "index",
	ReplicaIdentityNothing: "nothing",
	ReplicaIdentityFull:    "full",
}

type ReplicaIdentity uint8

type TupleKind uint8

type DumpInfo struct {
	StartLSN       string    `json:"LSN"`
	CreateDate     time.Time `json:"CreateDate"`
	Relation       Relation  `json:"Relation"`
	BackupDuration float64   `json:"BackupDuration"`
}

type Message interface {
	msg()
}

type DeltaMessage interface {
	GetSQL() string
	GetLSN() dbutils.Lsn
	GetRelationOID() dbutils.Oid
	GetTxId() int32
}

type DDLMessage struct {
	LastLSN     dbutils.Lsn
	TxId        int32
	RelationOID dbutils.Oid
	Origin      string
	Query       string
}

type DMLMessage struct {
	LastLSN     dbutils.Lsn
	TxId        int32
	RelationOID dbutils.Oid
	Origin      string
	Query       string
}

type NamespacedName struct {
	Namespace string
	Name      string
}

type Column struct {
	IsKey         bool        // column as part of the key.
	Name          string      // Name of the column.
	TypeOID       dbutils.Oid // OID of the column's data type.
	Mode          int32       // TypeOID modifier of the column (atttypmod).
	FormattedType string
}

type Tuple struct {
	Kind  TupleKind
	Value []byte
}

type Begin struct {
	Raw       []byte
	FinalLSN  dbutils.Lsn // LSN of the record that lead to this xact to be committed
	Timestamp time.Time   // Commit timestamp of the transaction
	XID       int32       // Xid of the transaction.
}

type Commit struct {
	Raw            []byte
	Flags          uint8       // Flags; currently unused (must be 0)
	LSN            dbutils.Lsn // The LastLSN of the commit.
	TransactionLSN dbutils.Lsn // LSN pointing to the end of the commit record + 1
	Timestamp      time.Time   // Commit timestamp of the transaction
}

type Origin struct {
	Raw  []byte
	LSN  dbutils.Lsn // The last LSN of the commit on the origin server.
	Name string
}

type Relation struct {
	NamespacedName

	Raw             []byte
	OID             dbutils.Oid     // OID of the relation.
	ReplicaIdentity ReplicaIdentity // Replica identity
	Columns         []Column        // Columns
}

type Insert struct {
	Raw         []byte
	RelationOID dbutils.Oid // OID of the relation corresponding to the OID in the relation message.
	IsNew       bool        // Identifies tuple as a new tuple.

	NewRow []Tuple
}

type Update struct {
	Raw         []byte
	RelationOID dbutils.Oid /// OID of the relation corresponding to the OID in the relation message.
	IsKey       bool        // OldRow contains columns which are part of REPLICA IDENTITY index.
	IsOld       bool        // OldRow contains old tuple in case of REPLICA IDENTITY set to FULL
	IsNew       bool        // Identifies tuple as a new tuple.

	OldRow []Tuple
	NewRow []Tuple
}

type Delete struct {
	Raw         []byte
	RelationOID dbutils.Oid // OID of the relation corresponding to the OID in the relation message.
	IsKey       bool        // OldRow contains columns which are part of REPLICA IDENTITY index.
	IsOld       bool        // OldRow contains old tuple in case of REPLICA IDENTITY set to FULL

	OldRow []Tuple
}

type Truncate struct {
	Raw             []byte
	Relations       uint32
	Cascade         bool
	RestartIdentity bool
	RelationOIDs    []dbutils.Oid
}

type Type struct {
	Raw       []byte
	ID        dbutils.Oid // OID of the data type
	Namespace string      // Namespace (empty string for pg_catalog).
	Name      string      // Name of the data type
}

func (Begin) msg()    {}
func (Relation) msg() {}
func (Update) msg()   {}
func (Insert) msg()   {}
func (Delete) msg()   {}
func (Commit) msg()   {}
func (Origin) msg()   {}
func (Type) msg()     {}
func (Truncate) msg() {}

func (tr Truncate) SQL() string {
	//TODO
	return ""
}

func (ins Insert) SQL(rel Relation) string {
	values := make([]string, 0)
	names := make([]string, 0)
	for i, v := range rel.Columns {
		names = append(names, pgx.Identifier{v.Name}.Sanitize())
		if ins.NewRow[i].Kind == TextValue {
			values = append(values, dbutils.QuoteLiteral(string(ins.NewRow[i].Value)))
		} else if ins.NewRow[i].Kind == NullValue {
			values = append(values, "null")
		}
	}

	return fmt.Sprintf("insert into %s (%s) values (%s);",
		rel.Sanitize(),
		strings.Join(names, ", "),
		strings.Join(values, ", "))
}

func (upd Update) SQL(rel Relation) string {
	values := make([]string, 0)
	cond := make([]string, 0)

	for i, v := range rel.Columns {
		if upd.NewRow[i].Kind == NullValue {
			values = append(values, fmt.Sprintf("%s = null", pgx.Identifier{string(v.Name)}.Sanitize()))
		} else if upd.NewRow[i].Kind == TextValue {
			values = append(values, fmt.Sprintf("%s = %s",
				pgx.Identifier{string(v.Name)}.Sanitize(),
				dbutils.QuoteLiteral(string(upd.NewRow[i].Value))))
		}

		if upd.IsKey || upd.IsOld {
			if upd.OldRow[i].Kind == TextValue {
				cond = append(cond, fmt.Sprintf("%s = %s",
					pgx.Identifier{string(v.Name)}.Sanitize(),
					dbutils.QuoteLiteral(string(upd.OldRow[i].Value))))
			} else if upd.OldRow[i].Kind == NullValue {
				cond = append(cond, fmt.Sprintf("%s is null", pgx.Identifier{string(v.Name)}.Sanitize()))
			}
		} else {
			if upd.NewRow[i].Kind == TextValue && v.IsKey {
				cond = append(cond, fmt.Sprintf("%s = %s",
					pgx.Identifier{string(v.Name)}.Sanitize(),
					dbutils.QuoteLiteral(string(upd.NewRow[i].Value))))
			} else if upd.NewRow[i].Kind == NullValue && v.IsKey {
				cond = append(cond, fmt.Sprintf("%s is null", pgx.Identifier{string(v.Name)}.Sanitize()))
			}
		}
	}

	sql := fmt.Sprintf("update %s set %s", rel.Sanitize(), strings.Join(values, ", "))
	if len(cond) > 0 {
		sql += " where " + strings.Join(cond, " and ")
	}
	sql += ";"

	return sql
}

func (del Delete) SQL(rel Relation) string {
	cond := make([]string, 0)
	for i, v := range rel.Columns {
		if del.OldRow[i].Kind == TextValue {
			cond = append(cond, fmt.Sprintf("%s = %s",
				pgx.Identifier{string(v.Name)}.Sanitize(),
				dbutils.QuoteLiteral(string(del.OldRow[i].Value))))
		}
	}

	return fmt.Sprintf("delete from %s where %s;", rel.Sanitize(), strings.Join(cond, " and "))
}

func (rel Relation) SQL(oldRel Relation) string {
	sqlCommands := make([]string, 0)

	quotedTableName := pgx.Identifier{rel.Namespace, rel.Name}.Sanitize()
	oldTableName := pgx.Identifier{rel.Namespace, rel.Name}.Sanitize()

	if oldTableName != quotedTableName {
		sqlCommands = append(sqlCommands, fmt.Sprintf("alter table %s rename to %s", oldTableName, quotedTableName))
	}

	newColumns := make([]Column, 0)
	deletedColumns := make(map[string]Column)
	alteredColumns := make(map[Column]Column)
	for _, c := range oldRel.Columns {
		deletedColumns[c.Name] = c
	}

	for id, col := range rel.Columns {
		oldCol, ok := deletedColumns[col.Name]
		if !ok {
			newColumns = append(newColumns, col)
			break
		}
		if oldCol.TypeOID != col.TypeOID || oldCol.Mode != col.Mode {
			alteredColumns[oldCol] = rel.Columns[id]
		}

		delete(deletedColumns, col.Name)
	}

	for _, col := range deletedColumns {
		sqlCommands = append(sqlCommands, fmt.Sprintf("alter table %s drop column %s;", quotedTableName, pgx.Identifier{col.Name}.Sanitize()))
	}

	for _, col := range newColumns {
		typMod := "NULL"
		if col.Mode > 0 {
			typMod = fmt.Sprintf("%d", col.Mode)
		}
		sqlCommands = append(sqlCommands,
			fmt.Sprintf(`do $_$begin execute 'alter table %s add column %s ' || format_type(%d, %s); end$_$;`,
				quotedTableName, pgx.Identifier{col.Name}.Sanitize(), col.TypeOID, typMod))
	}

	for oldCol, newCol := range alteredColumns {
		typMod := "NULL"
		if newCol.Mode > 0 {
			typMod = fmt.Sprintf("%d", newCol.Mode)
		}

		sqlCommands = append(sqlCommands,
			fmt.Sprintf(`do $_$begin execute 'alter table %s alter column %s type ' || format_type(%d, %s); end$_$;`,
				quotedTableName, pgx.Identifier{oldCol.Name}.Sanitize(), newCol.TypeOID, typMod))
	}

	if oldRel.ReplicaIdentity != rel.ReplicaIdentity {
		sqlCommands = append(sqlCommands, fmt.Sprintf("alter table %s replica identity %s;", quotedTableName, replicaIdentities[rel.ReplicaIdentity]))
	}

	return strings.Join(sqlCommands, " ")
}

func (m DMLMessage) GetSQL() string {
	return m.Query
}

func (m DMLMessage) GetLSN() dbutils.Lsn {
	return m.LastLSN
}

func (m DMLMessage) GetRelationOID() dbutils.Oid {
	return m.RelationOID
}

func (m DMLMessage) GetTxId() int32 {
	return m.TxId
}

func (m DDLMessage) GetSQL() string {
	return m.Query
}

func (m DDLMessage) GetLSN() dbutils.Lsn {
	return m.LastLSN
}

func (m DDLMessage) GetRelationOID() dbutils.Oid {
	return m.RelationOID
}

func (m DDLMessage) GetTxId() int32 {
	return m.TxId
}

func (r ReplicaIdentity) String() string {
	if name, ok := replicaIdentities[r]; !ok {
		return replicaIdentities[ReplicaIdentityDefault]
	} else {
		return name
	}
}

func (r ReplicaIdentity) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, r)), nil
}

func (r *ReplicaIdentity) UnmarshalJSON(b []byte) error {
	val := string(b)
	for k, v := range replicaIdentities {
		if val == fmt.Sprintf(`"%v"`, v) {
			*r = k
			return nil
		}
	}

	return fmt.Errorf("unknown replica identity")
}

func (t TupleKind) String() string {
	switch t {
	case NullValue:
		return "null"
	case ToastedValue:
		return "toasted"
	case TextValue:
		return "text"
	}

	return "unknown"
}

func (i NamespacedName) String() string {
	return pgx.Identifier{i.Namespace, i.Name}.Sanitize()
}

func (i NamespacedName) Sanitize() string {
	return pgx.Identifier{i.Namespace, i.Name}.Sanitize()
}

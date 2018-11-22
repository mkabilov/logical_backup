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
	StartLSN       string    `yaml:"StartLSN"`
	CreateDate     time.Time `yaml:"CreateDate"`
	Relation       Relation  `yaml:"Relation"`
	BackupDuration float64   `yaml:"BackupDuration"`
}

type Message interface {
	fmt.Stringer

	MsgType() string
}

type DeltaMessage interface {
	GetSQL() string
	GetLSN() dbutils.LSN
	GetRelationOID() dbutils.OID
	GetTxId() int32
}

type DDLMessage struct {
	LastLSN     dbutils.LSN
	TxId        int32
	RelationOID dbutils.OID
	Origin      string
	Query       string
}

type DMLMessage struct {
	LastLSN     dbutils.LSN
	TxId        int32
	RelationOID dbutils.OID
	Origin      string
	Query       string
}

type NamespacedName struct {
	Namespace string `yaml:"Namespace"`
	Name      string `yaml:"Name"`
}

type Column struct {
	IsKey         bool        `yaml:"IsKey"`         // column as part of the key.
	Name          string      `yaml:"Name"`          // Name of the column.
	TypeOID       dbutils.OID `yaml:"OID"`           // OID of the column's data type.
	Mode          int32       `yaml:"Mode"`          // OID modifier of the column (atttypmod).
	FormattedType string      `yaml:"FormattedType"` //
}

type Tuple struct {
	Kind  TupleKind
	Value []byte
}

type Begin struct {
	Raw       []byte
	FinalLSN  dbutils.LSN // LSN of the record that lead to this xact to be committed
	Timestamp time.Time   // Commit timestamp of the transaction
	XID       int32       // Xid of the transaction.
}

type Commit struct {
	Raw            []byte
	Flags          uint8       // Flags; currently unused (must be 0)
	LSN            dbutils.LSN // The LastLSN of the commit.
	TransactionLSN dbutils.LSN // LSN pointing to the end of the commit record + 1
	Timestamp      time.Time   // Commit timestamp of the transaction
}

type Origin struct {
	Raw  []byte
	LSN  dbutils.LSN // The last LSN of the commit on the origin server.
	Name string
}

type Relation struct {
	NamespacedName `yaml:"NamespacedName"`

	Raw             []byte          `yaml:"-"`
	OID             dbutils.OID     `yaml:"OID"`             // OID of the relation.
	ReplicaIdentity ReplicaIdentity `yaml:"ReplicaIdentity"` // Replica identity
	Columns         []Column        `yaml:"Columns"`         // Columns
}

type Insert struct {
	Raw         []byte
	RelationOID dbutils.OID // OID of the relation corresponding to the OID in the relation message.
	IsNew       bool        // Identifies tuple as a new tuple.

	NewRow []Tuple
}

type Update struct {
	Raw         []byte
	RelationOID dbutils.OID /// OID of the relation corresponding to the OID in the relation message.
	IsKey       bool        // OldRow contains columns which are part of REPLICA IDENTITY index.
	IsOld       bool        // OldRow contains old tuple in case of REPLICA IDENTITY set to FULL
	IsNew       bool        // Identifies tuple as a new tuple.

	OldRow []Tuple
	NewRow []Tuple
}

type Delete struct {
	Raw         []byte
	RelationOID dbutils.OID // OID of the relation corresponding to the OID in the relation message.
	IsKey       bool        // OldRow contains columns which are part of REPLICA IDENTITY index.
	IsOld       bool        // OldRow contains old tuple in case of REPLICA IDENTITY set to FULL

	OldRow []Tuple
}

type Truncate struct {
	Raw             []byte
	Cascade         bool
	RestartIdentity bool
	RelationOIDs    []dbutils.OID
}

type Type struct {
	NamespacedName

	Raw []byte
	OID dbutils.OID // OID of the data type
}

func (Begin) MsgType() string    { return "begin" }
func (Relation) MsgType() string { return "relation" }
func (Update) MsgType() string   { return "update" }
func (Insert) MsgType() string   { return "insert" }
func (Delete) MsgType() string   { return "delete" }
func (Commit) MsgType() string   { return "commit" }
func (Origin) MsgType() string   { return "origin" }
func (Type) MsgType() string     { return "type" }
func (Truncate) MsgType() string { return "truncate" }

func (t Tuple) String() string {
	switch t.Kind {
	case TextValue:
		return dbutils.QuoteLiteral(string(t.Value))
	case NullValue:
		return "null"
	case ToastedValue:
		return "[toasted value]"
	}

	return "unknown"
}

func (m Begin) String() string {
	return fmt.Sprintf("FinalLSN:%s Timestamp:%v XID:%d",
		m.FinalLSN.String(), m.Timestamp.Format(time.RFC3339), m.XID)
}

func (m Relation) String() string {
	parts := make([]string, 0)

	parts = append(parts, fmt.Sprintf("OID:%s", m.OID))
	parts = append(parts, fmt.Sprintf("Name:%s", m.NamespacedName))
	parts = append(parts, fmt.Sprintf("RepIdentity:%s", m.ReplicaIdentity))

	columns := make([]string, 0)
	for _, c := range m.Columns {
		var isKey, mode string

		if c.IsKey {
			isKey = " key"
		}

		if c.Mode != -1 {
			mode = fmt.Sprintf("atttypmod:%v", c.Mode)
		}
		colStr := fmt.Sprintf("%q (type:%s)%s%s", c.Name, c.TypeOID, isKey, mode)
		columns = append(columns, colStr)
	}

	parts = append(parts, fmt.Sprintf("Columns:[%s]", strings.Join(columns, ", ")))

	return strings.Join(parts, " ")
}

func (m Update) String() string {
	parts := make([]string, 0)
	newValues := make([]string, 0)
	oldValues := make([]string, 0)

	parts = append(parts, fmt.Sprintf("relOID:%s", m.RelationOID))

	if m.IsKey {
		parts = append(parts, "key")
	}
	if m.IsOld {
		parts = append(parts, "old")
	}
	if m.IsNew {
		parts = append(parts, "new")
	}

	for _, r := range m.NewRow {
		newValues = append(newValues, r.String())
	}

	for _, r := range m.OldRow {
		oldValues = append(oldValues, r.String())
	}

	if len(newValues) > 0 {
		parts = append(parts, fmt.Sprintf("newValues:[%s]", strings.Join(newValues, ", ")))
	}

	if len(oldValues) > 0 {
		parts = append(parts, fmt.Sprintf("oldValues:[%s]", strings.Join(oldValues, ", ")))
	}

	return strings.Join(parts, " ")
}

func (m Insert) String() string {
	parts := make([]string, 0)
	newValues := make([]string, 0)

	parts = append(parts, fmt.Sprintf("relOID:%s", m.RelationOID))

	if m.IsNew {
		parts = append(parts, "new")
	}

	for _, r := range m.NewRow {
		newValues = append(newValues, r.String())
	}

	if len(newValues) > 0 {
		parts = append(parts, fmt.Sprintf("values:[%s]", strings.Join(newValues, ", ")))
	}

	return strings.Join(parts, " ")
}

func (m Delete) String() string {
	parts := make([]string, 0)
	oldValues := make([]string, 0)

	parts = append(parts, fmt.Sprintf("relOID:%s", m.RelationOID))

	if m.IsKey {
		parts = append(parts, "key")
	}
	if m.IsOld {
		parts = append(parts, "old")
	}

	for _, r := range m.OldRow {
		oldValues = append(oldValues, r.String())
	}

	if len(oldValues) > 0 {
		parts = append(parts, fmt.Sprintf("oldValues:[%s]", strings.Join(oldValues, ", ")))
	}

	return strings.Join(parts, " ")
}

func (m Commit) String() string {
	return fmt.Sprintf("LSN:%s Timestamp:%v TxLSN:%s",
		m.LSN, m.Timestamp.Format(time.RFC3339), m.TransactionLSN)
}

func (m Origin) String() string {
	return fmt.Sprintf("LSN:%s Name:%s", m.LSN, m.Name)
}

func (m Type) String() string {
	return fmt.Sprintf("OID:%s Name:%s", m.OID, m.NamespacedName)
}

func (m Truncate) String() string {
	parts := make([]string, 0)
	oids := make([]string, 0)

	if m.Cascade {
		parts = append(parts, "cascade")
	}

	if m.RestartIdentity {
		parts = append(parts, "restart_identity")
	}

	for _, oid := range m.RelationOIDs {
		oids = append(oids, oid.String())
	}

	if len(oids) > 0 {
		parts = append(parts, fmt.Sprintf("tableOids:[%s]", strings.Join(oids, ", ")))
	}

	return strings.Join(parts, " ")
}

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

func (m DMLMessage) GetLSN() dbutils.LSN {
	return m.LastLSN
}

func (m DMLMessage) GetRelationOID() dbutils.OID {
	return m.RelationOID
}

func (m DMLMessage) GetTxId() int32 {
	return m.TxId
}

func (m DDLMessage) GetSQL() string {
	return m.Query
}

func (m DDLMessage) GetLSN() dbutils.LSN {
	return m.LastLSN
}

func (m DDLMessage) GetRelationOID() dbutils.OID {
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

func (r ReplicaIdentity) MarshalYAML() (interface{}, error) {
	return replicaIdentities[r], nil
}

func (r *ReplicaIdentity) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}

	for k, v := range replicaIdentities {
		if val == v {
			*r = k
			return nil
		}
	}

	return fmt.Errorf("unknown replica identity: %q", val)
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

func (n NamespacedName) String() string {
	if n.Namespace == "public" {
		return n.Name
	}

	return strings.Join([]string{n.Namespace, n.Name}, ".")
}

func (n NamespacedName) Sanitize() string {
	return pgx.Identifier{n.Namespace, n.Name}.Sanitize()
}

func (r Relation) Structure() string {
	result := fmt.Sprintf("%s.%s (OID: %v)", r.Namespace, r.Name, r.OID)

	cols := make([]string, 0)
	for _, c := range r.Columns {
		cols = append(cols, fmt.Sprintf("%s(%s)", c.Name, c.FormattedType))
	}

	if len(cols) > 0 {
		result += fmt.Sprintf(" Columns: %s", strings.Join(cols, ", "))
	}

	return result
}

package tablebackup

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"

	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/message"
)

func initPostgresql(conn *pgx.Conn) (*pgtype.ConnInfo, error) {
	const (
		namedOIDQuery = `select t.oid,
	case when nsp.nspname in ('pg_catalog', 'public') then t.typname
		else nsp.nspname||'.'||t.typname
	end
from pg_type t
left join pg_type base_type on t.typelem=base_type.oid
left join pg_namespace nsp on t.typnamespace=nsp.oid
where (
	  t.typtype in('b', 'p', 'r', 'e')
	  and (base_type.oid is null or base_type.typtype in('b', 'p', 'r'))
	)`
	)

	nameOIDs, err := connInfoFromRows(conn.Query(namedOIDQuery))
	if err != nil {
		return nil, err
	}

	cinfo := pgtype.NewConnInfo()
	cinfo.InitializeDataTypes(nameOIDs)

	if err = initConnInfoEnumArray(conn, cinfo); err != nil {
		return nil, err
	}

	return cinfo, nil
}

// initConnInfoEnumArray introspects for arrays of enums and registers a data type for them.
func initConnInfoEnumArray(conn *pgx.Conn, cinfo *pgtype.ConnInfo) error {
	nameOIDs := make(map[string]pgtype.OID, 16)
	rows, err := conn.Query(`select t.oid, t.typname
from pg_type t
  join pg_type base_type on t.typelem=base_type.oid
where t.typtype = 'b'
  and base_type.typtype = 'e'`)
	if err != nil {
		return err
	}

	for rows.Next() {
		var oid pgtype.OID
		var name pgtype.Text
		if err := rows.Scan(&oid, &name); err != nil {
			return err
		}

		nameOIDs[name.String] = oid
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	for name, oid := range nameOIDs {
		cinfo.RegisterDataType(pgtype.DataType{
			Value: &pgtype.EnumArray{},
			Name:  name,
			OID:   oid,
		})
	}

	return nil
}

func connInfoFromRows(rows *pgx.Rows, err error) (map[string]pgtype.OID, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nameOIDs := make(map[string]pgtype.OID, 256)
	for rows.Next() {
		var oid pgtype.OID
		var name pgtype.Text
		if err = rows.Scan(&oid, &name); err != nil {
			return nil, err
		}

		nameOIDs[name.String] = oid
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return nameOIDs, err
}

func FetchRelationInfo(tx *pgx.Tx, tbl message.NamespacedName) (message.Relation, error) {
	var (
		rel            message.Relation
		relOid         dbutils.OID
		relRepIdentity pgtype.BPChar
	)

	row := tx.QueryRow(`
		SELECT c.oid, c.relreplident::char 
		FROM pg_catalog.pg_class c
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'`,
		tbl.Namespace,
		tbl.Name)

	if err := row.Scan(&relOid, &relRepIdentity); err != nil {
		return rel, fmt.Errorf("could not fetch table info: %v", err)
	}

	rows, err := tx.Query(`
	SELECT a.attname, a.atttypid, a.atttypmod, format_type(a.atttypid, a.atttypmod)
	FROM pg_catalog.pg_attribute a  
	WHERE a.attrelid = $1 AND a.attnum > 0 AND a.attisdropped = false
	ORDER BY a.attnum`, relOid)
	if err != nil {
		return rel, fmt.Errorf("could not exec query: %v", err)
	}
	columns := make([]message.Column, 0)

	for rows.Next() {
		var (
			name       string
			attType    uint32
			typMod     int32
			formatType string
		)
		if err := rows.Scan(&name, &attType, &typMod, &formatType); err != nil {
			return rel, fmt.Errorf("could not scan row: %v", err)
		}
		columns = append(columns, message.Column{
			IsKey:         false,
			Name:          name,
			TypeOID:       dbutils.OID(attType),
			Mode:          typMod,
			FormattedType: formatType,
		})
	}

	rel.Namespace = tbl.Namespace
	rel.Name = tbl.Name
	rel.Columns = columns
	rel.ReplicaIdentity = message.ReplicaIdentity(relRepIdentity.String[0])
	rel.OID = relOid

	return rel, nil
}

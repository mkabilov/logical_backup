package dbutils

import (
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

type Lsn uint64

func (l Lsn) String() string {
	return pgx.FormatLSN(uint64(l))
}

func (l *Lsn) Parse(lsn string) error {
	tmp, err := pgx.ParseLSN(lsn)
	if err != nil {
		return err

	}
	*l = (Lsn)(tmp)
	return nil
}

type Oid uint32

// implement the Scanner interface in order to allow pgx to read Oid values from the DB.
func (o *Oid) Scan(src interface{}) error {
	var result pgtype.OID
	if err := result.Scan(src); err != nil {
		return err
	}
	*o = Oid(result)
	return nil
}

const (
	InvalidOid Oid = 0
	InvalidLsn Lsn = 0
)

func QuoteLiteral(str string) string {
	needsEscapeChar := false
	res := ""
	for _, r1 := range str {
		switch r1 {
		case '\\':
			res += `\\`
		case '\t':
			res += `\t`
			needsEscapeChar = true
		case '\r':
			res += `\r`
			needsEscapeChar = true
		case '\n':
			res += `\n`
			needsEscapeChar = true
		default:
			res += string(r1)
		}
	}

	if needsEscapeChar {
		return `E'` + res + `'`
	} else {
		return `'` + res + `'`
	}
}

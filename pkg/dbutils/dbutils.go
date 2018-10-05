package dbutils

import (
	"github.com/jackc/pgx"
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

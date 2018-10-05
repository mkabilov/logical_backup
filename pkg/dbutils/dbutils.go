package dbutils

import (
	"context"
	"fmt"

	"log"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

const (
	lockTimeoutCode = "55P03"
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

const InvalidLsn Lsn = 0

type Oid uint32

var MaxRetriesErr = errors.New("reached max retries")

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

func RepeatedlyTry(ctx context.Context, conn *pgx.Conn, timeoutSec, retries int, sql string, arguments ...interface{}) error {
	for i := 0; i < retries; i++ {
		log.Printf("try %d: %q", i, sql)

		tx, err := conn.Begin()
		if err != nil {
			return fmt.Errorf("could not start transaction: %v", err)
		}

		if _, err := tx.Exec(fmt.Sprintf(`set lock_timeout to '%d s'`, timeoutSec)); err != nil {
			tx.Rollback()
			return fmt.Errorf("could not set lock_timeout: %v", err)
		}

		select {
		case <-ctx.Done():
			tx.Rollback()
			return nil
		default:
		}

		_, err = tx.Exec(sql, arguments...)
		if err != nil {
			pgxErr, ok := err.(pgx.PgError)
			if !ok {
				tx.Rollback()
				return err
			}

			if pgxErr.Code != lockTimeoutCode {
				tx.Rollback()
				return err
			}
		} else {
			tx.Commit()
			return nil
		}

		tx.Rollback()
	}

	return MaxRetriesErr
}

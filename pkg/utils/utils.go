package utils

import (
	"fmt"
	"github.com/ikitiki/logical_backup/pkg/message"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/ikitiki/logical_backup/pkg/dbutils"
)

func TableDir(tbl message.NamespacedName, oid dbutils.Oid) string {

	if oid == dbutils.InvalidOid {
		panic(fmt.Sprintf("requested table directories for the table %s with invalid oid", tbl))
	}

	tblOidBytes := fmt.Sprintf("%08x", oid)

	return path.Join(tblOidBytes[6:8], tblOidBytes[4:6], tblOidBytes[2:4], fmt.Sprintf("%d", oid))
}

// Retry retries a given function until either it returns false, which indicates success, or the number of attempts
// reach the limit, or the global timeout is reached. The cool-off period between attempts is passed as well.
func Retry(fn func() (bool, error), numberOfAttempts int, timeBetweenAttempts time.Duration, totalTimeout time.Duration) error {

	var (
		globalTicker = time.NewTicker(totalTimeout)
		fail         = true
		timeout      bool
		err          error
	)

	for i := 0; i < numberOfAttempts; i++ {
		if fail, err = fn(); err != nil || !fail {
			break
		}
		select {
		case <-time.After(timeBetweenAttempts):
		case <-globalTicker.C:
			timeout = true
			break
		}
	}
	if !fail || err != nil {
		return err
	}
	if timeout {
		return fmt.Errorf("did not succeed after %s", totalTimeout)
	}
	return fmt.Errorf("did not succeed after %d attempts", numberOfAttempts)
}

func GetLSNFromDeltaFilename(filename string) (dbutils.Lsn, error) {
	lsnStr := filename
	if strings.Contains(filename, ".") {
		parts := strings.Split(filename, ".")
		lsnStr = parts[0]
	}

	lsn, err := strconv.ParseUint(lsnStr, 16, 64)
	return dbutils.Lsn(lsn), err
}

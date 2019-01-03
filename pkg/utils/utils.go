package utils

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ikitiki/logical_backup/pkg/dbutils"
)

func TableDir(oid dbutils.OID) string {
	tblOidBytes := fmt.Sprintf("%08x", uint32(oid))

	return path.Join(tblOidBytes[6:8], tblOidBytes[4:6], tblOidBytes[2:4], fmt.Sprintf("%d", oid))
}

// Retry retries a given function until either it returns false, which indicates success, or the number of attempts
// reach the limit, or the global timeout is reached. The cool-off period between attempts is passed as well.
// The cancellation should generally be handled outside of either this function. In other words, if total time is set
// to a significantly large value, there should be an external mechanism to terminate the routine to be retried with an
// error.
func Retry(fn func() (bool, error), numberOfAttempts int, timeBetweenAttempts time.Duration, totalTimeout time.Duration) error {
	var (
		globalTicker *time.Ticker
		fail         = true
		timeout      bool
		err          error
	)
	if totalTimeout > 0 {
		globalTicker = time.NewTicker(totalTimeout)
		defer globalTicker.Stop()
	}
loop:
	for i := 0; i < numberOfAttempts; i++ {
		if fail, err = fn(); err != nil || !fail {
			break
		}
		if totalTimeout > 0 && globalTicker != nil {
			select {
			case <-time.After(timeBetweenAttempts):
			case <-globalTicker.C:
				timeout = true
				break loop
			}
		} else {
			select {
			case <-time.After(timeBetweenAttempts):
			}
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

func GetLSNFromDeltaFilename(filename string) (dbutils.LSN, error) {
	lsnStr := filename
	if strings.Contains(filename, ".") {
		parts := strings.Split(filename, ".")
		lsnStr = parts[0]
	}

	lsn, err := strconv.ParseUint(lsnStr, 16, 64)

	return dbutils.LSN(lsn), err
}

func SyncFileAndDirectory(fp *os.File) error {
	if err := fp.Sync(); err != nil {
		return fmt.Errorf("could not sync file %s: %v", fp.Name(), err)
	}

	parentDir := filepath.Dir(fp.Name())

	// sync the directory entry
	dp, err := os.Open(parentDir)
	if err != nil {
		return fmt.Errorf("could not open directory %s to sync: %v", parentDir, err)
	}
	defer dp.Close()

	if err := dp.Sync(); err != nil {
		return fmt.Errorf("could not sync directory %s: %v", parentDir, err)
	}

	return nil
}

func Ternary(cond bool, ifTrue, ifFalse interface{}) interface{} {
	if cond {
		return ifTrue
	}
	return ifFalse
}

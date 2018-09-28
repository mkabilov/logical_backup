package utils

import (
	"crypto/md5"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/ikitiki/logical_backup/pkg/message"
)

func TableDir(tbl message.Identifier) string {
	tblHash := fmt.Sprintf("%x", md5.Sum([]byte(tbl.Sanitize())))

	// TODO: consider removing tblHash altogether to shorten the path
	return path.Join(tblHash[0:2], tblHash[2:4], tblHash[4:6], tblHash, fmt.Sprintf("%s.%s", tbl.Namespace, tbl.Name))
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

func GetLSNFromDeltaFilename(filename string) (lsn uint64, err error) {
	lsnStr := filename
	if strings.Contains(filename, ".") {
		parts := strings.Split(filename, ".")
		lsnStr = parts[0]
	}

	lsn, err = strconv.ParseUint(lsnStr, 16, 64)
	return
}

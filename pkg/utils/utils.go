package utils

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mkabilov/logical_backup/pkg/utils/dbutils"
)

func TableDir(oid dbutils.OID) string {
	tblOidBytes := fmt.Sprintf("%08x", uint32(oid))

	return path.Join(tblOidBytes[6:8], tblOidBytes[4:6], tblOidBytes[2:4], fmt.Sprintf("%d", oid))
}

// Retry retries a given function until either it returns true, which indicates success, or the number of attempts
// reach the limit, or the global timeout is reached. The cool-off period between attempts is passed as well.
// The cancellation should generally be handled outside of either this function. In other words, if total time is set
// to a significantly large value, there should be an external mechanism to terminate the routine to be retried with an
// error.
func Retry(fn func() (bool, error), numberOfAttempts int, timeBetweenAttempts time.Duration, totalTimeout time.Duration) error {
	var (
		globalTicker *time.Ticker
		stop         bool
		timeout      bool
		err          error
	)
	if totalTimeout > 0 {
		globalTicker = time.NewTicker(totalTimeout)
		defer globalTicker.Stop()
	}
loop:
	for i := 0; i < numberOfAttempts; i++ {
		if stop, err = fn(); err != nil || stop {
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
			<-time.After(timeBetweenAttempts)
		}
	}
	if stop || err != nil {
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

func CopyFile(src, dst string, fsync bool) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	srcFp, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer srcFp.Close()

	dstFp, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer dstFp.Close()

	nBytes, err := io.Copy(dstFp, srcFp)
	if err != nil {
		return nBytes, fmt.Errorf("could not copy %s to %s: %v", src, dst, err)
	}

	if fsync {
		if err := SyncFileAndDirectory(dstFp); err != nil {
			return nBytes, fmt.Errorf("could not sync %s: %v", dst, err)
		}
	}

	return nBytes, nil
}

func CreateDirs(dirs ...string) error {
	for _, dir := range dirs {
		if dir == "" {
			continue
		}

		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.Mkdir(dir, os.ModePerm); err != nil {
				return fmt.Errorf("could not create %q dir: %v", dir, err)
			}
		} else if err != nil {
			return fmt.Errorf("%q stat error: %v", dir, err)
		}
	}

	return nil
}

func FirstExistingFile(filepaths ...string) string {
	for _, file := range filepaths {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			continue
		}

		return file
	}

	return ""
}

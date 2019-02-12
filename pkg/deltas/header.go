package deltas

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/ikitiki/logical_backup/pkg/utils/dbutils"
)

type header struct {
	checksum uint32 //TODO

	minLSN      dbutils.LSN
	maxLSN      dbutils.LSN
	minTime     time.Time
	maxTime     time.Time
	messagesCnt uint32
}

func (h header) write(fp *os.File) error {
	if err := writeUint32(fp, h.checksum); err != nil {
		return fmt.Errorf("could not write checksum: %v", err)
	}

	if err := writeUint64(fp, uint64(h.minLSN)); err != nil {
		return fmt.Errorf("could not write startLSN: %v", err)
	}

	if err := writeUint64(fp, uint64(h.maxLSN)); err != nil {
		return fmt.Errorf("could not write endLSN: %v", err)
	}

	if err := writeUint64(fp, uint64(h.minTime.Unix())); err != nil {
		return fmt.Errorf("could not write startTime: %v", err)
	}

	if err := writeUint64(fp, uint64(h.maxTime.Unix())); err != nil {
		return fmt.Errorf("could not write endTime: %v", err)
	}

	if err := writeUint32(fp, h.messagesCnt); err != nil {
		return fmt.Errorf("could not write message count: %v", err)
	}

	return nil
}

func (h *header) read(fp *os.File) error {
	buf := make([]byte, 8)

	if _, err := fp.Read(buf[:4]); err != nil {
		return fmt.Errorf("could not read from file: %v", err)
	}
	h.checksum = binary.BigEndian.Uint32(buf[:4])

	if _, err := fp.Read(buf); err != nil {
		return fmt.Errorf("could not read from file: %v", err)
	}
	h.minLSN = dbutils.LSN(binary.BigEndian.Uint64(buf))

	if _, err := fp.Read(buf); err != nil {
		return fmt.Errorf("could not read from file: %v", err)
	}
	h.maxLSN = dbutils.LSN(binary.BigEndian.Uint64(buf))

	if _, err := fp.Read(buf); err != nil {
		return fmt.Errorf("could not read from file: %v", err)
	}
	h.minTime = time.Unix(int64(binary.BigEndian.Uint64(buf)), 0)

	if _, err := fp.Read(buf); err != nil {
		return fmt.Errorf("could not read from file: %v", err)
	}
	h.maxTime = time.Unix(int64(binary.BigEndian.Uint64(buf)), 0)

	if _, err := fp.Read(buf[:4]); err != nil {
		return fmt.Errorf("could not read from file: %v", err)
	}
	h.messagesCnt = binary.BigEndian.Uint32(buf[:4])

	return nil
}

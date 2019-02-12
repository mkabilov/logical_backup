package deltas

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/ikitiki/logical_backup/pkg/decoder"

	"github.com/pkg/errors"

	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/utils"
	"github.com/ikitiki/logical_backup/pkg/utils/dbutils"
)

//DirName represents directory name for the deltas
const DirName = "deltas"

// EmptyBuffer represents empty message buffer error
var EmptyBuffer = errors.New("Empty buffer")

// MessageCollector represents interface
type MessageCollector interface {
	AddMessage(message.Message)
	Save() (filename string, minLSN, maxLSN dbutils.LSN, err error)
	MessageCnt() uint32
	LastMessageTime() time.Time
	GetMessage() (message.Message, error)
	Load(filename string) error
	Close() error
}

// deltas represents storage for delta messages
type deltas struct {
	header
	fp    *os.File
	mutex sync.RWMutex

	tableDir    string
	buffer      *bytes.Buffer
	prevMinLSN  dbutils.LSN // min lsn of the previous delta file
	txLSN       dbutils.LSN // end lsn of the transaction
	lastMsgTime time.Time
	txTime      time.Time
	postfix     int
	fsync       bool
	buf         []byte
}

// New instantiates deltas
func New(tableDir string, fsync bool) *deltas {
	return &deltas{
		tableDir: tableDir,
		buffer:   &bytes.Buffer{},
		fsync:    fsync,
		buf:      make([]byte, 8),
	}
}

// AddMessage adds message to a buffer
func (d *deltas) AddMessage(msg message.Message) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	raw := msg.RawData()

	if msg, ok := msg.(message.Begin); ok {
		d.txLSN = msg.FinalLSN
		d.txTime = msg.Timestamp
	}

	if d.txLSN.IsValid() {
		if d.txLSN < d.minLSN || d.minLSN == dbutils.InvalidLSN {
			d.minLSN = d.txLSN
		}

		if d.txLSN > d.maxLSN || d.maxLSN == dbutils.InvalidLSN {
			d.maxLSN = d.txLSN
		}

		if d.txTime.Before(d.minTime) || d.minTime.IsZero() {
			d.minTime = d.txTime
		}

		if d.txTime.After(d.maxTime) || d.maxTime.IsZero() {
			d.maxTime = d.txTime
		}
	}
	d.messagesCnt++

	binary.BigEndian.PutUint64(d.buf, uint64(len(raw)+8))
	d.buffer.Write(d.buf)
	d.buffer.Write(raw)

	d.lastMsgTime = time.Now()
}

//GetMessage returns a message
func (d *deltas) GetMessage() (message.Message, error) {
	lnBuf := make([]byte, 8)
	if n, err := d.fp.Read(lnBuf); err == io.EOF && n == 0 {
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("could not read from file: %v", err)
	}
	ln := binary.BigEndian.Uint64(lnBuf)

	buf := make([]byte, ln-8)
	if n, err := d.fp.Read(buf); err == io.EOF && n == 0 {
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("could not read from file: %v", err)
	}

	return decoder.Parse(buf)
}

func (d *deltas) filename() string {
	filename := d.minLSN.Hex()

	if d.minLSN == d.prevMinLSN {
		d.postfix++
	} else {
		d.postfix = 0
	}

	if d.postfix != 0 {
		filename = fmt.Sprintf("%s.%d", filename, d.postfix)
	}

	return filename
}

// Save saves deltas to a file and returns lsn boundaries positions of the delta file
func (d *deltas) Save() (string, dbutils.LSN, dbutils.LSN, error) {
	filename := d.filename()
	if d.buffer.Len() == 0 {
		return "", dbutils.InvalidLSN, dbutils.InvalidLSN, EmptyBuffer
	}

	fp, err := os.OpenFile(path.Join(d.tableDir, DirName, filename), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return "", dbutils.InvalidLSN, dbutils.InvalidLSN, fmt.Errorf("could not open file: %v", err)
	}

	d.mutex.Lock()
	defer func() {
		fp.Close()
		d.prevMinLSN = d.minLSN
		d.buffer.Reset()
		d.messagesCnt = 0

		d.minTime, d.maxTime = time.Unix(0, 0), time.Unix(0, 0)
		d.minLSN, d.maxLSN = dbutils.InvalidLSN, dbutils.InvalidLSN

		d.mutex.Unlock()
	}()

	if err := d.header.write(fp); err != nil {
		return "", dbutils.InvalidLSN, dbutils.InvalidLSN, fmt.Errorf("could not write header: %v", err)
	}

	if _, err := d.buffer.WriteTo(fp); err != nil {
		return "", dbutils.InvalidLSN, dbutils.InvalidLSN, fmt.Errorf("could not write messages: %v", err)
	}

	if d.fsync {
		if err := utils.SyncFileAndDirectory(fp); err != nil {
			return "", dbutils.InvalidLSN, dbutils.InvalidLSN, fmt.Errorf("could not fsync")
		}
	}

	return filename, d.minLSN, d.maxLSN, nil
}

// Load loads the messages from the file
func (d *deltas) Load(filename string) error {
	var filepath string

	if d.tableDir == "" {
		filepath = filename
	} else {
		filepath = path.Join(d.tableDir, DirName, filename)
	}

	fp, err := os.OpenFile(filepath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	d.fp = fp

	if err := d.header.read(d.fp); err != nil {
		d.fp.Close()
		return fmt.Errorf("could not read header: %v", err)
	}

	return nil
}

// Close closes loaded delta file
func (d *deltas) Close() error {
	return d.fp.Close()
}

// MessageCnt returns number of messages in the current delta
func (d *deltas) MessageCnt() uint32 {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.header.messagesCnt
}

//LastMessageTime returns time of the last message added
func (d *deltas) LastMessageTime() time.Time {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.lastMsgTime
}

func writeUint64(fp *os.File, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	_, err := fp.Write(buf)

	return err
}

func writeUint32(fp *os.File, val uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, val)
	_, err := fp.Write(buf)

	return err
}

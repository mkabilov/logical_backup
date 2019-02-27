package archiver

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/mkabilov/logical_backup/pkg/utils"
	"github.com/mkabilov/logical_backup/pkg/utils/dbutils"
	"github.com/mkabilov/logical_backup/pkg/utils/queue"
)

const (
	workerNapTime = 500 * time.Millisecond
)

type file string

type deltaFile struct {
	filename string
	lsn      dbutils.LSN
}

// Archiver describes archiver interface
type Archiver interface {
	QueueFile(string)
	QueueDeltaFile(string, dbutils.LSN)
	SetBasebackupLSN(minLSN dbutils.LSN)
	Run()
	Wait()
}

type archive struct {
	wg    sync.WaitGroup
	ctx   context.Context
	queue *queue.Queue
	fsync bool

	basebackupLSN dbutils.LSN

	tableSourceDir      string
	tableDestinationDir string
}

// New instantiate archiver
func New(ctx context.Context, tableSourceDir, tableDestinationDir string, fsync bool) *archive {
	if tableSourceDir == "" || tableDestinationDir == "" || tableDestinationDir == tableSourceDir {
		panic("source and destination dirs must be set and different")
	}

	return &archive{
		queue:               queue.New(ctx),
		fsync:               fsync,
		ctx:                 ctx,
		tableSourceDir:      tableSourceDir,
		tableDestinationDir: tableDestinationDir,
		basebackupLSN:       dbutils.InvalidLSN,
	}
}

// Run runs the archiver
func (a *archive) Run() {
	a.wg.Add(1)

	go a.run()
}

// QueueFile puts the file into the queue
func (a *archive) QueueFile(filepath string) {
	a.queue.Put(file(filepath))
}

// QueueDeltaFile puts the delta file into the queue
// filepath is relative to the table directory
func (a *archive) QueueDeltaFile(filepath string, lsn dbutils.LSN) {
	a.queue.Put(deltaFile{filename: filepath, lsn: lsn})
}

// SetBasebackupLSN sets the minimum lsn
func (a *archive) SetBasebackupLSN(minLSN dbutils.LSN) {
	a.basebackupLSN = minLSN
}

func (a *archive) run() {
	defer a.wg.Done()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-time.After(workerNapTime):
			for {
				obj, err := a.queue.Get()
				if err == queue.ErrEmptyQueue {
					break
				} else if err == context.Canceled {
					return
				} else if err != nil {
					log.Fatalf("unhandled queue error: %v", err)
				}

				switch v := obj.(type) {
				case file:
					if err := a.archiveFile(string(v), false); err != nil {
						log.Printf("could not archive basebackup file %s: %v", v, err)
						continue
					}
				case deltaFile:
					if a.basebackupLSN.IsValid() {
						if v.lsn < a.basebackupLSN {
							if err := os.Remove(path.Join(a.tableSourceDir, v.filename)); err != nil {
								log.Printf("could not delete old file: %v", err)
							}

							log.Printf("skipping files prior to basebackup %s", v.filename)
							continue
						}
					}

					if err := a.archiveFile(v.filename, false); err != nil {
						log.Printf("could not archive delta file %s: %v", v.filename, err)
						continue
					}
				}
			}
		}
	}
}

// Wait waits for the goroutines to finish
func (a *archive) Wait() {
	a.wg.Wait()
}

func (a *archive) archiveFile(filename string, checkIfDstFileExists bool) error {
	srcFile := path.Join(a.tableSourceDir, filename)
	dstFile := path.Join(a.tableDestinationDir, filename)

	if _, err := os.Stat(srcFile); os.IsNotExist(err) {
		log.Printf("source file doesn't exist: %q; skipping", srcFile)
		return nil
	} else if err != nil {
		return fmt.Errorf("could not stat source file %q: %v", srcFile, err)
	}

	if st, err := os.Stat(dstFile); !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("could not stat destination file %q: %s", dstFile, err)
		}

		if checkIfDstFileExists {
			if st.Size() == 0 {
				if err := os.Remove(dstFile); err != nil {
					return fmt.Errorf("could not delete empty dst file: %v", err)
				}
			} else {
				return fmt.Errorf("destination file %q is not empty", dstFile)
			}
		}
	}

	if _, err := utils.CopyFile(srcFile, dstFile, a.fsync); err != nil {
		os.Remove(dstFile)
		return fmt.Errorf("could not move %s -> %s file: %v", srcFile, dstFile, err)
	}

	srcFInfo, srcErr := os.Stat(srcFile)
	if srcErr != nil {
		os.Remove(dstFile)
		return fmt.Errorf("could not stat source file: %v", srcErr)
	}

	dstFInfo, dstErr := os.Stat(dstFile)
	if dstErr != nil {
		os.Remove(dstFile)
		return fmt.Errorf("could not stat destination file: %v", dstErr)
	}

	if sSize, dSize := srcFInfo.Size(), dstFInfo.Size(); sSize != dSize {
		os.Remove(dstFile)
		return fmt.Errorf("source(%v) and destionation(%v) file size do not match", sSize, dSize)
	}

	if err := os.Remove(srcFile); err != nil {
		log.Printf("could not delete old file: %v", err)
	}

	log.Printf("successfully archived %s", dstFile)

	return nil
}

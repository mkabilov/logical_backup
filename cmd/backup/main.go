package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/logicalbackup"
)

func main() {
	ctx, done := context.WithCancel(context.Background())

	if len(os.Args) < 2 {
		fmt.Printf("usage:\n\t%s {config file}\n", os.Args[0])
		os.Exit(1)
	}

	cfg, err := config.New(os.Args[1])
	if err != nil {
		log.Fatalf("could not init config: %v", err)
	}

	log.Printf("Backup directory: %q", cfg.TempDir)
	log.Printf("Archive directory: %q", cfg.ArchiveDir)
	log.Printf("BackupThreshold: %v", cfg.BackupThreshold)
	log.Printf("DeltasPerFile: %v", cfg.DeltasPerFile)
	log.Printf("DB connection string: %s@%s:%d/%s slot:%q publication:%q",
		cfg.DB.User, cfg.DB.Host, cfg.DB.Port, cfg.DB.Database,
		cfg.Slotname, cfg.PublicationName)
	log.Printf("Backing up new tables: %t", cfg.TrackNewTables)

	log.Printf("Fsync: %t", cfg.Fsync)
	log.Printf("SendStatusOnCommit: %t", cfg.SendStatusOnCommit)

	lb, err := logicalbackup.New(ctx, cfg)
	if err != nil {
		log.Fatalf("could not create backup instance: %v", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)

	lb.Run()

	if cfg.InitialBasebackup {
		log.Printf("Queueing tables for the initial backup")
		lb.QueueBasebackupTables()
	}

loop:
	for {
		switch sig := <-sigs; sig {
		case syscall.SIGINT:
			fallthrough
		case syscall.SIGTERM:
			break loop
		case syscall.SIGHUP:
		default:
			log.Printf("unhandled signal: %v", sig)
		}
	}

	done()
	lb.Wait()
}

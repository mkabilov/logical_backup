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
	defer log.Printf("successfully shut down")

	ctx, done := context.WithCancel(context.Background())
	stopCh := make(chan struct{}, 128)

	if len(os.Args) < 2 {
		fmt.Printf("usage:\n\t%s {config file}\n", os.Args[0])
		os.Exit(1)
	}

	cfg, err := config.New(os.Args[1])
	if err != nil {
		log.Fatalf("could not init config: %v", err)
	}

	if cfg.StagingDir != "" {
		log.Printf("Staging directory: %q", cfg.StagingDir)
	} else {
		log.Printf("No staging directory. Writing directly to the archive dir")
	}
	log.Printf("Archive directory: %q", cfg.ArchiveDir)
	log.Printf("BackupThreshold: %v", cfg.BackupThreshold)
	log.Printf("DeltasPerFile: %v", cfg.DeltasPerFile)
	log.Printf("DB connection string: %s@%s:%d/%s slot:%q publication:%q",
		cfg.DB.User, cfg.DB.Host, cfg.DB.Port, cfg.DB.Database,
		cfg.Slotname, cfg.PublicationName)
	log.Printf("Backing up new tables: %t", cfg.TrackNewTables)

	log.Printf("Fsync: %t", cfg.Fsync)
	log.Printf("SendStatusOnCommit: %t", cfg.SendStatusOnCommit)
	if cfg.ForceBasebackupAfterInactivityInterval > 0 {
		log.Printf("Force new basebackup of a modified table after inactivity for: %v",
			cfg.ForceBasebackupAfterInactivityInterval)
	}

	lb, err := logicalbackup.New(ctx, stopCh, cfg)
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
		select {
		case sig := <-sigs:
			switch sig {
			case syscall.SIGABRT:
				fallthrough
			case syscall.SIGINT:
				fallthrough
			case syscall.SIGQUIT:
				fallthrough
			case syscall.SIGSTOP:
				fallthrough
			case syscall.SIGTERM:
				break loop
			case syscall.SIGHUP:
			default:
				log.Printf("unhandled signal: %v", sig)
			}
		case <-stopCh:
			{
				log.Printf("received termination request, cleaning up...")
				break loop
			}
		}
	}

	done()
	lb.Wait()
}

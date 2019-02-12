package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/consumer"
	"github.com/ikitiki/logical_backup/pkg/message"
	"github.com/ikitiki/logical_backup/pkg/utils/dbutils"
)

const (
	columnWidth    = 10
	lsnColumnWidth = 18
)

type dumper struct {
	consumer consumer.Interface
}

var (
	configFile = flag.String("config", "config.yaml", "path to the config file")
	version    = flag.Bool("version", false, "Print version information")
	startLSN   = flag.String("lsn", "0/0", "start lsn")

	Version  = "devel"
	Revision = "devel"

	GoVersion = runtime.Version()
)

func buildInfo() string {
	return fmt.Sprintf("logical protocol dumper version %s git revision %s go version %s", Version, Revision, GoVersion)
}

func main() {
	var lsn dbutils.LSN
	errCh := make(chan error)

	ctx, cancel := context.WithCancel(context.Background())

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", buildInfo())
		fmt.Fprintf(os.Stderr, "\nUsage:\n")
		flag.PrintDefaults()
	}

	flag.Parse()
	if *version {
		fmt.Println(buildInfo())
		os.Exit(1)
	}

	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Config file %s does not exist", *configFile)
		os.Exit(1)
	}

	cfg, err := config.New(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not load config file: %v", err)
		os.Exit(1)
	}

	if err := lsn.Parse(*startLSN); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid lsn: %q", *configFile)
		os.Exit(1)
	}

	dmp := &dumper{}
	dmp.consumer = consumer.New(ctx, errCh, cfg.DB, cfg.SlotName, cfg.PublicationName, lsn)
	if err := dmp.consumer.Run(dmp); err != nil {
		log.Fatalf("could not start consumer: %v", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-sigs:
	case err := <-errCh:
		log.Println(err)
	}
	cancel()

	dmp.consumer.Wait()
}

//HandleMessage processes message
func (d *dumper) HandleMessage(msg message.Message, lsn dbutils.LSN) error {
	msgType := msg.MsgType()
	lsnStr := lsn.String()
	log.Printf("%s%s%s:%s%s\n",
		lsnStr, strings.Repeat(" ", lsnColumnWidth-len(lsnStr)),
		msgType, strings.Repeat(" ", columnWidth-len(msgType.String())), msg.String())
	if msgType == message.MsgCommit {
		d.consumer.AdvanceLSN(lsn)
	}

	return nil
}

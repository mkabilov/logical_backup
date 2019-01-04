package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/ikitiki/logical_backup/pkg/config"
	"github.com/ikitiki/logical_backup/pkg/logger"
	"github.com/ikitiki/logical_backup/pkg/logicalbackup"
)

var (
	configFile = flag.String("config", "config.yaml", "path to the config file")
	version    = flag.Bool("version", false, "print version information")
	debug      = flag.Bool("debug", false, "enable debug mode")

	Version  string
	Revision string

	GoVersion = runtime.Version()
)

func buildInfo() string {
	return fmt.Sprintf("logical backup version %s git revision %s go version %s", Version, Revision, GoVersion)
}

func main() {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", buildInfo())
		fmt.Fprintf(os.Stderr, "\nUsage:\n")
		flag.PrintDefaults()
	}

	flag.Parse()
	if _, err := logger.InitGlobalLogger(*debug); err != nil {
		fmt.Fprintf(os.Stderr, "Could not initialize global logger")
		os.Exit(1)
	}

	if *version {
		fmt.Println(buildInfo())
		os.Exit(1)
	}

	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Config file %s does not exist", *configFile)
		os.Exit(1)
	}

	cfg, err := config.New(*configFile, *debug)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not load config file: %v", err)
		os.Exit(1)
	}

	cfg.Print()

	lb, err := logicalbackup.New(cfg)
	if err != nil {
		log.Fatalf("could not create backup instance: %v", err)
	}

	if err := lb.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

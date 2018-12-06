package config

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"
)

const (
	defaultPrometheusPort = 1999
)

type Config struct {
	DB                                     pgx.ConnConfig `yaml:"db"`
	SlotName                               string         `yaml:"slotname"`
	PublicationName                        string         `yaml:"publication"`
	TrackNewTables                         bool           `yaml:"trackNewTables"`
	DeltasPerFile                          int            `yaml:"deltasPerFile"`
	BackupThreshold                        int            `yaml:"backupThreshold"`
	ConcurrentBasebackups                  int            `yaml:"concurrentBasebackups"`
	InitialBasebackup                      bool           `yaml:"initialBasebackup"`
	Fsync                                  bool           `yaml:"fsync"`
	StagingDir                             string         `yaml:"StagingDir"`
	ArchiveDir                             string         `yaml:"archiveDir"`
	ForceBasebackupAfterInactivityInterval time.Duration  `yaml:"forceBasebackupAfterInactivityInterval"`
	ArchiverTimeout                        time.Duration  `yaml:"archiverTimeout"`
	PrometheusPort                         int            `yaml:"prometheusPort"`
}

func New(filename string) (*Config, error) {
	var cfg Config

	fp, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("could not open config file: %v", err)
	}
	defer fp.Close()

	if err := yaml.NewDecoder(fp).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("could not decode config file: %v", err)
	}
	// forcing backups with sub-minute inactivity period makes no sense.
	cfg.ForceBasebackupAfterInactivityInterval = cfg.ForceBasebackupAfterInactivityInterval.Truncate(time.Minute)

	if cfg.PrometheusPort == 0 {
		cfg.PrometheusPort = defaultPrometheusPort
	}

	return &cfg, nil
}

func (c Config) Print() {
	if c.StagingDir != "" {
		log.Printf("Staging directory: %q", c.StagingDir)
	} else {
		log.Printf("No staging directory. Writing directly to the archive dir")
	}

	log.Printf("Archive directory: %q", c.ArchiveDir)
	log.Printf("BackupThreshold: %v", c.BackupThreshold)
	log.Printf("DeltasPerFile: %v", c.DeltasPerFile)
	log.Printf("DB connection string: %s@%s:%d/%s slot:%q publication:%q",
		c.DB.User, c.DB.Host, c.DB.Port, c.DB.Database, c.SlotName, c.PublicationName)
	log.Printf("Backing up new tables: %t", c.TrackNewTables)
	log.Printf("Fsync: %t", c.Fsync)
	if c.ForceBasebackupAfterInactivityInterval > 0 {
		log.Printf("Force new basebackup of a modified table after inactivity for: %v",
			c.ForceBasebackupAfterInactivityInterval)
	}
}

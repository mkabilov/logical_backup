package config

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"

	"github.com/ikitiki/logical_backup/pkg/logger"
)

const (
	defaultPrometheusPort = 1999
)

type Config struct {
	DB                                     pgx.ConnConfig       `yaml:"db"`
	SlotName                               string               `yaml:"slotname"`
	PublicationName                        string               `yaml:"publication"`
	TrackNewTables                         bool                 `yaml:"trackNewTables"`
	DeltasPerFile                          int                  `yaml:"deltasPerFile"`
	BackupThreshold                        int                  `yaml:"backupThreshold"`
	ConcurrentBasebackups                  int                  `yaml:"concurrentBasebackups"`
	InitialBasebackup                      bool                 `yaml:"initialBasebackup"`
	Fsync                                  bool                 `yaml:"fsync"`
	StagingDir                             string               `yaml:"StagingDir"`
	ArchiveDir                             string               `yaml:"archiveDir"`
	ForceBasebackupAfterInactivityInterval time.Duration        `yaml:"forceBasebackupAfterInactivityInterval"`
	ArchiverTimeout                        time.Duration        `yaml:"archiverTimeout"`
	PrometheusPort                         int                  `yaml:"prometheusPort"`
	Log                                    *logger.LoggerConfig `yaml:"log"`
}

func New(filename string, developmentMode bool) (*Config, error) {
	var cfg Config

	fp, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("could not open config file: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			log.Printf("could not close file: %v", err)
		}
	}()

	if err := yaml.NewDecoder(fp).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("could not decode config file: %v", err)
	}

	// honor PGHOST, PGPORT and other libpq variables when set.
	envConfig, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, fmt.Errorf("could not parse libpq environment variables: %v", err)
	}
	cfg.DB = cfg.DB.Merge(envConfig)

	// forcing backups with sub-minute inactivity period makes no sense.
	cfg.ForceBasebackupAfterInactivityInterval = cfg.ForceBasebackupAfterInactivityInterval.Truncate(time.Minute)

	if cfg.PrometheusPort == 0 {
		cfg.PrometheusPort = defaultPrometheusPort
	}

	defaultLoggerConfig := logger.DefaultLogConfig()
	if cfg.Log == nil {
		cfg.Log = defaultLoggerConfig
	}
	if cfg.Log.Location == nil {
		cfg.Log.Location = defaultLoggerConfig.Location
	}

	if developmentMode {
		cfg.Log.Development = developmentMode
	}

	if err := logger.ValidateLogLevel(cfg.Log.Level); err != nil {
		return nil, err
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

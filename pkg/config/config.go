package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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

// New constructs a new Config instance.
func New(filename string, developmentMode bool) (*Config, error) {
	var cfg Config

	fp, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("could not open config file: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "error during deferred file close: %v", err)
		}
	}()

	if err := yaml.NewDecoder(fp).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("could not decode config file: %v", err)
	}
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
	pr := logger.PrintOption

	if c.StagingDir == "" {
		logger.G.Info("No staging directory specified. Files will be written directly to the archive directory")
	} else {
		pr("Staging directory", c.StagingDir)
	}
	pr("Archive directory", c.ArchiveDir)
	pr("BackupThreshold", strconv.Itoa(c.BackupThreshold))
	pr("DeltasPerFile", strconv.Itoa(c.DeltasPerFile))
	pr("DB Connection String",
		"%s@%s:%d/%s slot:%q publication:%q", c.DB.User, c.DB.Host, c.DB.Port, c.DB.Database, c.SlotName, c.PublicationName)
	pr("Track New Tables", strconv.FormatBool(c.TrackNewTables))
	pr("Fsync", strconv.FormatBool(c.Fsync))
	if c.ForceBasebackupAfterInactivityInterval > 0 {
		pr("Force new backups of a modified table after inactivity", c.ForceBasebackupAfterInactivityInterval.String())
	}
	pr("Log development mode", strconv.FormatBool(c.Log.Development))
	if c.Log.Level != "" {
		pr("Log level", strings.ToUpper(c.Log.Level))
	}
	pr("Log includes file location", strconv.FormatBool(*c.Log.Location))
}

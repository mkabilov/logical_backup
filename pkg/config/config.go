package config

import (
	"fmt"
	"os"
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
	defer fp.Close()

	if err := yaml.NewDecoder(fp).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("could not decode config file: %v", err)
	}
	// forcing backups with sub-minute inactivity period makes no sense.
	cfg.ForceBasebackupAfterInactivityInterval = cfg.ForceBasebackupAfterInactivityInterval.Truncate(time.Minute)

	if cfg.PrometheusPort == 0 {
		cfg.PrometheusPort = defaultPrometheusPort
	}
	if cfg.Log == nil {
		cfg.Log = logger.DefaultLogConfig()
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
	ops := [][]string{
		{"Staging directory", c.StagingDir},
		{"Archive directory", c.ArchiveDir},
		{"BackupThreshold", fmt.Sprintf("%d", c.BackupThreshold)},
		{"DeltasPerFile", fmt.Sprintf("%d", c.DeltasPerFile)},
		{"DB Connection String", fmt.Sprintf("%s@%s:%d/%s slot:%q publication:%q",
			c.DB.User, c.DB.Host, c.DB.Port, c.DB.Database, c.SlotName, c.PublicationName)},
		{"Track New Tables", fmt.Sprintf("%t", c.TrackNewTables)},
		{"Fsync", fmt.Sprintf("%t", c.Fsync)},
		{"Log development mode", fmt.Sprintf("%t", c.Log.Development)},
	}

	if c.StagingDir == "" {
		ops = ops[1:]
		logger.G.Infof("No staging directory specified. Files will be written directly to the archive directory")
	}
	if c.ForceBasebackupAfterInactivityInterval > 0 {
		ops = append(ops, []string{"Force new basebackup of a modified table after inactivity", fmt.Sprintf("%v", c.ForceBasebackupAfterInactivityInterval)})
	}
	if c.Log.Level != "" {
		ops = append(ops, []string{"Log level", strings.ToUpper(c.Log.Level)})
	}
	if c.Log.Location != nil {
		ops = append(ops, []string{"Log includes file location", fmt.Sprintf("%t", *c.Log.Location)})
	}

	for _, opt := range ops {
		logger.G.With(opt[0], opt[1]).Info("option")
	}

}

package config

import (
	"fmt"
	"os"

	"github.com/jackc/pgx"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Dir                   string         `yaml:"dir"`
	Tables                []string       `yaml:"tables"`
	DB                    pgx.ConnConfig `yaml:"db"`
	Slotname              string         `yaml:"slotname"`
	PublicationName       string         `yaml:"publication"`
	TrackNewTables        bool           `yaml:"trackNewTables"`
	DeltasPerFile         int            `yaml:"deltasPerFile"`
	BackupThreshold       int            `yaml:"backupThreshold"`
	ConcurrentBasebackups int            `yaml:"concurrentBasebackups"`
	InitialBasebackup     bool           `yaml:"initialBasebackup"`
}

func New(filename string) (*Config, error) {
	var cfg Config

	configFp, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("could not open config file: %v", err)
	}

	if err := yaml.NewDecoder(configFp).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("could not decode config file: %v", err)
	}

	return &cfg, nil
}

package logger

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/ikitiki/logical_backup/pkg/dbutils"
	"github.com/ikitiki/logical_backup/pkg/message"
)

// LoggerConfig describes the logger configuration. It couldn't be part of config
// due to cyclic imports (also, it is used outside of the scope of config in restore code).
type LoggerConfig struct {
	Level       string `yaml:"level"`
	Location    *bool  `yaml:"location"`
	Development bool   `yaml:"development"`
}

// Log is a wrapper around zap.SugarLogger, providing a bunch of With... functions to ease writing log messages.
// Note that all With... functions return the Log structure, never the underlying SugarLogger, to allow chain-linking them.
type Log struct {
	*zap.SugaredLogger
}

// G is a global package-level logger to use in the parts of the code where the specific logger is not defined
var G *Log

// InitGlobalLogger initializes the package-level logger. It should be called only once at start of the program.
func InitGlobalLogger(cfg *LoggerConfig, args ...interface{}) (err error) {
	G, err = NewLogger("global", cfg)
	if err == nil && len(args) > 0 {
		G = NewLoggerFrom(G, "", args...)
	}
	return
}

// WithError returns a logger with an error field provided in the logging context.
func (l *Log) WithError(err error) *Log {
	return &Log{l.With("error", err)}
}

// WithCustomNamedLSN returns a logger with an LSN provided in the logging context, providing a custom name for it.
func (l *Log) WithCustomNamedLSN(name string, lsn dbutils.LSN) *Log {
	return &Log{l.With(name, lsn)}
}

// WithLSN is a shortcut of WithCustomNamedLSN with a name pre-defined to LSN.
func (l *Log) WithLSN(lsn dbutils.LSN) *Log {
	return l.WithCustomNamedLSN("LSN", lsn)
}

// WithOID returns a logger with an OID provided in the logging context.
func (l *Log) WithOID(oid dbutils.OID) *Log {
	return &Log{l.With("OID", oid)}
}

// WithTableNameString returns a logger with a table name string provided in the logging context.
func (l *Log) WithTableNameString(t string) *Log {
	return &Log{l.With("table name", t)}
}

// WithTableName returns a logger with a table namespaced name provided in the logging context.
func (l *Log) WithTableName(n message.NamespacedName) *Log {
	return l.WithTableNameString(n.String())
}

// WithReplicationMessage returns a logger with a replication message provided in the logging context.
func (l *Log) WithReplicationMessage(message []byte) *Log {
	return &Log{l.With("message", message)}
}

// WithFilename returns a logger with a segment file name provided in the logging context.
func (l *Log) WithFilename(filename string) *Log {
	return &Log{l.With("filename", filename)}
}

// WithDetail returns a logger with extra details provided in the logging context. It can be used,
// for instance, to provide values of the parameters relevant to the logging message.
func (l *Log) WithDetail(template string, args ...interface{}) *Log {
	return &Log{l.With("detail", fmt.Sprintf(template, args...))}
}

// WithHint is identical to WithDetail, although provides hint instead of a detail. Use it
// to inform users about possible consequences of the event being logged or followup actions.
// for instance, to provide values of the parameters relevant to the logging message.
func (l *Log) WithHint(template string, args ...interface{}) *Log {
	return &Log{l.With("hint", fmt.Sprintf(template, args...))}
}

// NewLogger creates a new logger with a given name and configuration
func NewLogger(name string, cfg *LoggerConfig) (*Log, error) {
	log, err := newCustomLogger(cfg)
	if err != nil {
		return nil, err
	}
	return &Log{log.Sugar().Named(name)}, err
}

// NewLoggerFrom creates a new logger from the existing one, adding a name and, optionally, arbitrary fields with values.
func NewLoggerFrom(existing *Log, name string, withFields ...interface{}) (result *Log) {
	if len(withFields) > 0 {
		result = &Log{existing.Named(name).With(withFields...)}
	} else {
		result = &Log{existing.Named(name)}
	}
	return
}

// PrintMessageForDebug emits a log entry describing the current message received for debug purposes.
func PrintMessageForDebug(prefix string, msg message.Message, currentLSN dbutils.LSN, log *Log) {
	log.WithLSN(currentLSN).Debugf(prefix+" %T", msg)
}

// LevelFromString converts the textual logging level to the level that can be passed to zap.Config
func LevelFromString(text string) (level zapcore.Level, err error) {
	err = level.UnmarshalText([]byte(text))
	return
}

// ValidateLogLevel verifies that the log level text corresponds to the valid log level
func ValidateLogLevel(level string) error {
	if level == "" {
		return nil
	}
	if _, err := LevelFromString(level); err != nil {
		return fmt.Errorf("%v, valid levels are debug, info, warn, error, fatal, dpanic and panic", err)
	}
	return nil

}

// DefaultLogConfig returns a default logging configuration
func DefaultLogConfig() *LoggerConfig {
	var (
		showLocationByDefault = true
	)
	return &LoggerConfig{"", &showLocationByDefault, false}

}

// We need to make slight customization to the default zap development and production levels
// Namely, avoid stack traces for Warn in development (too verbose for our usage of Warn),
// allow customization of the default level and disable showing lines of code in the log output.
func newCustomLogger(lc *LoggerConfig) (*zap.Logger, error) {
	var (
		cfg zap.Config
	)
	opts := make([]zap.Option, 0)

	if lc.Development {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}
	// adjust the log level if necessary
	if lc.Level != "" {
		lv, err := LevelFromString(lc.Level)
		if err != nil {
			return nil, err
		}
		cfg.Level = zap.NewAtomicLevelAt(lv)
	}
	// disable logging source line numbers if instructed.
	if lc.Location != nil {
		cfg.DisableCaller = !*lc.Location
	}
	// default development configuration sets stacktraces from WarnLevel, override it here
	if lc.Development {
		opts = append(opts, zap.AddStacktrace(zap.ErrorLevel))
	}
	return cfg.Build(opts...)
}

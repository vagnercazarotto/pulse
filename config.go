package pulse

import "time"

const (
	defaultCollectInterval = 10 * time.Second
	defaultExportInterval  = 10 * time.Second
	defaultExportTimeout   = 3 * time.Second
	defaultBufferSize      = 10_000
	defaultShutdownTimeout = 5 * time.Second
)

// Config controls agent behavior.
type Config struct {
	CollectInterval time.Duration
	ExportInterval  time.Duration
	ExportTimeout   time.Duration
	BufferSize      int
	ShutdownTimeout time.Duration
	Exporters       []Exporter
	WAL             *WALConfig
}

func (c Config) withDefaults() Config {
	cfg := c
	if cfg.CollectInterval <= 0 {
		cfg.CollectInterval = defaultCollectInterval
	}
	if cfg.ExportInterval <= 0 {
		cfg.ExportInterval = defaultExportInterval
	}
	if cfg.ExportTimeout <= 0 {
		cfg.ExportTimeout = defaultExportTimeout
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = defaultBufferSize
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	return cfg
}

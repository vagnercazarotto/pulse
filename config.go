package pulse

import "time"

const (
	defaultCollectInterval      = 10 * time.Second
	defaultExportInterval       = 10 * time.Second
	defaultExportTimeout        = 3 * time.Second
	defaultExportMaxRetries     = 5
	defaultExportBackoffInitial = 500 * time.Millisecond
	defaultExportBackoffMax     = 30 * time.Second
	defaultExportBackoffJitter  = 0.20
	defaultBufferSize           = 10_000
	defaultShutdownTimeout      = 5 * time.Second
)

// Config controls agent behavior.
type Config struct {
	CollectInterval      time.Duration
	ExportInterval       time.Duration
	ExportTimeout        time.Duration
	ExportMaxRetries     int
	ExportBackoffInitial time.Duration
	ExportBackoffMax     time.Duration
	ExportBackoffJitter  float64
	BufferSize           int
	DisableHardware      bool
	ShutdownTimeout      time.Duration
	Exporters            []Exporter
	WAL                  *WALConfig
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
	if cfg.ExportMaxRetries < 0 {
		cfg.ExportMaxRetries = 0
	}
	if cfg.ExportMaxRetries == 0 {
		cfg.ExportMaxRetries = defaultExportMaxRetries
	}
	if cfg.ExportBackoffInitial <= 0 {
		cfg.ExportBackoffInitial = defaultExportBackoffInitial
	}
	if cfg.ExportBackoffMax <= 0 {
		cfg.ExportBackoffMax = defaultExportBackoffMax
	}
	if cfg.ExportBackoffMax < cfg.ExportBackoffInitial {
		cfg.ExportBackoffMax = cfg.ExportBackoffInitial
	}
	if cfg.ExportBackoffJitter < 0 {
		cfg.ExportBackoffJitter = 0
	}
	if cfg.ExportBackoffJitter > 1 {
		cfg.ExportBackoffJitter = 1
	}
	if cfg.ExportBackoffJitter == 0 {
		cfg.ExportBackoffJitter = defaultExportBackoffJitter
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = defaultBufferSize
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	return cfg
}

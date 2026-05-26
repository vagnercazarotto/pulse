package pulse

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
)

type LogExporterConfig struct {
	Output io.Writer
	Pretty bool
}

// LogExporter writes samples as JSON lines.
type LogExporter struct {
	mu     sync.Mutex
	out    io.Writer
	pretty bool
}

func NewLogExporter(cfg LogExporterConfig) *LogExporter {
	out := cfg.Output
	if out == nil {
		out = os.Stdout
	}
	return &LogExporter{out: out, pretty: cfg.Pretty}
}

func (e *LogExporter) Name() string { return "log" }

func (e *LogExporter) Export(_ context.Context, samples []Sample) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	enc := json.NewEncoder(e.out)
	if e.pretty {
		enc.SetIndent("", "  ")
	}
	for _, s := range samples {
		if err := enc.Encode(s); err != nil {
			return err
		}
	}
	return nil
}

func (e *LogExporter) Close() error {
	return nil
}

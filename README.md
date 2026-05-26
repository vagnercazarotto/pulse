# pulse

[![status: placeholder](https://img.shields.io/badge/status-placeholder-lightgrey)](#)

Embedded observability for industrial Go gateways and other edge Go services.

pulse is a Go library you embed directly into your process to collect runtime metrics, application metrics, and local hardware telemetry with offline-first buffering and export.

Use it when you need:

- in-process observability with minimal operational overhead
- local buffering during network or backend outages
- Linux edge visibility without depending on a cloud agent
- a small integration surface for Go services and gateways

Start here:

- Install: `go get github.com/vagnercazarotto/pulse`
- Minimal integration: see `Quick start` below
- Runnable examples: `examples/basic` and `examples/custom-exporter`
- Reference docs: `docs/index.html`, `docs/quickstart.html`, `docs/current-reference.html`

## Project status

Early implementation (v0.1.0 in progress).

Implemented baseline:

- Agent lifecycle contract
  - `Stop()` is idempotent
  - `Start()` after `Stop()` on the same instance returns `ErrAgentStopped`
- Metrics registry with canonical label identity (`name|k=v|...`)
- Runtime sample collection integrated into the agent loop
- Linux hardware collection integrated into the agent loop (cpu, mem, disk, load, temp when available)
- Ring buffer integrated into agent flow with overflow tracking (`overflow_count`)
- WAL persistence integrated into agent flow with replay on startup
- Export loop with context-based timeout, configurable retry/backoff, and requeue-on-error behavior

## Current scope (v0.1.0)

- Runtime collector
- Linux hardware collector (CPU, memory, disk, load, temperature when available)
- Metrics core (counter, gauge, histogram, timer)
- Ring buffer + WAL
- HTTP exporter + log exporter
- Local dashboard + health endpoints

Deferred to v0.2:

- Full cross-platform temperature support
- Full Windows PDH implementation
- Additional advanced exporters

## Install in another project

Add pulse to your Go module:

```bash
go get github.com/vagnercazarotto/pulse
```

Import it in your application:

```go
import "github.com/vagnercazarotto/pulse"
```

## Examples

- Basic integration: `examples/basic`
- Custom exporter: `examples/custom-exporter`

## Quick start (current API baseline)

```go
package main

import (
	"log"

	"github.com/vagnercazarotto/pulse"
)

func main() {
	agent := pulse.New(pulse.Config{})

	if err := agent.Start(); err != nil {
		log.Fatal(err)
	}
	defer agent.Stop()

	errors := agent.Metrics().Counter("modbus.errors", map[string]string{
		"device":   "boiler-01",
		"protocol": "tcp",
	})
	errors.Inc()
}
```

## Usage examples

### Basic agent with application metrics

```go
package main

import (
	"log"
	"time"

	"github.com/vagnercazarotto/pulse"
)

func main() {
	agent := pulse.New(pulse.Config{
		CollectInterval: 2 * time.Second,
	})

	requests := agent.Metrics().Counter("app.requests_total", map[string]string{
		"service": "gateway",
	})

	temperature := agent.Metrics().Gauge("app.temperature_c", map[string]string{
		"device": "boiler-01",
	})

	if err := agent.Start(); err != nil {
		log.Fatal(err)
	}
	defer agent.Stop()

	requests.Inc()
	temperature.Set(72.4)

	time.Sleep(5 * time.Second)
}
```

### Local HTTP visibility

```go
package main

import (
	"log"

	"github.com/vagnercazarotto/pulse"
)

func main() {
	httpExporter := pulse.NewHTTPExporter(pulse.HTTPExporterConfig{
		Addr: "127.0.0.1:9090",
	})

	agent := pulse.New(pulse.Config{
		Exporters: []pulse.Exporter{httpExporter},
	})

	if err := agent.Start(); err != nil {
		log.Fatal(err)
	}
	defer agent.Stop()

	select {}
}
```

Available endpoints:

- `/`
- `/health`
- `/metrics.json`
- `/metrics`

### Log exporter plus WAL persistence

```go
package main

import (
	"log"
	"time"

	"github.com/vagnercazarotto/pulse"
)

func main() {
	logExporter := pulse.NewLogExporter(pulse.LogExporterConfig{Pretty: true})

	agent := pulse.New(pulse.Config{
		ExportInterval: 1 * time.Second,
		Exporters:      []pulse.Exporter{logExporter},
		WAL: &pulse.WALConfig{
			Dir: "./pulse-wal",
		},
	})

	errors := agent.Metrics().Counter("modbus.errors_total", map[string]string{
		"device": "boiler-01",
	})

	if err := agent.Start(); err != nil {
		log.Fatal(err)
	}
	defer agent.Stop()

	errors.Inc()
	time.Sleep(2 * time.Second)
}
```

## Documentation

- Home: [Documentation Home](https://vagnercazarotto.github.io/pulse/)
- Quickstart: [Quickstart Guide](https://vagnercazarotto.github.io/pulse/quickstart.html)
- Current reference: [Current Reference](https://vagnercazarotto.github.io/pulse/current-reference.html)

## Default behavior

When zero values are provided in `pulse.Config`, defaults are applied:

- `CollectInterval`: 10s
- `ExportInterval`: 10s
- `ExportTimeout`: 3s
- `BufferSize`: 10,000 samples
- `ShutdownTimeout`: 5s
- `ExportMaxRetries`: 5
- `ExportBackoffInitial`: 500ms
- `ExportBackoffMax`: 30s
- `ExportBackoffJitter`: 0.20

Optional WAL:

- Set `Config.WAL` with a valid directory to enable disk persistence and replay.
- Successful exports acknowledge WAL segments and trigger compaction of acknowledged files.

## Runtime metrics currently collected

The agent currently includes runtime metrics in each sample, including:

- `runtime.goroutines`
- `runtime.heap_alloc_bytes`
- `runtime.heap_inuse_bytes`
- `runtime.heap_sys_bytes`
- `runtime.stack_inuse_bytes`
- `runtime.gc_count`
- `runtime.gc_pause_ns_last`
- `runtime.gc_pause_total_ns`
- `runtime.next_gc_bytes`
- `runtime.mallocs_total`
- `runtime.frees_total`
- `runtime.cgo_calls_total`
- `runtime.gc_cpu_fraction`

Application metrics from the registry are merged into the same sample payload.

## Hardware metrics currently collected (Linux)

- `hw.cpu_percent`
- `hw.mem_total_bytes`
- `hw.mem_available_bytes`
- `hw.mem_used_percent`
- `hw.disk_total_bytes`
- `hw.disk_used_bytes`
- `hw.disk_used_percent`
- `hw.cpu_temp_celsius` (when exposed by the Linux host)
- `hw.load1`
- `hw.load5`
- `hw.load15`

## Compatibility

Linux (runtime target):

- Linux hardware metrics are implemented and collected from `/proc` and filesystem stats.
- Recommended target for edge runtime and deployment baseline.

Windows (development notes):

- Project builds and root package tests are supported for development workflows.
- In environments with Application Control/AppLocker, `go test ./...` may fail because Go executes temporary test binaries from build cache paths.
- Practical workaround in restricted environments: prefer `go build ./...` for validation and run targeted tests where policy allows execution.
- Hardware collector on non-Linux platforms uses a safe no-op stub by design.

## Export error handling

- Retryable failures use exponential backoff with jitter.
- Non-retryable failures can be marked with `NonRetryable(err)`.
- Non-retryable failures fail fast (no retry attempts).
- Failed export batches are re-queued in memory.

## What is implemented vs planned

Implemented now:

- Core agent lifecycle and loops
- Runtime sample generation
- Linux hardware sample generation
- In-memory ring buffer
- WAL write/replay primitives and startup replay path
- Exporter contract and export loop wiring with configurable retry/backoff

Planned next:

- Dashboard endpoints expansion
- Export reliability hardening (error classification by exporter type)
- Additional integration tests and benchmarks

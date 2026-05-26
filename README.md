# pulse

Embedded observability for industrial Go gateways.

pulse is designed for constrained edge environments where reliability matters more than cloud dependency.
It runs in-process, keeps local telemetry during outages, and is being built with an offline-first architecture.

## Project status

Early implementation (v0.1.0 in progress).

Implemented baseline:

- Agent lifecycle contract
  - `Stop()` is idempotent
  - `Start()` after `Stop()` on the same instance returns `ErrAgentStopped`
- Metrics registry with canonical label identity (`name|k=v|...`)
- Runtime sample collection integrated into the agent loop
- Linux hardware collection integrated into the agent loop (cpu, mem, disk, load)
- Ring buffer integrated into agent flow with overflow tracking (`overflow_count`)
- WAL persistence integrated into agent flow with replay on startup
- Export loop with context-based timeout, configurable retry/backoff, and requeue-on-error behavior

## Current scope (v0.1.0)

- Runtime collector
- Linux hardware collector (CPU, memory, disk, load)
- Metrics core (counter, gauge, histogram, timer)
- Ring buffer + WAL
- HTTP exporter + log exporter
- Local dashboard + health endpoints

Deferred to v0.2:

- Full cross-platform temperature support
- Full Windows PDH implementation
- Additional advanced exporters

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

## Documentation

- Home: [docs/index.html](docs/index.html)
- Quickstart: [docs/quickstart.html](docs/quickstart.html)
- Current reference: [docs/current-reference.html](docs/current-reference.html)

## Publish docs with GitHub Pages

1. Open repository `Settings` on GitHub.
2. Go to `Pages`.
3. In `Build and deployment`, set `Source` to `Deploy from a branch`.
4. Select branch `main`.
5. Select folder `/docs`.
6. Save.

Expected URL:

- https://vagnercazarotto.github.io/pulse/

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
- `hw.load1`
- `hw.load5`
- `hw.load15`

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

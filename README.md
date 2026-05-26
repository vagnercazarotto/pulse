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
- Ring buffer integrated into agent flow with overflow tracking (`overflow_count`)
- WAL persistence integrated into agent flow with replay on startup
- Export loop with context-based timeout and requeue-on-error behavior

## Current scope (v0.1.0)

- Runtime collector
- Linux hardware collector (CPU, memory, disk, load) - planned
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

## Default behavior

When zero values are provided in `pulse.Config`, defaults are applied:

- `CollectInterval`: 10s
- `ExportInterval`: 10s
- `ExportTimeout`: 3s
- `BufferSize`: 10,000 samples
- `ShutdownTimeout`: 5s

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

## What is implemented vs planned

Implemented now:

- Core agent lifecycle and loops
- Runtime sample generation
- In-memory ring buffer
- WAL write/replay primitives and startup replay path
- Exporter contract and export loop wiring

Planned next:

- Hardware collector implementation (Linux)
- HTTP exporter implementation
- Log exporter implementation
- Local dashboard endpoints
- Retry strategy tuning (attempt budget, backoff + jitter policy)

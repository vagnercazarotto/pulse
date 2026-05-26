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
- Ring buffer with overflow tracking (`overflow_count`)
- WAL persistence with replay support, partial-tail truncation, and corruption handling

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

package pulse

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/vagnercazarotto/pulse/metrics"
)

// Agent orchestrates collection and export loops.
type Agent struct {
	cfg      Config
	registry *metrics.Registry
	buffer   *RingBuffer
	wal      *WAL
	hw       *hardwareCollector

	mu      sync.Mutex
	started bool
	stopped bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new agent with defaults applied.
func New(cfg Config) *Agent {
	resolved := cfg.withDefaults()
	buf, err := NewRingBuffer(resolved.BufferSize)
	if err != nil {
		buf, _ = NewRingBuffer(defaultBufferSize)
	}
	return &Agent{
		cfg:      resolved,
		registry: metrics.NewRegistry(),
		buffer:   buf,
		hw:       newHardwareCollector(),
	}
}

// Start starts agent loops. Start is idempotent while running.
// Start after Stop on the same instance returns ErrAgentStopped.
func (a *Agent) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.stopped {
		return ErrAgentStopped
	}
	if a.started {
		return nil
	}

	if a.cfg.WAL != nil && a.cfg.WAL.Dir != "" && a.wal == nil {
		walCfg := *a.cfg.WAL
		w, err := NewWAL(walCfg)
		if err != nil {
			return err
		}
		a.wal = w

		replayed, _, err := a.wal.ReplaySamples()
		if err != nil {
			_ = a.wal.Close()
			a.wal = nil
			return err
		}
		for _, s := range replayed {
			a.buffer.Push(s)
		}
	}

	a.ctx, a.cancel = context.WithCancel(context.Background())
	a.started = true

	a.wg.Add(1)
	go a.collectLoop()

	if len(a.cfg.Exporters) > 0 {
		a.wg.Add(1)
		go a.exportLoop()
	}

	return nil
}

// Stop stops loops and waits up to ShutdownTimeout. It is idempotent.
func (a *Agent) Stop() {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return
	}
	a.stopped = true
	started := a.started
	cancel := a.cancel
	timeout := a.cfg.ShutdownTimeout
	a.mu.Unlock()

	if !started {
		return
	}
	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
	}

	for _, exp := range a.cfg.Exporters {
		_ = exp.Close()
	}
	if a.wal != nil {
		_ = a.wal.Close()
	}
}

// Metrics returns the shared metric registry.
func (a *Agent) Metrics() *metrics.Registry {
	return a.registry
}

func (a *Agent) collectLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(a.cfg.CollectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			s := a.collectSample()
			a.buffer.Push(s)
			if a.wal != nil {
				_ = a.wal.WriteSample(s)
			}
		}
	}
}

func (a *Agent) exportLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(a.cfg.ExportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			a.flushExports()
			return
		case <-ticker.C:
			a.flushExports()
		}
	}
}

// BufferLen returns the number of in-memory buffered samples.
func (a *Agent) BufferLen() int {
	return a.buffer.Len()
}

// BufferOverflowCount returns total oldest-evict events in the ring buffer.
func (a *Agent) BufferOverflowCount() uint64 {
	return a.buffer.OverflowCount()
}

// PeekSamples returns up to n buffered samples in insertion order.
func (a *Agent) PeekSamples(n int) []Sample {
	return a.buffer.Peek(n)
}

func (a *Agent) collectSample() Sample {
	values := collectRuntimeValues()
	if !a.cfg.DisableHardware && a.hw != nil {
		for k, v := range a.hw.Collect() {
			values[k] = v
		}
	}
	app := a.registry.SnapshotValues()
	for k, v := range app {
		if _, exists := values[k]; !exists {
			values[k] = v
		}
	}

	return Sample{
		Timestamp: time.Now().UTC(),
		Values:    values,
	}
}

func collectRuntimeValues() map[string]float64 {
	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)

	lastPause := uint64(0)
	if ms.NumGC > 0 {
		idx := (ms.NumGC - 1) % uint32(len(ms.PauseNs))
		lastPause = ms.PauseNs[idx]
	}

	return map[string]float64{
		"runtime.goroutines":        float64(runtime.NumGoroutine()),
		"runtime.heap_alloc_bytes":  float64(ms.HeapAlloc),
		"runtime.heap_inuse_bytes":  float64(ms.HeapInuse),
		"runtime.heap_sys_bytes":    float64(ms.HeapSys),
		"runtime.stack_inuse_bytes": float64(ms.StackInuse),
		"runtime.gc_count":          float64(ms.NumGC),
		"runtime.gc_pause_ns_last":  float64(lastPause),
		"runtime.gc_pause_total_ns": float64(ms.PauseTotalNs),
		"runtime.next_gc_bytes":     float64(ms.NextGC),
		"runtime.mallocs_total":     float64(ms.Mallocs),
		"runtime.frees_total":       float64(ms.Frees),
		"runtime.cgo_calls_total":   float64(runtime.NumCgoCall()),
		"runtime.gc_cpu_fraction":   ms.GCCPUFraction,
	}
}

func (a *Agent) flushExports() {
	samples := a.buffer.DrainAll()
	if len(samples) == 0 || len(a.cfg.Exporters) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(a.ctx, a.cfg.ExportTimeout)
	defer cancel()

	for _, exp := range a.cfg.Exporters {
		if err := exp.Export(ctx, samples); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				for _, s := range samples {
					a.buffer.Push(s)
				}
				return
			}
			for _, s := range samples {
				a.buffer.Push(s)
			}
			return
		}
	}
}

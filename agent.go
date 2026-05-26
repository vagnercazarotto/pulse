package pulse

import (
	"context"
	"errors"
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

func (a *Agent) collectSample() Sample {
	return Sample{
		Timestamp: time.Now().UTC(),
		Values:    a.registry.SnapshotValues(),
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

package pulse

import (
	"context"
	"sync"
	"time"

	"github.com/vagnercazarotto/pulse/metrics"
)

// Agent orchestrates collection and export loops.
type Agent struct {
	cfg      Config
	registry *metrics.Registry

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
	return &Agent{
		cfg:      resolved,
		registry: metrics.NewRegistry(),
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
			// Collector implementation will be added in next increment.
		}
	}
}

func (a *Agent) exportLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(a.cfg.CollectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			// Export scheduling implementation will be added in next increment.
		}
	}
}

package pulse

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestAgentStartStopContract(t *testing.T) {
	a := New(Config{CollectInterval: 20 * time.Millisecond})
	if err := a.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if err := a.Start(); err != nil {
		t.Fatalf("start should be idempotent while running: %v", err)
	}

	a.Stop()
	a.Stop()

	if err := a.Start(); err != ErrAgentStopped {
		t.Fatalf("expected ErrAgentStopped, got %v", err)
	}
}

func TestAgentCollectsIntoBufferWithoutExporters(t *testing.T) {
	a := New(Config{CollectInterval: 10 * time.Millisecond})
	if err := a.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	time.Sleep(35 * time.Millisecond)
	a.Stop()

	if a.BufferLen() == 0 {
		t.Fatalf("expected buffered samples, got 0")
	}
}

type recordingExporter struct {
	mu      sync.Mutex
	batches int
	count   int
}

func (e *recordingExporter) Name() string { return "recording" }
func (e *recordingExporter) Close() error { return nil }
func (e *recordingExporter) Export(_ context.Context, samples []Sample) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.batches++
	e.count += len(samples)
	return nil
}

func (e *recordingExporter) totals() (int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.batches, e.count
}

func TestAgentReplaysWALAndExports(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(WALConfig{Dir: dir, SyncEvery: 1})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	if err := w.WriteSample(Sample{Timestamp: time.Now().UTC(), Values: map[string]float64{"boot": 1}}); err != nil {
		t.Fatalf("write sample: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	rec := &recordingExporter{}
	a := New(Config{
		CollectInterval: 500 * time.Millisecond,
		ExportInterval:  20 * time.Millisecond,
		ExportTimeout:   100 * time.Millisecond,
		WAL:             &WALConfig{Dir: dir, SyncEvery: 1},
		Exporters:       []Exporter{rec},
	})

	if err := a.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	time.Sleep(90 * time.Millisecond)
	a.Stop()

	_, count := rec.totals()
	if count == 0 {
		t.Fatalf("expected at least one exported sample from WAL replay")
	}
}

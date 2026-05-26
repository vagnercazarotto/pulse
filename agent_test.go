package pulse

import (
	"context"
	"errors"
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

	samples := a.PeekSamples(1)
	if len(samples) == 0 {
		t.Fatalf("expected at least one sample")
	}
	if _, ok := samples[0].Values["runtime.goroutines"]; !ok {
		t.Fatalf("expected runtime.goroutines in sample")
	}
	if _, ok := samples[0].Values["runtime.heap_alloc_bytes"]; !ok {
		t.Fatalf("expected runtime.heap_alloc_bytes in sample")
	}
}

func TestCollectSampleMergesRuntimeAndAppMetrics(t *testing.T) {
	a := New(Config{})
	a.Metrics().Counter("app.events", map[string]string{"device": "x"}).Inc()

	s := a.collectSample()
	if _, ok := s.Values["runtime.gc_count"]; !ok {
		t.Fatalf("expected runtime.gc_count in sample")
	}
	if _, ok := s.Values["app.events|device=x"]; !ok {
		t.Fatalf("expected app metric in sample")
	}
}

func TestCollectSampleWithHardwareDisabled(t *testing.T) {
	a := New(Config{DisableHardware: true})
	s := a.collectSample()
	if _, ok := s.Values["runtime.goroutines"]; !ok {
		t.Fatalf("expected runtime metrics even when hardware is disabled")
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

type flakyExporter struct {
	mu        sync.Mutex
	failsLeft int
	calls     int
	succeeded bool
	nonRetry  bool
}

func (e *flakyExporter) Name() string { return "flaky" }
func (e *flakyExporter) Close() error { return nil }
func (e *flakyExporter) Export(_ context.Context, _ []Sample) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	if e.failsLeft > 0 {
		e.failsLeft--
		if e.nonRetry {
			return NonRetryable(errors.New("permanent export error"))
		}
		return errors.New("transient export error")
	}
	e.succeeded = true
	return nil
}

func (e *flakyExporter) snapshot() (int, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls, e.succeeded
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

func TestFlushExportsRetriesAndEventuallySucceeds(t *testing.T) {
	fx := &flakyExporter{failsLeft: 2}
	a := New(Config{
		Exporters:            []Exporter{fx},
		ExportMaxRetries:     3,
		ExportBackoffInitial: 1 * time.Millisecond,
		ExportBackoffMax:     2 * time.Millisecond,
		ExportBackoffJitter:  0,
	})
	a.ctx = context.Background()
	a.buffer.Push(Sample{Timestamp: time.Now().UTC(), Values: map[string]float64{"x": 1}})

	a.flushExports()

	calls, succeeded := fx.snapshot()
	if !succeeded {
		t.Fatalf("expected exporter to succeed after retries")
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if a.BufferLen() != 0 {
		t.Fatalf("expected buffer to be empty after successful export")
	}
}

func TestFlushExportsRequeuesAfterExhaustedRetries(t *testing.T) {
	fx := &flakyExporter{failsLeft: 10}
	a := New(Config{
		Exporters:            []Exporter{fx},
		ExportMaxRetries:     2,
		ExportBackoffInitial: 1 * time.Millisecond,
		ExportBackoffMax:     2 * time.Millisecond,
		ExportBackoffJitter:  0,
	})
	a.ctx = context.Background()
	a.buffer.Push(Sample{Timestamp: time.Now().UTC(), Values: map[string]float64{"x": 1}})

	a.flushExports()

	calls, succeeded := fx.snapshot()
	if succeeded {
		t.Fatalf("expected exporter to fail after retries")
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls (1 + 2 retries), got %d", calls)
	}
	if a.BufferLen() == 0 {
		t.Fatalf("expected sample to be requeued")
	}
}

func TestFlushExportsNonRetryableFailsFast(t *testing.T) {
	fx := &flakyExporter{failsLeft: 1, nonRetry: true}
	a := New(Config{
		Exporters:            []Exporter{fx},
		ExportMaxRetries:     5,
		ExportBackoffInitial: 1 * time.Millisecond,
		ExportBackoffMax:     2 * time.Millisecond,
		ExportBackoffJitter:  0,
	})
	a.ctx = context.Background()
	a.buffer.Push(Sample{Timestamp: time.Now().UTC(), Values: map[string]float64{"x": 1}})

	a.flushExports()

	calls, succeeded := fx.snapshot()
	if succeeded {
		t.Fatalf("expected exporter to fail")
	}
	if calls != 1 {
		t.Fatalf("expected 1 call for non-retryable error, got %d", calls)
	}
	if a.BufferLen() == 0 {
		t.Fatalf("expected sample to be requeued")
	}
}

func TestFlushExportsAcknowledgesWALOnSuccess(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(WALConfig{Dir: dir, SegmentSize: 128, SyncEvery: 1})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer w.Close()

	rec := &recordingExporter{}
	a := New(Config{
		Exporters:            []Exporter{rec},
		ExportMaxRetries:     1,
		ExportBackoffInitial: 1 * time.Millisecond,
		ExportBackoffMax:     2 * time.Millisecond,
		ExportBackoffJitter:  0,
	})
	a.ctx = context.Background()
	a.wal = w

	maxSegment := -1
	for i := 0; i < 20; i++ {
		s := Sample{
			Timestamp: time.Now().UTC(),
			Values: map[string]float64{
				"v":   float64(i),
				"pad": float64(i * 1000),
			},
		}
		seg, err := w.WriteSampleWithSegment(s)
		if err != nil {
			t.Fatalf("write sample %d: %v", i, err)
		}
		if seg > maxSegment {
			maxSegment = seg
		}
		a.buffer.PushWithWALSegment(s, seg)
	}

	if maxSegment < 1 {
		t.Fatalf("expected multiple WAL segments, got max %d", maxSegment)
	}

	a.flushExports()

	if w.acknowledgedThrough != maxSegment {
		t.Fatalf("expected acknowledgedThrough=%d, got %d", maxSegment, w.acknowledgedThrough)
	}

	segments, err := listSegments(dir)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	for _, seg := range segments {
		if idx := segmentIndex(seg); idx < maxSegment {
			t.Fatalf("expected old segment compacted, found %s", seg)
		}
	}
}

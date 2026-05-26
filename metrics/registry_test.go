package metrics

import (
	"testing"
	"time"
)

func TestCanonicalKeySortsLabels(t *testing.T) {
	k := CanonicalKey("modbus.errors", map[string]string{"protocol": "tcp", "device": "boiler-01"})
	expected := "modbus.errors|device=boiler-01|protocol=tcp"
	if k != expected {
		t.Fatalf("expected %s, got %s", expected, k)
	}
}

func TestHistogramBucketMismatch(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Histogram("latency", map[string]string{"device": "a"}, 0.1, 1.0); err != nil {
		t.Fatalf("unexpected first registration error: %v", err)
	}
	if _, err := r.Histogram("latency", map[string]string{"device": "a"}, 0.2, 1.0); err != ErrBucketMismatch {
		t.Fatalf("expected ErrBucketMismatch, got %v", err)
	}
}

func TestGaugeArithmetic(t *testing.T) {
	r := NewRegistry()
	g := r.Gauge("queue.depth", nil)
	g.Set(2)
	g.Inc()
	g.Add(4.5)
	g.Dec()

	if got := g.Value(); got != 6.5 {
		t.Fatalf("expected gauge 6.5, got %v", got)
	}
}

func TestHistogramSnapshotValues(t *testing.T) {
	r := NewRegistry()
	h, err := r.Histogram("latency", map[string]string{"device": "a"}, 0.1, 1.0)
	if err != nil {
		t.Fatalf("unexpected histogram registration error: %v", err)
	}

	h.Observe(0.05)
	h.Observe(0.7)
	h.Observe(2.0)

	values := r.SnapshotValues()
	if got := values["latency_count|device=a"]; got != 3 {
		t.Fatalf("expected count 3, got %v", got)
	}
	if got := values["latency_bucket|device=a|le=0.1"]; got != 1 {
		t.Fatalf("expected first bucket 1, got %v", got)
	}
	if got := values["latency_bucket|device=a|le=1"]; got != 2 {
		t.Fatalf("expected second bucket 2, got %v", got)
	}
	if got := values["latency_bucket|device=a|le=+Inf"]; got != 3 {
		t.Fatalf("expected +Inf bucket 3, got %v", got)
	}
	if got := values["latency_sum|device=a"]; got < 2.74 || got > 2.76 {
		t.Fatalf("expected sum around 2.75, got %v", got)
	}
}

func TestTimerRecordAndStart(t *testing.T) {
	r := NewRegistry()
	timer, err := r.Timer("poll_latency_s", map[string]string{"device": "a"})
	if err != nil {
		t.Fatalf("unexpected timer registration error: %v", err)
	}

	timer.Record(50 * time.Millisecond)
	stop := timer.Start()
	time.Sleep(2 * time.Millisecond)
	stop()

	values := r.SnapshotValues()
	if got := values["poll_latency_s_count|device=a"]; got != 2 {
		t.Fatalf("expected count 2, got %v", got)
	}
	if got := values["poll_latency_s_sum|device=a"]; got <= 0.05 {
		t.Fatalf("expected sum above 0.05, got %v", got)
	}
}

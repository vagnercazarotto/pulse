package metrics

import "testing"

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

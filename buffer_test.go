package pulse

import (
	"testing"
	"time"
)

func TestRingBufferOverflowAndOrder(t *testing.T) {
	b, err := NewRingBuffer(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b.Push(Sample{Timestamp: time.Unix(1, 0), Values: map[string]float64{"v": 1}})
	b.Push(Sample{Timestamp: time.Unix(2, 0), Values: map[string]float64{"v": 2}})
	b.Push(Sample{Timestamp: time.Unix(3, 0), Values: map[string]float64{"v": 3}})

	if got := b.OverflowCount(); got != 1 {
		t.Fatalf("expected overflow 1, got %d", got)
	}

	drained := b.DrainAll()
	if len(drained) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(drained))
	}
	if drained[0].Values["v"] != 2 || drained[1].Values["v"] != 3 {
		t.Fatalf("expected values 2,3 got %v,%v", drained[0].Values["v"], drained[1].Values["v"])
	}
}

func TestRingBufferInvalidCapacity(t *testing.T) {
	if _, err := NewRingBuffer(0); err != ErrInvalidBufferCapacity {
		t.Fatalf("expected ErrInvalidBufferCapacity, got %v", err)
	}
}

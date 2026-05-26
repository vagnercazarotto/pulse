package pulse

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrInvalidBufferCapacity = errors.New("pulse: buffer capacity must be greater than zero")

// RingBuffer is a fixed-size, thread-safe circular buffer for samples.
type RingBuffer struct {
	mu   sync.Mutex
	data []bufferedSample
	head int
	size int

	overflow atomic.Uint64
}

type bufferedSample struct {
	sample     Sample
	walSegment int
}

// NewRingBuffer creates a new ring buffer with fixed capacity.
func NewRingBuffer(capacity int) (*RingBuffer, error) {
	if capacity <= 0 {
		return nil, ErrInvalidBufferCapacity
	}
	return &RingBuffer{data: make([]bufferedSample, capacity)}, nil
}

// Push inserts a sample. If full, the oldest sample is evicted.
func (b *RingBuffer) Push(s Sample) {
	b.PushWithWALSegment(s, -1)
}

// PushWithWALSegment inserts a sample and tracks the WAL segment id when known.
func (b *RingBuffer) PushWithWALSegment(s Sample, walSegment int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.data) == 0 {
		return
	}
	entry := bufferedSample{sample: cloneSample(s), walSegment: walSegment}

	if b.size == len(b.data) {
		b.data[b.head] = entry
		b.head = (b.head + 1) % len(b.data)
		b.overflow.Add(1)
		return
	}

	tail := (b.head + b.size) % len(b.data)
	b.data[tail] = entry
	b.size++
}

// DrainAllWithWALSegments returns all buffered entries and clears the buffer.
func (b *RingBuffer) DrainAllWithWALSegments() []bufferedSample {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.size == 0 {
		return nil
	}

	out := make([]bufferedSample, 0, b.size)
	for i := 0; i < b.size; i++ {
		idx := (b.head + i) % len(b.data)
		entry := b.data[idx]
		entry.sample = cloneSample(entry.sample)
		out = append(out, entry)
	}
	b.head = 0
	b.size = 0
	return out
}

// DrainAll returns all samples in insertion order and clears the buffer.
func (b *RingBuffer) DrainAll() []Sample {
	entries := b.DrainAllWithWALSegments()
	if len(entries) == 0 {
		return nil
	}
	out := make([]Sample, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.sample)
	}
	return out
}

// Peek returns up to n samples in insertion order without removing them.
func (b *RingBuffer) Peek(n int) []Sample {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.size == 0 || n <= 0 {
		return nil
	}
	if n > b.size {
		n = b.size
	}

	out := make([]Sample, 0, n)
	for i := 0; i < n; i++ {
		idx := (b.head + i) % len(b.data)
		out = append(out, cloneSample(b.data[idx].sample))
	}
	return out
}

func (b *RingBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

func (b *RingBuffer) Cap() int {
	return len(b.data)
}

func (b *RingBuffer) IsFull() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size == len(b.data)
}

func (b *RingBuffer) OverflowCount() uint64 {
	return b.overflow.Load()
}

func cloneSample(in Sample) Sample {
	out := in
	if len(in.Values) > 0 {
		out.Values = make(map[string]float64, len(in.Values))
		for k, v := range in.Values {
			out.Values[k] = v
		}
	}
	return out
}

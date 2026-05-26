package metrics

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var ErrBucketMismatch = errors.New("metrics: bucket mismatch for existing metric identity")

// Registry stores metrics keyed by canonical identity.
type Registry struct {
	mu         sync.RWMutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
}

func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
	}
}

func CanonicalKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteString("|")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(labels[k])
	}
	return b.String()
}

func copyLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	cp := make(map[string]string, len(labels))
	for k, v := range labels {
		cp[k] = v
	}
	return cp
}

type Counter struct {
	name   string
	labels map[string]string
	value  atomic.Uint64
}

func (r *Registry) Counter(name string, labels map[string]string) *Counter {
	key := CanonicalKey(name, labels)
	r.mu.RLock()
	if c, ok := r.counters[key]; ok {
		r.mu.RUnlock()
		return c
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[key]; ok {
		return c
	}
	c := &Counter{name: name, labels: copyLabels(labels)}
	r.counters[key] = c
	return c
}

func (c *Counter) Inc()          { c.value.Add(1) }
func (c *Counter) Add(v uint64)  { c.value.Add(v) }
func (c *Counter) Value() uint64 { return c.value.Load() }

type Gauge struct {
	name   string
	labels map[string]string
	value  atomic.Uint64
}

func (r *Registry) Gauge(name string, labels map[string]string) *Gauge {
	key := CanonicalKey(name, labels)
	r.mu.RLock()
	if g, ok := r.gauges[key]; ok {
		r.mu.RUnlock()
		return g
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauges[key]; ok {
		return g
	}
	g := &Gauge{name: name, labels: copyLabels(labels)}
	r.gauges[key] = g
	return g
}

func (g *Gauge) Set(v float64) { g.value.Store(mathFloat64bits(v)) }

func (g *Gauge) Add(delta float64) {
	for {
		currentBits := g.value.Load()
		next := mathFloat64frombits(currentBits) + delta
		if g.value.CompareAndSwap(currentBits, mathFloat64bits(next)) {
			return
		}
	}
}

func (g *Gauge) Inc() { g.Add(1) }

func (g *Gauge) Dec() { g.Add(-1) }

func (g *Gauge) Value() float64 { return mathFloat64frombits(g.value.Load()) }

type Histogram struct {
	name    string
	labels  map[string]string
	buckets []float64
	counts  []atomic.Uint64
	count   atomic.Uint64
	sumBits atomic.Uint64
}

func (r *Registry) Histogram(name string, labels map[string]string, buckets ...float64) (*Histogram, error) {
	if len(buckets) == 0 {
		return nil, fmt.Errorf("metrics: histogram %s must have at least one bucket", name)
	}
	key := CanonicalKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.histograms[key]; ok {
		if !equalFloatSlices(h.buckets, buckets) {
			return nil, ErrBucketMismatch
		}
		return h, nil
	}

	h := &Histogram{
		name:    name,
		labels:  copyLabels(labels),
		buckets: append([]float64(nil), buckets...),
		counts:  make([]atomic.Uint64, len(buckets)),
	}
	r.histograms[key] = h
	return h, nil
}

func (h *Histogram) Observe(v float64) {
	idx := sort.SearchFloat64s(h.buckets, v)
	if idx < len(h.counts) {
		h.counts[idx].Add(1)
	}
	h.count.Add(1)
	addAtomicFloat64(&h.sumBits, v)
}

func (h *Histogram) Count() uint64 {
	return h.count.Load()
}

func (h *Histogram) Sum() float64 {
	return mathFloat64frombits(h.sumBits.Load())
}

func equalFloatSlices(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SnapshotValues returns scalar values keyed by canonical metric identity.
// Counters, gauges, and histogram-derived series are included.
func (r *Registry) SnapshotValues() map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]float64, len(r.counters)+len(r.gauges)+(len(r.histograms)*4))
	for key, c := range r.counters {
		out[key] = float64(c.Value())
	}
	for key, g := range r.gauges {
		out[key] = g.Value()
	}
	for _, h := range r.histograms {
		count := h.Count()
		out[CanonicalKey(h.name+"_count", h.labels)] = float64(count)
		out[CanonicalKey(h.name+"_sum", h.labels)] = h.Sum()

		cumulative := uint64(0)
		for i, bucket := range h.buckets {
			cumulative += h.counts[i].Load()
			labels := copyLabels(h.labels)
			if labels == nil {
				labels = make(map[string]string, 1)
			}
			labels["le"] = formatBucket(bucket)
			out[CanonicalKey(h.name+"_bucket", labels)] = float64(cumulative)
		}

		labels := copyLabels(h.labels)
		if labels == nil {
			labels = make(map[string]string, 1)
		}
		labels["le"] = "+Inf"
		out[CanonicalKey(h.name+"_bucket", labels)] = float64(count)
	}
	return out
}

func addAtomicFloat64(dst *atomic.Uint64, delta float64) {
	for {
		currentBits := dst.Load()
		next := mathFloat64frombits(currentBits) + delta
		if dst.CompareAndSwap(currentBits, mathFloat64bits(next)) {
			return
		}
	}
}

func formatBucket(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

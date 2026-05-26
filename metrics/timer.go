package metrics

import "time"

var DefaultLatencyBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0}

type Timer struct {
	h *Histogram
}

func (r *Registry) Timer(name string, labels map[string]string) (*Timer, error) {
	h, err := r.Histogram(name, labels, DefaultLatencyBuckets...)
	if err != nil {
		return nil, err
	}
	return &Timer{h: h}, nil
}

func (r *Registry) TimerWithBuckets(name string, labels map[string]string, buckets ...float64) (*Timer, error) {
	h, err := r.Histogram(name, labels, buckets...)
	if err != nil {
		return nil, err
	}
	return &Timer{h: h}, nil
}

func (t *Timer) Start() func() {
	start := time.Now()
	return func() {
		_ = time.Since(start)
		// Observation storage will be implemented in the histogram hot path increment.
	}
}

func (t *Timer) Record(_ time.Duration) {
	// Observation storage will be implemented in the histogram hot path increment.
}

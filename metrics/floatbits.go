package metrics

import "math"

func mathFloat64bits(v float64) uint64 {
	return math.Float64bits(v)
}

func mathFloat64frombits(v uint64) float64 {
	return math.Float64frombits(v)
}

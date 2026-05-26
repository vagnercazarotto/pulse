package pulse

import "time"

// Sample is the unit exported by exporters.
type Sample struct {
	Timestamp time.Time          `json:"timestamp"`
	Values    map[string]float64 `json:"values,omitempty"`
}

//go:build !linux

package pulse

// hardwareCollector is a no-op collector for non-linux targets.
type hardwareCollector struct{}

func newHardwareCollector() *hardwareCollector { return &hardwareCollector{} }

func (h *hardwareCollector) Collect() map[string]float64 {
	return nil
}

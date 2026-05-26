package pulse

import "context"

// Exporter sends collected samples to an external sink.
type Exporter interface {
	Name() string
	Export(ctx context.Context, samples []Sample) error
	Close() error
}

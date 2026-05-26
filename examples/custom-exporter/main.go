package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/vagnercazarotto/pulse"
)

type validatingExporter struct{}

func (e *validatingExporter) Name() string { return "validating" }

func (e *validatingExporter) Export(_ context.Context, samples []pulse.Sample) error {
	for _, sample := range samples {
		if len(sample.Values) == 0 {
			return pulse.NonRetryable(fmt.Errorf("sample has no values"))
		}
	}

	last := samples[len(samples)-1]
	log.Printf("exported %d samples, last timestamp=%s, metrics=%d\n", len(samples), last.Timestamp.Format(time.RFC3339), len(last.Values))
	return nil
}

func (e *validatingExporter) Close() error { return nil }

func main() {
	exporter := &validatingExporter{}

	agent := pulse.New(pulse.Config{
		CollectInterval: 1 * time.Second,
		ExportInterval:  2 * time.Second,
		Exporters:       []pulse.Exporter{exporter},
	})

	processed := agent.Metrics().Counter("pipeline.processed_total", map[string]string{
		"pipeline": "ingest",
	})

	if err := agent.Start(); err != nil {
		log.Fatal(err)
	}
	defer agent.Stop()

	for i := 0; i < 3; i++ {
		processed.Inc()
		time.Sleep(1 * time.Second)
	}
}

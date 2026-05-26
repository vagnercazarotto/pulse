package main

import (
	"log"
	"time"

	"github.com/vagnercazarotto/pulse"
)

func main() {
	httpExporter := pulse.NewHTTPExporter(pulse.HTTPExporterConfig{
		Addr: "127.0.0.1:9090",
	})

	agent := pulse.New(pulse.Config{
		CollectInterval: 2 * time.Second,
		ExportInterval:  2 * time.Second,
		Exporters:       []pulse.Exporter{httpExporter},
		WAL: &pulse.WALConfig{
			Dir: "./pulse-wal",
		},
	})

	requests := agent.Metrics().Counter("app.requests_total", map[string]string{
		"service": "gateway",
	})
	queueDepth := agent.Metrics().Gauge("app.queue_depth", map[string]string{
		"service": "gateway",
	})

	if err := agent.Start(); err != nil {
		log.Fatal(err)
	}
	defer agent.Stop()

	for i := 0; i < 5; i++ {
		requests.Inc()
		queueDepth.Set(float64(10 - i))
		time.Sleep(1 * time.Second)
	}

	log.Println("HTTP endpoints available at http://127.0.0.1:9090")
	log.Println("  /health")
	log.Println("  /metrics.json")
	log.Println("  /metrics")

	time.Sleep(10 * time.Second)
}

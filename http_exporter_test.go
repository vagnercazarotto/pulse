package pulse

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestHTTPExporterServesHealthAndMetrics(t *testing.T) {
	exp := NewHTTPExporter(HTTPExporterConfig{Addr: "127.0.0.1:0", StaleAfter: 2 * time.Second})
	defer exp.Close()

	err := exp.Export(context.Background(), []Sample{{
		Timestamp: time.Now().UTC(),
		Values: map[string]float64{
			"runtime.goroutines":       5,
			"app.events|device=boiler": 2,
		},
	}})
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var healthURL, metricsJSONURL, metricsURL, dashboardURL string
	for time.Now().Before(deadline) {
		dashboardURL = exp.URL("/")
		healthURL = exp.URL("/health")
		metricsJSONURL = exp.URL("/metrics.json")
		metricsURL = exp.URL("/metrics")
		if healthURL != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if healthURL == "" {
		t.Fatalf("http exporter did not start")
	}

	dashboardResp, err := http.Get(dashboardURL)
	if err != nil {
		t.Fatalf("dashboard request failed: %v", err)
	}
	defer dashboardResp.Body.Close()
	if dashboardResp.StatusCode != http.StatusOK {
		t.Fatalf("expected dashboard 200, got %d", dashboardResp.StatusCode)
	}
	dashboardBody, _ := io.ReadAll(dashboardResp.Body)
	if !strings.Contains(string(dashboardBody), "pulse local dashboard") {
		t.Fatalf("expected dashboard page content")
	}

	healthResp, err := http.Get(healthURL)
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected health 200, got %d", healthResp.StatusCode)
	}

	jsonResp, err := http.Get(metricsJSONURL)
	if err != nil {
		t.Fatalf("metrics.json request failed: %v", err)
	}
	defer jsonResp.Body.Close()
	if jsonResp.StatusCode != http.StatusOK {
		t.Fatalf("expected metrics.json 200, got %d", jsonResp.StatusCode)
	}
	jsonBody, _ := io.ReadAll(jsonResp.Body)
	if !strings.Contains(string(jsonBody), "runtime.goroutines") {
		t.Fatalf("expected runtime metric in json body")
	}

	promResp, err := http.Get(metricsURL)
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer promResp.Body.Close()
	if promResp.StatusCode != http.StatusOK {
		t.Fatalf("expected metrics 200, got %d", promResp.StatusCode)
	}
	promBody, _ := io.ReadAll(promResp.Body)
	if !strings.Contains(string(promBody), "pulse_runtime_goroutines") {
		t.Fatalf("expected prometheus runtime metric")
	}
}

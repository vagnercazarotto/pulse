package pulse

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultHTTPExporterAddr         = "127.0.0.1:9090"
	defaultHTTPExporterReadTimeout  = 5 * time.Second
	defaultHTTPExporterWriteTimeout = 10 * time.Second
	defaultHTTPExporterStaleAfter   = 30 * time.Second
)

type HTTPExporterConfig struct {
	Addr         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	StaleAfter   time.Duration
}

// HTTPExporter exposes the latest sample over local HTTP endpoints.
type HTTPExporter struct {
	cfg HTTPExporterConfig

	mu        sync.RWMutex
	latest    Sample
	hasLatest bool
	latestAt  time.Time

	startOnce sync.Once
	startErr  error

	server   *http.Server
	listener net.Listener
}

func NewHTTPExporter(cfg HTTPExporterConfig) *HTTPExporter {
	resolved := cfg
	if resolved.Addr == "" {
		resolved.Addr = defaultHTTPExporterAddr
	}
	if resolved.ReadTimeout <= 0 {
		resolved.ReadTimeout = defaultHTTPExporterReadTimeout
	}
	if resolved.WriteTimeout <= 0 {
		resolved.WriteTimeout = defaultHTTPExporterWriteTimeout
	}
	if resolved.StaleAfter <= 0 {
		resolved.StaleAfter = defaultHTTPExporterStaleAfter
	}

	return &HTTPExporter{cfg: resolved}
}

func (e *HTTPExporter) Name() string { return "http" }

func (e *HTTPExporter) Export(_ context.Context, samples []Sample) error {
	e.startOnce.Do(func() {
		e.startErr = e.start()
	})
	if e.startErr != nil {
		return e.startErr
	}
	if len(samples) == 0 {
		return nil
	}

	last := samples[len(samples)-1]
	e.mu.Lock()
	e.latest = cloneSample(last)
	e.latestAt = time.Now().UTC()
	e.hasLatest = true
	e.mu.Unlock()
	return nil
}

func (e *HTTPExporter) Close() error {
	e.mu.Lock()
	srv := e.server
	e.mu.Unlock()
	if srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

// Addr returns the effective listening address, useful in tests.
func (e *HTTPExporter) Addr() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.listener == nil {
		return ""
	}
	return e.listener.Addr().String()
}

func (e *HTTPExporter) start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", e.handleHealth)
	mux.HandleFunc("/metrics.json", e.handleMetricsJSON)
	mux.HandleFunc("/metrics", e.handlePrometheus)

	ln, err := net.Listen("tcp", e.cfg.Addr)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  e.cfg.ReadTimeout,
		WriteTimeout: e.cfg.WriteTimeout,
	}

	e.mu.Lock()
	e.listener = ln
	e.server = srv
	e.mu.Unlock()

	go func() {
		_ = srv.Serve(ln)
	}()
	return nil
}

func (e *HTTPExporter) handleHealth(w http.ResponseWriter, _ *http.Request) {
	latest, latestAt, ok := e.getLatest()
	_ = latest

	stale := true
	if ok {
		stale = time.Since(latestAt) > e.cfg.StaleAfter
	}

	status := "ok"
	code := http.StatusOK
	if !ok || stale {
		status = "stale"
		code = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":      status,
		"has_sample":  ok,
		"updated_at":  latestAt,
		"stale_after": e.cfg.StaleAfter.String(),
	})
}

func (e *HTTPExporter) handleMetricsJSON(w http.ResponseWriter, _ *http.Request) {
	latest, _, ok := e.getLatest()
	if !ok {
		http.Error(w, "no samples yet", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(latest)
}

func (e *HTTPExporter) handlePrometheus(w http.ResponseWriter, _ *http.Request) {
	latest, _, ok := e.getLatest()
	if !ok {
		http.Error(w, "no samples yet", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(renderPrometheus(latest)))
}

func (e *HTTPExporter) getLatest() (Sample, time.Time, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.hasLatest {
		return Sample{}, time.Time{}, false
	}
	return cloneSample(e.latest), e.latestAt, true
}

func renderPrometheus(s Sample) string {
	var b strings.Builder
	for key, value := range s.Values {
		name, labels := splitCanonicalKey(key)
		metricName := sanitizePromName("pulse_" + name)
		if len(labels) == 0 {
			b.WriteString(metricName)
			b.WriteString(" ")
			b.WriteString(strconv.FormatFloat(value, 'f', -1, 64))
			b.WriteString("\n")
			continue
		}
		b.WriteString(metricName)
		b.WriteString("{")
		for i, kv := range labels {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(kv[0])
			b.WriteString("=\"")
			b.WriteString(strings.ReplaceAll(kv[1], "\"", "\\\""))
			b.WriteString("\"")
		}
		b.WriteString("} ")
		b.WriteString(strconv.FormatFloat(value, 'f', -1, 64))
		b.WriteString("\n")
	}
	return b.String()
}

func splitCanonicalKey(key string) (string, [][2]string) {
	parts := strings.Split(key, "|")
	if len(parts) == 1 {
		return parts[0], nil
	}
	name := parts[0]
	labels := make([][2]string, 0, len(parts)-1)
	for _, p := range parts[1:] {
		idx := strings.Index(p, "=")
		if idx <= 0 || idx >= len(p)-1 {
			continue
		}
		labels = append(labels, [2]string{sanitizePromLabelName(p[:idx]), p[idx+1:]})
	}
	return name, labels
}

var promNameCleaner = regexp.MustCompile(`[^a-zA-Z0-9_:]`)
var promLabelCleaner = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func sanitizePromName(name string) string {
	clean := promNameCleaner.ReplaceAllString(name, "_")
	if clean == "" {
		return "pulse_metric"
	}
	first := clean[0]
	if !(first == '_' || first == ':' || (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z')) {
		return "pulse_" + clean
	}
	return clean
}

func sanitizePromLabelName(name string) string {
	clean := promLabelCleaner.ReplaceAllString(name, "_")
	if clean == "" {
		return "label"
	}
	first := clean[0]
	if !(first == '_' || (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z')) {
		return "l_" + clean
	}
	return clean
}

func (e *HTTPExporter) URL(path string) string {
	addr := e.Addr()
	if addr == "" {
		return ""
	}
	return fmt.Sprintf("http://%s%s", addr, path)
}

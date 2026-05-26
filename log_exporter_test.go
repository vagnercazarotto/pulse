package pulse

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestLogExporterWritesJSONLines(t *testing.T) {
	var buf bytes.Buffer
	exp := NewLogExporter(LogExporterConfig{Output: &buf})

	err := exp.Export(context.Background(), []Sample{
		{Timestamp: time.Unix(1, 0).UTC(), Values: map[string]float64{"a": 1}},
		{Timestamp: time.Unix(2, 0).UTC(), Values: map[string]float64{"b": 2}},
	})
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	for i, line := range lines {
		var s Sample
		if err := json.Unmarshal([]byte(line), &s); err != nil {
			t.Fatalf("line %d invalid json: %v", i, err)
		}
	}
}

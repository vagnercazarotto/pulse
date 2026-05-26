package pulse

import (
	"testing"
	"time"
)

func TestAgentStartStopContract(t *testing.T) {
	a := New(Config{CollectInterval: 20 * time.Millisecond})
	if err := a.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if err := a.Start(); err != nil {
		t.Fatalf("start should be idempotent while running: %v", err)
	}

	a.Stop()
	a.Stop()

	if err := a.Start(); err != ErrAgentStopped {
		t.Fatalf("expected ErrAgentStopped, got %v", err)
	}
}

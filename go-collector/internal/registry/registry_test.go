package registry_test

import (
	"context"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

func TestHeartbeatChecker_MarksInactive(t *testing.T) {
	reg := registry.New()
	if err := reg.Register("node-1", "host1", "1.2.3.4", "v1"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// timeout=50ms, interval=20ms — very short for tests
	reg.StartHeartbeatChecker(ctx, 50*time.Millisecond, 20*time.Millisecond)

	// node should be active right after register
	assertStatus(t, reg, "node-1", "active")

	// wait longer than timeout without sending a heartbeat
	time.Sleep(100 * time.Millisecond)
	assertStatus(t, reg, "node-1", "inactive")
}

func TestHeartbeatChecker_StaysActiveWithHeartbeat(t *testing.T) {
	reg := registry.New()
	if err := reg.Register("node-2", "host2", "1.2.3.5", "v1"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg.StartHeartbeatChecker(ctx, 80*time.Millisecond, 20*time.Millisecond)

	// send heartbeats every 30ms for 150ms total → should stay active
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.After(150 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				reg.Heartbeat("node-2")
			case <-deadline:
				close(done)
				return
			}
		}
	}()
	<-done

	assertStatus(t, reg, "node-2", "active")
}

func TestHeartbeatChecker_StopsOnContextCancel(t *testing.T) {
	reg := registry.New()
	ctx, cancel := context.WithCancel(context.Background())

	reg.StartHeartbeatChecker(ctx, 10*time.Millisecond, 5*time.Millisecond)
	cancel() // goroutine should exit cleanly — no panic or hang
	time.Sleep(20 * time.Millisecond)
}

func assertStatus(t *testing.T, reg *registry.Registry, nodeID, want string) {
	t.Helper()
	for _, n := range reg.List() {
		if n.NodeID == nodeID {
			if n.Status != want {
				t.Errorf("node %s: status = %q, want %q", nodeID, n.Status, want)
			}
			return
		}
	}
	t.Errorf("node %s not found", nodeID)
}

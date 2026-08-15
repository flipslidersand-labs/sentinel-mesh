package registry_test

import (
	"context"
	"testing"
	"time"

	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

func TestHeartbeatChecker_MarksInactive(t *testing.T) {
	reg := registry.New()
	if err := reg.Register("node-1", "host1", "1.2.3.4", "v1", "us-east"); err != nil {
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
	if err := reg.Register("node-2", "host2", "1.2.3.5", "v1", "us-west"); err != nil {
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

func TestRegister_EmptyRegionNormalizedToDefault(t *testing.T) {
	reg := registry.New()
	if err := reg.Register("node-x", "h", "ip", "v1", ""); err != nil {
		t.Fatal(err)
	}
	nodes := reg.ListByRegion("")
	if len(nodes) != 1 || nodes[0].Region != registry.DefaultRegion {
		t.Fatalf("empty region should normalize to %q, got %+v", registry.DefaultRegion, nodes)
	}
	// Explicitly querying DefaultRegion returns the same node.
	if len(reg.ListByRegion(registry.DefaultRegion)) != 1 {
		t.Error("ListByRegion(DefaultRegion) should find the node")
	}
}

func TestListByRegion_And_Regions(t *testing.T) {
	reg := registry.New()
	mustRegister(t, reg, "a1", "us-east")
	mustRegister(t, reg, "a2", "us-east")
	mustRegister(t, reg, "b1", "eu-west")

	east := reg.ListByRegion("us-east")
	if len(east) != 2 {
		t.Errorf("us-east: got %d nodes, want 2", len(east))
	}
	if got := reg.ListByRegion("ap-south"); len(got) != 0 {
		t.Errorf("unknown region should be empty, got %d", len(got))
	}

	// Regions() is sorted and unique.
	regions := reg.Regions()
	want := []string{"eu-west", "us-east"}
	if len(regions) != len(want) {
		t.Fatalf("regions = %v, want %v", regions, want)
	}
	for i := range want {
		if regions[i] != want[i] {
			t.Errorf("regions[%d] = %q, want %q", i, regions[i], want[i])
		}
	}
}

func TestRegister_ReRegisterUpdatesRegion(t *testing.T) {
	reg := registry.New()
	mustRegister(t, reg, "n", "us-east")
	mustRegister(t, reg, "n", "eu-west") // same node re-registers in a new region
	if got := reg.ListByRegion("us-east"); len(got) != 0 {
		t.Errorf("node should have moved off us-east, got %d", len(got))
	}
	if got := reg.ListByRegion("eu-west"); len(got) != 1 {
		t.Errorf("node should be in eu-west, got %d", len(got))
	}
}

func mustRegister(t *testing.T, reg *registry.Registry, nodeID, region string) {
	t.Helper()
	if err := reg.Register(nodeID, "host", "ip", "v1", region); err != nil {
		t.Fatal(err)
	}
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

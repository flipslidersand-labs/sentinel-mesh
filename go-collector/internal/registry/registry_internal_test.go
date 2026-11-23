package registry

import (
	"testing"
	"time"
)

// inactiveSince is unexported, so these checks live in the internal package.

func TestSetInactive_DoesNotSetInactiveSince(t *testing.T) {
	reg := New()
	if err := reg.Register("node-a", "host-a", "192.0.2.1", "v1", "us-east"); err != nil {
		t.Fatal(err)
	}

	reg.SetInactive("node-a")

	if !reg.nodes["node-a"].inactiveSince.IsZero() {
		t.Fatal("SetInactive set inactiveSince; the doc comment says it does not")
	}
}

func TestHeartbeat_ClearsInactiveSince(t *testing.T) {
	reg := New()
	if err := reg.Register("node-a", "host-a", "192.0.2.1", "v1", "us-east"); err != nil {
		t.Fatal(err)
	}
	// Let the checker's staleness path mark it inactive and stamp inactiveSince.
	time.Sleep(5 * time.Millisecond)
	reg.markStaleInactive(time.Millisecond, time.Hour)
	n := reg.nodes["node-a"]
	if n.Status != "inactive" || n.inactiveSince.IsZero() {
		t.Fatalf("precondition: status=%q inactiveSince zero=%v, want inactive/non-zero", n.Status, n.inactiveSince.IsZero())
	}

	reg.Heartbeat("node-a")

	if n.Status != StatusActive {
		t.Errorf("status = %q, want active", n.Status)
	}
	if !n.inactiveSince.IsZero() {
		t.Error("inactiveSince not cleared by Heartbeat")
	}
}

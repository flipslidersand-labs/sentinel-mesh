package registry

import (
	"testing"
	"time"
)

func TestReapInactive(t *testing.T) {
	r := New()
	if err := r.Register("node-a", "host-a", "10.0.0.1", "v1"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Not yet stale: a generous timeout keeps the node active.
	if reaped := r.ReapInactive(time.Hour); len(reaped) != 0 {
		t.Fatalf("expected no reap, got %v", reaped)
	}
	if got := r.List()[0].Status; got != "active" {
		t.Fatalf("status = %q, want active", got)
	}

	// Let LastSeen age, then reap with a tiny timeout.
	time.Sleep(5 * time.Millisecond)
	reaped := r.ReapInactive(time.Millisecond)
	if len(reaped) != 1 || reaped[0] != "node-a" {
		t.Fatalf("reaped = %v, want [node-a]", reaped)
	}
	if got := r.List()[0].Status; got != "inactive" {
		t.Fatalf("status = %q, want inactive", got)
	}

	// Idempotent: an already-inactive node is not reaped again.
	if reaped := r.ReapInactive(time.Millisecond); len(reaped) != 0 {
		t.Fatalf("expected no second reap, got %v", reaped)
	}
}

func TestHeartbeatKeepsActive(t *testing.T) {
	r := New()
	_ = r.Register("node-b", "host-b", "10.0.0.2", "v1")
	time.Sleep(5 * time.Millisecond)
	r.Heartbeat("node-b") // refresh LastSeen

	if reaped := r.ReapInactive(time.Hour); len(reaped) != 0 {
		t.Fatalf("fresh heartbeat should not be reaped, got %v", reaped)
	}
}

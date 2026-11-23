package receiver

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func touchCount(fs *fakeStorage, nodeID string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.touches[nodeID]
}

func waitTouches(t *testing.T, fs *fakeStorage, nodeID string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for touchCount(fs, nodeID) < want {
		if time.Now().After(deadline) {
			t.Fatalf("touches[%s] = %d, want >= %d", nodeID, touchCount(fs, nodeID), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// #537: a node that stays connected but sends no events must still have its
// region record refreshed, from the liveness loop, with the registry's region.
func TestLiveness_TouchesNodeRegionWithoutEvents(t *testing.T) {
	fs := &fakeStorage{}
	reg := registry.New()
	if err := reg.Register("node-1", "h", "192.0.2.1", "v", "eu-west"); err != nil {
		t.Fatal(err)
	}
	s := &server{st: fs, reg: reg, log: zap.NewNop(), livenessInterval: 10 * time.Millisecond}
	stop, _ := s.startLiveness(context.Background(), "node-1", nil)
	defer stop()

	waitTouches(t, fs, "node-1", 2)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.regions["node-1"] != "eu-west" {
		t.Fatalf("regions = %v, want node-1 kept in eu-west", fs.regions)
	}
}

// A node the registry does not know has no confirmed region: liveness must not
// touch (and so never persist) a guessed one.
func TestLiveness_UnknownNodeIsNotTouched(t *testing.T) {
	fs := &fakeStorage{}
	s := &server{st: fs, reg: registry.New(), defaultRegion: "default", log: zap.NewNop(), livenessInterval: 10 * time.Millisecond}
	stop, _ := s.startLiveness(context.Background(), "node-1", nil)
	time.Sleep(100 * time.Millisecond)
	stop()
	if n := touchCount(fs, "node-1"); n != 0 {
		t.Fatalf("touches = %d, want 0 for a node missing from the registry", n)
	}
}

// A busy or closed store is logged only: liveness keeps heartbeating and
// retrying.
func TestLiveness_TouchFailureKeepsLivenessRunning(t *testing.T) {
	fs := &fakeStorage{touchErr: store.ErrBusy}
	reg := registry.New()
	if err := reg.Register("node-1", "h", "192.0.2.1", "v", "eu-west"); err != nil {
		t.Fatal(err)
	}
	s := &server{st: fs, reg: reg, log: zap.NewNop(), livenessInterval: 10 * time.Millisecond}
	stop, _ := s.startLiveness(context.Background(), "node-1", nil)
	defer stop()
	waitTouches(t, fs, "node-1", 3) // kept retrying after the failures
}

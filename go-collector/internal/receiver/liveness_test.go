package receiver

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

// idleStream blocks in Recv until its context is done, like a connected
// agent that sends nothing.
type idleStream struct {
	fakeStream
	ctx context.Context
}

func (s *idleStream) Recv() (*pb.KernelEvent, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}
func (s *idleStream) Context() context.Context { return s.ctx }

func statusOf(reg *registry.Registry, id string) string {
	for _, n := range reg.List() {
		if n.NodeID == id {
			return string(n.Status)
		}
	}
	return ""
}

// TestStreamEvents_IdleStreamStaysActive covers #260: an open stream with no
// events must not go inactive, and must go inactive once the stream closes.
func TestStreamEvents_IdleStreamStaysActive(t *testing.T) {
	const timeout = 200 * time.Millisecond
	reg := registry.New()
	if err := reg.Register("node-1", "h", "10.0.0.1", "v", ""); err != nil {
		t.Fatal(err)
	}
	checkerCtx, stopChecker := context.WithCancel(context.Background())
	defer stopChecker()
	reg.StartHeartbeatChecker(checkerCtx, timeout, 20*time.Millisecond, time.Hour)

	s := &server{reg: reg, log: zap.NewNop(), livenessInterval: timeout / 4}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), authedNodeIDKey{}, "node-1"))
	done := make(chan error, 1)
	go func() { done <- s.StreamEvents(&idleStream{ctx: ctx}) }()

	// Well past the timeout with no events: still active.
	time.Sleep(3 * timeout)
	if got := statusOf(reg, "node-1"); got != "active" {
		t.Fatalf("idle open stream: status = %q, want active", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("StreamEvents did not return after ctx cancel")
	}
	time.Sleep(3 * timeout)
	if got := statusOf(reg, "node-1"); got != "inactive" {
		t.Fatalf("after stream closed: status = %q, want inactive", got)
	}
}

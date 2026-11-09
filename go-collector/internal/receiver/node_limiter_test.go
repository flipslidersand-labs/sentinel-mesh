package receiver

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func authedStreamCtx(nodeID string) context.Context {
	return context.WithValue(context.Background(), authedNodeIDKey{}, nodeID)
}

func newLimiterTestServer(t *testing.T) (*server, *store.Store) {
	t.Helper()
	st, err := store.New(filepath.Join(t.TempDir(), "badger"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return &server{st: st, reg: registry.New(), log: zap.NewNop()}, st
}

// drainNodeBudget pushes more invalid events than the burst through one
// stream for nodeID (invalid events still spend rate-limit tokens, #170).
func drainNodeBudget(t *testing.T, s *server, nodeID string) {
	t.Helper()
	n := int(streamEventsRateBurst) + 2000
	events := make([]*pb.KernelEvent, 0, n)
	for i := 0; i < n; i++ {
		events = append(events, &pb.KernelEvent{NodeId: nodeID + "\x00evil", EventId: "bad", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()})
	}
	if err := s.StreamEvents(&fakeStream{events: events, ctx: authedStreamCtx(nodeID)}); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
}

func validEvent(nodeID string) *pb.KernelEvent {
	return &pb.KernelEvent{NodeId: nodeID, EventId: "evt-valid", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()}
}

// #254: the rate limit must be shared by every stream of the same
// authenticated node_id, so opening more connections/streams with one token
// cannot multiply the allowed rate.
func TestStreamEvents_RateLimitSharedAcrossStreamsOfSameNode(t *testing.T) {
	s, st := newLimiterTestServer(t)
	drainNodeBudget(t, s, "node-1")

	stream := &fakeStream{events: []*pb.KernelEvent{validEvent("node-1")}, ctx: authedStreamCtx("node-1")}
	if err := s.StreamEvents(stream); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	if len(stream.acks) != 1 || stream.acks[0].Ok {
		t.Fatalf("second stream of the same node must be throttled, acks=%v", stream.acks)
	}
	got, err := st.ListEvents(context.Background(), "node-1", 10)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(got) != 0 {
		t.Fatal("throttled event must not be persisted")
	}
}

func TestStreamEvents_RateLimitIndependentAcrossNodes(t *testing.T) {
	s, st := newLimiterTestServer(t)
	drainNodeBudget(t, s, "node-1")

	stream := &fakeStream{events: []*pb.KernelEvent{validEvent("node-2")}, ctx: authedStreamCtx("node-2")}
	if err := s.StreamEvents(stream); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	if len(stream.acks) != 1 || !stream.acks[0].Ok {
		t.Fatalf("node-2 must not be affected by node-1's flood, acks=%v", stream.acks)
	}
	got, _ := st.ListEvents(context.Background(), "node-2", 10)
	if len(got) != 1 {
		t.Fatalf("node-2 event should be stored, got %d", len(got))
	}
}

func TestNodeLimiters_ConcurrentStreamsShareOneBudget(t *testing.T) {
	var nl nodeLimiters
	var allowed int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, release := nl.acquire("node-1")
			defer release()
			for j := 0; j < 2000; j++ {
				if b.allow() {
					atomic.AddInt64(&allowed, 1)
				}
			}
		}()
	}
	wg.Wait()
	// 16000 attempts; only the burst (plus negligible refill) may pass.
	if allowed > int64(streamEventsRateBurst)+500 {
		t.Fatalf("allowed %d events across streams, want about %d", allowed, int(streamEventsRateBurst))
	}
}

func TestNodeLimiters_SweepsIdleEntriesButKeepsActive(t *testing.T) {
	var nl nodeLimiters
	now := time.Now()
	nl.now = func() time.Time { return now }

	_, releaseActive := nl.acquire("active")
	_, releaseIdle := nl.acquire("idle")
	releaseIdle()

	now = now.Add(nodeLimiterIdleTTL + nodeLimiterSweepInterval + time.Second)
	_, releaseNew := nl.acquire("new") // triggers sweep
	defer releaseNew()
	defer releaseActive()

	nl.mu.Lock()
	defer nl.mu.Unlock()
	if _, ok := nl.m["idle"]; ok {
		t.Error("idle entry should have been swept")
	}
	if _, ok := nl.m["active"]; !ok {
		t.Error("entry with a live stream must be kept")
	}
	if len(nl.m) != 2 {
		t.Errorf("len = %d, want 2", len(nl.m))
	}
}

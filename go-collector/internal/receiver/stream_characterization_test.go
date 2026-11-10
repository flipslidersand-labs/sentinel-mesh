package receiver

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func newStageTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.New(filepath.Join(t.TempDir(), "badger"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// Characterization (#226): a valid event that triggers a detector alert is
// persisted, heartbeats the node, saves the alert, and is acked Ok=true, in
// stream order.
func TestStreamEvents_ValidEventWithAlertAcksOkAndPersistsAll(t *testing.T) {
	st := newStageTestStore(t)
	reg := registry.New()
	s := &server{
		st:       st,
		reg:      reg,
		detector: anomaly.New([]anomaly.WindowConfig{{Duration: time.Minute, Threshold: 1}}),
		notifier: &notify.Dispatcher{},
		tracer:   otel.Tracer("test"),
		log:      zap.NewNop(),
	}
	stream := &fakeStream{events: []*pb.KernelEvent{
		{NodeId: "node-1", EventId: "e1", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()},
		{NodeId: "node-1", EventId: "e2", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()},
	}}
	if err := s.StreamEvents(stream); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	if len(stream.acks) != 2 || !stream.acks[0].Ok || !stream.acks[1].Ok {
		t.Fatalf("want 2 Ok acks, got %v", stream.acks)
	}
	evs, err := st.ListEvents(context.Background(), "node-1", 10)
	if err != nil || len(evs) != 2 {
		t.Fatalf("ListEvents = %d, %v; want 2", len(evs), err)
	}
	alerts, err := st.ListAlerts(context.Background(), "node-1", 10)
	if err != nil || len(alerts) == 0 {
		t.Fatalf("ListAlerts = %d, %v; want >0", len(alerts), err)
	}
}

// Characterization: a node_id mismatch with the authenticated identity is
// acked Ok=false and the stream continues with the next event.
func TestStreamEvents_RejectThenContinue(t *testing.T) {
	st := newStageTestStore(t)
	s := &server{st: st, reg: registry.New(), log: zap.NewNop()}
	stream := &fakeStream{
		ctx: context.WithValue(context.Background(), authedNodeIDKey{}, "node-1"),
		events: []*pb.KernelEvent{
			{NodeId: "other", EventId: "bad", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()},
			{NodeId: "node-1", EventId: "good", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()},
		}}
	if err := s.StreamEvents(stream); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	if len(stream.acks) != 2 || stream.acks[0].Ok || !stream.acks[1].Ok {
		t.Fatalf("want [false true], got %v", stream.acks)
	}
}

// newClosedStageTestStore returns a store that is already closed, so every
// write fails (simulated storage outage).
func newClosedStageTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.New(filepath.Join(t.TempDir(), "badger"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}
	return st
}

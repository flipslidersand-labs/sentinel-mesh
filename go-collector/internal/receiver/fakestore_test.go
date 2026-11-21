package receiver

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

// fakeStorage is an in-memory Storage: no BadgerDB needed.
type fakeStorage struct {
	mu     sync.Mutex
	events []model.Event
	alerts []model.Alert
	tokens map[string]string
}

func (f *fakeStorage) SaveEvent(_ context.Context, e model.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
	return nil
}

func (f *fakeStorage) SaveAlert(_ context.Context, a model.Alert) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alerts = append(f.alerts, a)
	return nil
}

func (f *fakeStorage) LookupNodeID(_ context.Context, token string) (string, bool, error) {
	n, ok := f.tokens[token]
	return n, ok, nil
}

func TestStreamEvents_WithFakeStorage(t *testing.T) {
	fs := &fakeStorage{}
	s := &server{st: fs, reg: registry.New(), log: zap.NewNop()}
	stream := &fakeStream{events: []*pb.KernelEvent{
		{NodeId: "node-1", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()},
	}}
	if err := s.StreamEvents(stream); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	if len(stream.acks) != 1 || !stream.acks[0].Ok {
		t.Fatalf("expected one ok ack, got %+v", stream.acks)
	}
	if len(fs.events) != 1 || fs.events[0].NodeID != "node-1" {
		t.Fatalf("fake store did not receive the event: %+v", fs.events)
	}
}

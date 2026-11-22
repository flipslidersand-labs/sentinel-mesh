package receiver

import (
	"context"
	"errors"
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
	// seen tracks (node, event_id) pairs like the real store: a repeat is
	// reported as a duplicate and not appended. Empty event_id never dedups.
	seen map[string]bool
	// alertErrs makes the next N SaveAlertDedup calls fail; seenAlerts mirrors
	// the real (node, event_id, rule) alert index (#465).
	alertErrs  int
	seenAlerts map[string]bool
	// regions is the persisted node -> region map; regionLookupErr makes
	// NodeRegion fail (#512).
	regions         map[string]string
	regionLookupErr error
}

func (f *fakeStorage) SaveNodeRegion(_ context.Context, nodeID, region string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.regions == nil {
		f.regions = map[string]string{}
	}
	f.regions[nodeID] = region
	return nil
}

func (f *fakeStorage) NodeRegion(_ context.Context, nodeID string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.regions[nodeID]
	return r, ok, f.regionLookupErr
}

func (f *fakeStorage) SaveEventDedup(_ context.Context, e model.Event) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e.EventID != "" {
		k := e.NodeID + "\x00" + e.EventID
		if f.seen == nil {
			f.seen = map[string]bool{}
		}
		if f.seen[k] {
			return true, nil
		}
		f.seen[k] = true
	}
	f.events = append(f.events, e)
	return false, nil
}

func (f *fakeStorage) SaveEvent(ctx context.Context, e model.Event) error {
	_, err := f.SaveEventDedup(ctx, e)
	return err
}

func (f *fakeStorage) SaveAlertDedup(_ context.Context, a model.Alert) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.alertErrs > 0 {
		f.alertErrs--
		return false, errors.New("injected alert save failure")
	}
	if a.EventID != "" {
		k := a.NodeID + "\x00" + a.EventID + "\x00" + a.RuleID
		if f.seenAlerts == nil {
			f.seenAlerts = map[string]bool{}
		}
		if f.seenAlerts[k] {
			return true, nil
		}
		f.seenAlerts[k] = true
	}
	f.alerts = append(f.alerts, a)
	return false, nil
}

func (f *fakeStorage) LookupNodeID(_ context.Context, token string) (string, bool, error) {
	n, ok := f.tokens[token]
	return n, ok, nil
}

func TestStreamEvents_WithFakeStorage(t *testing.T) {
	fs := &fakeStorage{}
	s := &server{st: fs, reg: registry.New(), log: zap.NewNop()}
	stream := &fakeStream{events: []*pb.KernelEvent{
		{NodeId: "node-1", Type: pb.EventType_EXEC, Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Comm: "sh"}}, Timestamp: time.Now().UnixNano()},
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

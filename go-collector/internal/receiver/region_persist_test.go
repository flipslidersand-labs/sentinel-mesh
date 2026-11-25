package receiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func regionTestEvent() *pb.KernelEvent {
	return &pb.KernelEvent{NodeId: "node-1", EventId: "e1", Type: pb.EventType_EXEC, Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Comm: "sh"}}, Timestamp: time.Now().UnixNano()}
}

// #512: a node registered in a non-default region keeps it when it is
// re-created after eviction or a restart (fresh registry, same store), and its
// events carry that region.
func TestStreamEvents_RecreatedNodeKeepsRegion(t *testing.T) {
	fs := &fakeStorage{}
	first := &server{st: fs, reg: registry.New(), defaultRegion: "default", log: zap.NewNop()}
	resp, err := first.Register(context.Background(), &pb.RegisterRequest{NodeId: "node-1", Hostname: "h", Version: "v", Region: "eu-west"})
	if err != nil || !resp.Ok {
		t.Fatalf("Register = %+v, %v", resp, err)
	}

	reg := registry.New() // restart / eviction: the registry forgot node-1
	s := &server{st: fs, reg: reg, defaultRegion: "default", log: zap.NewNop()}
	stream := &fakeStream{events: []*pb.KernelEvent{regionTestEvent()}}
	if err := s.StreamEvents(stream); err != nil {
		t.Fatal(err)
	}
	if got := reg.List(); len(got) != 1 || got[0].Region != "eu-west" {
		t.Fatalf("re-created node = %+v, want region eu-west", got)
	}
	if len(fs.events) != 1 || fs.events[0].Region != "eu-west" {
		t.Fatalf("stored events = %+v, want one stamped eu-west", fs.events)
	}
}

func TestStreamEvents_NodeWithoutStoredRegionGetsDefault(t *testing.T) {
	fs := &fakeStorage{}
	reg := registry.New()
	s := &server{st: fs, reg: reg, defaultRegion: "fallback", log: zap.NewNop()}
	if err := s.StreamEvents(&fakeStream{events: []*pb.KernelEvent{regionTestEvent()}}); err != nil {
		t.Fatal(err)
	}
	if got := reg.List(); len(got) != 1 || got[0].Region != "fallback" {
		t.Fatalf("node = %+v, want default region fallback", got)
	}
	if len(fs.events) != 1 || fs.events[0].Region != "fallback" {
		t.Fatalf("stored events = %+v, want region fallback", fs.events)
	}
}

// A failed lookup must not bake the default region into the registry or the
// event: the node stays unregistered (retried on the next event) and the event
// is stored without a region.
func TestStreamEvents_RegionLookupFailureDoesNotGuess(t *testing.T) {
	fs := &fakeStorage{regions: map[string]string{"node-1": "eu-west"}, regionLookupErr: errors.New("injected lookup failure")}
	reg := registry.New()
	s := &server{st: fs, reg: reg, defaultRegion: "default", log: zap.NewNop()}
	stream := &fakeStream{events: []*pb.KernelEvent{regionTestEvent()}}
	if err := s.StreamEvents(stream); err != nil {
		t.Fatal(err)
	}
	if got := reg.List(); len(got) != 0 {
		t.Fatalf("registry = %+v, want node left unregistered", got)
	}
	if len(stream.acks) != 1 || !stream.acks[0].Ok || len(fs.events) != 1 || fs.events[0].Region != "" {
		t.Fatalf("acks=%+v events=%+v, want event saved without region", stream.acks, fs.events)
	}
}

func TestRegister_PersistsNormalisedRegion(t *testing.T) {
	fs := &fakeStorage{}
	s := &server{st: fs, reg: registry.New(), log: zap.NewNop()} // no default configured
	if resp, err := s.Register(context.Background(), &pb.RegisterRequest{NodeId: "node-1", Hostname: "h", Version: "v"}); err != nil || !resp.Ok {
		t.Fatalf("Register = %+v, %v", resp, err)
	}
	if got := fs.regions["node-1"]; got != registry.DefaultRegion {
		t.Fatalf("persisted region = %q, want %q", got, registry.DefaultRegion)
	}
}

// #530: every event of a connected node asks the store to keep the region
// record fresh (the store throttles the actual writes), whether or not the node
// was just re-created; the record keeps the region Register saved.
func TestStreamEvents_EventsTouchNodeRegion(t *testing.T) {
	fs := &fakeStorage{}
	reg := registry.New()
	s := &server{st: fs, reg: reg, defaultRegion: "default", log: zap.NewNop()}
	if resp, err := s.Register(context.Background(), &pb.RegisterRequest{NodeId: "node-1", Hostname: "h", Version: "v", Region: "eu-west"}); err != nil || !resp.Ok {
		t.Fatalf("Register = %+v, %v", resp, err)
	}
	e2 := regionTestEvent()
	e2.EventId = "e2"
	if err := s.StreamEvents(&fakeStream{events: []*pb.KernelEvent{regionTestEvent(), e2}}); err != nil {
		t.Fatal(err)
	}
	if fs.touches["node-1"] != 2 || fs.regions["node-1"] != "eu-west" {
		t.Fatalf("touches=%v regions=%v, want 2 touches keeping eu-west", fs.touches, fs.regions)
	}
}

// A failing refresh (store busy or closed, #532/#535) is logged only: the event
// is still stored and acked.
func TestStreamEvents_TouchNodeRegionFailureDoesNotFailEvent(t *testing.T) {
	fs := &fakeStorage{touchErr: store.ErrBusy}
	s := &server{st: fs, reg: registry.New(), defaultRegion: "default", log: zap.NewNop()}
	stream := &fakeStream{events: []*pb.KernelEvent{regionTestEvent()}}
	if err := s.StreamEvents(stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.acks) != 1 || !stream.acks[0].Ok || len(fs.events) != 1 {
		t.Fatalf("acks=%+v events=%+v, want the event saved and acked", stream.acks, fs.events)
	}
}

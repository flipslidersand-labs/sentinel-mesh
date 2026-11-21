package receiver

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

type countNotifier struct {
	mu sync.Mutex
	n  int
}

func (c *countNotifier) Name() string { return "count" }
func (c *countNotifier) Send(_ context.Context, _ model.Alert) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return nil
}
func (c *countNotifier) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func waitNotified(t *testing.T, c *countNotifier, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for c.count() < want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // let any unexpected extra dispatch land
	if got := c.count(); got != want {
		t.Fatalf("notifications = %d, want %d", got, want)
	}
}

func newIdemServer(fs *fakeStorage, c *countNotifier) *server {
	return &server{
		st: fs, reg: registry.New(), engine: rootExecEngine(),
		notifier: notify.NewDispatcher(notify.Options{Notifiers: []notify.Notifier{c}, MinSeverity: "info"}),
		tracer:   otel.Tracer("test"), log: zap.NewNop(),
	}
}

// #465: the event save succeeded but the alert save failed (Ack ok=false). The
// resend is a duplicate event yet must still produce the alert, exactly once.
func TestStreamEvents_ResendAfterAlertSaveFailure(t *testing.T) {
	fs := &fakeStorage{alertErrs: 1}
	c := &countNotifier{}
	s := newIdemServer(fs, c)

	first := &fakeStream{events: []*pb.KernelEvent{execEvent("e1")}}
	if err := s.StreamEvents(first); err != nil {
		t.Fatal(err)
	}
	if len(first.acks) != 1 || first.acks[0].Ok {
		t.Fatalf("first ack = %v, want ok=false", first.acks)
	}
	if len(fs.alerts) != 0 {
		t.Fatalf("alerts after failure = %d, want 0", len(fs.alerts))
	}

	resend := &fakeStream{events: []*pb.KernelEvent{execEvent("e1")}}
	if err := s.StreamEvents(resend); err != nil {
		t.Fatal(err)
	}
	if len(resend.acks) != 1 || !resend.acks[0].Ok {
		t.Fatalf("resend ack = %v, want ok=true", resend.acks)
	}
	if len(fs.events) != 1 {
		t.Fatalf("events = %d, want 1", len(fs.events))
	}
	if len(fs.alerts) != 1 {
		t.Fatalf("alerts = %d, want 1", len(fs.alerts))
	}
	waitNotified(t, c, 1)
}

// A resend after everything was saved adds no alert and no notification.
func TestStreamEvents_ResendAfterSuccessAddsNothing(t *testing.T) {
	fs := &fakeStorage{}
	c := &countNotifier{}
	s := newIdemServer(fs, c)
	for i := 0; i < 2; i++ {
		st := &fakeStream{events: []*pb.KernelEvent{execEvent("e1")}}
		if err := s.StreamEvents(st); err != nil {
			t.Fatal(err)
		}
		if len(st.acks) != 1 || !st.acks[0].Ok {
			t.Fatalf("ack %d = %v", i, st.acks)
		}
	}
	if len(fs.alerts) != 1 {
		t.Fatalf("alerts = %d, want 1", len(fs.alerts))
	}
	waitNotified(t, c, 1)
}

// A duplicate event whose alert save fails still acks ok=false.
func TestStreamEvents_DuplicateAlertSaveFailureNacks(t *testing.T) {
	fs := &fakeStorage{alertErrs: 2}
	s := newIdemServer(fs, &countNotifier{})
	for i := 0; i < 2; i++ {
		st := &fakeStream{events: []*pb.KernelEvent{execEvent("e1")}}
		if err := s.StreamEvents(st); err != nil {
			t.Fatal(err)
		}
		if st.acks[0].Ok {
			t.Fatalf("ack %d ok=true, want false", i)
		}
	}
}

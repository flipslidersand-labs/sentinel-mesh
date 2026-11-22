package receiver

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

// #491: an anomaly alert whose save failed is saved by a resend of the same
// event_id, exactly once, without re-counting the event in the detector.
func TestStreamEvents_AnomalyAlertSavedOnResend(t *testing.T) {
	fs := &fakeStorage{alertErrs: 1}
	c := &countNotifier{}
	det := anomaly.New([]anomaly.WindowConfig{{Duration: time.Minute, Threshold: 0}})
	s := &server{
		st: fs, reg: registry.New(), detector: det,
		notifier: notify.NewDispatcher(notify.Options{Notifiers: []notify.Notifier{c}, MinSeverity: "info"}),
		tracer:   otel.Tracer("test"), log: zap.NewNop(),
	}

	send := func() *fakeStream {
		st := &fakeStream{events: []*pb.KernelEvent{execEvent("e1")}}
		if err := s.StreamEvents(st); err != nil {
			t.Fatal(err)
		}
		return st
	}
	if st := send(); st.acks[0].Ok {
		t.Fatal("first ack ok=true, want false")
	}
	if len(fs.alerts) != 0 {
		t.Fatalf("alerts after failure = %d, want 0", len(fs.alerts))
	}
	for i := 0; i < 3; i++ {
		if st := send(); !st.acks[0].Ok {
			t.Fatalf("resend %d ack ok=false", i)
		}
		if len(fs.alerts) != 1 {
			t.Fatalf("resend %d: alerts = %d, want 1", i, len(fs.alerts))
		}
	}
	if got := det.WindowStats()["exec"]["1m"]; got != 1 {
		t.Fatalf("detector count = %d, want 1", got)
	}
}

func newAnomalyServer(fs *fakeStorage, c *countNotifier) (*server, *anomaly.Detector) {
	det := anomaly.New([]anomaly.WindowConfig{{Duration: time.Minute, Threshold: 0}})
	return &server{
		st: fs, reg: registry.New(), detector: det,
		notifier: notify.NewDispatcher(notify.Options{Notifiers: []notify.Notifier{c}, MinSeverity: "info"}),
		tracer:   otel.Tracer("test"), log: zap.NewNop(),
	}, det
}

// #500: at the cap the oldest remembered anomaly is dropped, newer ones stay.
func TestRememberAnomaly_EvictsOldestFirst(t *testing.T) {
	const limit = 8
	s := &server{maxPending: limit}
	ev := func(i int) model.Event {
		return model.Event{NodeID: "node-1", EventID: fmt.Sprintf("e%d", i)}
	}
	for i := 0; i < limit+3; i++ {
		s.rememberAnomaly(model.Alert{NodeID: "node-1", EventID: fmt.Sprintf("e%d", i)})
	}
	if len(s.pendingAnomaly) != limit {
		t.Fatalf("pending = %d, want %d", len(s.pendingAnomaly), limit)
	}
	for i := 0; i < limit+3; i++ {
		_, ok := s.pendingAnomalyFor(ev(i))
		if want := i >= 3; ok != want {
			t.Fatalf("e%d remembered = %v, want %v", i, ok, want)
		}
	}
	// A forgotten entry frees a slot: no eviction on the next insert.
	s.forgetAnomaly(model.Alert{NodeID: "node-1", EventID: "e5"})
	s.rememberAnomaly(model.Alert{NodeID: "node-1", EventID: "e100"})
	if _, ok := s.pendingAnomalyFor(ev(3)); !ok {
		t.Fatal("e3 evicted although a slot was free")
	}
}

// #500: a resend whose re-save also fails stays pending and is saved by the
// next resend.
func TestStreamEvents_AnomalyResaveFailureReregisters(t *testing.T) {
	fs := &fakeStorage{alertErrs: 2}
	c := &countNotifier{}
	s, det := newAnomalyServer(fs, c)
	send := func() bool {
		st := &fakeStream{events: []*pb.KernelEvent{execEvent("e1")}}
		if err := s.StreamEvents(st); err != nil {
			t.Fatal(err)
		}
		return st.acks[0].Ok
	}
	if send() || send() {
		t.Fatal("ack ok=true while alert saves are failing")
	}
	if len(fs.alerts) != 0 {
		t.Fatalf("alerts = %d, want 0", len(fs.alerts))
	}
	if !send() {
		t.Fatal("third send ack ok=false")
	}
	if len(fs.alerts) != 1 {
		t.Fatalf("alerts = %d, want 1", len(fs.alerts))
	}
	if got := det.WindowStats()["exec"]["1m"]; got != 1 {
		t.Fatalf("detector count = %d, want 1", got)
	}
}

// #500: concurrent resends of one event_id save the anomaly once (run with -race).
func TestStreamEvents_AnomalyConcurrentResendSavesOnce(t *testing.T) {
	fs := &fakeStorage{alertErrs: 1}
	c := &countNotifier{}
	s, _ := newAnomalyServer(fs, c)
	first := &fakeStream{events: []*pb.KernelEvent{execEvent("e1")}}
	if err := s.StreamEvents(first); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st := &fakeStream{events: []*pb.KernelEvent{execEvent("e1")}}
			if err := s.StreamEvents(st); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	fs.mu.Lock()
	n := len(fs.alerts)
	fs.mu.Unlock()
	if n != 1 {
		t.Fatalf("alerts = %d, want 1", n)
	}
}

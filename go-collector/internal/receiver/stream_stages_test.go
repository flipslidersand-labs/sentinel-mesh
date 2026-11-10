package receiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

type errSendStream struct{ fakeStream }

func (e *errSendStream) Send(*pb.EventAck) error { return errors.New("send failed") }

func TestRejectEvent(t *testing.T) {
	t.Run("logs then sends Ok=false", func(t *testing.T) {
		stream := &fakeStream{}
		logged := false
		if err := rejectEvent(stream, func() { logged = true }); err != nil {
			t.Fatalf("rejectEvent: %v", err)
		}
		if !logged {
			t.Error("logFn not called")
		}
		if len(stream.acks) != 1 || stream.acks[0].Ok {
			t.Errorf("acks = %v, want one Ok=false", stream.acks)
		}
	})
	t.Run("propagates send error", func(t *testing.T) {
		if err := rejectEvent(&errSendStream{}, func() {}); err == nil {
			t.Error("want send error")
		}
	})
}

func TestPersistEvent(t *testing.T) {
	ev := store.Event{EventID: "e", NodeID: "n", Timestamp: time.Now().UTC(), Type: "exec"}
	t.Run("ok", func(t *testing.T) {
		st := newStageTestStore(t)
		s := &server{st: st, log: zap.NewNop()}
		if !s.persistEvent(context.Background(), ev) {
			t.Fatal("want ok=true")
		}
		got, _ := st.ListEvents(context.Background(), "n", 10)
		if len(got) != 1 {
			t.Fatalf("persisted %d events, want 1", len(got))
		}
	})
	t.Run("store failure", func(t *testing.T) {
		st := newClosedStageTestStore(t)
		s := &server{st: st, log: zap.NewNop()}
		if s.persistEvent(context.Background(), ev) {
			t.Fatal("want ok=false")
		}
	})
}

func TestEvaluateAndNotify(t *testing.T) {
	ev := store.Event{EventID: "e", NodeID: "n", Timestamp: time.Now().UTC(), Type: "exec"}
	newDet := func() *anomaly.Detector {
		return anomaly.New([]anomaly.WindowConfig{{Duration: time.Minute, Threshold: 1}})
	}
	t.Run("no engine or detector", func(t *testing.T) {
		s := &server{log: zap.NewNop()}
		alerts, ok := s.evaluateAndNotify(context.Background(), ev)
		if len(alerts) != 0 || !ok {
			t.Fatalf("got %v, %v; want none, true", alerts, ok)
		}
	})
	t.Run("alert saved", func(t *testing.T) {
		st := newStageTestStore(t)
		s := &server{st: st, detector: newDet(), notifier: &notify.Dispatcher{}, tracer: otel.Tracer("t"), log: zap.NewNop()}
		s.evaluateAndNotify(context.Background(), ev)
		alerts, ok := s.evaluateAndNotify(context.Background(), ev)
		if len(alerts) == 0 || !ok {
			t.Fatalf("got %d alerts, ok=%v; want >0, true", len(alerts), ok)
		}
		saved, _ := st.ListAlerts(context.Background(), "n", 10)
		if len(saved) == 0 {
			t.Fatal("alert not persisted")
		}
	})
	t.Run("alert save failure", func(t *testing.T) {
		st := newClosedStageTestStore(t)
		s := &server{st: st, detector: newDet(), notifier: &notify.Dispatcher{}, tracer: otel.Tracer("t"), log: zap.NewNop()}
		s.evaluateAndNotify(context.Background(), ev)
		alerts, ok := s.evaluateAndNotify(context.Background(), ev)
		if len(alerts) == 0 || ok {
			t.Fatalf("got %d alerts, ok=%v; want >0, false", len(alerts), ok)
		}
	})
}

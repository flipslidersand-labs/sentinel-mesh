package receiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
)

type errSendStream struct{ fakeStream }

func (e *errSendStream) Send(*pb.EventAck) error { return errors.New("send failed") }

func TestRejectEvent(t *testing.T) {
	t.Run("logs then sends Ok=false", func(t *testing.T) {
		stream := &fakeStream{}
		logged := false
		if err := rejectEvent(stream, pb.AckReason_ACK_REASON_INVALID, func() { logged = true }); err != nil {
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
		if err := rejectEvent(&errSendStream{}, pb.AckReason_ACK_REASON_INVALID, func() {}); err == nil {
			t.Error("want send error")
		}
	})
}

func TestPersistEvent(t *testing.T) {
	ev := model.Event{EventID: "e", NodeID: "n", Timestamp: time.Now().UTC(), Type: "exec"}
	t.Run("ok", func(t *testing.T) {
		st := newStageTestStore(t)
		s := &server{st: st, log: zap.NewNop()}
		if ok, _ := s.persistEvent(context.Background(), ev); !ok {
			t.Fatal("want ok=true")
		}
		got, _ := st.ListEventsMatching(context.Background(), func(e model.Event) bool { return e.NodeID == "n" }, 10)
		if len(got) != 1 {
			t.Fatalf("persisted %d events, want 1", len(got))
		}
	})
	t.Run("store failure", func(t *testing.T) {
		st := newClosedStageTestStore(t)
		s := &server{st: st, log: zap.NewNop()}
		if ok, _ := s.persistEvent(context.Background(), ev); ok {
			t.Fatal("want ok=false")
		}
	})
}

func TestEvaluateAndNotify(t *testing.T) {
	ev := model.Event{EventID: "e", NodeID: "n", Timestamp: time.Now().UTC(), Type: "exec"}
	newDet := func() *anomaly.Detector {
		return anomaly.New([]anomaly.WindowConfig{{Duration: time.Minute, Threshold: 1}})
	}
	t.Run("no engine or detector", func(t *testing.T) {
		s := &server{log: zap.NewNop()}
		alerts, ok := s.evaluateAndNotify(context.Background(), ev, false)
		if len(alerts) != 0 || !ok {
			t.Fatalf("got %v, %v; want none, true", alerts, ok)
		}
	})
	t.Run("alert saved", func(t *testing.T) {
		st := newStageTestStore(t)
		s := &server{st: st, detector: newDet(), notifier: &notify.Dispatcher{}, tracer: otel.Tracer("t"), log: zap.NewNop()}
		s.evaluateAndNotify(context.Background(), ev, false)
		alerts, ok := s.evaluateAndNotify(context.Background(), ev, false)
		if len(alerts) == 0 || !ok {
			t.Fatalf("got %d alerts, ok=%v; want >0, true", len(alerts), ok)
		}
		saved, _ := st.ListAlertsMatching(context.Background(), func(a model.Alert) bool { return a.NodeID == "n" }, 10)
		if len(saved) == 0 {
			t.Fatal("alert not persisted")
		}
	})
	t.Run("alert save failure", func(t *testing.T) {
		st := newClosedStageTestStore(t)
		s := &server{st: st, detector: newDet(), notifier: &notify.Dispatcher{}, tracer: otel.Tracer("t"), log: zap.NewNop()}
		s.evaluateAndNotify(context.Background(), ev, false)
		alerts, ok := s.evaluateAndNotify(context.Background(), ev, false)
		if len(alerts) == 0 || ok {
			t.Fatalf("got %d alerts, ok=%v; want >0, false", len(alerts), ok)
		}
	})
}

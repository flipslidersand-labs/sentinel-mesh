package receiver

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
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

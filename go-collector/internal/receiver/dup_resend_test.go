package receiver

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/alerting"
	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/model"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

func rootExecEngine() *alerting.Engine {
	return alerting.New(&alerting.RuleSet{Rules: []alerting.Rule{{
		ID: "root-exec", EventType: "exec", Severity: "high", Message: "root exec",
		Conditions: []alerting.Condition{{Field: "uid", Operator: "eq", Value: "0"}},
	}}}, zap.NewNop())
}

func execEvent(id string) *pb.KernelEvent {
	return &pb.KernelEvent{NodeId: "node-1", EventId: id, Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano(),
		Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Pid: 9, Ppid: 1, Comm: "sh", Uid: 0}}}
}

// #457: a resent event_id (fake store reports duplicate) must not re-run
// alert evaluation, yet is still acked ok=true.
func TestStreamEvents_DuplicateResendFiresAlertOnce_Fake(t *testing.T) {
	fs := &fakeStorage{}
	s := &server{
		st: fs, reg: registry.New(), engine: rootExecEngine(),
		notifier: &notify.Dispatcher{}, tracer: otel.Tracer("test"), log: zap.NewNop(),
	}
	stream := &fakeStream{events: []*pb.KernelEvent{execEvent("e1"), execEvent("e1")}}
	if err := s.StreamEvents(stream); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	if len(stream.acks) != 2 || !stream.acks[0].Ok || !stream.acks[1].Ok {
		t.Fatalf("want 2 ok acks, got %v", stream.acks)
	}
	if len(fs.events) != 1 {
		t.Fatalf("events stored = %d, want 1", len(fs.events))
	}
	if len(fs.alerts) != 1 {
		t.Fatalf("alerts = %d, want 1 (resend must not re-alert)", len(fs.alerts))
	}
}

// Same, against the real store, with engine and detector, across two
// streams (a reconnect resend).
func TestStreamEvents_DuplicateResendFiresAlertOnce_RealStore(t *testing.T) {
	st := newStageTestStore(t)
	s := &server{
		st: st, reg: registry.New(), engine: rootExecEngine(),
		// Threshold 1: a second counted event in the window raises an anomaly.
		detector: anomaly.New([]anomaly.WindowConfig{{Duration: time.Minute, Threshold: 1}}),
		notifier: &notify.Dispatcher{}, tracer: otel.Tracer("test"), log: zap.NewNop(),
	}
	first := &fakeStream{events: []*pb.KernelEvent{execEvent("e1")}}
	if err := s.StreamEvents(first); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	resend := &fakeStream{events: []*pb.KernelEvent{execEvent("e1")}}
	if err := s.StreamEvents(resend); err != nil {
		t.Fatalf("StreamEvents resend: %v", err)
	}
	if len(resend.acks) != 1 || !resend.acks[0].Ok {
		t.Fatalf("resend ack = %v, want ok", resend.acks)
	}
	alerts, err := st.ListAlertsMatching(context.Background(), func(a model.Alert) bool { return a.NodeID == "node-1" }, 10)
	if err != nil {
		t.Fatalf("ListAlerts: %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("alerts = %d, want 1 (rule only; detector must not count the resend)", len(alerts))
	}
}

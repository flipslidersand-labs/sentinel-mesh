package receiver

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

// #531: an alert is stored with the region of the node that raised it, also
// when the node was re-created after eviction or a restart.
func TestStreamEvents_AlertCarriesNodeRegion(t *testing.T) {
	st := newStageTestStore(t)
	first := &server{st: st, reg: registry.New(), defaultRegion: "default", log: zap.NewNop()}
	if resp, err := first.Register(context.Background(), &pb.RegisterRequest{NodeId: "node-1", Hostname: "h", Version: "v", Region: "eu-west"}); err != nil || !resp.Ok {
		t.Fatalf("Register = %+v, %v", resp, err)
	}

	s := &server{ // restart / eviction: the registry forgot node-1
		st:            st,
		reg:           registry.New(),
		defaultRegion: "default",
		detector:      anomaly.New([]anomaly.WindowConfig{{Duration: time.Minute, Threshold: 1}}),
		notifier:      &notify.Dispatcher{},
		tracer:        otel.Tracer("test"),
		log:           zap.NewNop(),
	}
	stream := &fakeStream{events: []*pb.KernelEvent{
		{NodeId: "node-1", EventId: "e1", Type: pb.EventType_EXEC, Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Comm: "sh"}}, Timestamp: time.Now().UnixNano()},
		{NodeId: "node-1", EventId: "e2", Type: pb.EventType_EXEC, Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Comm: "sh"}}, Timestamp: time.Now().UnixNano()},
	}}
	if err := s.StreamEvents(stream); err != nil {
		t.Fatal(err)
	}
	alerts, err := st.ListAlertsMatching(context.Background(), nil, 10)
	if err != nil || len(alerts) == 0 {
		t.Fatalf("alerts = %d, %v; want >0", len(alerts), err)
	}
	for _, a := range alerts {
		if a.Region != "eu-west" {
			t.Errorf("alert %s region = %q, want eu-west", a.AlertID, a.Region)
		}
	}
}

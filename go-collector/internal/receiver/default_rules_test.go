package receiver

import (
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/alerting"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// loadDefaultRules loads the shipped rules.yaml (repo root), not a
// hand-written copy, so a wrong field name in it is caught (#248).
func loadDefaultRules(t *testing.T) *alerting.RuleSet {
	t.Helper()
	rs, err := alerting.LoadRules(filepath.Join("..", "..", "..", "rules.yaml"))
	if err != nil {
		t.Fatalf("load default rules: %v", err)
	}
	if err := rs.Validate(); err != nil {
		t.Fatalf("validate default rules: %v", err)
	}
	return rs
}

func TestDefaultRulesHaveNoUnknownFields(t *testing.T) {
	if w := loadDefaultRules(t).UnknownFieldWarnings(); len(w) != 0 {
		t.Fatalf("default rules.yaml references fields absent from payloads: %v", w)
	}
}

// TestDefaultRuleTCPSuspiciousPortFires drives a real TCP event through the
// receiver's payload generation and the shipped rules (#248).
func TestDefaultRuleTCPSuspiciousPortFires(t *testing.T) {
	s := &server{}
	ev := &pb.KernelEvent{
		EventId: "e1", NodeId: "n1", Type: pb.EventType_TCP,
		Payload: &pb.KernelEvent_Tcp{Tcp: &pb.TcpEvent{
			Pid: 1, Comm: "nc", SrcIp: "10.0.0.1", SrcPort: 4444,
			DstIp: "10.0.0.2", DstPort: 31337, Direction: "outbound",
		}},
	}
	stored := store.Event{
		EventID: ev.EventId, NodeID: ev.NodeId, Timestamp: time.Now().UTC(),
		Type: s.eventTypeName(ev), Payload: s.eventPayload(ev),
	}
	alerts := alerting.New(loadDefaultRules(t), zap.NewNop()).Evaluate(stored)
	for _, a := range alerts {
		if a.RuleID == "tcp_to_suspicious_port" {
			return
		}
	}
	t.Fatalf("tcp_to_suspicious_port did not fire for dst_port=31337; alerts=%v", alerts)
}

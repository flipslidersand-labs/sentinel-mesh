package receiver

import (
	"encoding/json"
	"reflect"
	"testing"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/alerting"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func decodePayload(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
	return m
}

// legacyPayload reproduces the pre-#249 behaviour (plain json.Marshal of the
// generated struct) so the new payload can be diffed against it.
func legacyPayload(t *testing.T, e *pb.KernelEvent) map[string]any {
	t.Helper()
	var v any
	switch e.Type {
	case pb.EventType_EXEC:
		v = e.GetExec()
	case pb.EventType_TCP:
		v = e.GetTcp()
	case pb.EventType_FILE:
		v = e.GetFile()
	}
	raw, _ := json.Marshal(v)
	return decodePayload(t, raw)
}

func TestEventPayloadKeepsZeroNumbers(t *testing.T) {
	s := &server{log: zap.NewNop()}
	tests := []struct {
		name     string
		ev       *pb.KernelEvent
		addedKey map[string]any // keys that must appear now but not before
	}{
		{"exec uid=0 ppid=0", &pb.KernelEvent{Type: pb.EventType_EXEC,
			Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Pid: 10, Comm: "sh", Cmdline: "sh -c x", Cwd: "/"}}},
			map[string]any{"ppid": float64(0), "uid": float64(0)}},
		{"exec nonzero", &pb.KernelEvent{Type: pb.EventType_EXEC,
			Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Pid: 10, Ppid: 1, Comm: "sh", Uid: 1000}}},
			map[string]any{}},
		{"exec empty strings stay omitted", &pb.KernelEvent{Type: pb.EventType_EXEC,
			Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Pid: 1, Ppid: 1, Uid: 5}}},
			map[string]any{}},
		{"tcp port=0", &pb.KernelEvent{Type: pb.EventType_TCP,
			Payload: &pb.KernelEvent_Tcp{Tcp: &pb.TcpEvent{Pid: 3, Comm: "c", SrcIp: "1.1.1.1", DstIp: "2.2.2.2", Direction: "out"}}},
			map[string]any{"src_port": float64(0), "dst_port": float64(0)}},
		{"tcp nonzero", &pb.KernelEvent{Type: pb.EventType_TCP,
			Payload: &pb.KernelEvent_Tcp{Tcp: &pb.TcpEvent{Pid: 3, SrcPort: 5, DstPort: 443}}},
			map[string]any{}},
		{"file ret=0", &pb.KernelEvent{Type: pb.EventType_FILE,
			Payload: &pb.KernelEvent_File{File: &pb.FileEvent{Pid: 4, Comm: "c", Path: "/etc/passwd", Op: "open"}}},
			map[string]any{"ret": float64(0)}},
		{"file ret=-13", &pb.KernelEvent{Type: pb.EventType_FILE,
			Payload: &pb.KernelEvent_File{File: &pb.FileEvent{Pid: 4, Ret: -13}}},
			map[string]any{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := decodePayload(t, s.eventPayload(tc.ev))
			want := legacyPayload(t, tc.ev)
			for k, v := range tc.addedKey {
				if _, had := want[k]; had {
					t.Fatalf("legacy already had %q; test case wrong", k)
				}
				want[k] = v
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("payload mismatch\n got: %v\nwant: %v", got, want)
			}
		})
	}
}

func TestEventPayloadUnsetBodyIsNull(t *testing.T) {
	s := &server{log: zap.NewNop()}
	if got := string(s.eventPayload(&pb.KernelEvent{Type: pb.EventType_EXEC})); got != "null" {
		t.Fatalf("got %s, want null (unchanged behaviour)", got)
	}
}

func TestUIDZeroExecFiresUIDEqZeroRule(t *testing.T) {
	s := &server{log: zap.NewNop()}
	rules := &alerting.RuleSet{Rules: []alerting.Rule{{
		ID: "root-exec", EventType: "exec", Severity: "high", Message: "root exec",
		Conditions: []alerting.Condition{{Field: "uid", Operator: "eq", Value: "0"}},
	}}}
	eng := alerting.New(rules, zap.NewNop())

	ev := &pb.KernelEvent{Type: pb.EventType_EXEC,
		Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Pid: 9, Ppid: 1, Comm: "sh", Uid: 0}}}
	alerts := eng.Evaluate(store.Event{Type: "exec", Payload: s.eventPayload(ev)})
	if len(alerts) != 1 {
		t.Fatalf("uid=0 exec: got %d alerts, want 1", len(alerts))
	}

	ev.GetExec().Uid = 1000
	if n := len(eng.Evaluate(store.Event{Type: "exec", Payload: s.eventPayload(ev)})); n != 0 {
		t.Fatalf("uid=1000 exec: got %d alerts, want 0", n)
	}
}

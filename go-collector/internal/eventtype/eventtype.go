// Package eventtype is the single source of truth for event type names and
// their JSON payload schemas. The receiver (pb -> stored event) and alerting
// (rule validation, known payload fields) both read this table, so adding a
// type or field means touching one place (#353).
package eventtype

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
)

// Event type names as stored in model.Event.Type and used in rules.
const (
	Exec = "exec"
	TCP  = "tcp"
	File = "file"
	// Any is the rule wildcard matching every event type.
	Any = "*"
)

// The generated pb structs tag every field `omitempty`, so json.Marshal drops
// numeric zeros (uid=0, ports, ret=0) and `uid eq 0` rules never fire (#249).
// These mirror them with identical keys/order/types but no omitempty on
// numbers. String fields keep omitempty to preserve the existing payload.
// TestPayloadMatchesPB fails if they drift from the pb messages.
type ExecPayload struct {
	Pid     uint32 `json:"pid"`
	Ppid    uint32 `json:"ppid"`
	Comm    string `json:"comm,omitempty"`
	Cmdline string `json:"cmdline,omitempty"`
	Cwd     string `json:"cwd,omitempty"`
	Uid     uint32 `json:"uid"`
}

type TCPPayload struct {
	Pid       uint32 `json:"pid"`
	Comm      string `json:"comm,omitempty"`
	SrcIp     string `json:"src_ip,omitempty"`
	SrcPort   uint32 `json:"src_port"`
	DstIp     string `json:"dst_ip,omitempty"`
	DstPort   uint32 `json:"dst_port"`
	Direction string `json:"direction,omitempty"`
}

type FilePayload struct {
	Pid  uint32 `json:"pid"`
	Comm string `json:"comm,omitempty"`
	Path string `json:"path,omitempty"`
	Op   string `json:"op,omitempty"`
	Ret  int32  `json:"ret"`
}

// Info describes one event type.
type Info struct {
	Name string
	// Schema is a zero value of the payload struct actually serialised.
	Schema any
	// Payload builds the payload from the event's oneof body; nil when the
	// body is absent.
	Payload func(e *pb.KernelEvent) any
	// Strings returns the free-form string fields to validate (charset and
	// length), in validation order; nil when the body is absent.
	Strings func(e *pb.KernelEvent) []Field
	// IPs returns the IP-address fields to validate after Strings; may be nil.
	IPs func(e *pb.KernelEvent) []Field
}

// Field is a named string value checked by receiver validation.
type Field struct{ Name, Value string }

var table = map[pb.EventType]Info{
	pb.EventType_EXEC: {Exec, ExecPayload{}, func(e *pb.KernelEvent) any {
		if x := e.GetExec(); x != nil {
			return ExecPayload{x.Pid, x.Ppid, x.Comm, x.Cmdline, x.Cwd, x.Uid}
		}
		return nil
	}, func(e *pb.KernelEvent) []Field {
		if x := e.GetExec(); x != nil {
			return []Field{{"comm", x.Comm}, {"cmdline", x.Cmdline}, {"cwd", x.Cwd}}
		}
		return nil
	}, nil},
	pb.EventType_TCP: {TCP, TCPPayload{}, func(e *pb.KernelEvent) any {
		if t := e.GetTcp(); t != nil {
			return TCPPayload{t.Pid, t.Comm, t.SrcIp, t.SrcPort, t.DstIp, t.DstPort, t.Direction}
		}
		return nil
	}, func(e *pb.KernelEvent) []Field {
		if t := e.GetTcp(); t != nil {
			return []Field{{"comm", t.Comm}, {"direction", t.Direction}}
		}
		return nil
	}, func(e *pb.KernelEvent) []Field {
		if t := e.GetTcp(); t != nil {
			return []Field{{"src_ip", t.SrcIp}, {"dst_ip", t.DstIp}}
		}
		return nil
	}},
	pb.EventType_FILE: {File, FilePayload{}, func(e *pb.KernelEvent) any {
		if f := e.GetFile(); f != nil {
			return FilePayload{f.Pid, f.Comm, f.Path, f.Op, f.Ret}
		}
		return nil
	}, func(e *pb.KernelEvent) []Field {
		if f := e.GetFile(); f != nil {
			return []Field{{"comm", f.Comm}, {"path", f.Path}, {"op", f.Op}}
		}
		return nil
	}, nil},
}

// Lookup returns the table entry for a pb event type.
func Lookup(t pb.EventType) (Info, bool) {
	i, ok := table[t]
	return i, ok
}

// Name returns the stored name for a pb event type, or "unknown(N)".
func Name(t pb.EventType) string {
	if i, ok := table[t]; ok {
		return i.Name
	}
	return fmt.Sprintf("unknown(%d)", t)
}

// IsValid reports whether name is a known event type name (not "*").
func IsValid(name string) bool {
	for _, i := range table {
		if i.Name == name {
			return true
		}
	}
	return false
}

// Fields returns the JSON keys of the payload actually emitted for the named
// type, derived from the payload struct's json tags. An unknown name
// (including "*") yields the union across all types.
func Fields(name string) map[string]bool {
	set := map[string]bool{}
	for _, i := range table {
		if name == i.Name || !IsValid(name) {
			for _, k := range jsonKeys(i.Schema) {
				set[k] = true
			}
		}
	}
	return set
}

func jsonKeys(v any) []string {
	t := reflect.TypeOf(v)
	var keys []string
	for i := 0; i < t.NumField(); i++ {
		k, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if k != "" && k != "-" {
			keys = append(keys, k)
		}
	}
	return keys
}

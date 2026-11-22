package eventtype

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
)

func pbFields(m proto.Message) []string {
	fs := m.ProtoReflect().Descriptor().Fields()
	var out []string
	for i := 0; i < fs.Len(); i++ {
		out = append(out, string(fs.Get(i).Name()))
	}
	return out
}

func TestPayloadMatchesPB(t *testing.T) {
	msgs := map[pb.EventType][]string{
		pb.EventType_EXEC: pbFields(&pb.ExecEvent{}),
		pb.EventType_TCP:  pbFields(&pb.TcpEvent{}),
		pb.EventType_FILE: pbFields(&pb.FileEvent{}),
	}
	for typ, want := range msgs {
		info, ok := Lookup(typ)
		if !ok {
			t.Fatalf("type %v missing from table", typ)
		}
		if got := jsonKeys(info.Schema); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: payload keys %v != pb fields %v", info.Name, got, want)
		}
	}
}

func TestEveryPBTypeInTable(t *testing.T) {
	for v, n := range pb.EventType_name {
		if _, ok := Lookup(pb.EventType(v)); !ok {
			t.Errorf("pb.EventType %s not in table", n)
		}
	}
}

func TestFields(t *testing.T) {
	if !Fields(Exec)["uid"] || Fields(Exec)["dst_port"] {
		t.Error("exec fields wrong")
	}
	if !Fields(Any)["dst_port"] || !Fields(Any)["uid"] {
		t.Error("union wrong")
	}
}

// TestValidatedFieldsCoverPBStrings fails when a pb payload gains a string
// field that Strings/IPs do not validate (#154 injection hardening).
func TestValidatedFieldsCoverPBStrings(t *testing.T) {
	cases := map[pb.EventType]struct {
		msg proto.Message
		ev  *pb.KernelEvent
	}{
		pb.EventType_EXEC: {&pb.ExecEvent{}, &pb.KernelEvent{Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{}}}},
		pb.EventType_TCP:  {&pb.TcpEvent{}, &pb.KernelEvent{Payload: &pb.KernelEvent_Tcp{Tcp: &pb.TcpEvent{}}}},
		pb.EventType_FILE: {&pb.FileEvent{}, &pb.KernelEvent{Payload: &pb.KernelEvent_File{File: &pb.FileEvent{}}}},
	}
	for typ, c := range cases {
		info, ok := Lookup(typ)
		if !ok {
			t.Fatalf("%v missing from table", typ)
		}
		want := map[string]bool{}
		fs := c.msg.ProtoReflect().Descriptor().Fields()
		for i := 0; i < fs.Len(); i++ {
			if fs.Get(i).Kind() == protoreflect.StringKind {
				want[string(fs.Get(i).Name())] = true
			}
		}
		got := map[string]bool{}
		for _, fn := range []func(*pb.KernelEvent) []Field{info.Strings, info.IPs} {
			if fn == nil {
				continue
			}
			for _, f := range fn(c.ev) {
				if got[f.Name] {
					t.Errorf("%s: field %q validated twice", info.Name, f.Name)
				}
				got[f.Name] = true
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: validated string fields %v != pb string fields %v", info.Name, got, want)
		}
	}
}

package eventtype

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

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

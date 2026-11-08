package alerting

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
)

// payloadMessages maps an event type to the protobuf message the receiver
// serialises (json.Marshal) as the event payload. The JSON keys of that
// payload are the proto field names, so the known-field sets are derived
// from the generated descriptors rather than hand-maintained (#248).
var payloadMessages = map[string]proto.Message{
	"exec": &pb.ExecEvent{},
	"tcp":  &pb.TcpEvent{},
	"file": &pb.FileEvent{},
}

// payloadFields returns the set of top-level payload keys for an event type.
// "*" yields the union across all event types.
func payloadFields(eventType string) map[string]bool {
	set := map[string]bool{}
	add := func(m proto.Message) {
		fs := m.ProtoReflect().Descriptor().Fields()
		for i := 0; i < fs.Len(); i++ {
			set[string(fs.Get(i).Name())] = true
		}
	}
	if m, ok := payloadMessages[eventType]; ok {
		add(m)
		return set
	}
	for _, m := range payloadMessages {
		add(m)
	}
	return set
}

// UnknownFieldWarnings lists conditions whose field is not a key of the
// payload for the rule's event type. Such a condition can never match (the
// engine treats a missing field as no-match), which is how a typo like
// "dport" silently disabled a rule (#248). These are warnings, not
// Validate errors, so an operator's existing rules file never becomes
// un-startable because of them.
func (rs *RuleSet) UnknownFieldWarnings() []string {
	var out []string
	for i, rule := range rs.Rules {
		known := payloadFields(rule.EventType)
		for j, cond := range rule.Conditions {
			if !known[cond.Field] {
				out = append(out, fmt.Sprintf("rule[%d] (id=%q): condition[%d]: field %q is not a payload field for event_type %q and will never match", i, rule.ID, j, cond.Field, rule.EventType))
			}
		}
	}
	return out
}

package alerting

import (
	"fmt"

	"github.com/flipslidersand/sentinel-mesh/internal/eventtype"
)

// payloadFields returns the set of top-level payload keys for an event type.
// "*" yields the union across all event types. The keys come from the payload
// structs the receiver actually serialises (eventtype), not from the pb
// descriptors, so they cannot drift from the real payload (#248, #353).
func payloadFields(eventType string) map[string]bool {
	return eventtype.Fields(eventType)
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

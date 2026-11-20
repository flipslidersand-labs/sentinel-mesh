package alerting

import (
	"encoding/json"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func regexRuleSet(pattern string) *RuleSet {
	return &RuleSet{Rules: []Rule{{
		ID: "r", EventType: "exec", Severity: SeverityWarning, Message: "m",
		Conditions: []Condition{{Field: "comm", Operator: "regex", Value: pattern}},
	}}}
}

func TestNewWarnsOnInvalidRegex(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	e := New(regexRuleSet("("), zap.New(core))
	if logs.Len() != 1 {
		t.Fatalf("want 1 warning at New for invalid regex, got %d", logs.Len())
	}
	if got := e.matchCondition(map[string]interface{}{"comm": "("}, e.rules.Rules[0].Conditions[0]); got {
		t.Fatal("invalid regex must never match")
	}
}

func TestPrecompiledRegexMatchUnchanged(t *testing.T) {
	e := New(regexRuleSet(`^(nc|ncat)$`), zap.NewNop())
	c := e.rules.Rules[0].Conditions[0]
	for comm, want := range map[string]bool{"nc": true, "ncat": true, "ncx": false, "xnc": false} {
		if got := e.matchCondition(map[string]interface{}{"comm": comm}, c); got != want {
			t.Errorf("comm=%q got %v want %v", comm, got, want)
		}
	}
}

func TestRegexNotInPrecompiledSetStillWorks(t *testing.T) {
	// A Condition built after New (not in the cache) falls back to compiling.
	e := New(nil, zap.NewNop())
	c := Condition{Field: "comm", Operator: "regex", Value: "^a+$"}
	if !e.matchCondition(map[string]interface{}{"comm": "aaa"}, c) {
		t.Fatal("fallback compile should match")
	}
}

func BenchmarkEvaluateRegex(b *testing.B) {
	e := New(regexRuleSet(`^(nc|ncat|socat)\s+-[el]\S*$`), zap.NewNop())
	payload, _ := json.Marshal(map[string]string{"comm": "ncat -lvp"})
	ev := store.Event{EventID: "b", NodeID: "n", Type: "exec", Payload: payload}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Evaluate(ev)
	}
}

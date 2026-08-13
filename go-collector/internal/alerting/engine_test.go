package alerting

import (
	"encoding/json"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

func TestEngineEvaluateExecRule(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	ruleset := &RuleSet{
		Rules: []Rule{
			{
				ID:        "test_exec",
				EventType: "exec",
				Severity:  "warning",
				Message:   "exec detected",
				Conditions: []Condition{
					{
						Field:    "comm",
						Operator: "eq",
						Value:    "bash",
					},
				},
			},
		},
	}

	engine := New(ruleset, logger)

	payload := map[string]interface{}{
		"pid":  1234,
		"comm": "bash",
	}
	raw, _ := json.Marshal(payload)

	event := store.Event{
		EventID:   "test-1",
		NodeID:    "node-1",
		Timestamp: time.Now().UTC(),
		Type:      "exec",
		Payload:   raw,
	}

	alerts := engine.Evaluate(event)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}

	alert := alerts[0]
	if alert.RuleID != "test_exec" {
		t.Errorf("expected rule_id=test_exec, got %s", alert.RuleID)
	}
	if alert.EventID != "test-1" {
		t.Errorf("expected event_id=test-1, got %s", alert.EventID)
	}
}

func TestEngineEvaluateNoMatch(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	ruleset := &RuleSet{
		Rules: []Rule{
			{
				ID:        "test_exec",
				EventType: "exec",
				Severity:  "warning",
				Message:   "exec detected",
				Conditions: []Condition{
					{
						Field:    "comm",
						Operator: "eq",
						Value:    "bash",
					},
				},
			},
		},
	}

	engine := New(ruleset, logger)

	payload := map[string]interface{}{
		"pid":  1234,
		"comm": "python",
	}
	raw, _ := json.Marshal(payload)

	event := store.Event{
		EventID:   "test-2",
		NodeID:    "node-1",
		Timestamp: time.Now().UTC(),
		Type:      "exec",
		Payload:   raw,
	}

	alerts := engine.Evaluate(event)
	if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts, got %d", len(alerts))
	}
}

func TestEngineEvaluateWildcardEventType(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	ruleset := &RuleSet{
		Rules: []Rule{
			{
				ID:        "catch_all",
				EventType: "*",
				Severity:  "info",
				Message:   "any event",
				Conditions: []Condition{
					{
						Field:    "test",
						Operator: "eq",
						Value:    "yes",
					},
				},
			},
		},
	}

	engine := New(ruleset, logger)

	payload := map[string]interface{}{
		"test": "yes",
	}
	raw, _ := json.Marshal(payload)

	event := store.Event{
		EventID:   "test-3",
		NodeID:    "node-1",
		Timestamp: time.Now().UTC(),
		Type:      "tcp",
		Payload:   raw,
	}

	alerts := engine.Evaluate(event)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
}

func TestEngineNumericComparison(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	ruleset := &RuleSet{
		Rules: []Rule{
			{
				ID:        "high_port",
				EventType: "tcp",
				Severity:  "warning",
				Message:   "high port",
				Conditions: []Condition{
					{
						Field:    "dport",
						Operator: "gt",
						Value:    "5000",
					},
				},
			},
		},
	}

	engine := New(ruleset, logger)

	payload := map[string]interface{}{
		"dport": 8080,
	}
	raw, _ := json.Marshal(payload)

	event := store.Event{
		EventID:   "test-4",
		NodeID:    "node-1",
		Timestamp: time.Now().UTC(),
		Type:      "tcp",
		Payload:   raw,
	}

	alerts := engine.Evaluate(event)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
}

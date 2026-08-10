package alerting

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// Alert is an alias for store.Alert for convenience in this package.
type Alert = store.Alert

// Engine evaluates events against rules and generates alerts.
type Engine struct {
	rules  *RuleSet
	logger *zap.Logger
}

// New creates a new alerting engine.
func New(rules *RuleSet, logger *zap.Logger) *Engine {
	if rules == nil {
		rules = &RuleSet{Rules: []Rule{}}
	}
	return &Engine{
		rules:  rules,
		logger: logger,
	}
}

// Evaluate checks an event against all rules and returns matching alerts.
func (e *Engine) Evaluate(event store.Event) []Alert {
	var alerts []Alert

	for _, rule := range e.rules.Rules {
		if rule.EventType != "*" && rule.EventType != event.Type {
			continue
		}

		if e.matchConditions(event, rule.Conditions) {
			alert := Alert{
				AlertID:   uuid.New().String(),
				RuleID:    rule.ID,
				NodeID:    event.NodeID,
				EventID:   event.EventID,
				Timestamp: time.Now().UTC(),
				Message:   rule.Message,
				Severity:  rule.Severity,
			}
			alerts = append(alerts, alert)
			e.logger.Warn("alert triggered",
				zap.String("rule_id", rule.ID),
				zap.String("node_id", event.NodeID),
				zap.String("severity", rule.Severity),
				zap.String("message", rule.Message),
			)
		}
	}

	return alerts
}

// matchConditions checks if all conditions match the event payload.
func (e *Engine) matchConditions(event store.Event, conditions []Condition) bool {
	if len(conditions) == 0 {
		return false
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		e.logger.Debug("failed to unmarshal event payload", zap.Error(err))
		return false
	}

	for _, cond := range conditions {
		if !e.matchCondition(payload, cond) {
			return false
		}
	}

	return true
}

// matchCondition checks a single condition against the payload.
func (e *Engine) matchCondition(payload map[string]interface{}, cond Condition) bool {
	value, ok := e.getFieldValue(payload, cond.Field)
	if !ok {
		return false
	}

	switch cond.Operator {
	case "eq":
		return fmt.Sprintf("%v", value) == cond.Value

	case "contains":
		return strings.Contains(fmt.Sprintf("%v", value), cond.Value)

	case "regex":
		re, err := regexp.Compile(cond.Value)
		if err != nil {
			e.logger.Debug("invalid regex in condition", zap.Error(err))
			return false
		}
		return re.MatchString(fmt.Sprintf("%v", value))

	case "gt":
		return e.compareNumeric(value, cond.Value, func(a, b float64) bool {
			return a > b
		})

	case "lt":
		return e.compareNumeric(value, cond.Value, func(a, b float64) bool {
			return a < b
		})

	default:
		e.logger.Debug("unknown operator", zap.String("operator", cond.Operator))
		return false
	}
}

// getFieldValue retrieves a value from payload using dot notation (e.g., "parent.child.field").
func (e *Engine) getFieldValue(payload map[string]interface{}, fieldPath string) (interface{}, bool) {
	parts := strings.Split(fieldPath, ".")
	current := interface{}(payload)

	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		var found bool
		current, found = m[part]
		if !found {
			return nil, false
		}
	}

	return current, true
}

// compareNumeric converts values to float64 and applies a comparison function.
func (e *Engine) compareNumeric(value interface{}, target string, cmp func(a, b float64) bool) bool {
	a, err := toFloat64(value)
	if err != nil {
		return false
	}
	b, err := strconv.ParseFloat(target, 64)
	if err != nil {
		return false
	}
	return cmp(a, b)
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case string:
		return strconv.ParseFloat(val, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

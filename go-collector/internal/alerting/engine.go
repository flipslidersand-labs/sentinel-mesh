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

	"github.com/flipslidersand/sentinel-mesh/internal/model"
)

// Alert is an alias for model.Alert for convenience in this package.
type Alert = model.Alert

// Engine evaluates events against rules and generates alerts.
type Engine struct {
	rules  *RuleSet
	logger *zap.Logger
	// regexes holds regex patterns compiled once in New, keyed by pattern.
	// It is read-only after New, so concurrent Evaluate calls are safe.
	regexes map[string]*regexp.Regexp
}

// New creates a new alerting engine.
func New(rules *RuleSet, logger *zap.Logger) *Engine {
	if rules == nil {
		rules = &RuleSet{Rules: []Rule{}}
	}
	e := &Engine{
		rules:   rules,
		logger:  logger,
		regexes: make(map[string]*regexp.Regexp),
	}
	e.precompileRegexes()
	return e
}

// precompileRegexes compiles every regex condition once so the Evaluate hot
// path does not recompile per event. Invalid patterns (normally rejected by
// RuleSet.Validate) are logged at warn level and never match.
func (e *Engine) precompileRegexes() {
	for _, rule := range e.rules.Rules {
		for _, cond := range rule.Conditions {
			if cond.Operator != "regex" {
				continue
			}
			if _, done := e.regexes[cond.Value]; done {
				continue
			}
			re, err := regexp.Compile(cond.Value)
			if err != nil {
				e.logger.Warn("invalid regex in rule condition; it will never match",
					zap.String("rule_id", rule.ID),
					zap.String("pattern", cond.Value),
					zap.Error(err))
				e.regexes[cond.Value] = nil
				continue
			}
			e.regexes[cond.Value] = re
		}
	}
}

// Evaluate checks an event against all rules and returns matching alerts.
func (e *Engine) Evaluate(event model.Event) []Alert {
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
func (e *Engine) matchConditions(event model.Event, conditions []Condition) bool {
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
		return valueToString(value) == cond.Value

	case "contains":
		return strings.Contains(valueToString(value), cond.Value)

	case "regex":
		re, ok := e.regexes[cond.Value]
		if !ok {
			// Condition not seen at New (e.g. built afterwards): compile on demand.
			var err error
			if re, err = regexp.Compile(cond.Value); err != nil {
				e.logger.Debug("invalid regex in condition", zap.Error(err))
				return false
			}
		}
		if re == nil {
			return false
		}
		return re.MatchString(valueToString(value))

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

// valueToString renders a payload value for string comparison. float64 values
// (all JSON numbers) use plain decimal notation so that large integers such as
// pid 1234567 are not rendered as "1.234567e+06".
func valueToString(v interface{}) string {
	if f, ok := v.(float64); ok {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return fmt.Sprintf("%v", v)
}

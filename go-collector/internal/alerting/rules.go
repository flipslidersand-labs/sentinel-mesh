package alerting

import (
	"fmt"
	"os"
	"regexp"
	"strconv"

	"gopkg.in/yaml.v3"

	"github.com/flipslidersand/sentinel-mesh/internal/eventtype"
)

// validEventType reports whether t is an event type a Rule may match against.
func validEventType(t string) bool { return t == eventtype.Any || eventtype.IsValid(t) }

// validOperators is the set of operators a Condition may use.
var validOperators = map[string]bool{
	"eq":       true,
	"contains": true,
	"regex":    true,
	"gt":       true,
	"lt":       true,
}

// LoadRules loads alert rules from a YAML file. An empty path means no rules
// file was requested and yields an empty RuleSet. A non-empty path whose
// file does not exist is an error, so a typo'd --rules path fails loudly
// instead of silently starting with no rules.
func LoadRules(path string) (*RuleSet, error) {
	if path == "" {
		return &RuleSet{Rules: []Rule{}}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("rules file %q: %w", path, err)
	}

	var ruleset RuleSet
	if err := yaml.Unmarshal(data, &ruleset); err != nil {
		return nil, err
	}

	return &ruleset, nil
}

// Validate checks that rules are well-formed: each rule has a non-empty,
// unique ID, a known event type, a known severity, at least one condition, and each
// condition uses a known operator with a syntactically valid regex value
// when the operator is "regex".
func (rs *RuleSet) Validate() error {
	seenIDs := make(map[string]bool, len(rs.Rules))

	for i, rule := range rs.Rules {
		if rule.ID == "" {
			return fmt.Errorf("rule[%d]: id must not be empty", i)
		}
		if seenIDs[rule.ID] {
			return fmt.Errorf("rule[%d] (id=%q): duplicate id", i, rule.ID)
		}
		seenIDs[rule.ID] = true

		if !validEventType(rule.EventType) {
			return fmt.Errorf("rule[%d] (id=%q): unknown event_type %q", i, rule.ID, rule.EventType)
		}

		if _, ok := SeverityRank(rule.Severity); !ok {
			return fmt.Errorf("rule[%d] (id=%q): unknown severity %q (want %s, %s or %s)", i, rule.ID, rule.Severity, SeverityInfo, SeverityWarning, SeverityCritical)
		}

		if len(rule.Conditions) == 0 {
			return fmt.Errorf("rule[%d] (id=%q): conditions must not be empty", i, rule.ID)
		}

		for j, cond := range rule.Conditions {
			if !validOperators[cond.Operator] {
				return fmt.Errorf("rule[%d] (id=%q): condition[%d]: unknown operator %q", i, rule.ID, j, cond.Operator)
			}
			if cond.Operator == "regex" {
				if _, err := regexp.Compile(cond.Value); err != nil {
					return fmt.Errorf("rule[%d] (id=%q): condition[%d]: invalid regex %q: %w", i, rule.ID, j, cond.Value, err)
				}
			}
			if cond.Operator == "gt" || cond.Operator == "lt" {
				// Same parse as Engine.compareNumeric so Validate and
				// matching never disagree.
				if _, err := strconv.ParseFloat(cond.Value, 64); err != nil {
					return fmt.Errorf("rule[%d] (id=%q): condition[%d]: operator %q requires a numeric value, got %q: %w", i, rule.ID, j, cond.Operator, cond.Value, err)
				}
			}
		}
	}

	return nil
}

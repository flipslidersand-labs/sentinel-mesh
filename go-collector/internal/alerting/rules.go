package alerting

import (
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// validEventTypes is the set of event types a Rule may match against.
var validEventTypes = map[string]bool{
	"exec": true,
	"tcp":  true,
	"file": true,
	"*":    true,
}

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

	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("rules file %q: %w", path, err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var ruleset RuleSet
	if err := yaml.Unmarshal(data, &ruleset); err != nil {
		return nil, err
	}

	return &ruleset, nil
}

// Validate checks that rules are well-formed: each rule has a non-empty,
// unique ID, a known event type, at least one condition, and each
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

		if !validEventTypes[rule.EventType] {
			return fmt.Errorf("rule[%d] (id=%q): unknown event_type %q", i, rule.ID, rule.EventType)
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
		}
	}

	return nil
}

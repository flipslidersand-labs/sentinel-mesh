package alerting

import (
	"os"

	"gopkg.in/yaml.v3"
)

// LoadRules loads alert rules from a YAML file.
func LoadRules(path string) (*RuleSet, error) {
	if _, err := os.Stat(path); err != nil {
		return &RuleSet{Rules: []Rule{}}, nil
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

// Validate checks that rules are well-formed.
func (rs *RuleSet) Validate() error {
	// Add validation logic as needed (e.g., ensure field selectors are valid)
	return nil
}

package alerting

// Rule represents an alerting rule.
type Rule struct {
	ID          string            `yaml:"id"`
	Description string            `yaml:"description"`
	EventType   string            `yaml:"event_type"` // "exec" | "tcp" | "file" | "*"
	Conditions  []Condition       `yaml:"conditions"`
	Severity    string            `yaml:"severity"`
	Message     string            `yaml:"message"`
}

// Condition represents a single condition to match.
type Condition struct {
	Field    string `yaml:"field"`    // JSONPath-like field selector
	Operator string `yaml:"operator"` // "eq" | "contains" | "regex" | "gt" | "lt"
	Value    string `yaml:"value"`
}

// RuleSet holds all active rules.
type RuleSet struct {
	Rules []Rule `yaml:"rules"`
}

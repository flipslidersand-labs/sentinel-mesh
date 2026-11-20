package alerting

// Canonical severity vocabulary. This is the single definition: rules.yaml
// is validated against it (RuleSet.Validate) and the notify package orders
// alerts by it (SeverityRank). The web UI renders the same three values.
const (
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)

// SeverityRank returns the ordering of a canonical severity (info < warning <
// critical). ok is false for anything else; matching is exact (no aliases, no
// case folding).
func SeverityRank(s string) (rank int, ok bool) {
	switch s {
	case SeverityInfo:
		return 0, true
	case SeverityWarning:
		return 1, true
	case SeverityCritical:
		return 2, true
	}
	return 0, false
}

// Rule represents an alerting rule.
type Rule struct {
	ID          string      `yaml:"id"`
	Description string      `yaml:"description"`
	EventType   string      `yaml:"event_type"` // "exec" | "tcp" | "file" | "*"
	Conditions  []Condition `yaml:"conditions"`
	Severity    string      `yaml:"severity"`
	Message     string      `yaml:"message"`
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

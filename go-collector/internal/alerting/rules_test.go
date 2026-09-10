package alerting

import (
	"os"
	"path/filepath"
	"testing"
)

func validRule() Rule {
	return Rule{
		ID:        "r1",
		EventType: "exec",
		Conditions: []Condition{
			{Field: "comm", Operator: "eq", Value: "bash"},
		},
	}
}

func TestValidateOK(t *testing.T) {
	rs := &RuleSet{Rules: []Rule{validRule()}}
	if err := rs.Validate(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidateEmptyRuleSetOK(t *testing.T) {
	rs := &RuleSet{}
	if err := rs.Validate(); err != nil {
		t.Fatalf("expected no error for empty ruleset, got %v", err)
	}
}

func TestValidateEmptyID(t *testing.T) {
	rule := validRule()
	rule.ID = ""
	rs := &RuleSet{Rules: []Rule{rule}}
	if err := rs.Validate(); err == nil {
		t.Fatal("expected error for empty id")
	}
}

func TestValidateDuplicateID(t *testing.T) {
	rs := &RuleSet{Rules: []Rule{validRule(), validRule()}}
	if err := rs.Validate(); err == nil {
		t.Fatal("expected error for duplicate id")
	}
}

func TestValidateUnknownEventType(t *testing.T) {
	rule := validRule()
	rule.EventType = "bogus"
	rs := &RuleSet{Rules: []Rule{rule}}
	if err := rs.Validate(); err == nil {
		t.Fatal("expected error for unknown event_type")
	}
}

func TestValidateEmptyConditions(t *testing.T) {
	rule := validRule()
	rule.Conditions = nil
	rs := &RuleSet{Rules: []Rule{rule}}
	if err := rs.Validate(); err == nil {
		t.Fatal("expected error for empty conditions")
	}
}

func TestValidateUnknownOperator(t *testing.T) {
	rule := validRule()
	rule.Conditions = []Condition{{Field: "comm", Operator: "bogus", Value: "x"}}
	rs := &RuleSet{Rules: []Rule{rule}}
	if err := rs.Validate(); err == nil {
		t.Fatal("expected error for unknown operator")
	}
}

func TestValidateInvalidRegex(t *testing.T) {
	rule := validRule()
	rule.Conditions = []Condition{{Field: "comm", Operator: "regex", Value: "("}}
	rs := &RuleSet{Rules: []Rule{rule}}
	if err := rs.Validate(); err == nil {
		t.Fatal("expected error for invalid regex")
	}
}

func TestValidateValidRegex(t *testing.T) {
	rule := validRule()
	rule.Conditions = []Condition{{Field: "comm", Operator: "regex", Value: "^ba.*"}}
	rs := &RuleSet{Rules: []Rule{rule}}
	if err := rs.Validate(); err != nil {
		t.Fatalf("expected no error for valid regex, got %v", err)
	}
}

func TestLoadRulesEmptyPathReturnsEmptyRuleSet(t *testing.T) {
	rs, err := LoadRules("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(rs.Rules) != 0 {
		t.Fatalf("expected empty ruleset, got %d rules", len(rs.Rules))
	}
}

func TestLoadRulesMissingFileReturnsError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")
	_, err := LoadRules(missing)
	if err == nil {
		t.Fatal("expected error for missing rules file")
	}
}

func TestLoadRulesValidFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	content := `rules:
  - id: r1
    event_type: exec
    conditions:
      - field: comm
        operator: eq
        value: bash
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp rules file: %v", err)
	}

	rs, err := LoadRules(path)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(rs.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rs.Rules))
	}
	if err := rs.Validate(); err != nil {
		t.Fatalf("expected loaded ruleset to validate, got %v", err)
	}
}

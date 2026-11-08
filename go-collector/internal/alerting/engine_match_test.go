package alerting

import (
	"testing"

	"go.uber.org/zap"
)

func TestMatchCondition(t *testing.T) {
	e := New(nil, zap.NewNop())

	payload := map[string]interface{}{
		"comm":   "bash",
		"path":   "/usr/bin/curl",
		"count":  float64(10),
		"ratio":  1.5,
		"numstr": "42",
		"text":   "abc",
		"flag":   true,
		"nilval": nil,
		"proc": map[string]interface{}{
			"name": "sshd",
			"pid":  float64(1234),
			"user": map[string]interface{}{"uid": float64(0)},
		},
	}

	tests := []struct {
		name string
		cond Condition
		want bool
	}{
		// eq
		{"eq string match", Condition{Field: "comm", Operator: "eq", Value: "bash"}, true},
		{"eq string mismatch", Condition{Field: "comm", Operator: "eq", Value: "zsh"}, false},
		{"eq number formatted with %v", Condition{Field: "count", Operator: "eq", Value: "10"}, true},
		{"eq float formatted with %v", Condition{Field: "ratio", Operator: "eq", Value: "1.5"}, true},
		{"eq number mismatch", Condition{Field: "count", Operator: "eq", Value: "11"}, false},
		{"eq number does not match 10.0 text", Condition{Field: "count", Operator: "eq", Value: "10.0"}, false},
		{"eq bool", Condition{Field: "flag", Operator: "eq", Value: "true"}, true},

		// contains
		{"contains match", Condition{Field: "path", Operator: "contains", Value: "bin/cu"}, true},
		{"contains mismatch", Condition{Field: "path", Operator: "contains", Value: "wget"}, false},
		{"contains empty target", Condition{Field: "path", Operator: "contains", Value: ""}, true},
		{"contains on number", Condition{Field: "count", Operator: "contains", Value: "1"}, true},

		// regex
		{"regex match", Condition{Field: "path", Operator: "regex", Value: `^/usr/bin/\w+$`}, true},
		{"regex mismatch", Condition{Field: "path", Operator: "regex", Value: `^/sbin/`}, false},
		{"regex invalid", Condition{Field: "path", Operator: "regex", Value: `([`}, false},
		{"regex on number", Condition{Field: "count", Operator: "regex", Value: `^\d+$`}, true},

		// gt
		{"gt true", Condition{Field: "count", Operator: "gt", Value: "5"}, true},
		{"gt false", Condition{Field: "count", Operator: "gt", Value: "20"}, false},
		{"gt boundary equal is false", Condition{Field: "count", Operator: "gt", Value: "10"}, false},
		{"gt float", Condition{Field: "ratio", Operator: "gt", Value: "1.4"}, true},
		{"gt numeric string value", Condition{Field: "numstr", Operator: "gt", Value: "41"}, true},
		{"gt non-numeric target", Condition{Field: "count", Operator: "gt", Value: "abc"}, false},
		{"gt non-numeric value", Condition{Field: "text", Operator: "gt", Value: "1"}, false},
		{"gt bool value", Condition{Field: "flag", Operator: "gt", Value: "0"}, false},
		{"gt nil value", Condition{Field: "nilval", Operator: "gt", Value: "0"}, false},

		// lt
		{"lt true", Condition{Field: "count", Operator: "lt", Value: "20"}, true},
		{"lt false", Condition{Field: "count", Operator: "lt", Value: "5"}, false},
		{"lt boundary equal is false", Condition{Field: "count", Operator: "lt", Value: "10"}, false},
		{"lt numeric string value", Condition{Field: "numstr", Operator: "lt", Value: "43"}, true},
		{"lt non-numeric target", Condition{Field: "count", Operator: "lt", Value: "abc"}, false},
		{"lt non-numeric value", Condition{Field: "text", Operator: "lt", Value: "1"}, false},

		// missing field / nested path
		{"missing field", Condition{Field: "nope", Operator: "eq", Value: "x"}, false},
		{"nested eq", Condition{Field: "proc.name", Operator: "eq", Value: "sshd"}, true},
		{"nested gt", Condition{Field: "proc.pid", Operator: "gt", Value: "1000"}, true},
		{"deeply nested", Condition{Field: "proc.user.uid", Operator: "eq", Value: "0"}, true},
		{"nested missing leaf", Condition{Field: "proc.nope", Operator: "eq", Value: "x"}, false},
		{"nested missing intermediate", Condition{Field: "nope.name", Operator: "eq", Value: "x"}, false},
		{"intermediate is non-map string", Condition{Field: "comm.x", Operator: "eq", Value: "bash"}, false},
		{"intermediate is non-map number", Condition{Field: "count.x", Operator: "eq", Value: "10"}, false},
		{"path continues past leaf", Condition{Field: "proc.name.first", Operator: "eq", Value: "s"}, false},
		{"empty field path", Condition{Field: "", Operator: "eq", Value: "x"}, false},
		{"trailing dot", Condition{Field: "proc.", Operator: "eq", Value: "x"}, false},

		// operator
		{"unknown operator", Condition{Field: "comm", Operator: "neq", Value: "zsh"}, false},
		{"empty operator", Condition{Field: "comm", Operator: "", Value: "bash"}, false},
		{"operator is case sensitive", Condition{Field: "comm", Operator: "EQ", Value: "bash"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := e.matchCondition(payload, tt.cond); got != tt.want {
				t.Errorf("matchCondition(%+v) = %v, want %v", tt.cond, got, tt.want)
			}
		})
	}
}

func TestMatchConditionNilPayload(t *testing.T) {
	e := New(nil, zap.NewNop())
	if e.matchCondition(nil, Condition{Field: "a", Operator: "eq", Value: "x"}) {
		t.Error("nil payload should not match")
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name    string
		in      interface{}
		want    float64
		wantErr bool
	}{
		{"float64", float64(1.5), 1.5, false},
		{"int", 7, 7, false},
		{"negative int", -3, -3, false},
		{"numeric string", "3.25", 3.25, false},
		{"integer string", "42", 42, false},
		{"non-numeric string", "abc", 0, true},
		{"empty string", "", 0, true},
		{"bool", true, 0, true},
		{"nil", nil, 0, true},
		{"map", map[string]interface{}{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toFloat64(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("toFloat64(%v) err = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("toFloat64(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

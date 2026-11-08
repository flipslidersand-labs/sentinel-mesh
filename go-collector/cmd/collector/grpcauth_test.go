package main

import "testing"

func TestParseGRPCAuthMode(t *testing.T) {
	cases := []struct {
		in      string
		want    bool
		wantErr bool
	}{
		{"required", true, false},
		{"disabled", false, false},
		{"", false, true},
		{"Required", false, true},
		{"true", false, true},
		{"none", false, true},
	}
	for _, c := range cases {
		got, err := parseGRPCAuthMode(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("parseGRPCAuthMode(%q) err = %v, wantErr %v", c.in, err, c.wantErr)
			continue
		}
		if err == nil && got != c.want {
			t.Errorf("parseGRPCAuthMode(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestServeCmd_GRPCAuthFlagDefaultsToRequired(t *testing.T) {
	f := serveCmd().Flags().Lookup("grpc-auth")
	if f == nil {
		t.Fatal("--grpc-auth flag not defined")
	}
	if f.DefValue != "required" {
		t.Errorf("default = %q, want required", f.DefValue)
	}
}

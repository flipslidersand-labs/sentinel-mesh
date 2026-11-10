package main

import (
	"strings"
	"testing"
)

func TestServeCmd_HTTPAuthFlagDefaultsToRequired(t *testing.T) {
	f := serveCmd().Flags().Lookup("http-auth")
	if f == nil {
		t.Fatal("--http-auth flag not defined")
	}
	if f.DefValue != "required" {
		t.Errorf("default = %q, want required", f.DefValue)
	}
}

// Both normal and aggregate mode must refuse to start without a token unless
// --http-auth=disabled is explicit. The check runs before the store lock or
// any listener, so these use no data dir.
func TestServe_RefusesUnauthenticatedRESTByDefault(t *testing.T) {
	t.Setenv("SENTINEL_API_TOKEN", "")
	for _, args := range [][]string{
		{"--data-dir", t.TempDir()},
		{"--aggregate", "--upstreams", "r=http://127.0.0.1:1"},
	} {
		cmd := serveCmd()
		cmd.SetArgs(args)
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		err := cmd.Execute()
		if err == nil {
			t.Fatalf("args %v: expected error, got nil", args)
		}
		// The failure must be the auth refusal itself, not an unrelated error.
		for _, want := range []string{"SENTINEL_API_TOKEN", "--http-auth=disabled"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("args %v: error %q does not mention %q", args, err, want)
			}
		}
	}
}

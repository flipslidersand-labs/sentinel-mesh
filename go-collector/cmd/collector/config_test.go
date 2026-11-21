package main

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestLoadServeConfig_UnknownFlagReturnsError(t *testing.T) {
	// A command missing a flag loadServeConfig reads must error, not yield "".
	cmd := &cobra.Command{Use: "serve"}
	if _, err := loadServeConfig(cmd); err == nil {
		t.Fatal("expected error for missing flag definitions")
	}
}

func TestLoadServeConfig_Defaults(t *testing.T) {
	t.Setenv("SENTINEL_API_TOKEN", "0123456789abcdef0123456789abcdef")
	cfg, err := loadServeConfig(serveCmd())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.grpcAddr != defaultGRPCAddr || cfg.httpAddr != ":8081" || !cfg.grpcAuthEnabled || cfg.region != "default" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestNormalOnlyFlagsAreDefined(t *testing.T) {
	cmd := serveCmd()
	for _, n := range append([]string{"grpc-addr"}, normalOnlyFlags...) {
		if cmd.Flags().Lookup(n) == nil {
			t.Errorf("normalOnlyFlags references undefined flag %q", n)
		}
	}
}

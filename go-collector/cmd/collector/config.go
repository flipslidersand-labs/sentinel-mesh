package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/flipslidersand/sentinel-mesh/internal/httpauth"
)

// defaultGRPCAddr is the --grpc-addr default, shared by the flag definition
// and the aggregate-mode "ignored flag" warning.
const defaultGRPCAddr = ":50051"

// normalOnlyFlags are the flags (besides --grpc-addr, which has a
// default-value check) that are ignored in --aggregate mode. The warning in
// warnIgnoredNormalModeFlags iterates this list.
var normalOnlyFlags = []string{"data-dir", "rules", "grpc-tls-cert", "grpc-tls-key", "grpc-auth", "region"}

// serveConfig holds every value `serve` reads from flags/env.
type serveConfig struct {
	aggregate         bool
	grpcAddr          string
	httpAddr          string
	dataDir           string
	heartbeatTimeout  time.Duration
	rulesPath         string
	otelEndpoint      string
	region            string
	staticDir         string
	corsOrigins       []string
	upstreams         []string
	pollInterval      time.Duration
	allowInsecureHTTP bool
	grpcTLSCert       string
	grpcTLSKey        string
	httpTLSCert       string
	httpTLSKey        string
	grpcAuthEnabled   bool
	apiToken          string
	restUnauth        bool
}

// flagReader collects the first flag lookup error instead of discarding it,
// so a mistyped flag name fails loudly rather than yielding a zero value.
type flagReader struct {
	cmd *cobra.Command
	err error
}

func (r *flagReader) str(name string) string {
	v, err := r.cmd.Flags().GetString(name)
	r.note(err)
	return v
}

func (r *flagReader) strs(name string) []string {
	v, err := r.cmd.Flags().GetStringSlice(name)
	r.note(err)
	return v
}

func (r *flagReader) boolean(name string) bool {
	v, err := r.cmd.Flags().GetBool(name)
	r.note(err)
	return v
}

func (r *flagReader) dur(name string) time.Duration {
	v, err := r.cmd.Flags().GetDuration(name)
	r.note(err)
	return v
}

func (r *flagReader) note(err error) {
	if err != nil && r.err == nil {
		r.err = err
	}
}

// loadServeConfig reads and validates all serve flags. Validation order is
// preserved from the original RunE: --grpc-auth, aggregate/upstreams
// consistency, then --http-auth/SENTINEL_API_TOKEN.
func loadServeConfig(cmd *cobra.Command) (*serveConfig, error) {
	r := &flagReader{cmd: cmd}
	c := &serveConfig{
		aggregate:         r.boolean("aggregate"),
		grpcAddr:          r.str("grpc-addr"),
		httpAddr:          r.str("http-addr"),
		dataDir:           r.str("data-dir"),
		heartbeatTimeout:  r.dur("heartbeat-timeout"),
		rulesPath:         r.str("rules"),
		otelEndpoint:      r.str("otel-endpoint"),
		region:            r.str("region"),
		staticDir:         r.str("static-dir"),
		corsOrigins:       r.strs("cors-origins"),
		upstreams:         r.strs("upstreams"),
		pollInterval:      r.dur("poll-interval"),
		allowInsecureHTTP: r.boolean("aggregate-allow-insecure-http"),
		grpcTLSCert:       r.str("grpc-tls-cert"),
		grpcTLSKey:        r.str("grpc-tls-key"),
		httpTLSCert:       r.str("http-tls-cert"),
		httpTLSKey:        r.str("http-tls-key"),
	}
	grpcAuthMode := r.str("grpc-auth")
	httpAuthMode := r.str("http-auth")
	if r.err != nil {
		return nil, fmt.Errorf("read serve flags: %w", r.err)
	}

	var err error
	if c.grpcAuthEnabled, err = httpauth.ParseMode("grpc-auth", grpcAuthMode); err != nil {
		return nil, err
	}
	upstreamsSet := cmd.Flags().Changed("upstreams") || cmd.Flags().Changed("poll-interval")
	if !c.aggregate && upstreamsSet {
		return nil, fmt.Errorf("--upstreams/--poll-interval require --aggregate")
	}
	if c.apiToken, c.restUnauth, err = httpauth.ResolveToken(httpAuthMode); err != nil {
		return nil, err
	}
	return c, nil
}

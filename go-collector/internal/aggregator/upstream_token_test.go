package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

func TestUpstreamTokenEnv(t *testing.T) {
	cases := map[string]string{
		"us-east":  "SENTINEL_UPSTREAM_TOKEN_US_EAST",
		"eu.west1": "SENTINEL_UPSTREAM_TOKEN_EU_WEST1",
		"AP_south": "SENTINEL_UPSTREAM_TOKEN_AP_SOUTH",
	}
	for in, want := range cases {
		if got := UpstreamTokenEnv(in); got != want {
			t.Errorf("UpstreamTokenEnv(%q) = %q, want %q", in, got, want)
		}
	}
}

// authRecorder is an upstream that records every Authorization header it sees.
func authRecorder(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		_ = json.NewEncoder(w).Encode([]registry.AgentNode{})
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func assertAll(t *testing.T, name string, got []string, want string) {
	t.Helper()
	if len(got) == 0 {
		t.Fatalf("%s: upstream received no requests", name)
	}
	for _, g := range got {
		if g != want {
			t.Errorf("%s: Authorization = %q, want %q", name, g, want)
		}
	}
}

// Per-region env tokens are picked up by ParseUpstreams and each upstream
// receives only its own token; a region without one gets the shared token.
func TestAggregator_PerUpstreamTokens(t *testing.T) {
	// Clear first so a developer's own environment cannot influence the test.
	t.Setenv("SENTINEL_UPSTREAM_TOKEN_CA_CENTRAL", "")
	t.Setenv("SENTINEL_UPSTREAM_TOKEN_US_EAST", "tok-east")
	t.Setenv("SENTINEL_UPSTREAM_TOKEN_EU_WEST", "tok-west")
	t.Setenv("SENTINEL_UPSTREAM_TOKEN_AP_SOUTH", "") // unset-equivalent

	east, eastSeen := authRecorder(t)
	west, westSeen := authRecorder(t)
	other, otherSeen := authRecorder(t)
	south, southSeen := authRecorder(t)

	ups, err := ParseUpstreams([]string{
		"us-east=" + east.URL, "eu-west=" + west.URL, "ca-central=" + other.URL, "ap-south=" + south.URL,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	agg := New(ups, 0, "shared", nil)
	agg.PollOnce(context.Background())

	assertAll(t, "us-east", eastSeen(), "Bearer tok-east")
	assertAll(t, "eu-west", westSeen(), "Bearer tok-west")
	assertAll(t, "ca-central (no env)", otherSeen(), "Bearer shared")
	assertAll(t, "ap-south (empty env)", southSeen(), "Bearer shared")
}

// The per-upstream token is also used by the on-demand events/alerts fetch.
func TestAggregator_PerUpstreamTokenOnDemandFetch(t *testing.T) {
	t.Setenv("SENTINEL_UPSTREAM_TOKEN_R1", "")
	srv, seen := authRecorder(t)
	agg := New([]Upstream{{Region: "r1", URL: srv.URL, Token: "tok-r1"}}, 0, "shared", nil)
	agg.FetchEvents(context.Background(), "", "", 10)
	assertAll(t, "events", seen(), "Bearer tok-r1")
}

// With no env vars and no shared token, no Authorization header is sent
// (unchanged behaviour), and the caller's slice is not mutated.
func TestAggregator_NoTokenNoHeaderAndNoMutation(t *testing.T) {
	t.Setenv("SENTINEL_UPSTREAM_TOKEN_R1", "")
	t.Setenv("SENTINEL_UPSTREAM_TOKEN_R2", "")
	srv, seen := authRecorder(t)
	ups := []Upstream{{Region: "r1", URL: srv.URL}}
	agg := New(ups, 0, "", nil)
	agg.PollOnce(context.Background())
	assertAll(t, "no token", seen(), "")

	srv2, _ := authRecorder(t)
	ups2 := []Upstream{{Region: "r2", URL: srv2.URL}}
	_ = New(ups2, 0, "shared", nil)
	if ups2[0].Token != "" {
		t.Errorf("New mutated caller's slice: Token = %q", ups2[0].Token)
	}
}

func TestParseUpstreams_TokenEnvCollision(t *testing.T) {
	colliding := [][]string{
		{"us-east=http://192.0.2.1:8081", "us.east=http://192.0.2.2:8081"},
		{"us-east=http://192.0.2.1:8081", "us_east=http://192.0.2.2:8081"},
		{"us-east=http://192.0.2.1:8081", "US-EAST=http://192.0.2.2:8081"},
	}
	// Env set: ambiguous, so rejected.
	t.Setenv("SENTINEL_UPSTREAM_TOKEN_US_EAST", "tok")
	for _, specs := range colliding {
		_, err := ParseUpstreams(specs, true)
		if err == nil {
			t.Fatalf("specs %v: expected collision error", specs)
		}
		if !strings.Contains(err.Error(), "SENTINEL_UPSTREAM_TOKEN_US_EAST") {
			t.Errorf("error should name the env var: %v", err)
		}
		if strings.Contains(err.Error(), "192.0.2.") || strings.Contains(err.Error(), "tok\"") {
			t.Errorf("error must not include URLs or token: %v", err)
		}
	}
	// Env unset (or empty): both use the shared token, so they are accepted.
	t.Setenv("SENTINEL_UPSTREAM_TOKEN_US_EAST", "")
	for _, specs := range colliding {
		ups, err := ParseUpstreams(specs, true)
		if err != nil || len(ups) != 2 || ups[0].Token != "" || ups[1].Token != "" {
			t.Fatalf("specs %v without env: ups=%+v err=%v", specs, ups, err)
		}
	}
	// Distinct after normalisation: fine.
	if _, err := ParseUpstreams([]string{"us-east=http://192.0.2.1:8081", "us-west=http://192.0.2.2:8081"}, true); err != nil {
		t.Fatalf("distinct regions rejected: %v", err)
	}
}

// Tokens must not appear in ParseUpstreams errors, logs, RegionStatus or
// formatted Upstream values.
func TestUpstreamTokens_NeverLeaked(t *testing.T) {
	const secret = "tok-SECRET-do-not-leak"
	t.Setenv("SENTINEL_UPSTREAM_TOKEN_DOWN", secret)

	// Parse error path with the env set.
	_, err := ParseUpstreams([]string{"down=ftp://192.0.2.1"}, true)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("parse error missing or leaks token: %v", err)
	}

	// Unreachable upstream: warning log + RegionStatus.
	dead := httptest.NewServer(http.NewServeMux())
	deadURL := dead.URL
	dead.Close()
	ups, err := ParseUpstreams([]string{"down=" + deadURL}, true)
	if err != nil {
		t.Fatal(err)
	}
	if ups[0].Token != secret {
		t.Fatal("test setup: env token not picked up")
	}
	core, logs := observer.New(zap.DebugLevel)
	agg := New(ups, 0, "", zap.New(core))
	agg.PollOnce(context.Background())

	if logs.Len() == 0 {
		t.Fatal("expected a warning log for the unreachable upstream")
	}
	for _, e := range logs.All() {
		out := e.Message + fmt.Sprint(e.ContextMap())
		if strings.Contains(out, secret) {
			t.Errorf("log leaks token: %s", out)
		}
	}
	b, _ := json.Marshal(agg.Regions())
	if strings.Contains(string(b), secret) {
		t.Errorf("RegionStatus leaks token: %s", b)
	}
	for _, f := range []string{"%v", "%+v", "%s"} {
		if out := fmt.Sprintf(f, ups[0]); strings.Contains(out, secret) {
			t.Errorf("Sprintf(%s) leaks token: %s", f, out)
		}
	}
}

package receiver

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/flipslidersand/sentinel-mesh/internal/otel"
)

// TestCheckAuth_RecordsFailureReasons covers #223 (3): Unauthenticated and
// rate-limited rejections are counted separately with fixed reason labels.
func TestCheckAuth_RecordsFailureReasons(t *testing.T) {
	lim, _ := newTestLimiter()
	got := map[string]int{}
	lim.onFail = func(reason string) { got[reason]++ }
	st := newAuthTestStore(t)

	// missing metadata / missing header
	_, _ = checkAuthLimited(context.Background(), st, true, lim)
	noHeader := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x", "y"))
	_, _ = checkAuthLimited(noHeader, st, true, lim)
	if got[otel.AuthFailMissingCredentials] != 2 {
		t.Fatalf("missing_credentials = %d, want 2 (%v)", got[otel.AuthFailMissingCredentials], got)
	}

	// invalid tokens up to the burst, then rate limited
	for i := 0; i < int(authFailBurst); i++ {
		_, _ = checkAuthLimited(authCtx("203.0.113.1", "bad"), st, true, lim)
	}
	_, _ = checkAuthLimited(authCtx("203.0.113.1", "bad"), st, true, lim)
	if got[otel.AuthFailInvalidToken] != int(authFailBurst) {
		t.Fatalf("invalid_token = %d, want %d (%v)", got[otel.AuthFailInvalidToken], int(authFailBurst), got)
	}
	if got[otel.AuthFailRateLimited] != 1 {
		t.Fatalf("rate_limited = %d, want 1 (%v)", got[otel.AuthFailRateLimited], got)
	}

	// success records nothing
	before := len(got)
	tok, err := st.IssueToken(context.Background(), "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkAuthLimited(authCtx("203.0.113.5", tok), st, true, lim); err != nil {
		t.Fatal(err)
	}
	if len(got) != before {
		t.Fatalf("success must not record failures: %v", got)
	}
}

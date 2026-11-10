package receiver

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/flipslidersand/sentinel-mesh/internal/otel"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
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

	// success records nothing: compare full counts, not just key count
	before := copyCounts(got)
	tok, err := st.IssueToken(context.Background(), "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkAuthLimited(authCtx("203.0.113.5", tok), st, true, lim); err != nil {
		t.Fatal(err)
	}
	assertCounts(t, got, before)
}

func copyCounts(m map[string]int) map[string]int {
	c := make(map[string]int, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func assertCounts(t *testing.T, got, want map[string]int) {
	t.Helper()
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("count[%s] = %d, want %d (got %v)", k, got[k], v, got)
		}
	}
	for k, v := range got {
		if want[k] != v {
			t.Fatalf("count[%s] = %d, want %d (got %v)", k, v, want[k], got)
		}
	}
}

// A non-"Bearer " scheme is counted once as invalid_token and never touches
// the per-IP failure budget.
func TestCheckAuth_BadSchemeRecordsInvalidToken(t *testing.T) {
	lim, _ := newTestLimiter()
	got := map[string]int{}
	lim.onFail = func(reason string) { got[reason]++ }
	st := newAuthTestStore(t)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic abc"))
	_, err := checkAuthLimited(ctx, st, true, lim)
	assertUnauthenticated(t, err)
	assertCounts(t, got, map[string]int{otel.AuthFailInvalidToken: 1})
}

// A well-formed (correct length) but unknown token is counted as invalid_token.
func TestCheckAuth_UnknownTokenRecordsInvalidToken(t *testing.T) {
	lim, _ := newTestLimiter()
	got := map[string]int{}
	lim.onFail = func(reason string) { got[reason]++ }
	st := newAuthTestStore(t)

	unknown := strings.Repeat("a", store.TokenLength)
	_, err := checkAuthLimited(authCtx("203.0.113.9", unknown), st, true, lim)
	assertUnauthenticated(t, err)
	assertCounts(t, got, map[string]int{otel.AuthFailInvalidToken: 1})
}

// A store error is Internal and is not the caller's failure: nothing counted.
func TestCheckAuth_StoreErrorRecordsNothing(t *testing.T) {
	lim, _ := newTestLimiter()
	got := map[string]int{}
	lim.onFail = func(reason string) { got[reason]++ }
	st := newAuthTestStore(t)

	// LookupNodeID returns ctx.Err() for a cancelled context, which
	// checkAuthLimited maps to Internal.
	ctx, cancel := context.WithCancel(authCtx("203.0.113.7", strings.Repeat("a", store.TokenLength)))
	cancel()
	_, err := checkAuthLimited(ctx, st, true, lim)
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal (err=%v)", status.Code(err), err)
	}
	assertCounts(t, got, map[string]int{})
}

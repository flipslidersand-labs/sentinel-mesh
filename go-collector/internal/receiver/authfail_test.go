package receiver

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func authCtx(ip, token string) context.Context {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	return peer.NewContext(ctx, &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 4000}})
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestLimiter() (*authFailLimiter, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	l := newAuthFailLimiter()
	l.now = clk.now
	return l, clk
}

func TestCheckAuth_RateLimitsRepeatedFailuresPerIP(t *testing.T) {
	lim, _ := newTestLimiter()
	st := newAuthTestStore(t)
	for i := 0; i < int(authFailBurst); i++ {
		_, err := checkAuthLimited(authCtx("203.0.113.1", "bad"), st, true, lim)
		assertUnauthenticated(t, err)
	}
	_, err := checkAuthLimited(authCtx("203.0.113.1", "bad"), st, true, lim)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted after burst, got %v", status.Code(err))
	}
	_, err = checkAuthLimited(authCtx("203.0.113.2", "bad"), st, true, lim)
	assertUnauthenticated(t, err)
}

func TestAuthFailLimiter_ConcurrentReserveIsAtomic(t *testing.T) {
	lim, _ := newTestLimiter()
	var passed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if lim.reserve("198.51.100.7") {
				passed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := passed.Load(); got != int64(authFailBurst) {
		t.Fatalf("expected exactly %d to pass, got %d", int(authFailBurst), got)
	}
}

func TestAuthFailLimiter_RecoversOverTime(t *testing.T) {
	lim, clk := newTestLimiter()
	for i := 0; i < int(authFailBurst); i++ {
		lim.reserve("198.51.100.7")
	}
	if lim.reserve("198.51.100.7") {
		t.Fatal("expected blocked")
	}
	clk.add(time.Minute / 5) // 5/min => 1 token per 12s
	if !lim.reserve("198.51.100.7") {
		t.Fatal("expected one attempt after refill")
	}
	if lim.reserve("198.51.100.7") {
		t.Fatal("expected blocked again")
	}
}

func TestCheckAuth_SuccessReleasesReservation(t *testing.T) {
	lim, _ := newTestLimiter()
	st := newAuthTestStore(t)
	token, err := st.IssueToken(context.Background(), "node-a")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < int(authFailBurst)*3; i++ {
		if _, err := checkAuthLimited(authCtx("203.0.113.9", token), st, true, lim); err != nil {
			t.Fatalf("valid attempt %d rejected: %v", i, err)
		}
	}
	if lim.order.Len() != 0 {
		t.Fatalf("expected table empty after successes, got %d", lim.order.Len())
	}
}

func TestAuthFailLimiter_IPv6SharesSlash64(t *testing.T) {
	lim, _ := newTestLimiter()
	for i := 0; i < int(authFailBurst); i++ {
		lim.reserve("2001:db8:1:2::1")
	}
	if lim.reserve("2001:db8:1:2:ffff::9") {
		t.Fatal("same /64 should share the budget")
	}
	if !lim.reserve("2001:db8:1:3::1") {
		t.Fatal("different /64 must be independent")
	}
}

func TestAuthFailLimiter_CapEvictsOldestAndKeepsTracking(t *testing.T) {
	lim, clk := newTestLimiter()
	lim.maxKeys = 3
	for _, ip := range []string{"192.0.2.1", "192.0.2.2", "192.0.2.3"} {
		for i := 0; i < int(authFailBurst); i++ {
			lim.reserve(ip)
		}
		clk.add(time.Second)
	}
	lim.reserve("192.0.2.4") // evicts 192.0.2.1 (least recently used)
	if lim.order.Len() != 3 {
		t.Fatalf("table size %d, want 3", lim.order.Len())
	}
	if _, ok := lim.entries["192.0.2.1"]; ok {
		t.Fatal("oldest entry should be evicted")
	}
	// New IP is tracked (not fail-open): it gets throttled after its burst.
	for i := 0; i < int(authFailBurst)-1; i++ {
		lim.reserve("192.0.2.4")
	}
	if lim.reserve("192.0.2.4") {
		t.Fatal("new IP must be throttled even when the table is full")
	}
}

type authFakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f authFakeStream) Context() context.Context { return f.ctx }

func TestStreamAuthInterceptor_RateLimits(t *testing.T) {
	lim, _ := newTestLimiter()
	st := newAuthTestStore(t)
	itc := streamAuthInterceptor(st, true, lim)
	handler := func(any, grpc.ServerStream) error { return nil }
	for i := 0; i < int(authFailBurst); i++ {
		err := itc(nil, authFakeStream{ctx: authCtx("203.0.113.5", "bad")}, &grpc.StreamServerInfo{}, handler)
		assertUnauthenticated(t, err)
	}
	err := itc(nil, authFakeStream{ctx: authCtx("203.0.113.5", "bad")}, &grpc.StreamServerInfo{}, handler)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", status.Code(err))
	}
}

// #232: once the source IP has exhausted its failure budget (e.g. one bad
// agent behind a shared proxy/LB IP), a valid token from the same IP must
// still authenticate — only failures are throttled.
func TestCheckAuth_ValidTokenPassesWhenIPExhausted(t *testing.T) {
	lim, _ := newTestLimiter()
	st := newAuthTestStore(t)
	token, err := st.IssueToken(context.Background(), "node-a")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	const ip = "198.51.100.7"
	for i := 0; i < int(authFailBurst); i++ {
		_, err := checkAuthLimited(authCtx(ip, "bad"), st, true, lim)
		assertUnauthenticated(t, err)
	}
	if _, err := checkAuthLimited(authCtx(ip, "bad"), st, true, lim); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted for bad token after burst, got %v", status.Code(err))
	}
	nodeID, err := checkAuthLimited(authCtx(ip, token), st, true, lim)
	if err != nil || nodeID != "node-a" {
		t.Fatalf("valid token from exhausted IP = (%q, %v), want (node-a, nil)", nodeID, err)
	}
	// Still throttled for failures afterwards: a success must not refund
	// the attacker's budget.
	if _, err := checkAuthLimited(authCtx(ip, "bad"), st, true, lim); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted after success, got %v", status.Code(err))
	}
}

// A Bearer value that can't be a valid token (wrong length) is rejected
// before hashing and still counts as a failure, so oversized values can't
// be used to burn CPU on SHA-256 without hitting the limiter.
func TestCheckAuth_WrongLengthTokenCountsAsFailure(t *testing.T) {
	lim, _ := newTestLimiter()
	st := newAuthTestStore(t)
	const ip = "198.51.100.9"
	huge := strings.Repeat("x", 1<<20)
	for i := 0; i < int(authFailBurst); i++ {
		_, err := checkAuthLimited(authCtx(ip, huge), st, true, lim)
		assertUnauthenticated(t, err)
	}
	if _, err := checkAuthLimited(authCtx(ip, huge), st, true, lim); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted after burst, got %v", status.Code(err))
	}
}

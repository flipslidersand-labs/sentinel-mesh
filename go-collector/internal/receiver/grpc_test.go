package receiver

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestCheckAuth_NoTokenConfigured(t *testing.T) {
	// Auth disabled: any context (even with no metadata) passes.
	if err := checkAuth(context.Background(), ""); err != nil {
		t.Fatalf("expected no-op pass, got %v", err)
	}
}

func TestCheckAuth_MissingMetadata(t *testing.T) {
	err := checkAuth(context.Background(), "secret")
	assertUnauthenticated(t, err)
}

func TestCheckAuth_MissingAuthorizationHeader(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
	err := checkAuth(ctx, "secret")
	assertUnauthenticated(t, err)
}

func TestCheckAuth_WrongScheme(t *testing.T) {
	md := metadata.Pairs("authorization", "Basic secret")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	err := checkAuth(ctx, "secret")
	assertUnauthenticated(t, err)
}

func TestCheckAuth_WrongToken(t *testing.T) {
	md := metadata.Pairs("authorization", "Bearer wrong")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	err := checkAuth(ctx, "secret")
	assertUnauthenticated(t, err)
}

func TestCheckAuth_ValidToken(t *testing.T) {
	md := metadata.Pairs("authorization", "Bearer secret")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	if err := checkAuth(ctx, "secret"); err != nil {
		t.Fatalf("expected valid token to pass, got %v", err)
	}
}

func TestTokenBucket_AllowsUpToBurstThenBlocks(t *testing.T) {
	b := newTokenBucket(1, 5) // 1/sec sustained, burst of 5
	for i := 0; i < 5; i++ {
		if !b.allow() {
			t.Fatalf("call %d within burst should be allowed", i)
		}
	}
	if b.allow() {
		t.Fatal("call beyond burst should be rejected")
	}
}

func TestTokenBucket_RefillsOverTime(t *testing.T) {
	b := newTokenBucket(1000, 1) // fast refill so the test doesn't sleep long
	if !b.allow() {
		t.Fatal("first call should be allowed")
	}
	if b.allow() {
		t.Fatal("immediate second call should be rejected (burst=1)")
	}
	time.Sleep(5 * time.Millisecond) // >> 1/1000/sec refill interval
	if !b.allow() {
		t.Fatal("call after refill window should be allowed")
	}
}

func assertUnauthenticated(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", status.Code(err))
	}
}

package receiver

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func authCtx(ip, token string) context.Context {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	return peer.NewContext(ctx, &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 4000}})
}

func TestCheckAuth_RateLimitsRepeatedFailuresPerIP(t *testing.T) {
	authFailures = newAuthFailLimiter()
	st := newAuthTestStore(t)

	for i := 0; i < int(authFailBurst); i++ {
		_, err := checkAuth(authCtx("203.0.113.1", "bad"), st, true)
		assertUnauthenticated(t, err)
	}
	_, err := checkAuth(authCtx("203.0.113.1", "bad"), st, true)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted after burst, got %v", status.Code(err))
	}
	// A different IP is unaffected.
	_, err = checkAuth(authCtx("203.0.113.2", "bad"), st, true)
	assertUnauthenticated(t, err)
}

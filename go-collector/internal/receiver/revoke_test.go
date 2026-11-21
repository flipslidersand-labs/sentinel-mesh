package receiver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/flipslidersand/sentinel-mesh/internal/registry"
)

// switchStore is a TokenResolver/Storage whose lookup result can be flipped
// at runtime (revoke / transient error).
type switchStore struct {
	fakeStorage
	mu2     sync.Mutex
	node    string
	revoked bool
	err     error
	calls   int
}

func (s *switchStore) LookupNodeID(context.Context, string) (string, bool, error) {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	s.calls++
	if s.err != nil {
		return "", false, s.err
	}
	return s.node, !s.revoked, nil
}

func (s *switchStore) set(revoked bool, err error) {
	s.mu2.Lock()
	s.revoked, s.err = revoked, err
	s.mu2.Unlock()
}

func newAuthedIdleStream(t *testing.T, node string) (*idleStream, context.CancelFunc) {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	md := metadata.Pairs("authorization", "Bearer "+hex.EncodeToString(b))
	ctx := metadata.NewIncomingContext(context.Background(), md)
	ctx = context.WithValue(ctx, authedNodeIDKey{}, node)
	ctx, cancel := context.WithCancel(ctx)
	return &idleStream{ctx: ctx}, cancel
}

func TestStreamEvents_RevokedTokenClosesStream(t *testing.T) {
	const timeout = 200 * time.Millisecond
	reg := registry.New()
	if err := reg.Register("node-1", "h", "10.0.0.1", "v", ""); err != nil {
		t.Fatal(err)
	}
	checkerCtx, stopChecker := context.WithCancel(context.Background())
	defer stopChecker()
	reg.StartHeartbeatChecker(checkerCtx, timeout, 20*time.Millisecond, time.Hour)

	st := &switchStore{node: "node-1"}
	s := &server{st: st, reg: reg, log: zap.NewNop(), livenessInterval: timeout / 4}
	stream, cancel := newAuthedIdleStream(t, "node-1")
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.StreamEvents(stream) }()

	time.Sleep(2 * timeout)
	select {
	case err := <-done:
		t.Fatalf("stream closed while token valid: %v", err)
	default:
	}
	if got := statusOf(reg, "node-1"); got != "active" {
		t.Fatalf("valid token: status = %q, want active", got)
	}

	st.set(true, nil)
	select {
	case err := <-done:
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("err = %v, want Unauthenticated", err)
		}
	case <-time.After(timeout):
		t.Fatal("stream not closed within threshold after revoke")
	}
	time.Sleep(3 * timeout)
	if got := statusOf(reg, "node-1"); got != "inactive" {
		t.Fatalf("after revoke: status = %q, want inactive", got)
	}
}

func TestStreamEvents_TransientLookupErrorKeepsStream(t *testing.T) {
	st := &switchStore{node: "node-1"}
	st.set(false, errors.New("busy"))
	s := &server{st: st, reg: registry.New(), log: zap.NewNop(), livenessInterval: 10 * time.Millisecond}
	stream, cancel := newAuthedIdleStream(t, "node-1")
	done := make(chan error, 1)
	go func() { done <- s.StreamEvents(stream) }()

	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("stream closed on transient lookup error: %v", err)
	default:
	}
	st.mu2.Lock()
	calls := st.calls
	st.mu2.Unlock()
	if calls == 0 {
		t.Fatal("token was never re-checked")
	}
	// Once the store recovers and the token is revoked, the stream closes.
	st.set(true, nil)
	select {
	case err := <-done:
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("err = %v, want Unauthenticated", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream not closed after revoke")
	}
	cancel()
}

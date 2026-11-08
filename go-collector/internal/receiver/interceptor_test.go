package receiver

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// spyCollector records what authedNodeIDFromContext returns inside the real
// handler context, i.e. after the auth interceptors ran on an actual RPC.
type spyCollector struct {
	pb.UnimplementedSentinelCollectorServer
	registerCalls chan string
	streamCalls   chan string
}

func (s *spyCollector) Register(ctx context.Context, _ *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	select {
	case s.registerCalls <- authedNodeIDFromContext(ctx):
	default: // never block a server goroutine if a test stops draining
	}
	return &pb.RegisterResponse{Ok: true}, nil
}

func (s *spyCollector) StreamEvents(stream grpc.BidiStreamingServer[pb.KernelEvent, pb.EventAck]) error {
	select {
	case s.streamCalls <- authedNodeIDFromContext(stream.Context()):
	default:
	}
	return nil
}

// newInterceptorClient serves a spyCollector behind both auth interceptors
// over bufconn and returns a client plus the spy.
func newInterceptorClient(t *testing.T, st *store.Store, authEnabled bool) (pb.SentinelCollectorClient, *spyCollector) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	lim := newAuthFailLimiter() // fresh per test: failures here never hit another test's budget
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryAuthInterceptor(st, authEnabled, lim)),
		grpc.ChainStreamInterceptor(streamAuthInterceptor(st, authEnabled, lim)),
	)
	spy := &spyCollector{registerCalls: make(chan string, 16), streamCalls: make(chan string, 16)}
	pb.RegisterSentinelCollectorServer(srv, spy)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewSentinelCollectorClient(conn), spy
}

func bearer(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

// openStream starts StreamEvents and waits for the RPC to finish, returning
// its terminal error (nil on clean server-side return).
func openStream(t *testing.T, c pb.SentinelCollectorClient, ctx context.Context) error {
	t.Helper()
	stream, err := c.StreamEvents(ctx)
	if err != nil {
		return err
	}
	_ = stream.CloseSend()
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func expectNoCall(t *testing.T, ch chan string, what string) {
	t.Helper()
	select {
	case id := <-ch:
		t.Fatalf("%s handler ran (node_id=%q) despite failed auth", what, id)
	default:
	}
}

func expectCall(t *testing.T, ch chan string, want, what string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("%s: authedNodeIDFromContext = %q, want %q", what, got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s handler was not called", what)
	}
}

// (1) Missing / malformed / unknown tokens are rejected before any handler runs.
func TestAuthInterceptors_RejectBadCredentialsWithoutCallingHandler(t *testing.T) {
	st := newAuthTestStore(t)
	if _, err := st.IssueToken(context.Background(), "node-a"); err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	c, spy := newInterceptorClient(t, st, true)

	// Right length but never issued. Over bufconn peerIP is the constant
	// "bufconn", so all cases share one limiter key. Only the "wrong length"
	// and "unknown token" cases count as failures (the others return before
	// authFailure): 2 cases x (Register+StreamEvents) = 4 < authFailBurst (5).
	// Adding a counted case here would trip ResourceExhausted.
	unknown := strings.Repeat("0", store.TokenLength)
	cases := map[string]func(context.Context) context.Context{
		"no token":      func(ctx context.Context) context.Context { return ctx },
		"wrong length":  func(ctx context.Context) context.Context { return bearer(ctx, "bad") },
		"unknown token": func(ctx context.Context) context.Context { return bearer(ctx, unknown) },
		"wrong scheme": func(ctx context.Context) context.Context {
			return metadata.AppendToOutgoingContext(ctx, "authorization", "Basic "+unknown)
		},
	}
	for name, mk := range cases {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		_, err := c.Register(mk(ctx), &pb.RegisterRequest{NodeId: "node-a"})
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s: Register code = %v, want Unauthenticated", name, status.Code(err))
		}
		err = openStream(t, c, mk(ctx))
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s: StreamEvents code = %v, want Unauthenticated", name, status.Code(err))
		}
	}
	expectNoCall(t, spy.registerCalls, "Register")
	expectNoCall(t, spy.streamCalls, "StreamEvents")
}

// (2) A valid token puts the node_id it was issued for into the handler
// context on both the unary and the streaming path, per token (no mix-up).
func TestAuthInterceptors_ValidTokenPropagatesIssuedNodeID(t *testing.T) {
	st := newAuthTestStore(t)
	tokA, err := st.IssueToken(context.Background(), "node-a")
	if err != nil {
		t.Fatalf("IssueToken a: %v", err)
	}
	tokB, err := st.IssueToken(context.Background(), "node-b")
	if err != nil {
		t.Fatalf("IssueToken b: %v", err)
	}
	c, spy := newInterceptorClient(t, st, true)

	for _, tc := range []struct{ token, node string }{{tokA, "node-a"}, {tokB, "node-b"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		// Body NodeId deliberately differs from the token's node: the handler
		// must see the token-derived identity, not the request body.
		if _, err := c.Register(bearer(ctx, tc.token), &pb.RegisterRequest{NodeId: "someone-else"}); err != nil {
			t.Fatalf("Register(%s): %v", tc.node, err)
		}
		expectCall(t, spy.registerCalls, tc.node, "Register")
		if err := openStream(t, c, bearer(ctx, tc.token)); err != nil {
			t.Fatalf("StreamEvents(%s): %v", tc.node, err)
		}
		expectCall(t, spy.streamCalls, tc.node, "StreamEvents")
	}
}

// (3) With auth disabled the interceptors pass everything through and no
// node_id is attached.
func TestAuthInterceptors_AuthDisabledPassesThroughWithoutNodeID(t *testing.T) {
	st := newAuthTestStore(t)
	c, spy := newInterceptorClient(t, st, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Register(ctx, &pb.RegisterRequest{NodeId: "node-a"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	expectCall(t, spy.registerCalls, "", "Register")
	if err := openStream(t, c, ctx); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	expectCall(t, spy.streamCalls, "", "StreamEvents")
}

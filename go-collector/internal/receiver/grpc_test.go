package receiver

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
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

func TestValidateRegisterRequest_ValidPasses(t *testing.T) {
	req := &pb.RegisterRequest{NodeId: "node-1", Hostname: "host-1", Ip: "10.0.0.1", Version: "v1.2.3", Region: "us-east"}
	if err := validateRegisterRequest(req); err != nil {
		t.Fatalf("expected valid request to pass, got %v", err)
	}
}

func TestValidateRegisterRequest_AllowsEmptyIP(t *testing.T) {
	req := &pb.RegisterRequest{NodeId: "node-1", Hostname: "host-1", Ip: "", Version: "v1", Region: "us-east"}
	if err := validateRegisterRequest(req); err != nil {
		t.Fatalf("expected empty ip to pass, got %v", err)
	}
}

func TestValidateRegisterRequest_RejectsOversizedField(t *testing.T) {
	req := &pb.RegisterRequest{NodeId: strings.Repeat("a", maxRegisterFieldLength+1), Hostname: "h", Ip: "", Version: "v1", Region: "us-east"}
	if err := validateRegisterRequest(req); err == nil {
		t.Fatal("expected oversized node_id to be rejected")
	}
}

func TestValidateRegisterRequest_RejectsControlCharacters(t *testing.T) {
	req := &pb.RegisterRequest{NodeId: "node-1\x00evil", Hostname: "h", Ip: "", Version: "v1", Region: "us-east"}
	if err := validateRegisterRequest(req); err == nil {
		t.Fatal("expected control characters in node_id to be rejected")
	}
}

func TestValidateRegisterRequest_RejectsNewlineInHostname(t *testing.T) {
	req := &pb.RegisterRequest{NodeId: "node-1", Hostname: "h\nInjected-Header: 1", Ip: "", Version: "v1", Region: "us-east"}
	if err := validateRegisterRequest(req); err == nil {
		t.Fatal("expected newline in hostname to be rejected")
	}
}

func TestValidateRegisterRequest_RejectsInvalidIP(t *testing.T) {
	req := &pb.RegisterRequest{NodeId: "node-1", Hostname: "h", Ip: "not-an-ip", Version: "v1", Region: "us-east"}
	if err := validateRegisterRequest(req); err == nil {
		t.Fatal("expected invalid ip to be rejected")
	}
}

func TestServerRegister_RejectsInvalidInput(t *testing.T) {
	s := &server{reg: registry.New(), log: zap.NewNop()}
	resp, err := s.Register(context.Background(), &pb.RegisterRequest{NodeId: "node-1\x00", Hostname: "h", Ip: "", Version: "v1", Region: ""})
	if err != nil {
		t.Fatalf("expected no transport error, got %v", err)
	}
	if resp.Ok {
		t.Fatal("expected Ok=false for invalid input")
	}
	if len(s.reg.List()) != 0 {
		t.Fatal("invalid registration must not be persisted to the registry")
	}
}

// fakeStream is a minimal pb.SentinelCollector_StreamEventsServer that feeds
// a fixed set of events to StreamEvents and records the EventAcks sent back,
// so tests can assert on Ack.Ok without a real gRPC connection.
type fakeStream struct {
	events []*pb.KernelEvent
	next   int
	acks   []*pb.EventAck
}

func (f *fakeStream) Recv() (*pb.KernelEvent, error) {
	if f.next >= len(f.events) {
		return nil, io.EOF
	}
	e := f.events[f.next]
	f.next++
	return e, nil
}

func (f *fakeStream) Send(ack *pb.EventAck) error {
	f.acks = append(f.acks, ack)
	return nil
}

func (f *fakeStream) Context() context.Context     { return context.Background() }
func (f *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeStream) SetTrailer(metadata.MD)       {}
func (f *fakeStream) SendMsg(m any) error          { return nil }
func (f *fakeStream) RecvMsg(m any) error          { return nil }

// TestStreamEvents_AckReflectsSaveFailure covers #103: when the store fails
// to persist an event, StreamEvents must no longer silently ack Ok: true —
// the failure must be surfaced to the caller (and counted).
func TestStreamEvents_AckReflectsSaveFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "badger")
	st, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	// Close the store immediately so any subsequent SaveEvent call fails —
	// simulating a persistent storage outage (disk full, corruption, ...).
	if err := st.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}

	s := &server{
		st:  st,
		reg: registry.New(),
		log: zap.NewNop(),
	}

	stream := &fakeStream{events: []*pb.KernelEvent{
		{NodeId: "node-1", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()},
	}}

	if err := s.StreamEvents(stream); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}

	if len(stream.acks) != 1 {
		t.Fatalf("expected 1 ack, got %d", len(stream.acks))
	}
	if stream.acks[0].Ok {
		t.Fatal("expected Ack.Ok=false when the store write fails, got true")
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

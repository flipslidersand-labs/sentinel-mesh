package receiver

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
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

func TestValidateKernelEvent_ValidExecPasses(t *testing.T) {
	e := &pb.KernelEvent{NodeId: "node-1", EventId: "evt-1", Type: pb.EventType_EXEC,
		Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Comm: "sh", Cmdline: "sh -c ls", Cwd: "/tmp"}}}
	if err := validateKernelEvent(e); err != nil {
		t.Fatalf("expected valid exec event to pass, got %v", err)
	}
}

func TestValidateKernelEvent_RejectsOversizedNodeId(t *testing.T) {
	e := &pb.KernelEvent{NodeId: strings.Repeat("a", maxRegisterFieldLength+1), EventId: "evt-1", Type: pb.EventType_EXEC}
	if err := validateKernelEvent(e); err == nil {
		t.Fatal("expected oversized node_id to be rejected")
	}
}

func TestValidateKernelEvent_RejectsControlCharInEventId(t *testing.T) {
	e := &pb.KernelEvent{NodeId: "node-1", EventId: "evt-1\x00", Type: pb.EventType_EXEC}
	if err := validateKernelEvent(e); err == nil {
		t.Fatal("expected control character in event_id to be rejected")
	}
}

func TestValidateKernelEvent_RejectsControlCharInExecCmdline(t *testing.T) {
	e := &pb.KernelEvent{NodeId: "node-1", EventId: "evt-1", Type: pb.EventType_EXEC,
		Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Comm: "sh", Cmdline: "sh -c \x00rm -rf /", Cwd: "/tmp"}}}
	if err := validateKernelEvent(e); err == nil {
		t.Fatal("expected control character in exec cmdline to be rejected")
	}
}

func TestValidateKernelEvent_RejectsInvalidPathInFileEvent(t *testing.T) {
	e := &pb.KernelEvent{NodeId: "node-1", EventId: "evt-1", Type: pb.EventType_FILE,
		Payload: &pb.KernelEvent_File{File: &pb.FileEvent{Comm: "sh", Path: "/etc/passwd\nInjected: 1", Op: "open"}}}
	if err := validateKernelEvent(e); err == nil {
		t.Fatal("expected newline in file path to be rejected")
	}
}

func TestValidateKernelEvent_RejectsInvalidSrcIpInTcpEvent(t *testing.T) {
	e := &pb.KernelEvent{NodeId: "node-1", EventId: "evt-1", Type: pb.EventType_TCP,
		Payload: &pb.KernelEvent_Tcp{Tcp: &pb.TcpEvent{Comm: "sh", SrcIp: "not-an-ip", DstIp: "10.0.0.1", Direction: "out"}}}
	if err := validateKernelEvent(e); err == nil {
		t.Fatal("expected invalid src_ip to be rejected")
	}
}

func TestValidateKernelEvent_ValidTcpPasses(t *testing.T) {
	e := &pb.KernelEvent{NodeId: "node-1", EventId: "evt-1", Type: pb.EventType_TCP,
		Payload: &pb.KernelEvent_Tcp{Tcp: &pb.TcpEvent{Comm: "sh", SrcIp: "10.0.0.1", DstIp: "10.0.0.2", Direction: "out"}}}
	if err := validateKernelEvent(e); err != nil {
		t.Fatalf("expected valid tcp event to pass, got %v", err)
	}
}

// TestStreamEvents_RejectsInvalidEventWithoutPersisting covers #154: an
// invalid node_id/event_id/payload string must be rejected with
// EventAck{Ok:false} before it is ever handed to the registry or the store.
func TestStreamEvents_RejectsInvalidEventWithoutPersisting(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "badger")
	st, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	reg := registry.New()
	s := &server{st: st, reg: reg, log: zap.NewNop()}

	stream := &fakeStream{events: []*pb.KernelEvent{
		{NodeId: "node-1\x00evil", EventId: "evt-1", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()},
	}}

	if err := s.StreamEvents(stream); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}

	if len(stream.acks) != 1 || stream.acks[0].Ok {
		t.Fatalf("expected a single Ack.Ok=false, got %+v", stream.acks)
	}
	if len(reg.List()) != 0 {
		t.Fatal("invalid node_id must not reach the registry (no heartbeat)")
	}
	events, err := st.ListEvents("node-1\x00evil", 10)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 0 {
		t.Fatal("invalid event must not be persisted to the store")
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

// TestStreamEvents_DetectorRunsWithoutEngine covers #150: the anomaly
// detector must record events (and raise alerts) even when no alerting
// engine is configured, instead of being silently skipped because it was
// nested inside `if s.engine != nil`.
func TestStreamEvents_DetectorRunsWithoutEngine(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "badger")
	st, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	// Threshold of 1 within a long window means the very first event of a
	// given (node, type) pair triggers an anomaly alert.
	detector := anomaly.New([]anomaly.WindowConfig{{Duration: time.Minute, Threshold: 1}})

	s := &server{
		st:       st,
		reg:      registry.New(),
		engine:   nil, // no alerting engine configured
		detector: detector,
		notifier: &notify.Dispatcher{},
		tracer:   otel.Tracer("test"),
		log:      zap.NewNop(),
	}

	// Two events within the window exceed Threshold:1, triggering an alert
	// on the second one.
	stream := &fakeStream{events: []*pb.KernelEvent{
		{NodeId: "node-1", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()},
		{NodeId: "node-1", Type: pb.EventType_EXEC, Timestamp: time.Now().UnixNano()},
	}}

	if err := s.StreamEvents(stream); err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}

	alerts, err := st.ListAlerts("node-1", 10)
	if err != nil {
		t.Fatalf("ListAlerts: %v", err)
	}
	if len(alerts) == 0 {
		t.Fatal("expected the detector to raise an anomaly alert even with a nil engine")
	}
}

// TestServe_StopsGracefullyWhenContextCancelled covers #117: Serve must
// stop (and return) when its ctx is cancelled, instead of blocking on
// srv.Serve forever — otherwise SIGINT/SIGTERM never lets the process exit.
func TestServe_StopsGracefullyWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- Serve(ctx, "127.0.0.1:0", nil, registry.New(), nil, nil, nil, nil, nil, "default", zap.NewNop(), "", "", "")
	}()

	// Give the server a moment to start listening before cancelling —
	// cancelling immediately would race the listener setup.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected graceful shutdown to return nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within 5s of context cancellation")
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

package receiver

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
)

type recvSizeSpy struct {
	pb.UnimplementedSentinelCollectorServer
}

func (recvSizeSpy) Register(context.Context, *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	return &pb.RegisterResponse{Ok: true}, nil
}

func (recvSizeSpy) StreamEvents(stream pb.SentinelCollector_StreamEventsServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		if err := stream.Send(&pb.EventAck{Ok: true}); err != nil {
			return err
		}
	}
}

// newRecvSizeClient serves a spy with the production limit options (#361).
func newRecvSizeClient(t *testing.T) pb.SentinelCollectorClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(limitServerOptions()...)
	pb.RegisterSentinelCollectorServer(srv, recvSizeSpy{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewSentinelCollectorClient(conn)
}

func maxStr() string { return strings.Repeat("a", maxRegisterFieldLength) }

// maxTCPEvent is the largest event validateKernelEvent accepts.
func maxTCPEvent() *pb.KernelEvent {
	ip := "2001:0db8:0000:0000:0000:0000:0000:0001"
	return &pb.KernelEvent{
		EventId: maxStr(), NodeId: maxStr(), Timestamp: -1 << 62, Type: pb.EventType_TCP,
		Payload: &pb.KernelEvent_Tcp{Tcp: &pb.TcpEvent{
			Pid: 1<<32 - 1, Comm: maxStr(), SrcIp: ip, SrcPort: 1<<32 - 1,
			DstIp: ip, DstPort: 1<<32 - 1, Direction: maxStr(),
		}},
	}
}

func TestMaxRecvMsgSizeCoversLargestValidMessages(t *testing.T) {
	ev := maxTCPEvent()
	if err := validateKernelEvent(ev); err != nil {
		t.Fatalf("test event must be valid: %v", err)
	}
	// Fill the IPs up to the field cap too (validateIP allows 256 bytes but
	// requires a parsable address, so the real maximum is far smaller; the
	// computation in grpc.go uses the 256 B upper bound).
	c := newRecvSizeClient(t)
	ctx := context.Background()

	stream, err := c.StreamEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(ev); err != nil {
		t.Fatalf("send max valid event: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("max valid event rejected: %v", err)
	}

	// Worst-case wire size with all 6 strings at 256 B must still fit.
	worst := maxTCPEvent()
	worst.Payload = &pb.KernelEvent_Tcp{Tcp: &pb.TcpEvent{
		Pid: 1<<32 - 1, Comm: maxStr(), SrcIp: maxStr(), SrcPort: 1<<32 - 1,
		DstIp: maxStr(), DstPort: 1<<32 - 1, Direction: maxStr(),
	}}
	if err := stream.Send(worst); err != nil {
		t.Fatalf("send worst-case event: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("worst-case event rejected: %v", err)
	}

	resp, err := c.Register(ctx, &pb.RegisterRequest{
		NodeId: maxStr(), Hostname: maxStr(), Ip: maxStr(), Version: maxStr(), Region: maxStr(),
	})
	if err != nil {
		t.Fatalf("max-size Register rejected by transport: %v", err)
	}
	if resp == nil {
		t.Fatal("nil response")
	}
}

func TestMaxRecvMsgSizeIsBounded(t *testing.T) {
	const ceiling = 64 * 1024 // #361: must stay far below the old 4MB
	if maxRecvMsgSize > ceiling {
		t.Fatalf("maxRecvMsgSize = %d, want <= %d", maxRecvMsgSize, ceiling)
	}
}

func TestMaxRecvMsgSizeRejectsOversized(t *testing.T) {
	c := newRecvSizeClient(t)
	ctx := context.Background()
	big := strings.Repeat("a", maxRecvMsgSize+1)

	t.Run("StreamEvents", func(t *testing.T) {
		stream, err := c.StreamEvents(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Send(&pb.KernelEvent{EventId: big, NodeId: "n", Type: pb.EventType_EXEC})
		_, err = stream.Recv()
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("code = %v (err=%v), want ResourceExhausted", got, err)
		}
	})
	t.Run("Register", func(t *testing.T) {
		_, err := c.Register(ctx, &pb.RegisterRequest{NodeId: "n", Hostname: big})
		if got := status.Code(err); got != codes.ResourceExhausted {
			t.Fatalf("code = %v (err=%v), want ResourceExhausted", got, err)
		}
	})
}

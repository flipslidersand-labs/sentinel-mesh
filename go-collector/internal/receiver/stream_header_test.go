package receiver

import (
	"context"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
)

// #231: the agent's tonic client awaits the response headers before it starts
// feeding events, so StreamEvents must send headers before the first Recv.
func TestStreamEvents_SendsHeaderBeforeAnyEvent(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterSentinelCollectorServer(srv, &server{log: zap.NewNop()})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := pb.NewSentinelCollectorClient(conn).StreamEvents(ctx)
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}

	// No event is sent; Header() must still return promptly.
	done := make(chan error, 1)
	go func() { _, err := stream.Header(); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Header: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for response header without sending any event")
	}
}

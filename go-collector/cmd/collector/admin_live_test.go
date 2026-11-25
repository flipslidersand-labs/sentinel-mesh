package main

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/flipslidersand/sentinel-mesh/internal/pb"
)

// freeAddr returns a loopback address that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// shortTempDir returns a short temp directory: the admin socket path must fit
// in sockaddr_un (108 bytes), which t.TempDir() can exceed.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// Revoking through the CLI while the real serve path (runNormal) holds the
// BadgerDB lock must succeed, and the already-open stream of the revoked
// token must be closed with Unauthenticated within heartbeat-timeout/3 (#572).
func TestRunNormal_LiveRevokeClosesStream(t *testing.T) {
	dataDir := shortTempDir(t)
	out, err := runTokenCmd(t, dataDir, "", "issue", "node-live")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	token := strings.TrimSpace(out)

	cmd := serveCmd()
	grpcAddr := freeAddr(t)
	for k, v := range map[string]string{
		"data-dir": dataDir, "grpc-addr": grpcAddr, "http-addr": freeAddr(t),
		"http-auth": "disabled", "grpc-auth": "required", "heartbeat-timeout": "600ms",
	} {
		if err := cmd.Flags().Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := loadServeConfig(cmd)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- runNormal(ctx, cfg, zap.NewNop()) }()
	defer func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(15 * time.Second):
			t.Error("runNormal did not stop")
		}
	}()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sctx, scancel := context.WithTimeout(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token), 30*time.Second)
	defer scancel()

	var stream pb.SentinelCollector_StreamEventsClient
	deadline := time.Now().Add(10 * time.Second)
	for {
		stream, err = pb.NewSentinelCollectorClient(conn).StreamEvents(sctx, grpc.WaitForReady(true))
		if err == nil {
			err = stream.Send(&pb.KernelEvent{NodeId: "node-live", Type: pb.EventType_EXEC,
				Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Comm: "sh"}}, Timestamp: time.Now().UnixNano()})
		}
		if err == nil {
			if _, err = stream.Recv(); err == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream never established: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if _, err := runTokenCmd(t, dataDir, "", "revoke", "--yes", "node-live"); err != nil {
		t.Fatalf("revoke against a running serve: %v", err)
	}

	recvErr := make(chan error, 1)
	go func() { _, err := stream.Recv(); recvErr <- err }()
	select {
	case err := <-recvErr:
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("stream err = %v, want Unauthenticated", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream not closed after revoke")
	}
}

// Without an admin socket, a locked data-dir error must say what still needs
// serve stopped (issue/list/revoke are delegated when the socket exists, #574).
func TestOpenTokenStore_LockedErrorMentionsForceOnly(t *testing.T) {
	dir := shortTempDir(t)
	if _, err := runTokenCmd(t, dir, "", "issue", "n1"); err != nil {
		t.Fatal(err)
	}
	st, err := openTokenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck
	_, err = runTokenCmd(t, dir, "", "list")
	if err == nil {
		t.Fatal("list against a locked data-dir must fail")
	}
	if !strings.Contains(err.Error(), "revoke --force") {
		t.Fatalf("error does not mention revoke --force: %v", err)
	}
}

// A token issued through the CLI while the real serve path (runNormal) holds
// the BadgerDB lock must authenticate a gRPC stream at once (#574): the
// delegated IssueToken updates the same in-memory index the receiver reads.
func TestRunNormal_LiveIssuedTokenAuthenticatesStream(t *testing.T) {
	dataDir := shortTempDir(t)
	cmd := serveCmd()
	grpcAddr := freeAddr(t)
	for k, v := range map[string]string{
		"data-dir": dataDir, "grpc-addr": grpcAddr, "http-addr": freeAddr(t),
		"http-auth": "disabled", "grpc-auth": "required",
	} {
		if err := cmd.Flags().Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := loadServeConfig(cmd)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- runNormal(ctx, cfg, zap.NewNop()) }()
	defer func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(15 * time.Second):
			t.Error("runNormal did not stop")
		}
	}()

	// Wait for the admin socket, then issue and list through the CLI.
	var out string
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, serr := os.Lstat(adminSocketPath(dataDir)); serr == nil {
			if out, err = runTokenCmd(t, dataDir, "", "issue", "node-issued-live"); err == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("issue against a running serve: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	token := strings.TrimSpace(out)
	if listOut, err := runTokenCmd(t, dataDir, "", "list"); err != nil || !strings.Contains(listOut, "node-issued-live") {
		t.Fatalf("list against a running serve: %q, %v", listOut, err)
	}

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sctx, scancel := context.WithTimeout(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token), 15*time.Second)
	defer scancel()
	stream, err := pb.NewSentinelCollectorClient(conn).StreamEvents(sctx, grpc.WaitForReady(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.KernelEvent{NodeId: "node-issued-live", Type: pb.EventType_EXEC,
		Payload: &pb.KernelEvent_Exec{Exec: &pb.ExecEvent{Comm: "sh"}}, Timestamp: time.Now().UnixNano()}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("stream with a live-issued token failed: %v", err)
	}
}

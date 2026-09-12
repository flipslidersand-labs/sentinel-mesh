package receiver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/flipslidersand/sentinel-mesh/internal/alerting"
	"github.com/flipslidersand/sentinel-mesh/internal/anomaly"
	"github.com/flipslidersand/sentinel-mesh/internal/notify"
	"github.com/flipslidersand/sentinel-mesh/internal/otel"
	"github.com/flipslidersand/sentinel-mesh/internal/pb"
	"github.com/flipslidersand/sentinel-mesh/internal/registry"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

const (
	// streamEventsRateLimit/streamEventsRateBurst bound how many
	// KernelEvents a single StreamEvents connection may push per second
	// (#75). Real agents (mock or eBPF) run at a few events/sec; this is
	// generous headroom while still capping a flooding agent.
	streamEventsRateLimit float64 = 2000
	streamEventsRateBurst float64 = 4000

	// maxConcurrentStreams/maxRecvMsgSize bound server-wide resource use
	// against a flood of connections or oversized messages (#75).
	maxConcurrentStreams uint32 = 256
	maxRecvMsgSize       int    = 4 * 1024 * 1024 // 4MB
)

// tokenBucket is a minimal, dependency-free rate limiter (events/sec with a
// burst allowance). Not safe for concurrent use — each StreamEvents stream
// is processed sequentially by its own goroutine, so no locking is needed.
type tokenBucket struct {
	tokens float64
	max    float64
	rate   float64
	last   time.Time
}

func newTokenBucket(ratePerSec, burst float64) *tokenBucket {
	return &tokenBucket{tokens: burst, max: burst, rate: ratePerSec, last: time.Now()}
}

func (b *tokenBucket) allow() bool {
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.max {
		b.tokens = b.max
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

type server struct {
	pb.UnimplementedSentinelCollectorServer
	st            *store.Store
	reg           *registry.Registry
	engine        *alerting.Engine
	detector      *anomaly.Detector
	notifier      *notify.Dispatcher
	metrics       *otel.MetricsProvider
	tracer        trace.Tracer
	defaultRegion string // used when an agent registers without a region
	log           *zap.Logger
}

// maxRegisterFieldLength bounds node_id/hostname/ip/version/region on
// Register so an unauthenticated client (gRPC often runs without a Bearer
// token configured) can't push oversized or control-character strings that
// get persisted to BadgerDB and rendered verbatim on the /api/nodes
// dashboard (#106).
const maxRegisterFieldLength = 256

// isPrintableASCII reports whether s contains only printable ASCII
// characters (0x20-0x7E) — no control characters, no newlines, no non-ASCII.
func isPrintableASCII(s string) bool {
	for _, r := range s {
		if r < 0x20 || r > 0x7e {
			return false
		}
	}
	return true
}

// validateRegisterRequest enforces a length cap and an ASCII-printable
// charset on node_id/hostname/version/region, and validates ip (when set)
// as a real IP address (#106).
func validateRegisterRequest(req *pb.RegisterRequest) error {
	fields := []struct{ name, value string }{
		{"node_id", req.NodeId},
		{"hostname", req.Hostname},
		{"version", req.Version},
		{"region", req.Region},
	}
	for _, f := range fields {
		if len(f.value) > maxRegisterFieldLength {
			return fmt.Errorf("%s exceeds max length of %d bytes", f.name, maxRegisterFieldLength)
		}
		if !isPrintableASCII(f.value) {
			return fmt.Errorf("%s must contain only printable ASCII characters", f.name)
		}
	}
	if req.Ip != "" {
		if len(req.Ip) > maxRegisterFieldLength {
			return fmt.Errorf("ip exceeds max length of %d bytes", maxRegisterFieldLength)
		}
		if net.ParseIP(req.Ip) == nil {
			return fmt.Errorf("ip is not a valid IP address")
		}
	}
	return nil
}

// Register handles agent registration (unary RPC).
func (s *server) Register(_ context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if err := validateRegisterRequest(req); err != nil {
		return &pb.RegisterResponse{Ok: false, Message: err.Error()}, nil
	}
	region := req.Region
	if region == "" {
		region = s.defaultRegion // fall back to the collector's default region
	}
	if err := s.reg.Register(req.NodeId, req.Hostname, req.Ip, req.Version, region); err != nil {
		return &pb.RegisterResponse{Ok: false, Message: err.Error()}, nil
	}
	s.log.Info("agent registered",
		zap.String("node_id", req.NodeId),
		zap.String("host", req.Hostname),
		zap.String("region", region))
	return &pb.RegisterResponse{Ok: true, Message: "registered"}, nil
}

// StreamEvents receives a bidirectional stream of KernelEvents.
func (s *server) StreamEvents(stream pb.SentinelCollector_StreamEventsServer) error {
	// Per-stream (i.e. per connected agent) event rate limit — without one, a
	// single flooding agent (malicious or misbehaving) can push events at an
	// unbounded rate, exhausting store/alert-eval/notify capacity for every
	// agent sharing the collector (#75). One tokenBucket per stream, used
	// only by this goroutine, needs no locking.
	limiter := newTokenBucket(streamEventsRateLimit, streamEventsRateBurst)
	var throttled uint64

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		s.reg.Heartbeat(event.NodeId)

		if !limiter.allow() {
			throttled++
			if throttled == 1 || throttled%1000 == 0 {
				s.log.Warn("StreamEvents rate limit exceeded, dropping event",
					zap.String("node_id", event.NodeId),
					zap.Uint64("throttled_total", throttled))
			}
			if err := stream.Send(&pb.EventAck{Ok: false}); err != nil {
				return err
			}
			continue
		}

		storedEvent := store.Event{
			EventID:   event.EventId,
			NodeID:    event.NodeId,
			Timestamp: time.Unix(0, event.Timestamp).UTC(),
			Type:      s.eventTypeName(event),
			Payload:   s.eventPayload(event),
		}

		saveOk := true
		if storeErr := s.st.SaveEvent(storedEvent); storeErr != nil {
			s.log.Error("save event", zap.Error(storeErr))
			saveOk = false
			if s.metrics != nil {
				s.metrics.RecordStoreWriteFailure("event")
			}
		}

		// Phase 6: record event metric
		if s.metrics != nil {
			s.metrics.RecordEvent(storedEvent.Type, storedEvent.NodeID)
		}

		// Phase 5: evaluate alerts
		if s.engine != nil {
			_, span := s.tracer.Start(stream.Context(), "evaluate_alerts",
				trace.WithAttributes(
					attribute.String("event.id", storedEvent.EventID),
					attribute.String("event.type", storedEvent.Type),
					attribute.String("node.id", storedEvent.NodeID),
				),
			)
			allAlerts := s.engine.Evaluate(storedEvent)

			// Phase 7: frequency-based anomaly detection
			if s.detector != nil {
				allAlerts = append(allAlerts, s.detector.Record(storedEvent)...)
			}

			for _, alert := range allAlerts {
				if err := s.st.SaveAlert(alert); err != nil {
					s.log.Error("save alert", zap.Error(err))
					saveOk = false
					if s.metrics != nil {
						s.metrics.RecordStoreWriteFailure("alert")
					}
				}

				// Phase 6: record alert metric and trace
				if s.metrics != nil {
					s.metrics.RecordAlert(alert.RuleID, alert.Severity, alert.NodeID)
				}
				span.AddEvent("alert_triggered", trace.WithAttributes(
					attribute.String("rule.id", alert.RuleID),
					attribute.String("severity", alert.Severity),
					attribute.String("message", alert.Message),
				))

				// Notify external channels off the hot path (best-effort).
				// DispatchAsync is bounded — a flood of alerts can no longer
				// spawn an unbounded number of goroutines (#75).
				s.notifier.DispatchAsync(alert)
			}

			// End the span at the end of this iteration, not the whole
			// StreamEvents loop — a `defer` here would accumulate one span
			// per event for the life of the (long-lived) stream (#74).
			span.End()
		}

		if err := stream.Send(&pb.EventAck{Ok: saveOk}); err != nil {
			return err
		}
	}
}

func (s *server) eventTypeName(e *pb.KernelEvent) string {
	switch e.Type {
	case pb.EventType_EXEC:
		return "exec"
	case pb.EventType_TCP:
		return "tcp"
	case pb.EventType_FILE:
		return "file"
	default:
		return fmt.Sprintf("unknown(%d)", e.Type)
	}
}

func (s *server) eventPayload(e *pb.KernelEvent) json.RawMessage {
	var payload any

	switch e.Type {
	case pb.EventType_EXEC:
		if ex := e.GetExec(); ex != nil {
			payload = ex
		}
	case pb.EventType_TCP:
		if t := e.GetTcp(); t != nil {
			payload = t
		}
	case pb.EventType_FILE:
		if f := e.GetFile(); f != nil {
			payload = f
		}
	}

	raw, _ := json.Marshal(payload)
	return raw
}

// Serve starts the gRPC server on addr. If tlsCertFile/tlsKeyFile are both
// set, the server requires TLS; otherwise it serves in plaintext (caller is
// expected to warn). If token is non-empty, every RPC must present a matching
// `authorization: Bearer <token>` metadata entry.
//
// Serve blocks until either the server stops on its own (e.g. a listener
// error) or ctx is done, in which case it gracefully stops the server
// (waiting for in-flight RPCs to finish) before returning.
func Serve(ctx context.Context, addr string, st *store.Store, reg *registry.Registry, engine *alerting.Engine,
	detector *anomaly.Detector, notifier *notify.Dispatcher, metrics *otel.MetricsProvider,
	tracer trace.Tracer, defaultRegion string, log *zap.Logger,
	tlsCertFile, tlsKeyFile, token string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	var opts []grpc.ServerOption
	if tlsCertFile != "" && tlsKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(tlsCertFile, tlsKeyFile)
		if err != nil {
			return fmt.Errorf("load TLS cert/key: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	opts = append(opts,
		grpc.ChainUnaryInterceptor(unaryAuthInterceptor(token)),
		grpc.ChainStreamInterceptor(streamAuthInterceptor(token)),
		// A single agent could otherwise open unbounded concurrent
		// StreamEvents connections or send oversized messages, exhausting
		// collector resources (#75).
		grpc.MaxConcurrentStreams(maxConcurrentStreams),
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	srv := grpc.NewServer(opts...)

	s := &server{st: st, reg: reg, engine: engine, detector: detector, notifier: notifier, metrics: metrics, tracer: tracer, defaultRegion: defaultRegion, log: log}
	pb.RegisterSentinelCollectorServer(srv, s)

	// Stop the server when ctx is cancelled (e.g. SIGINT/SIGTERM) so the
	// process can shut down gracefully instead of blocking forever on
	// srv.Serve (#117). GracefulStop waits for pending RPCs to finish;
	// srv.Serve returns grpc.ErrServerStopped once it does, which is not
	// a real failure and is swallowed below.
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			log.Info("gRPC server shutting down")
			srv.GracefulStop()
		case <-stopped:
		}
	}()

	log.Info("gRPC server listening", zap.String("addr", addr))
	err = srv.Serve(lis)
	close(stopped)
	if err != nil && errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// checkAuth validates the `authorization: Bearer <token>` metadata entry in
// ctx against the expected token. A no-op (always ok) if token is empty.
func checkAuth(ctx context.Context, token string) error {
	if token == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	got, ok := strings.CutPrefix(values[0], "Bearer ")
	if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid token")
	}
	return nil
}

func unaryAuthInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := checkAuth(ctx, token); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func streamAuthInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkAuth(ss.Context(), token); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

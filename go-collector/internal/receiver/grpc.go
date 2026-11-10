package receiver

import (
	"context"
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
	"google.golang.org/grpc/peer"
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

	// grpcShutdownGracePeriod bounds how long Serve's shutdown goroutine
	// waits for srv.GracefulStop() before falling back to the forceful
	// srv.Stop(). GracefulStop only waits for in-flight *RPCs*, not for a
	// live StreamEvents connection whose agent has gone quiet without
	// disconnecting — without this fallback, one such connection would hang
	// the whole graceful shutdown forever (#155). Matches
	// cmd/collector/main.go's httpShutdownTimeout for the REST API.
	grpcShutdownGracePeriod = 10 * time.Second

	// storeOpTimeout bounds how long a single StreamEvents iteration may
	// wait on a store write. BadgerDB value-log GC/compaction/disk I/O can
	// stall db.Update for an unbounded time; without a cap, that stall
	// blocks the goroutine handling this stream — and, during shutdown,
	// GracefulStop along with it (#155).
	storeOpTimeout = 5 * time.Second
)

// tokenBucket is a minimal, dependency-free rate limiter (events/sec with a
// burst allowance). Not safe for concurrent use — each StreamEvents stream
// is processed sequentially by its own goroutine, so no locking is needed.
type tokenBucket struct {
	tokens float64
	max    float64
	rate   float64
	last   time.Time
	now    func() time.Time // clock; nil means time.Now (tests inject a manual clock)
}

func newTokenBucket(ratePerSec, burst float64) *tokenBucket {
	return newTokenBucketWithClock(ratePerSec, burst, nil)
}

func newTokenBucketWithClock(ratePerSec, burst float64, now func() time.Time) *tokenBucket {
	b := &tokenBucket{tokens: burst, max: burst, rate: ratePerSec, now: now}
	b.last = b.clock()
	return b
}

func (b *tokenBucket) clock() time.Time {
	if b.now != nil {
		return b.now()
	}
	return time.Now()
}

func (b *tokenBucket) allow() bool {
	now := b.clock()
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
	authFail      *authFailLimiter // per-source auth throttle (shared with the interceptors)
	nodeLimiters  nodeLimiters     // per-authenticated-node event rate limit shared across streams (#254)
	defaultRegion string           // used when an agent registers without a region
	log           *zap.Logger
}

// maxRegisterFieldLength bounds free-form string fields accepted from
// agents over gRPC (which often runs without a Bearer token configured), so
// an unauthenticated or compromised agent can't push oversized or
// control-character strings that get persisted to BadgerDB and rendered
// verbatim on the dashboard (#106, #154). It is shared by Register and
// StreamEvents validation.
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

// namedField pairs a field name (used in error messages) with its value, so
// validateFields can report which field failed.
type namedField struct{ name, value string }

// validateFields enforces a length cap and an ASCII-printable charset on
// each field in order, returning the first violation found. It is the
// common validation core for both Register and StreamEvents (#106, #154).
func validateFields(fields []namedField) error {
	for _, f := range fields {
		if len(f.value) > maxRegisterFieldLength {
			return fmt.Errorf("%s exceeds max length of %d bytes", f.name, maxRegisterFieldLength)
		}
		if !isPrintableASCII(f.value) {
			return fmt.Errorf("%s must contain only printable ASCII characters", f.name)
		}
	}
	return nil
}

// validateIP enforces a length cap and requires value (when non-empty) to
// parse as a real IP address.
func validateIP(name, value string) error {
	if value == "" {
		return nil
	}
	if len(value) > maxRegisterFieldLength {
		return fmt.Errorf("%s exceeds max length of %d bytes", name, maxRegisterFieldLength)
	}
	if net.ParseIP(value) == nil {
		return fmt.Errorf("%s is not a valid IP address", name)
	}
	return nil
}

// validateRegisterRequest enforces a length cap and an ASCII-printable
// charset on node_id/hostname/version/region, and validates ip (when set)
// as a real IP address (#106).
func validateRegisterRequest(req *pb.RegisterRequest) error {
	if err := validateFields([]namedField{
		{"node_id", req.NodeId},
		{"hostname", req.Hostname},
		{"version", req.Version},
		{"region", req.Region},
	}); err != nil {
		return err
	}
	return validateIP("ip", req.Ip)
}

// validateKernelEvent applies the same length-cap/ASCII-printable
// validation Register uses to StreamEvents' NodeId/EventId and the
// free-form string fields of the event's payload (Exec/Tcp/File), so an
// unauthenticated or compromised agent can't smuggle control characters,
// NULs, or oversized strings into BadgerDB and the /api/events and
// /api/alerts dashboards (#154).
func validateKernelEvent(e *pb.KernelEvent) error {
	fields := []namedField{
		{"node_id", e.NodeId},
		{"event_id", e.EventId},
	}
	switch e.Type {
	case pb.EventType_EXEC:
		if ex := e.GetExec(); ex != nil {
			fields = append(fields,
				namedField{"comm", ex.Comm},
				namedField{"cmdline", ex.Cmdline},
				namedField{"cwd", ex.Cwd},
			)
		}
	case pb.EventType_TCP:
		if t := e.GetTcp(); t != nil {
			fields = append(fields,
				namedField{"comm", t.Comm},
				namedField{"direction", t.Direction},
			)
			if err := validateFields(fields); err != nil {
				return err
			}
			if err := validateIP("src_ip", t.SrcIp); err != nil {
				return err
			}
			return validateIP("dst_ip", t.DstIp)
		}
	case pb.EventType_FILE:
		if f := e.GetFile(); f != nil {
			fields = append(fields,
				namedField{"comm", f.Comm},
				namedField{"path", f.Path},
				namedField{"op", f.Op},
			)
		}
	default:
		return fmt.Errorf("unknown event type %d", int32(e.Type))
	}
	return validateFields(fields)
}

// peerIP extracts the caller's address from the gRPC peer connection and
// strips the port. Register derives the agent's IP this way rather than
// trusting req.Ip (#175): the real agent never populates that field (it's
// always sent empty), and even if it did, a client-supplied IP could be
// spoofed by anything not going through mTLS client-cert verification.
func peerIP(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String()
	}
	return host
}

// Register handles agent registration (unary RPC).
func (s *server) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if err := validateRegisterRequest(req); err != nil {
		return &pb.RegisterResponse{Ok: false, Message: err.Error()}, nil
	}
	// Same reasoning as StreamEvents below: when auth is enabled, an agent
	// must not be able to register under a node_id other than the one its
	// token resolved to (#183/#190, per ADR-005).
	if authedNodeID := authedNodeIDFromContext(ctx); authedNodeID != "" && req.NodeId != authedNodeID {
		return &pb.RegisterResponse{Ok: false, Message: "node_id does not match authenticated identity"}, nil
	}
	region := req.Region
	if region == "" {
		region = s.defaultRegion // fall back to the collector's default region
	}
	if err := s.reg.Register(req.NodeId, req.Hostname, peerIP(ctx), req.Version, region); err != nil {
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
	// Event rate limit (#75). Keyed by the authenticated node_id so that every
	// stream/connection of one token shares a single bucket: the gRPC
	// MaxConcurrentStreams cap is per connection, so a per-stream bucket let
	// one token multiply its rate by opening more connections/streams (#254).
	// With auth disabled there is no trusted identity, so fall back to a
	// per-stream bucket (never key on the client-supplied event.NodeId or IP).
	authedNodeID := authedNodeIDFromContext(stream.Context())
	var limiter interface{ allow() bool }
	if authedNodeID != "" {
		b, release := s.nodeLimiters.acquire(authedNodeID)
		defer release()
		limiter = b
	} else {
		limiter = newTokenBucket(streamEventsRateLimit, streamEventsRateBurst)
	}
	var throttled uint64

	// Flush response headers before the first Recv (auth already passed in
	// the interceptor). The agent's client awaits headers before it starts
	// feeding events; without this, no header goes out until the first Ack,
	// which needs the first event: a deadlock until RPC_TIMEOUT (#231).
	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}

	// authedNodeID (resolved above) comes from the auth interceptor
	// (#183/#190, per ADR-005) — "" when auth is disabled; constant for the
	// lifetime of this stream.

	for {
		// Bail out promptly once the stream's context is done (e.g. the
		// server is shutting down and srv.GracefulStop()/srv.Stop() has
		// cancelled it). Without this check, an agent that neither sends
		// nor disconnects leaves stream.Recv() below blocked forever, and
		// GracefulStop waits for this RPC indefinitely (#155).
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}

		event, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// Apply the rate limit before validation, not after: validation
		// failures must consume the same per-stream token budget as valid
		// events, otherwise a stream of trivially-invalid messages bypasses
		// the limiter entirely and reintroduces the unbounded per-agent
		// resource consumption #75 closed (#170) — just via the validation
		// path (protobuf deserialize + field checks + Ack) instead of the
		// storage path.
		if !limiter.allow() {
			throttled++
			if err := rejectEvent(stream, func() {
				if throttled == 1 || throttled%1000 == 0 {
					s.log.Warn("StreamEvents rate limit exceeded, dropping event",
						zap.String("node_id", event.NodeId),
						zap.Uint64("throttled_total", throttled))
				}
			}); err != nil {
				return err
			}
			continue
		}

		// Reject invalid node_id/event_id/payload strings before they ever
		// reach the registry or the store — an unauthenticated/compromised
		// agent must not be able to persist or heartbeat under a
		// control-character or oversized identifier (#154). The rejected
		// value itself is never logged, since it is by definition unvalidated.
		if err := validateKernelEvent(event); err != nil {
			if sendErr := rejectEvent(stream, func() {
				s.log.Warn("StreamEvents rejected invalid event", zap.Error(err))
			}); sendErr != nil {
				return sendErr
			}
			continue
		}

		// When auth is enabled, trust the node_id the token store resolved
		// at connection time, not the client-supplied field on each event —
		// same reasoning as deriving agent IP from the gRPC peer instead of
		// a client-supplied field (#178). An agent authenticated as one
		// node_id must not be able to write/heartbeat as another.
		if authedNodeID != "" && event.NodeId != authedNodeID {
			if sendErr := rejectEvent(stream, func() {
				s.log.Warn("StreamEvents event node_id does not match authenticated identity",
					zap.String("authed_node_id", authedNodeID))
			}); sendErr != nil {
				return sendErr
			}
			continue
		}

		s.reg.Heartbeat(event.NodeId)

		storedEvent := store.Event{
			EventID:   event.EventId,
			NodeID:    event.NodeId,
			Timestamp: time.Unix(0, event.Timestamp).UTC(),
			Type:      s.eventTypeName(event),
			Payload:   s.eventPayload(event),
		}

		saveOk := s.persistEvent(stream.Context(), storedEvent)
		if _, alertsOk := s.evaluateAndNotify(stream.Context(), storedEvent); !alertsOk {
			saveOk = false
		}

		if err := stream.Send(&pb.EventAck{Ok: saveOk}); err != nil {
			return err
		}
	}
}

// rejectEvent runs logFn (the caller's rejection log) and then sends a
// failure Ack (Ok=false). A non-nil return is the Send error; the caller must
// end the stream with it, otherwise continue with the next event.
func rejectEvent(stream pb.SentinelCollector_StreamEventsServer, logFn func()) error {
	if logFn != nil {
		logFn()
	}
	return stream.Send(&pb.EventAck{Ok: false})
}

// persistEvent saves the event (bounded by storeOpTimeout) and records the
// store-failure and event metrics. It reports whether the save succeeded.
func (s *server) persistEvent(ctx context.Context, ev store.Event) (ok bool) {
	ok = true
	saveCtx, saveCancel := context.WithTimeout(ctx, storeOpTimeout)
	storeErr := s.st.SaveEvent(saveCtx, ev)
	saveCancel()
	if storeErr != nil {
		s.log.Error("save event", zap.Error(storeErr))
		ok = false
		if s.metrics != nil {
			s.metrics.RecordStoreWriteFailure("event")
		}
	}

	// Phase 6: record event metric
	if s.metrics != nil {
		s.metrics.RecordEvent(ev.Type, ev.NodeID)
	}
	return ok
}

// evaluateAndNotify runs the alert engine and anomaly detector on ev, saves,
// records and traces each resulting alert and dispatches it to notifiers.
// saveOk is false if any alert failed to save. engine and detector run
// independently so a nil engine never disables the detector or vice versa
// (#150).
func (s *server) evaluateAndNotify(ctx context.Context, ev store.Event) (alerts []alerting.Alert, saveOk bool) {
	saveOk = true
	if s.engine == nil && s.detector == nil {
		return nil, saveOk
	}
	_, span := s.tracer.Start(ctx, "evaluate_alerts",
		trace.WithAttributes(
			attribute.String("event.id", ev.EventID),
			attribute.String("event.type", ev.Type),
			attribute.String("node.id", ev.NodeID),
		),
	)
	// End the span per call, not per stream (#74).
	defer span.End()

	if s.engine != nil {
		alerts = append(alerts, s.engine.Evaluate(ev)...)
	}
	// Phase 7: frequency-based anomaly detection
	if s.detector != nil {
		alerts = append(alerts, s.detector.Record(ev)...)
	}

	for _, alert := range alerts {
		alertCtx, alertCancel := context.WithTimeout(ctx, storeOpTimeout)
		alertErr := s.st.SaveAlert(alertCtx, alert)
		alertCancel()
		if alertErr != nil {
			s.log.Error("save alert", zap.Error(alertErr))
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
		// DispatchAsync is bounded (#75).
		s.notifier.DispatchAsync(alert)
	}
	return alerts, saveOk
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

// The generated pb structs tag every field `omitempty`, so json.Marshal drops
// numeric zeros (uid=0, ports, ret=0) and `uid eq 0` rules never fire (#249).
// These mirror them with identical keys/order/types but no omitempty on
// numbers. String fields keep omitempty to preserve the existing payload.
type execPayload struct {
	Pid     uint32 `json:"pid"`
	Ppid    uint32 `json:"ppid"`
	Comm    string `json:"comm,omitempty"`
	Cmdline string `json:"cmdline,omitempty"`
	Cwd     string `json:"cwd,omitempty"`
	Uid     uint32 `json:"uid"`
}

type tcpPayload struct {
	Pid       uint32 `json:"pid"`
	Comm      string `json:"comm,omitempty"`
	SrcIp     string `json:"src_ip,omitempty"`
	SrcPort   uint32 `json:"src_port"`
	DstIp     string `json:"dst_ip,omitempty"`
	DstPort   uint32 `json:"dst_port"`
	Direction string `json:"direction,omitempty"`
}

type filePayload struct {
	Pid  uint32 `json:"pid"`
	Comm string `json:"comm,omitempty"`
	Path string `json:"path,omitempty"`
	Op   string `json:"op,omitempty"`
	Ret  int32  `json:"ret"`
}

func (s *server) eventPayload(e *pb.KernelEvent) json.RawMessage {
	var payload any

	switch e.Type {
	case pb.EventType_EXEC:
		if ex := e.GetExec(); ex != nil {
			payload = execPayload{ex.Pid, ex.Ppid, ex.Comm, ex.Cmdline, ex.Cwd, ex.Uid}
		}
	case pb.EventType_TCP:
		if t := e.GetTcp(); t != nil {
			payload = tcpPayload{t.Pid, t.Comm, t.SrcIp, t.SrcPort, t.DstIp, t.DstPort, t.Direction}
		}
	case pb.EventType_FILE:
		if f := e.GetFile(); f != nil {
			payload = filePayload{f.Pid, f.Comm, f.Path, f.Op, f.Ret}
		}
	}

	raw, _ := json.Marshal(payload)
	return raw
}

// gracefulStopWithTimeout calls srv.GracefulStop() and waits up to
// gracePeriod for it to return. GracefulStop only returns once every
// in-flight RPC has ended, and a StreamEvents connection whose agent has
// gone quiet without disconnecting never ends on its own — so without this
// bound, GracefulStop (and therefore the whole graceful shutdown sequence)
// can hang forever. If gracePeriod elapses first, it falls back to
// srv.Stop(), which forcibly cancels every stream's context and closes the
// underlying connections (#155).
func gracefulStopWithTimeout(srv *grpc.Server, gracePeriod time.Duration, log *zap.Logger) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(gracePeriod):
		log.Warn("gRPC graceful stop timed out, forcing stop")
		srv.Stop()
	}
}

// Serve starts the gRPC server on addr. If tlsCertFile/tlsKeyFile are both
// set, the server requires TLS; otherwise it serves in plaintext (caller is
// expected to warn). If authEnabled, every RPC must present an
// `authorization: Bearer <token>` metadata entry naming a token issued via
// st.IssueToken (#189) — unlike the REST API's single shared
// SENTINEL_API_TOKEN, each agent authenticates with its own per-agent
// token (#183, per ADR-005).
//
// Serve blocks until either the server stops on its own (e.g. a listener
// error) or ctx is done, in which case it gracefully stops the server
// (waiting for in-flight RPCs to finish) before returning.
func Serve(ctx context.Context, addr string, st *store.Store, reg *registry.Registry, engine *alerting.Engine,
	detector *anomaly.Detector, notifier *notify.Dispatcher, metrics *otel.MetricsProvider,
	tracer trace.Tracer, defaultRegion string, log *zap.Logger,
	tlsCertFile, tlsKeyFile string, authEnabled bool) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	authLim := newAuthFailLimiter()
	if metrics != nil {
		authLim.onFail = metrics.RecordAuthFailure
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
		grpc.ChainUnaryInterceptor(unaryAuthInterceptor(st, authEnabled, authLim)),
		grpc.ChainStreamInterceptor(streamAuthInterceptor(st, authEnabled, authLim)),
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

	s := &server{st: st, reg: reg, engine: engine, detector: detector, notifier: notifier, metrics: metrics, tracer: tracer, defaultRegion: defaultRegion, log: log, authFail: authLim}
	pb.RegisterSentinelCollectorServer(srv, s)

	// Stop the server when ctx is cancelled (e.g. SIGINT/SIGTERM) so the
	// process can shut down gracefully instead of blocking forever on
	// srv.Serve (#117). GracefulStop waits for pending RPCs to finish;
	// srv.Serve returns grpc.ErrServerStopped once it does, which is not
	// a real failure and is swallowed below.
	//
	// GracefulStop alone can block indefinitely: it only returns once every
	// RPC has ended, and a StreamEvents connection whose agent has gone
	// quiet without disconnecting never ends on its own. So GracefulStop is
	// bounded by grpcShutdownGracePeriod; if it hasn't finished by then, we
	// fall back to srv.Stop(), which forcibly cancels every stream's context
	// and closes the underlying connections (#155). This mirrors
	// cmd/collector/main.go's http.Server.Shutdown timeout for the REST API.
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			log.Info("gRPC server shutting down")
			gracefulStopWithTimeout(srv, grpcShutdownGracePeriod, log)
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

// authedNodeIDKey is the context key StreamEvents/Register read the
// caller's authenticated node_id from (#183/#190, per ADR-005) — set by
// checkAuth via the interceptors below, never by a handler itself.
type authedNodeIDKey struct{}

// authedNodeIDFromContext returns the node_id checkAuth resolved for this
// RPC/stream, or "" if auth is disabled (authEnabled=false) or the identity
// couldn't be determined for some other reason. Callers must not treat ""
// as "authenticated as the empty node_id".
func authedNodeIDFromContext(ctx context.Context) string {
	nodeID, _ := ctx.Value(authedNodeIDKey{}).(string)
	return nodeID
}

// checkAuth validates the `authorization: Bearer <token>` metadata entry in
// ctx against st's per-agent token store (#189) and returns the resolved
// node_id. A no-op (nodeID="", err=nil) if authEnabled is false — this is
// the same "unauthenticated dev mode" escape hatch the old single shared
// token had, just gated by a separate flag now that there's no longer one
// token whose emptiness could signal it.
func checkAuth(ctx context.Context, st *store.Store, authEnabled bool) (nodeID string, err error) {
	return checkAuthLimited(ctx, st, authEnabled, nil)
}

// checkAuthLimited is checkAuth plus per-source-IP throttling via lim (nil
// disables throttling of failed attempts; valid tokens are never throttled).
func checkAuthLimited(ctx context.Context, st *store.Store, authEnabled bool, lim *authFailLimiter) (nodeID string, err error) {
	if !authEnabled {
		return "", nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		lim.recordFail(otel.AuthFailMissingCredentials)
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		lim.recordFail(otel.AuthFailMissingCredentials)
		return "", status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	got, ok := strings.CutPrefix(values[0], "Bearer ")
	if !ok {
		lim.recordFail(otel.AuthFailInvalidToken)
		return "", status.Error(codes.Unauthenticated, "invalid token")
	}
	// A value that can't be a token is a failure, counted before any
	// hashing so oversized Bearer values can't burn CPU unthrottled.
	if len(got) != store.TokenLength {
		return "", authFailure(ctx, lim)
	}
	// The lookup is an O(1) index hit (#232), so it runs first: a valid
	// token always succeeds regardless of the source IP's failure budget —
	// one misbehaving client behind a shared proxy/LB IP can't lock out the
	// rest. Only failed attempts are counted against the source.
	nodeID, ok, err = st.LookupNodeID(ctx, got)
	if err != nil {
		return "", status.Error(codes.Internal, "auth lookup failed") // store error is not the caller's failure
	}
	if ok {
		return nodeID, nil
	}
	return "", authFailure(ctx, lim)
}

// authFailure counts a failed attempt against the caller's source and
// returns the matching status: ResourceExhausted once its budget is spent,
// Unauthenticated otherwise.
func authFailure(ctx context.Context, lim *authFailLimiter) error {
	if ip := peerIP(ctx); ip != "" && !lim.reserve(ip) {
		lim.recordFail(otel.AuthFailRateLimited)
		return status.Error(codes.ResourceExhausted, "too many authentication failures")
	}
	lim.recordFail(otel.AuthFailInvalidToken)
	return status.Error(codes.Unauthenticated, "invalid token")
}

func unaryAuthInterceptor(st *store.Store, authEnabled bool, lim *authFailLimiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		nodeID, err := checkAuthLimited(ctx, st, authEnabled, lim)
		if err != nil {
			return nil, err
		}
		return handler(context.WithValue(ctx, authedNodeIDKey{}, nodeID), req)
	}
}

// authedServerStream wraps a grpc.ServerStream to override Context(), the
// only way to thread the authenticated node_id (resolved once, at stream
// setup) down to the per-message loop in StreamEvents — grpc.ServerStream
// has no other per-stream value-passing mechanism.
type authedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authedServerStream) Context() context.Context { return s.ctx }

func streamAuthInterceptor(st *store.Store, authEnabled bool, lim *authFailLimiter) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		nodeID, err := checkAuthLimited(ss.Context(), st, authEnabled, lim)
		if err != nil {
			return err
		}
		wrapped := &authedServerStream{
			ServerStream: ss,
			ctx:          context.WithValue(ss.Context(), authedNodeIDKey{}, nodeID),
		}
		return handler(srv, wrapped)
	}
}

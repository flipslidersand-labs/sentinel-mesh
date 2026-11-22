package receiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
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
	"github.com/flipslidersand/sentinel-mesh/internal/authlimit"
	"github.com/flipslidersand/sentinel-mesh/internal/eventtype"
	"github.com/flipslidersand/sentinel-mesh/internal/model"
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
	//
	// maxRecvMsgSize is derived from the validated field limits (#361), not
	// guessed. Every string field is capped at maxRegisterFieldLength (256
	// bytes) and costs at most 259 bytes on the wire (1 tag + 2 length
	// varint + 256 data). The largest legitimate messages are:
	//   - KernelEvent/TCP: node_id, event_id + comm, src_ip, dst_ip,
	//     direction = 6 strings = 1554 B, plus timestamp (11), type (2),
	//     payload tag/len (3), pid/ports (~16)  => about 1.6 KB
	//   - KernelEvent/EXEC, FILE: 5 strings = 1295 B + numerics => about 1.4 KB
	//   - RegisterRequest: 5 strings = 1295 B
	// 8 KB leaves roughly 5x headroom for new fields while being 512x
	// smaller than the old 4 MB. The limit is checked on the wire size,
	// before decoding and before the per-stream rate limiter, so an
	// oversized frame is rejected with ResourceExhausted without being
	// decoded.
	maxRecvMsgSize int = 8 * 1024

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

// EventSaver persists events and the alerts they trigger.
type EventSaver interface {
	// SaveEventDedup reports duplicate=true when e was skipped because its
	// (node_id, event_id) was already stored (#457).
	SaveEventDedup(ctx context.Context, e model.Event) (duplicate bool, err error)
	// SaveAlertDedup reports duplicate=true when a was skipped because an alert
	// for the same (node_id, event_id, rule_id) was already stored (#465).
	SaveAlertDedup(ctx context.Context, a model.Alert) (duplicate bool, err error)
}

// TokenResolver maps a bearer token to the node_id it was issued for.
type TokenResolver interface {
	LookupNodeID(ctx context.Context, token string) (nodeID string, ok bool, err error)
}

// Storage is everything the receiver needs from the store; *store.Store
// satisfies it.
type Storage interface {
	EventSaver
	TokenResolver
}

var _ Storage = (*store.Store)(nil)

type server struct {
	pb.UnimplementedSentinelCollectorServer
	st            Storage
	reg           *registry.Registry
	engine        *alerting.Engine
	detector      *anomaly.Detector
	notifier      *notify.Dispatcher
	metrics       *otel.MetricsProvider
	tracer        trace.Tracer
	authFail      *authlimit.Limiter // per-source auth throttle (shared with the interceptors)
	nodeLimiters  nodeLimiters       // per-authenticated-node event rate limit shared across streams (#254)
	defaultRegion string             // used when an agent registers without a region
	log           *zap.Logger
	// livenessInterval is how often a live StreamEvents stream refreshes its
	// node's registry LastSeen while idle (#260). Zero means
	// defaultLivenessInterval.
	livenessInterval time.Duration

	// pendingAnomaly holds anomaly alerts whose save failed, keyed by
	// (node, event_id), so a resend of that event can save them without
	// re-counting it in the detector (#491). Bounded by maxPendingAnomaly.
	pendingMu      sync.Mutex
	pendingAnomaly map[pendingKey]model.Alert
	pendingOrder   []pendingKey // insertion order, oldest first (FIFO eviction)
	maxPending     int          // zero means maxPendingAnomaly; tests lower it
}

type pendingKey struct{ nodeID, eventID string }

const maxPendingAnomaly = 1024

func (s *server) rememberAnomaly(a model.Alert) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if s.pendingAnomaly == nil {
		s.pendingAnomaly = make(map[pendingKey]model.Alert)
	}
	k := pendingKey{a.NodeID, a.EventID}
	limit := s.maxPending
	if limit <= 0 {
		limit = maxPendingAnomaly
	}
	if _, ok := s.pendingAnomaly[k]; !ok && len(s.pendingAnomaly) >= limit {
		delete(s.pendingAnomaly, s.pendingOrder[0]) // drop the oldest entry
		s.pendingOrder = s.pendingOrder[1:]
	}
	if _, ok := s.pendingAnomaly[k]; !ok {
		s.pendingOrder = append(s.pendingOrder, k)
	}
	s.pendingAnomaly[k] = a
}

func (s *server) forgetAnomaly(a model.Alert) {
	s.pendingMu.Lock()
	k := pendingKey{a.NodeID, a.EventID}
	if _, ok := s.pendingAnomaly[k]; ok {
		delete(s.pendingAnomaly, k)
		for i, o := range s.pendingOrder {
			if o == k {
				s.pendingOrder = append(s.pendingOrder[:i], s.pendingOrder[i+1:]...)
				break
			}
		}
	}
	s.pendingMu.Unlock()
}

func (s *server) pendingAnomalyFor(ev model.Event) (model.Alert, bool) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	a, ok := s.pendingAnomaly[pendingKey{ev.NodeID, ev.EventID}]
	return a, ok
}

// defaultLivenessInterval is a third of the registry's default heartbeat
// timeout, so an idle but connected agent is refreshed several times before
// it could be marked inactive.
const defaultLivenessInterval = registry.DefaultHeartbeatTimeout / 3

// startLiveness refreshes nodeID's registry entry every interval until the
// returned stop function is called (which also waits for the goroutine to
// exit) or ctx is done. Heartbeat is a no-op for unknown nodes, so this never
// registers anything by itself.
//
// If valid is non-nil it is consulted on every tick before the heartbeat
// (#482). When it reports false the goroutine closes the returned revoked
// channel and exits without heartbeating, so the node ages out normally.
// revoked is nil (never ready) when valid is nil.
func (s *server) startLiveness(ctx context.Context, nodeID string, valid func(context.Context) bool) (stop func(), revoked <-chan struct{}) {
	interval := s.livenessInterval
	if interval <= 0 {
		interval = defaultLivenessInterval
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var rev chan struct{}
	if valid != nil {
		rev = make(chan struct{})
	}
	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if valid != nil && !valid(ctx) {
					close(rev)
					return
				}
				s.reg.Heartbeat(nodeID)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}, rev
}

// tokenStillValid returns a check that re-resolves the stream's bearer token
// through the same TokenResolver used at connect time. It reports false only
// when the token definitely no longer maps to nodeID (revoked). A lookup error
// is treated as fail-open: the store being briefly busy must not disconnect
// every authenticated stream at once; the next tick retries. Returns nil when
// there is nothing to re-check (auth disabled or no token in metadata).
func (s *server) tokenStillValid(ctx context.Context, nodeID string) func(context.Context) bool {
	if nodeID == "" || s.st == nil {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return nil
	}
	token, ok := strings.CutPrefix(values[0], "Bearer ")
	if !ok {
		return nil
	}
	return func(ctx context.Context) bool {
		got, found, err := s.st.LookupNodeID(ctx, token)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Warn("StreamEvents token re-check failed, keeping stream", zap.Error(err))
			}
			return true
		}
		return found && got == nodeID
	}
}

// recvOrRevoked is stream.Recv that also returns once revoked is closed.
// Recv itself cannot be interrupted from here, so it runs in a goroutine that
// ends when the handler returns and the stream context is cancelled.
func recvOrRevoked(stream pb.SentinelCollector_StreamEventsServer, revoked <-chan struct{}) (*pb.KernelEvent, error) {
	if revoked == nil {
		return stream.Recv()
	}
	// A closed revoked channel must win over a Recv that is ready at the same
	// time (select picks randomly), so check it before and after.
	if isClosed(revoked) {
		return nil, errTokenRevoked
	}
	type result struct {
		ev  *pb.KernelEvent
		err error
	}
	ch := make(chan result, 1)
	go func() {
		ev, err := stream.Recv()
		ch <- result{ev, err}
	}()
	select {
	case r := <-ch:
		if isClosed(revoked) {
			return nil, errTokenRevoked
		}
		return r.ev, r.err
	case <-revoked:
		return nil, errTokenRevoked
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

var errTokenRevoked = status.Error(codes.Unauthenticated, "token revoked")

// limitServerOptions returns the resource-limit server options. A single
// agent could otherwise open unbounded concurrent StreamEvents connections
// or send oversized messages, exhausting collector resources (#75, #361).
func limitServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxConcurrentStreams(maxConcurrentStreams),
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}
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
		// node_id is the identity key for the registry, dedup and tokens, so
		// it is required (#468). Other fields, including event_id (the store
		// skips dedup for an empty one, for older agents), may be empty.
		if f.name == "node_id" && f.value == "" {
			return fmt.Errorf("node_id must not be empty")
		}
		if len(f.value) > maxRegisterFieldLength {
			return fmt.Errorf("%s exceeds max length of %d bytes", f.name, maxRegisterFieldLength)
		}
		if !isPrintableASCII(f.value) {
			return fmt.Errorf("%s must contain only printable ASCII characters", f.name)
		}
	}
	return nil
}

// ValidateNodeID applies the node_id rules Register and StreamEvents enforce
// (length cap, printable ASCII), so callers such as "token issue" can reject
// IDs that Register would always refuse.
func ValidateNodeID(nodeID string) error {
	return validateFields([]namedField{{"node_id", nodeID}})
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
	info, ok := eventtype.Lookup(e.Type)
	if !ok {
		return fmt.Errorf("unknown event type %d", int32(e.Type))
	}
	if info.Strings != nil {
		for _, f := range info.Strings(e) {
			fields = append(fields, namedField{f.Name, f.Value})
		}
	}
	if err := validateFields(fields); err != nil {
		return err
	}
	if info.IPs != nil {
		for _, f := range info.IPs(e) {
			if err := validateIP(f.Name, f.Value); err != nil {
				return err
			}
		}
	}
	return nil
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

// maxLoggedNodeIDLen bounds how much of an unvalidated, client-supplied
// node_id may appear in a log line (#254).
const maxLoggedNodeIDLen = 64

// safeLogNodeID returns the identity to log for a stream. The authenticated
// node_id is preferred; otherwise the client-supplied value (not yet
// validated) is escaped (control chars, quotes, non-ASCII) and cut to
// maxLoggedNodeIDLen bytes so it can't inject or bloat log lines.
func safeLogNodeID(authedNodeID, clientNodeID string) string {
	if authedNodeID != "" {
		clientNodeID = authedNodeID
	}
	var b strings.Builder
	for _, r := range clientNodeID {
		q := strconv.QuoteToASCII(string(r))
		q = q[1 : len(q)-1] // strip surrounding quotes
		if b.Len()+len(q) > maxLoggedNodeIDLen {
			break
		}
		b.WriteString(q)
	}
	return b.String()
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

	// Keep the node active while the stream is open but idle (#260). With
	// auth the node is known now; without auth there is no trusted identity,
	// so the ticker starts after the first accepted event, using the same
	// node_id HeartbeatOrRegister registered.
	var stopLiveness func()
	var revoked <-chan struct{}
	defer func() {
		if stopLiveness != nil {
			stopLiveness()
		}
	}()
	if authedNodeID != "" {
		stopLiveness, revoked = s.startLiveness(stream.Context(), authedNodeID,
			s.tokenStillValid(stream.Context(), authedNodeID))
	}

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

		event, err := recvOrRevoked(stream, revoked)
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
			if err := rejectEvent(stream, pb.AckReason_ACK_REASON_RATE_LIMITED, func() {
				if throttled == 1 || throttled%1000 == 0 {
					s.log.Warn("StreamEvents rate limit exceeded, dropping event",
						zap.String("node_id", safeLogNodeID(authedNodeID, event.NodeId)),
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
			if sendErr := rejectEvent(stream, pb.AckReason_ACK_REASON_INVALID, func() {
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
			if sendErr := rejectEvent(stream, pb.AckReason_ACK_REASON_INVALID, func() {
				s.log.Warn("StreamEvents event node_id does not match authenticated identity",
					zap.String("authed_node_id", authedNodeID))
			}); sendErr != nil {
				return sendErr
			}
			continue
		}

		s.reg.HeartbeatOrRegister(event.NodeId, peerIP(stream.Context()), s.defaultRegion)
		if stopLiveness == nil {
			// No trusted identity here, so there is no token to re-check.
			stopLiveness, _ = s.startLiveness(stream.Context(), event.NodeId, nil)
		}

		storedEvent := model.Event{
			EventID:   event.EventId,
			NodeID:    event.NodeId,
			Timestamp: time.Unix(0, event.Timestamp).UTC(),
			Type:      s.eventTypeName(event),
			Payload:   s.eventPayload(event),
		}

		saveOk, duplicate := s.persistEvent(stream.Context(), storedEvent)
		// A resent event already had its metrics and anomaly counting done on
		// the first delivery, but its alerts may not have been saved (the event
		// and alert writes are separate transactions), so rules are still
		// evaluated; already-saved alerts are skipped by the store (#465).
		if _, alertsOk := s.evaluateAndNotify(stream.Context(), storedEvent, duplicate); !alertsOk {
			saveOk = false
		}

		ack := &pb.EventAck{Ok: saveOk}
		if !saveOk {
			// Store failure (incl. timeout/busy): transient, the agent may resend (#488).
			ack.Reason = pb.AckReason_ACK_REASON_RETRYABLE
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

// rejectEvent runs logFn (the caller's rejection log) and then sends a
// failure Ack (Ok=false, with reason). A non-nil return is the Send error; the caller must
// end the stream with it, otherwise continue with the next event.
func rejectEvent(stream pb.SentinelCollector_StreamEventsServer, reason pb.AckReason, logFn func()) error {
	if logFn != nil {
		logFn()
	}
	return stream.Send(&pb.EventAck{Ok: false, Reason: reason})
}

// persistEvent saves the event (bounded by storeOpTimeout) and records the
// store-failure and event metrics. It reports whether the save succeeded and
// whether the store skipped ev as a duplicate (no event metric is recorded
// then).
func (s *server) persistEvent(ctx context.Context, ev model.Event) (ok, duplicate bool) {
	ok = true
	saveCtx, saveCancel := context.WithTimeout(ctx, storeOpTimeout)
	// Note: ok=false does not prove the event is unsaved. On a store timeout the
	// background write may still complete (#325). A retry with the same
	// event_id is then reported as a duplicate by the store (#452, #457).
	duplicate, storeErr := s.st.SaveEventDedup(saveCtx, ev)
	saveCancel()
	if storeErr != nil {
		s.log.Error("save event", zap.Error(storeErr))
		ok = false
		if s.metrics != nil {
			s.metrics.RecordStoreWriteFailure("event")
		}
	}

	if duplicate {
		return ok, true
	}

	// Phase 6: record event metric
	if s.metrics != nil {
		s.metrics.RecordEvent(ev.Type, ev.NodeID)
	}
	return ok, false
}

// evaluateAndNotify runs the alert engine and anomaly detector on ev, saves,
// records and traces each resulting alert and dispatches it to notifiers.
// saveOk is false if any alert failed to save.
//
// engine and detector run independently so a nil engine never disables the
// detector or vice versa (#150).
//
// For a duplicate (resent) event (#465):
//   - the anomaly detector is skipped so the event is not counted twice. An
//     anomaly alert that failed to save on the first delivery is kept in
//     memory and saved again on the resend (#491); it is lost if the process
//     restarts in between.
//   - an alert the store already holds is neither recorded nor notified again.
//   - notification is at-least-once: an alert is notified even when its save
//     failed, so a resend that then saves it notifies a second time. A
//     duplicate notification is preferred over a missing one.
func (s *server) evaluateAndNotify(ctx context.Context, ev model.Event, duplicate bool) (alerts []alerting.Alert, saveOk bool) {
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
	anomalyIDs := map[string]bool{}
	if s.detector != nil && !duplicate {
		for _, a := range s.detector.Record(ev) {
			anomalyIDs[a.AlertID] = true
			alerts = append(alerts, a)
		}
	} else if duplicate {
		if a, ok := s.pendingAnomalyFor(ev); ok {
			anomalyIDs[a.AlertID] = true
			alerts = append(alerts, a)
		}
	}

	for _, alert := range alerts {
		alertCtx, alertCancel := context.WithTimeout(ctx, storeOpTimeout)
		alertDup, alertErr := s.st.SaveAlertDedup(alertCtx, alert)
		alertCancel()
		if alertErr != nil {
			s.log.Error("save alert", zap.Error(alertErr))
			saveOk = false
			if s.metrics != nil {
				s.metrics.RecordStoreWriteFailure("alert")
			}
			if anomalyIDs[alert.AlertID] {
				s.rememberAnomaly(alert)
			}
		} else if anomalyIDs[alert.AlertID] {
			s.forgetAnomaly(alert)
		}
		if alertDup {
			continue
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
	return eventtype.Name(e.Type)
}

func (s *server) eventPayload(e *pb.KernelEvent) json.RawMessage {
	var payload any
	if info, ok := eventtype.Lookup(e.Type); ok {
		payload = info.Payload(e)
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

// Config holds the scalar settings for Serve.
//
// Zero values behave exactly as the former positional arguments did:
// TLS is enabled only when BOTH TLSCertFile and TLSKeyFile are non-empty
// (setting just one serves plaintext); AuthEnabled false serves
// unauthenticated; DefaultRegion is passed through unchanged (no
// defaulting); Addr is handed to net.Listen as-is.
type Config struct {
	Addr          string
	TLSCertFile   string
	TLSKeyFile    string
	AuthEnabled   bool
	DefaultRegion string
	// LivenessInterval is how often an idle open StreamEvents stream
	// refreshes its node in the registry. Zero uses a third of
	// registry.DefaultHeartbeatTimeout; set it to a third of the heartbeat
	// timeout in use.
	LivenessInterval time.Duration
}

// Deps holds the collaborators for Serve. Fields are passed through
// unchanged with no defaulting, as before: Metrics may be nil (auth-failure
// metrics are then skipped), and the other nilable fields keep whatever
// nil handling the RPC handlers already had. Logger must be non-nil
// (Serve logs unconditionally).
type Deps struct {
	Store    Storage
	Registry *registry.Registry
	Engine   *alerting.Engine
	Detector *anomaly.Detector
	Notifier *notify.Dispatcher
	Metrics  *otel.MetricsProvider
	Tracer   trace.Tracer
	Logger   *zap.Logger
}

// Serve starts the gRPC server on cfg.Addr. If cfg.TLSCertFile/TLSKeyFile
// are both set, the server requires TLS; otherwise it serves in plaintext
// (caller is expected to warn). If cfg.AuthEnabled, every RPC must present an
// `authorization: Bearer <token>` metadata entry naming a token issued via
// deps.Store.IssueToken (#189) — unlike the REST API's single shared
// SENTINEL_API_TOKEN, each agent authenticates with its own per-agent
// token (#183, per ADR-005).
//
// Serve blocks until either the server stops on its own (e.g. a listener
// error) or ctx is done, in which case it gracefully stops the server
// (waiting for in-flight RPCs to finish) before returning.
func Serve(ctx context.Context, cfg Config, deps Deps) error {
	addr, log, st, metrics := cfg.Addr, deps.Logger, deps.Store, deps.Metrics
	tlsCertFile, tlsKeyFile, authEnabled := cfg.TLSCertFile, cfg.TLSKeyFile, cfg.AuthEnabled
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	authLim := authlimit.New()
	if metrics != nil {
		authLim.OnFail = metrics.RecordAuthFailure
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
	)
	opts = append(opts, limitServerOptions()...)
	srv := grpc.NewServer(opts...)

	s := &server{st: st, reg: deps.Registry, engine: deps.Engine, detector: deps.Detector, notifier: deps.Notifier, metrics: metrics, tracer: deps.Tracer, defaultRegion: cfg.DefaultRegion, log: log, authFail: authLim, livenessInterval: cfg.LivenessInterval}
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

// checkAuthLimited validates the `authorization: Bearer <token>` metadata entry in
// ctx against st's per-agent token store (#189) and returns the resolved
// node_id. A no-op (nodeID="", err=nil) if authEnabled is false — this is
// the same "unauthenticated dev mode" escape hatch the old single shared
// token had, just gated by a separate flag now that there's no longer one
// token whose emptiness could signal it.
//
// Per-source-IP throttling of failed attempts goes via lim (nil disables it;
// valid tokens are never throttled).
func checkAuthLimited(ctx context.Context, st TokenResolver, authEnabled bool, lim *authlimit.Limiter) (nodeID string, err error) {
	if !authEnabled {
		return "", nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		lim.RecordFail(otel.AuthFailMissingCredentials)
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		lim.RecordFail(otel.AuthFailMissingCredentials)
		return "", status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	got, ok := strings.CutPrefix(values[0], "Bearer ")
	if !ok {
		lim.RecordFail(otel.AuthFailInvalidToken)
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
func authFailure(ctx context.Context, lim *authlimit.Limiter) error {
	if ip := peerIP(ctx); ip != "" && !lim.Reserve(ip) {
		lim.RecordFail(otel.AuthFailRateLimited)
		return status.Error(codes.ResourceExhausted, "too many authentication failures")
	}
	lim.RecordFail(otel.AuthFailInvalidToken)
	return status.Error(codes.Unauthenticated, "invalid token")
}

func unaryAuthInterceptor(st TokenResolver, authEnabled bool, lim *authlimit.Limiter) grpc.UnaryServerInterceptor {
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

func streamAuthInterceptor(st TokenResolver, authEnabled bool, lim *authlimit.Limiter) grpc.StreamServerInterceptor {
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

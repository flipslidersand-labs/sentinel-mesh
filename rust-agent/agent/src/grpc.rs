use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::pb::{sentinel_collector_client::SentinelCollectorClient, KernelEvent, RegisterRequest};

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// A connection counts as healthy (and resets the reconnect backoff) only once
/// register succeeded AND the event stream has been up this long. Register
/// success alone is not enough: a collector that accepts Register then drops
/// the stream immediately would otherwise make us reconnect at
/// INITIAL_BACKOFF in a tight loop (#313).
const STABLE_CONNECTION: Duration = Duration::from_secs(10);

/// Returns `(wait, next)`: the base delay to sleep before reconnecting and the
/// backoff to carry into the following failure. `connected_for` is how long
/// the event stream had been established when the attempt ended (`None` if it
/// never got that far). A stable connection resets `wait` to INITIAL_BACKOFF.
fn next_backoff(current: Duration, connected_for: Option<Duration>) -> (Duration, Duration) {
    let wait = match connected_for {
        Some(d) if d >= STABLE_CONNECTION => INITIAL_BACKOFF,
        _ => current,
    };
    (wait, (wait * 2).min(MAX_BACKOFF))
}

/// Cap on how long a single `connect()` attempt may block. Without this, a
/// firewall that silently drops packets leaves `.connect().await` hanging
/// until the OS/TCP stack's own timeout (which can be minutes), during which
/// `run_once` never returns `Err` and the backoff-reconnect loop in
/// `stream_to_collector` never runs (#108).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on how long an RPC may wait for the collector's *response headers*
/// once connected (`Register` end to end, and `StreamEvents` until its
/// headers arrive). tonic applies `Endpoint::timeout` only to the
/// future that resolves with the response head (transport/service/
/// grpc_timeout.rs), not to the event stream body afterwards — a stalled
/// stream is caught by the HTTP/2 keepalive below instead (#236).
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP/2 PING interval. Once `StreamEvents` headers are back (#233) nothing
/// else bounds the stream, so a collector host that dies without sending FIN
/// (power loss, network partition, firewall DROP) would otherwise leave
/// `response.message()` pending forever while `rx` fills up (#236). 30s stays
/// well above the collector's keepalive EnforcementPolicy MinTime (10s,
/// go-collector/internal/receiver/grpc.go), so pings are never treated as
/// abuse (GOAWAY too_many_pings).
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// How long to wait for a PING ack before declaring the connection dead.
/// Kept generous because on a congested uplink the collector's PING ack can
/// queue behind our own unsent event data; 10s risked false disconnects.
/// Worst-case detection of a half-open connection ≈ interval + timeout (50s).
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Connection timing knobs, bundled so tests can shrink them to milliseconds.
#[derive(Clone, Copy, Debug)]
struct ConnTuning {
    connect_timeout: Duration,
    rpc_timeout: Duration,
    keepalive_interval: Duration,
    keepalive_timeout: Duration,
}

impl Default for ConnTuning {
    fn default() -> Self {
        Self {
            connect_timeout: CONNECT_TIMEOUT,
            rpc_timeout: RPC_TIMEOUT,
            keepalive_interval: KEEPALIVE_INTERVAL,
            keepalive_timeout: KEEPALIVE_TIMEOUT,
        }
    }
}

/// Attaches `authorization: Bearer <token>` metadata to every outgoing RPC.
/// A no-op if no token is configured.
#[derive(Clone)]
struct AuthInterceptor {
    token: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> std::result::Result<Request<()>, Status> {
        if let Some(token) = &self.token {
            req.metadata_mut().insert("authorization", token.clone());
        }
        Ok(req)
    }
}

/// Connects to the collector and streams events until the connection drops or
/// fails, reconnecting with exponential backoff + jitter (100ms, capped at
/// 30s) rather than exiting the process (#73). `rx` is held across
/// reconnects — a dropped connection doesn't lose events already queued by
/// the source, and events keep queuing (bounded by the channel) while a
/// reconnect is in progress.
pub async fn stream_to_collector(
    endpoint: String,
    node_id: String,
    region: String,
    token: Option<String>,
    ca_cert_path: Option<String>,
    rx: Receiver<KernelEvent>,
) -> Result<()> {
    stream_to_collector_with(
        endpoint,
        node_id,
        region,
        token,
        ca_cert_path,
        ConnTuning::default(),
        rx,
    )
    .await
}

/// `stream_to_collector` with explicit timing knobs (tests shrink them).
async fn stream_to_collector_with(
    endpoint: String,
    node_id: String,
    region: String,
    token: Option<String>,
    ca_cert_path: Option<String>,
    tuning: ConnTuning,
    mut rx: Receiver<KernelEvent>,
) -> Result<()> {
    let auth_token = token
        .map(|t| MetadataValue::try_from(format!("Bearer {t}")))
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid token: {e}"))?;

    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_else(|_| node_id.clone());

    let mut backoff = INITIAL_BACKOFF;
    // Owned here so it survives reconnects (#241).
    let mut pending = Pending::default();

    loop {
        let mut stats = DeliveryStats::default();
        let session = Session {
            endpoint: &endpoint,
            node_id: &node_id,
            region: &region,
            hostname: &hostname,
            auth_token: auth_token.clone(),
            ca_cert_path: &ca_cert_path,
            tuning,
        };
        match run_once_tracked(session, &mut rx, &mut pending, &mut stats).await {
            // rx.recv() returned None: every event-source Sender was dropped,
            // so there will never be another event. Exit for good.
            Ok(()) => return Ok(()),
            Err(e) if e.downcast_ref::<RegistrationRejected>().is_some() => {
                // Retrying can't help; surface the reason and exit non-zero.
                eprintln!("{e}");
                return Err(e);
            }
            Err(e) => {
                let connected_for = stats.established_at.map(|t| t.elapsed());
                let (base, next) = next_backoff(backoff, connected_for);
                backoff = next;
                let wait = jittered(base);
                eprintln!("collector connection lost/failed: {e} — reconnecting in {wait:.1?}");
                eprintln!("{}", pending.summary());
                tokio::time::sleep(wait).await;
            }
        }
    }
}

/// The collector permanently refused this agent's registration (validation
/// failure, identity mismatch, or an auth/argument gRPC status). Not retried:
/// `stream_to_collector` returns it so the process exits non-zero (#380).
#[derive(Debug)]
struct RegistrationRejected(String);

impl std::fmt::Display for RegistrationRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            write!(f, "registration rejected by collector (no reason given)")
        } else {
            write!(f, "registration rejected by collector: {}", self.0)
        }
    }
}

impl std::error::Error for RegistrationRejected {}

/// gRPC codes that retrying the same request cannot fix.
fn is_permanent_status(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::InvalidArgument | tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
    )
}

/// Adds up to ±25% jitter to `base` so many agents reconnecting after a
/// shared collector outage don't all retry in lockstep. Derived from the
/// current time rather than pulling in a `rand` dependency for one call site.
fn jittered(base: Duration) -> Duration {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let factor = 0.75 + (nanos % 1000) as f64 / 2000.0; // 0.75..=1.25
    base.mul_f64(factor)
}

/// Parameters shared by every connection attempt (identity, auth, TLS, timing).
struct Session<'a> {
    endpoint: &'a str,
    node_id: &'a str,
    region: &'a str,
    hostname: &'a str,
    auth_token: Option<MetadataValue<tonic::metadata::Ascii>>,
    ca_cert_path: &'a Option<String>,
    tuning: ConnTuning,
}

/// One connect→register→stream attempt. Returns `Ok(())` only when the event
/// source is exhausted (rx closed); any connection/RPC failure is returned as
/// `Err` for the caller to retry.
///
/// On `Err`, events already taken from `rx` that the collector never acked
/// stay in `pending` and are re-sent first on the next connection (#241,
/// at-least-once). The error text reports how many are queued. Events whose
/// ack was lost are sent again, so the collector may see duplicates (it
/// dedups by `event_id`).
#[cfg(test)]
async fn run_once(
    session: Session<'_>,
    rx: &mut Receiver<KernelEvent>,
    pending: &mut Pending,
) -> Result<()> {
    let mut stats = DeliveryStats::default();
    run_once_tracked(session, rx, pending, &mut stats).await
}

/// `run_once` that leaves its `DeliveryStats` (incl. when the stream was
/// established) with the caller, so the reconnect loop can decide on backoff.
async fn run_once_tracked(
    session: Session<'_>,
    rx: &mut Receiver<KernelEvent>,
    pending: &mut Pending,
    stats: &mut DeliveryStats,
) -> Result<()> {
    let res = run_session(session, rx, pending, stats).await;
    res.map_err(|e| {
        let n = pending.q.len();
        if n > 0 {
            anyhow::anyhow!("{e}; {n} events queued for resend")
        } else {
            e
        }
    })
}

/// Per-connection state.
#[derive(Default)]
struct DeliveryStats {
    /// Set once Register succeeded and the event stream is open.
    established_at: Option<Instant>,
}

/// Max events / encoded bytes held un-acked (the smaller limit wins). When
/// full, `rx` is not read; the upstream bounded channel then drops and counts
/// (events.rs), so loss stays visible.
const PENDING_MAX_EVENTS: usize = 2048;
const PENDING_MAX_BYTES: usize = 2 * 1024 * 1024;

/// Events taken from `rx` and not yet acked, oldest first. Lives across
/// reconnects. The collector acks every received event in order, so each ack
/// confirms the front of the queue.
struct Pending {
    q: std::collections::VecDeque<KernelEvent>,
    bytes: usize,
    max_events: usize,
    max_bytes: usize,
    /// Events re-sent after a reconnect (cumulative).
    resent: u64,
    /// Times reading `rx` was suspended because the queue was full.
    full_stalls: u64,
    max_len: usize,
    was_full: bool,
}

impl Default for Pending {
    fn default() -> Self {
        Self::new(PENDING_MAX_EVENTS, PENDING_MAX_BYTES)
    }
}

impl Pending {
    fn new(max_events: usize, max_bytes: usize) -> Self {
        Self {
            q: Default::default(),
            bytes: 0,
            max_events,
            max_bytes,
            resent: 0,
            full_stalls: 0,
            max_len: 0,
            was_full: false,
        }
    }

    fn is_full(&self) -> bool {
        self.q.len() >= self.max_events || self.bytes >= self.max_bytes
    }

    /// Call before deciding whether to read `rx`; counts full transitions.
    fn note_fullness(&mut self) {
        let full = self.is_full();
        if full && !self.was_full {
            self.full_stalls += 1;
        }
        self.was_full = full;
    }

    fn push(&mut self, e: KernelEvent) {
        self.bytes += prost::Message::encoded_len(&e);
        self.q.push_back(e);
        self.max_len = self.max_len.max(self.q.len());
    }

    fn ack(&mut self) -> Option<KernelEvent> {
        let e = self.q.pop_front()?;
        self.bytes = self.bytes.saturating_sub(prost::Message::encoded_len(&e));
        Some(e)
    }

    fn summary(&self) -> String {
        format!(
            "resend queue: {} queued, {} resent in total, {} full stalls, max {} queued",
            self.q.len(),
            self.resent,
            self.full_stalls,
            self.max_len
        )
    }
}

/// TLS config for https endpoints: always trust the platform roots (tonic
/// 0.11's `tls-roots` did this implicitly; 0.14 needs an explicit call),
/// plus the extra CA from `ca_cert_path` when given.
fn build_tls_config(ca_cert_path: &Option<String>) -> Result<ClientTlsConfig> {
    let mut tls = ClientTlsConfig::new().with_native_roots();
    if let Some(path) = ca_cert_path {
        let pem = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read CA cert {path}: {e}"))?;
        tls = tls.ca_certificate(Certificate::from_pem(pem));
    }
    Ok(tls)
}

async fn run_session(
    session: Session<'_>,
    rx: &mut Receiver<KernelEvent>,
    pending: &mut Pending,
    stats: &mut DeliveryStats,
) -> Result<()> {
    let Session {
        endpoint,
        node_id,
        region,
        hostname,
        auth_token,
        ca_cert_path,
        tuning,
    } = session;
    let mut builder = Endpoint::from_shared(endpoint.to_string())?
        .connect_timeout(tuning.connect_timeout)
        .timeout(tuning.rpc_timeout)
        .http2_keep_alive_interval(tuning.keepalive_interval)
        .keep_alive_timeout(tuning.keepalive_timeout)
        // Ping even with no open stream: covers the window between
        // connect and `StreamEvents` and keeps detection uniform.
        .keep_alive_while_idle(true);
    if endpoint.starts_with("https://") {
        builder = builder.tls_config(build_tls_config(ca_cert_path)?)?;
    }
    let channel = builder
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("connect to {endpoint}: {e}"))?;

    let mut client =
        SentinelCollectorClient::with_interceptor(channel, AuthInterceptor { token: auth_token });

    let resp = client
        .register(RegisterRequest {
            node_id: node_id.to_string(),
            hostname: hostname.to_string(),
            ip: String::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            region: region.to_string(),
        })
        .await
        .map_err(|st| {
            if is_permanent_status(st.code()) {
                anyhow::Error::new(RegistrationRejected(format!(
                    "{:?}: {}",
                    st.code(),
                    st.message()
                )))
            } else {
                anyhow::Error::new(st)
            }
        })?;

    // The collector answers every validation / identity failure with
    // `ok=false` + a message (go-collector receiver Register), and none of
    // those can succeed on retry, so treat it as permanent.
    let resp = resp.into_inner();
    if !resp.ok {
        return Err(anyhow::Error::new(RegistrationRejected(resp.message)));
    }
    println!("registered with collector");

    // tonic's stream_events requires an owned ('static) request stream, so we
    // can't feed it directly from `rx` (borrowed — it must survive this
    // function returning on error, so the caller can retry with it intact).
    // Bridge through a fresh per-attempt channel instead: `bridge_rx` is
    // owned and moved into the stream (satisfying 'static), while this
    // function keeps driving the outer `rx` and forwarding into `bridge_tx`.
    let (bridge_tx, bridge_rx) = tokio::sync::mpsc::channel::<KernelEvent>(256);
    let outbound = tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(bridge_rx);

    let mut response = client.stream_events(outbound).await?.into_inner();
    stats.established_at = Some(Instant::now());

    let mut rejects = RejectTracker::new(REJECT_REPORT_INTERVAL);

    // Run the pump as a block so that every exit path (clean end or any
    // error) flushes pending rejection counts before returning.
    // Everything still queued is (re)sent on this connection, oldest first.
    pending.resent += pending.q.len() as u64;
    // Number of queue-front events already sent on this connection.
    let mut cursor = 0usize;
    let mut rx_closed = false;

    let result: Result<()> = async {
        loop {
            pending.note_fullness();
            if rx_closed && pending.q.is_empty() {
                // Source exhausted and everything acked: close cleanly.
                drop(bridge_tx);
                return Ok(());
            }
            let can_send = cursor < pending.q.len();
            let can_read = !rx_closed && !pending.is_full();

            tokio::select! {
                biased;

                ack = response.message() => {
                    match ack? {
                        Some(a) => {
                            // Acks are 1:1 and in order with sent events.
                            if cursor == 0 || pending.ack().is_none() {
                                anyhow::bail!("collector sent an ack with no event in flight");
                            }
                            cursor -= 1;
                            // ok=false (validation error, rate limit) is only
                            // logged and NOT re-sent: the same event would be
                            // rejected again, looping forever.
                            if !a.ok {
                                if let Some(r) = rejects.record(Instant::now()) {
                                    log_rejects(r);
                                }
                            }
                        }
                        // Server closed the stream — a connection failure, not a
                        // clean shutdown; retry (unacked events are re-sent).
                        None => anyhow::bail!("collector closed the event stream"),
                    }
                }

                permit = bridge_tx.reserve(), if can_send => {
                    let permit = permit
                        .map_err(|_| anyhow::anyhow!("outbound stream to collector closed"))?;
                    permit.send(pending.q[cursor].clone());
                    cursor += 1;
                }

                maybe_event = rx.recv(), if can_read => {
                    match maybe_event {
                        Some(mut event) => {
                            event.node_id = node_id.to_string();
                            pending.push(event);
                        }
                        // Every event-source Sender was dropped. Keep going
                        // until the queue is fully acked.
                        None => rx_closed = true,
                    }
                }
            }
        }
    }
    .await;
    flush_rejects_on_exit(&mut rejects, Instant::now());
    result
}

fn flush_rejects_on_exit(rejects: &mut RejectTracker, now: Instant) {
    if let Some(r) = rejects.flush(now) {
        log_rejects(r);
    }
}

/// Minimum gap between "collector rejected events" log lines, so a node above
/// the collector's per-node rate limit does not flood the journal (#378).
const REJECT_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// Counts `ok=false` acks. Same scheme as `events::DropTracker`: the first
/// rejection is reported immediately, later ones are batched and reported at
/// most once per `interval`.
struct RejectTracker {
    total: u64,
    unreported: u64,
    last_report: Option<Instant>,
    interval: Duration,
}

impl RejectTracker {
    fn new(interval: Duration) -> Self {
        Self {
            total: 0,
            unreported: 0,
            last_report: None,
            interval,
        }
    }

    /// Record one rejected ack; returns `(since_last, total)` when it is time to log.
    fn record(&mut self, now: Instant) -> Option<(u64, u64)> {
        self.total += 1;
        self.unreported += 1;
        let due = match self.last_report {
            None => true,
            Some(t) => now.saturating_duration_since(t) >= self.interval,
        };
        if due {
            self.flush(now)
        } else {
            None
        }
    }

    /// Report rejections not yet logged (e.g. when the stream ends).
    fn flush(&mut self, now: Instant) -> Option<(u64, u64)> {
        if self.unreported == 0 {
            return None;
        }
        let r = (self.unreported, self.total);
        self.unreported = 0;
        self.last_report = Some(now);
        Some(r)
    }
}

fn log_rejects((since_last, total): (u64, u64)) {
    eprintln!("warn: collector rejected {since_last} event(s) since last report ({total} total)");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_config_enables_native_roots_without_ca_path() {
        let tls = build_tls_config(&None).unwrap();
        assert!(format!("{tls:?}").contains("with_native_roots: true"));
    }

    #[test]
    fn tls_config_keeps_native_roots_with_extra_ca() {
        let dir = std::env::temp_dir().join(format!("tlscfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca = dir.join("ca.pem");
        std::fs::write(
            &ca,
            "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        let tls = build_tls_config(&Some(ca.to_string_lossy().into_owned())).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        let dbg = format!("{tls:?}");
        assert!(dbg.contains("with_native_roots: true"));
        assert!(dbg.contains("certs: [Certificate"));
    }

    #[test]
    fn tls_config_missing_ca_file_is_error() {
        let e = build_tls_config(&Some("/nonexistent/ca.pem".into())).unwrap_err();
        assert!(e.to_string().contains("read CA cert"));
    }

    #[test]
    fn pending_rejects_are_flushed_on_exit() {
        let t0 = Instant::now();
        let mut d = RejectTracker::new(Duration::from_secs(10));
        d.record(t0);
        d.record(t0 + Duration::from_secs(1));
        assert_eq!(d.unreported, 1);
        flush_rejects_on_exit(&mut d, t0 + Duration::from_secs(2));
        assert_eq!(d.unreported, 0);
    }

    #[test]
    fn reject_tracker_throttles_with_injected_clock() {
        let t0 = Instant::now();
        let mut d = RejectTracker::new(Duration::from_secs(10));
        // First rejection is reported immediately.
        assert_eq!(d.record(t0), Some((1, 1)));
        // A burst inside the interval is silent.
        for i in 1..=1000 {
            assert_eq!(d.record(t0 + Duration::from_millis(i)), None);
        }
        // Nothing to flush-report early via record; once the interval has
        // elapsed the next rejection reports the whole batch in one line.
        assert_eq!(d.record(t0 + Duration::from_secs(10)), Some((1001, 1002)));
        // Nothing pending -> flush is empty; a pending one is flushed.
        assert_eq!(d.flush(t0 + Duration::from_secs(11)), None);
        assert_eq!(d.record(t0 + Duration::from_secs(12)), None);
        assert_eq!(d.flush(t0 + Duration::from_secs(12)), Some((1, 1003)));
    }

    #[test]
    fn pending_full_by_count_or_bytes_and_acks_in_order() {
        let ev = |id: &str| KernelEvent {
            event_id: id.into(),
            ..Default::default()
        };
        let mut p = Pending::new(2, usize::MAX);
        p.push(ev("a"));
        assert!(!p.is_full());
        p.push(ev("b"));
        p.note_fullness();
        assert!(p.is_full());
        assert_eq!(p.full_stalls, 1);
        assert_eq!(p.ack().unwrap().event_id, "a");
        assert!(!p.is_full());
        assert_eq!(p.max_len, 2);

        let mut p = Pending::new(usize::MAX, 10);
        p.push(ev(&"x".repeat(20)));
        assert!(p.is_full(), "byte limit must stop reads");
        p.ack();
        assert_eq!(p.bytes, 0);
        assert!(p.ack().is_none());
    }

    #[test]
    fn jittered_stays_within_25_percent() {
        let base = Duration::from_secs(1);
        for _ in 0..200 {
            let got = jittered(base);
            assert!(
                got >= base.mul_f64(0.75) && got <= base.mul_f64(1.25),
                "jittered({base:?}) = {got:?} out of the expected ±25% range"
            );
        }
    }

    #[test]
    fn connect_timeout_is_set_and_bounded() {
        // Regression guard for #108: `run_once` must bound how long
        // `.connect().await` can block, or a silently-dropping firewall
        // stalls it past the OS/TCP default (minutes) and the
        // backoff-reconnect loop in `stream_to_collector` never runs.
        assert!(CONNECT_TIMEOUT > Duration::ZERO);
        assert!(CONNECT_TIMEOUT <= Duration::from_secs(30));
    }

    #[test]
    fn backoff_doubles_and_caps_at_max() {
        let mut b = INITIAL_BACKOFF;
        assert_eq!(b, Duration::from_millis(100));
        b = (b * 2).min(MAX_BACKOFF);
        assert_eq!(b, Duration::from_millis(200));
        for _ in 0..20 {
            b = (b * 2).min(MAX_BACKOFF);
        }
        assert_eq!(b, MAX_BACKOFF);
    }

    #[test]
    fn backoff_resets_after_stable_connection() {
        // Escalated to the cap, then a connection that lived long enough:
        // the next wait restarts from INITIAL_BACKOFF (#313).
        let (wait, next) = next_backoff(MAX_BACKOFF, Some(STABLE_CONNECTION));
        assert_eq!(wait, INITIAL_BACKOFF);
        assert_eq!(next, INITIAL_BACKOFF * 2);
        let (wait, _) = next_backoff(MAX_BACKOFF, Some(STABLE_CONNECTION * 10));
        assert_eq!(wait, INITIAL_BACKOFF);
    }

    #[test]
    fn backoff_not_reset_by_short_lived_or_unestablished_connection() {
        // register ok but stream dropped right away: must keep escalating,
        // otherwise a flapping collector gets a fast reconnect loop.
        let just_under = STABLE_CONNECTION - Duration::from_millis(1);
        let (wait, next) = next_backoff(Duration::from_secs(4), Some(just_under));
        assert_eq!(wait, Duration::from_secs(4));
        assert_eq!(next, Duration::from_secs(8));
        // never got as far as register/stream
        let (wait, next) = next_backoff(Duration::from_secs(4), None);
        assert_eq!(wait, Duration::from_secs(4));
        assert_eq!(next, Duration::from_secs(8));
        // still capped
        let (_, next) = next_backoff(MAX_BACKOFF, None);
        assert_eq!(next, MAX_BACKOFF);
    }

    #[test]
    fn keepalive_defaults_are_bounded_and_respect_collector_policy() {
        // The collector's EnforcementPolicy MinTime is 10s
        // (go-collector/internal/receiver/grpc.go:535-538 — keep in sync);
        // pinging faster gets the connection GOAWAY'd. Detection should stay
        // under ~1 min.
        assert!(KEEPALIVE_INTERVAL >= Duration::from_secs(10));
        assert!(KEEPALIVE_TIMEOUT > Duration::ZERO);
        assert!(KEEPALIVE_INTERVAL + KEEPALIVE_TIMEOUT <= Duration::from_secs(60));
    }

    mod half_open {
        use std::pin::Pin;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        use tonic::codegen::tokio_stream::Stream;
        use tonic::{Response, Streaming};

        use super::*;
        use crate::pb::sentinel_collector_server::{SentinelCollector, SentinelCollectorServer};
        use crate::pb::{EventAck, RegisterResponse};

        /// Acks every event and reports the first one, so the test knows the
        /// stream is fully established before cutting the network.
        struct FakeCollector {
            got_event: tokio::sync::mpsc::Sender<()>,
            registered: tokio::sync::mpsc::Sender<()>,
            /// event_id of every event received, in arrival order.
            seen: Arc<std::sync::Mutex<Vec<String>>>,
        }

        #[tonic::async_trait]
        impl SentinelCollector for FakeCollector {
            async fn register(
                &self,
                _: Request<RegisterRequest>,
            ) -> std::result::Result<Response<RegisterResponse>, Status> {
                let _ = self.registered.try_send(());
                Ok(Response::new(RegisterResponse {
                    ok: true,
                    message: String::new(),
                }))
            }

            type StreamEventsStream =
                Pin<Box<dyn Stream<Item = std::result::Result<EventAck, Status>> + Send>>;

            async fn stream_events(
                &self,
                req: Request<Streaming<KernelEvent>>,
            ) -> std::result::Result<Response<Self::StreamEventsStream>, Status> {
                let mut inbound = req.into_inner();
                let got_event = self.got_event.clone();
                let seen = self.seen.clone();
                let s = async_stream::stream! {
                    while let Ok(Some(ev)) = inbound.message().await {
                        seen.lock().unwrap().push(ev.event_id);
                        let _ = got_event.try_send(());
                        yield Ok(EventAck { ok: true });
                    }
                };
                Ok(Response::new(Box::pin(s)))
            }
        }

        /// Forwards bytes one way until `frozen` is set, then swallows input
        /// and parks forever while still holding both sockets open: from the
        /// agent's side the peer has vanished without FIN or RST.
        async fn pipe(
            mut from: tokio::net::tcp::OwnedReadHalf,
            mut to: tokio::net::tcp::OwnedWriteHalf,
            frozen: Arc<AtomicBool>,
        ) {
            let mut buf = [0u8; 8192];
            loop {
                let n = match from.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                if frozen.load(Ordering::SeqCst) {
                    break;
                }
                if to.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
            // Keep `from`/`to` alive so neither side sees the TCP close.
            let _hold = (from, to);
            std::future::pending::<()>().await;
        }

        /// Freeze flags of every connection the proxy has accepted so far.
        /// `freeze()` silences only those, so a reconnect gets a working link.
        #[derive(Clone, Default)]
        struct Conns(Arc<std::sync::Mutex<Vec<(Arc<AtomicBool>, Arc<AtomicBool>)>>>);

        impl Conns {
            /// Both directions go silent.
            fn freeze(&self) {
                for (up, down) in self.0.lock().unwrap().iter() {
                    up.store(true, Ordering::SeqCst);
                    down.store(true, Ordering::SeqCst);
                }
            }

            /// Only collector -> agent goes silent: events still arrive and
            /// are stored, but their acks are lost.
            fn freeze_acks(&self) {
                for (_, down) in self.0.lock().unwrap().iter() {
                    down.store(true, Ordering::SeqCst);
                }
            }
        }

        async fn freezing_proxy(upstream: std::net::SocketAddr) -> (std::net::SocketAddr, Conns) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let conns = Conns::default();
            let c = conns.clone();
            tokio::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    let server = TcpStream::connect(upstream).await.unwrap();
                    let up = Arc::new(AtomicBool::new(false));
                    let down = Arc::new(AtomicBool::new(false));
                    c.0.lock().unwrap().push((up.clone(), down.clone()));
                    let (cr, cw) = client.into_split();
                    let (sr, sw) = server.into_split();
                    tokio::spawn(pipe(cr, sw, up));
                    tokio::spawn(pipe(sr, cw, down));
                }
            });
            (addr, conns)
        }

        struct Harness {
            endpoint: String,
            conns: Conns,
            got_event: tokio::sync::mpsc::Receiver<()>,
            registered: tokio::sync::mpsc::Receiver<()>,
            seen: Arc<std::sync::Mutex<Vec<String>>>,
        }

        /// Real tonic collector behind a proxy that can go silent.
        async fn harness() -> Harness {
            let srv_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let srv_addr = srv_listener.local_addr().unwrap();
            let (got_tx, got_event) = tokio::sync::mpsc::channel(1);
            let (reg_tx, registered) = tokio::sync::mpsc::channel(8);
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(SentinelCollectorServer::new(FakeCollector {
                        got_event: got_tx,
                        registered: reg_tx,
                        seen: seen.clone(),
                    }))
                    .serve_with_incoming(tokio_stream_from(srv_listener)),
            );
            let (proxy_addr, conns) = freezing_proxy(srv_addr).await;
            Harness {
                endpoint: format!("http://{proxy_addr}"),
                conns,
                got_event,
                registered,
                seen,
            }
        }

        fn fast_tuning() -> ConnTuning {
            ConnTuning {
                connect_timeout: Duration::from_secs(5),
                // Deliberately long: the stream must be cut by keepalive,
                // not by the header timeout.
                rpc_timeout: Duration::from_secs(60),
                keepalive_interval: Duration::from_millis(200),
                keepalive_timeout: Duration::from_millis(300),
            }
        }

        async fn within<T>(secs: u64, what: &str, f: impl std::future::Future<Output = T>) -> T {
            tokio::time::timeout(Duration::from_secs(secs), f)
                .await
                .unwrap_or_else(|_| panic!("timed out: {what}"))
        }

        #[tokio::test]
        async fn run_once_errors_when_collector_goes_silent_mid_stream() {
            let mut h = harness().await;
            let (ev_tx, mut ev_rx) = tokio::sync::mpsc::channel::<KernelEvent>(16);
            let endpoint = h.endpoint.clone();
            let agent = tokio::spawn(async move {
                run_once(
                    Session {
                        endpoint: &endpoint,
                        node_id: "n1",
                        region: "",
                        hostname: "h",
                        auth_token: None,
                        ca_cert_path: &None,
                        tuning: fast_tuning(),
                    },
                    &mut ev_rx,
                    &mut Pending::default(),
                )
                .await
            });

            ev_tx.send(KernelEvent::default()).await.unwrap();
            within(5, "stream never got established", h.got_event.recv()).await;

            // Collector host "disappears": no FIN, no RST, no PING acks.
            h.conns.freeze();

            let res = within(5, "run_once did not detect the half-open connection", agent)
                .await
                .unwrap();
            assert!(
                res.is_err(),
                "expected Err so the reconnect loop runs, got {res:?}"
            );
            drop(ev_tx);
        }

        #[tokio::test]
        async fn run_once_errors_while_backlogged_on_half_open_connection() {
            // The #236 symptom path: after the link dies, events keep
            // arriving, the HTTP/2 send window fills, and run_once sits in
            // `bridge_tx.send().await` instead of polling acks. It must still
            // bail out once keepalive declares the connection dead.
            let mut h = harness().await;
            let (ev_tx, mut ev_rx) = tokio::sync::mpsc::channel::<KernelEvent>(256);
            let endpoint = h.endpoint.clone();
            let agent = tokio::spawn(async move {
                run_once(
                    Session {
                        endpoint: &endpoint,
                        node_id: "n1",
                        region: "",
                        hostname: "h",
                        auth_token: None,
                        ca_cert_path: &None,
                        tuning: fast_tuning(),
                    },
                    &mut ev_rx,
                    &mut Pending::default(),
                )
                .await
            });

            ev_tx.send(KernelEvent::default()).await.unwrap();
            within(5, "stream never got established", h.got_event.recv()).await;
            h.conns.freeze();

            let flood = tokio::spawn(async move {
                let mut sent = 0u32;
                // ~200B each, ~8MB total: far beyond the HTTP/2 windows,
                // the bridge channel and `rx`, so the backlog really forms.
                let event = KernelEvent {
                    event_id: "x".repeat(200),
                    ..Default::default()
                };
                for _ in 0..40_000 {
                    if ev_tx.send(event.clone()).await.is_err() {
                        break;
                    }
                    sent += 1;
                }
                sent
            });

            let started = std::time::Instant::now();
            let res = within(5, "run_once stayed stuck behind the backlog", agent)
                .await
                .unwrap();
            assert!(res.is_err(), "expected Err, got {res:?}");
            let elapsed = started.elapsed();
            // run_once dropped `rx`, so the flood stops at the first failed send.
            let sent = flood.await.unwrap();
            eprintln!("backlogged run_once returned Err after {elapsed:?} ({sent} events queued)");
            assert!(
                sent < 40_000,
                "backlog never formed ({sent} events accepted)"
            );
        }

        #[tokio::test]
        async fn run_once_error_reports_unacknowledged_event_count() {
            // #241: events already taken from `rx` but never acked are kept for
            // resend on disconnect; the error (which the caller logs) says how many.
            let mut h = harness().await;
            let (ev_tx, mut ev_rx) = tokio::sync::mpsc::channel::<KernelEvent>(256);
            let endpoint = h.endpoint.clone();
            let agent = tokio::spawn(async move {
                run_once(
                    Session {
                        endpoint: &endpoint,
                        node_id: "n1",
                        region: "",
                        hostname: "h",
                        auth_token: None,
                        ca_cert_path: &None,
                        tuning: fast_tuning(),
                    },
                    &mut ev_rx,
                    &mut Pending::default(),
                )
                .await
            });

            ev_tx.send(KernelEvent::default()).await.unwrap();
            within(5, "stream never got established", h.got_event.recv()).await;
            h.conns.freeze();

            let flood = tokio::spawn(async move {
                let event = KernelEvent {
                    event_id: "x".repeat(200),
                    ..Default::default()
                };
                for _ in 0..40_000 {
                    if ev_tx.send(event.clone()).await.is_err() {
                        break;
                    }
                }
            });

            let res = within(5, "run_once stayed stuck behind the backlog", agent)
                .await
                .unwrap();
            let _ = flood.await;
            let msg = format!("{}", res.unwrap_err());
            let n: u64 = msg
                .split(" events queued for resend")
                .next()
                .and_then(|r| r.rsplit(' ').next())
                .and_then(|n| n.parse().ok())
                .unwrap_or_else(|| panic!("no 'N events queued for resend' in error: {msg}"));
            // The bridge channel alone holds 256 frozen-link events.
            assert!(n >= 256, "count {n} too small: {msg}");
        }

        #[tokio::test]
        async fn stream_to_collector_reregisters_after_half_open() {
            let mut h = harness().await;
            let (ev_tx, ev_rx) = tokio::sync::mpsc::channel::<KernelEvent>(16);
            let agent = tokio::spawn(stream_to_collector_with(
                h.endpoint.clone(),
                "n1".into(),
                String::new(),
                None,
                None,
                fast_tuning(),
                ev_rx,
            ));

            within(5, "first register", h.registered.recv()).await;
            ev_tx.send(KernelEvent::default()).await.unwrap();
            within(5, "stream never got established", h.got_event.recv()).await;

            h.conns.freeze();

            // keepalive (~0.5s) + first backoff (~0.1s) + reconnect.
            within(
                5,
                "agent never re-registered after the link died",
                h.registered.recv(),
            )
            .await;
            ev_tx.send(KernelEvent::default()).await.unwrap();
            within(
                5,
                "events not flowing on the new connection",
                h.got_event.recv(),
            )
            .await;
            agent.abort();
        }

        /// #241: unique ids survive a link loss. `ack_only` freezes just the
        /// collector -> agent direction (events stored, acks lost).
        async fn resend_case(ack_only: bool) {
            const FIRST: usize = 50;
            const TOTAL: usize = 400;
            let mut h = harness().await;
            let (ev_tx, ev_rx) = tokio::sync::mpsc::channel::<KernelEvent>(16);
            let agent = tokio::spawn(stream_to_collector_with(
                h.endpoint.clone(),
                "n1".into(),
                String::new(),
                None,
                None,
                fast_tuning(),
                ev_rx,
            ));
            let ev = |i: usize| KernelEvent {
                event_id: format!("ev-{i}"),
                ..Default::default()
            };

            within(5, "first register", h.registered.recv()).await;
            for i in 0..FIRST {
                ev_tx.send(ev(i)).await.unwrap();
            }
            within(5, "stream never got established", h.got_event.recv()).await;

            if ack_only {
                h.conns.freeze_acks();
            } else {
                h.conns.freeze();
            }
            for i in FIRST..TOTAL {
                ev_tx.send(ev(i)).await.unwrap();
            }
            // Source exhausted; the agent may only finish once all are acked.
            drop(ev_tx);

            let res = within(20, "agent did not finish after reconnect", agent)
                .await
                .unwrap();
            assert!(res.is_ok(), "expected clean exit, got {res:?}");

            // Reconnected at least once.
            let mut registers = 0;
            while h.registered.try_recv().is_ok() {
                registers += 1;
            }
            assert!(registers >= 1, "agent never re-registered");

            let seen = h.seen.lock().unwrap().clone();
            let unique: std::collections::HashSet<_> = seen.iter().cloned().collect();
            let dups = seen.len() - unique.len();
            eprintln!(
                "ack_only={ack_only}: {} received, {dups} duplicates",
                seen.len()
            );
            assert_eq!(unique.len(), TOTAL, "events went missing");
            if ack_only {
                // Stored but un-acked events must have been sent again.
                assert!(dups > 0, "expected duplicates from lost acks");
            }
        }

        #[tokio::test]
        async fn no_events_lost_across_full_freeze() {
            resend_case(false).await;
        }

        #[tokio::test]
        async fn no_events_lost_when_acks_are_lost() {
            resend_case(true).await;
        }

        /// Collector whose `Register` always answers with `reply`.
        struct RegisterStub {
            reply: std::result::Result<RegisterResponse, Status>,
            calls: Arc<std::sync::atomic::AtomicUsize>,
        }

        #[tonic::async_trait]
        impl SentinelCollector for RegisterStub {
            async fn register(
                &self,
                _: Request<RegisterRequest>,
            ) -> std::result::Result<Response<RegisterResponse>, Status> {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.reply.clone().map(Response::new)
            }

            type StreamEventsStream =
                Pin<Box<dyn Stream<Item = std::result::Result<EventAck, Status>> + Send>>;

            async fn stream_events(
                &self,
                _: Request<Streaming<KernelEvent>>,
            ) -> std::result::Result<Response<Self::StreamEventsStream>, Status> {
                Err(Status::unimplemented("test stub"))
            }
        }

        /// Runs the agent against a collector stub; returns the agent's
        /// result (None if it was still retrying after `secs`) and the number
        /// of Register calls seen.
        async fn run_against_stub(
            reply: std::result::Result<RegisterResponse, Status>,
            secs: u64,
        ) -> (Option<Result<()>>, usize) {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = l.local_addr().unwrap();
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(SentinelCollectorServer::new(RegisterStub {
                        reply,
                        calls: calls.clone(),
                    }))
                    .serve_with_incoming(tokio_stream_from(l)),
            );
            // Keep the sender alive so a clean "source exhausted" exit can't
            // mask a retry loop.
            let (_ev_tx, ev_rx) = tokio::sync::mpsc::channel::<KernelEvent>(1);
            let res = tokio::time::timeout(
                Duration::from_secs(secs),
                stream_to_collector_with(
                    format!("http://{addr}"),
                    "n1".into(),
                    String::new(),
                    None,
                    None,
                    fast_tuning(),
                    ev_rx,
                ),
            )
            .await
            .ok();
            (res, calls.load(std::sync::atomic::Ordering::SeqCst))
        }

        #[tokio::test]
        async fn register_ok_false_is_fatal_and_reports_message() {
            let (res, calls) = run_against_stub(
                Ok(RegisterResponse {
                    ok: false,
                    message: "node_id does not match authenticated identity".into(),
                }),
                5,
            )
            .await;
            let err = res
                .expect("agent kept retrying a rejected registration")
                .unwrap_err();
            let text = format!("{err:#}");
            assert!(
                text.contains("node_id does not match authenticated identity"),
                "rejection reason missing from error: {text}"
            );
            assert_eq!(calls, 1, "must not retry a rejected registration");
        }

        #[tokio::test]
        async fn register_permanent_status_codes_are_fatal() {
            for st in [
                Status::invalid_argument("bad field"),
                Status::unauthenticated("bad token"),
                Status::permission_denied("nope"),
            ] {
                let code = st.code();
                let (res, calls) = run_against_stub(Err(st), 5).await;
                let err = res
                    .unwrap_or_else(|| panic!("{code:?}: agent kept retrying"))
                    .unwrap_err();
                assert!(
                    format!("{err:#}").contains("rejected"),
                    "{code:?}: error should say registration was rejected: {err:#}"
                );
                assert_eq!(calls, 1, "{code:?}: must not retry");
            }
        }

        #[tokio::test]
        async fn register_unavailable_is_retried() {
            let (res, calls) = run_against_stub(Err(Status::unavailable("restarting")), 2).await;
            assert!(res.is_none(), "transient error must keep retrying");
            assert!(calls >= 2, "expected retries, saw {calls} Register calls");
        }

        fn tokio_stream_from(l: TcpListener) -> impl Stream<Item = std::io::Result<TcpStream>> {
            async_stream::stream! {
                loop {
                    yield l.accept().await.map(|(s, _)| s);
                }
            }
        }
    }
}

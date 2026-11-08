use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::pb::{sentinel_collector_client::SentinelCollectorClient, KernelEvent, RegisterRequest};

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Cap on how long a single `connect()` attempt may block. Without this, a
/// firewall that silently drops packets leaves `.connect().await` hanging
/// until the OS/TCP stack's own timeout (which can be minutes), during which
/// `run_once` never returns `Err` and the backoff-reconnect loop in
/// `stream_to_collector` never runs (#108).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on how long an RPC may wait for the collector's *response headers*
/// once connected (`Register` end to end, and `StreamEvents` until its
/// headers arrive). tonic 0.11 applies `Endpoint::timeout` only to the
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
/// Worst-case detection of a half-open connection ≈ interval + timeout (40s).
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

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

    loop {
        match run_once(
            &endpoint,
            &node_id,
            &region,
            &hostname,
            auth_token.clone(),
            &ca_cert_path,
            ConnTuning::default(),
            &mut rx,
        )
        .await
        {
            // rx.recv() returned None: every event-source Sender was dropped,
            // so there will never be another event. Exit for good.
            Ok(()) => return Ok(()),
            Err(e) => {
                let wait = jittered(backoff);
                eprintln!("collector connection lost/failed: {e} — reconnecting in {wait:.1?}");
                tokio::time::sleep(wait).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
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

/// One connect→register→stream attempt. Returns `Ok(())` only when the event
/// source is exhausted (rx closed); any connection/RPC failure is returned as
/// `Err` for the caller to retry.
#[allow(clippy::too_many_arguments)]
async fn run_once(
    endpoint: &str,
    node_id: &str,
    region: &str,
    hostname: &str,
    auth_token: Option<MetadataValue<tonic::metadata::Ascii>>,
    ca_cert_path: &Option<String>,
    tuning: ConnTuning,
    rx: &mut Receiver<KernelEvent>,
) -> Result<()> {
    let mut builder = Endpoint::from_shared(endpoint.to_string())?
        .connect_timeout(tuning.connect_timeout)
        .timeout(tuning.rpc_timeout)
        .http2_keep_alive_interval(tuning.keepalive_interval)
        .keep_alive_timeout(tuning.keepalive_timeout)
        // Ping even with no open stream: covers the window between
        // connect and `StreamEvents` and keeps detection uniform.
        .keep_alive_while_idle(true);
    if endpoint.starts_with("https://") {
        let mut tls = ClientTlsConfig::new();
        if let Some(path) = ca_cert_path {
            let pem = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("read CA cert {path}: {e}"))?;
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        builder = builder.tls_config(tls)?;
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
        .await?;

    if !resp.into_inner().ok {
        anyhow::bail!("registration rejected by collector");
    }
    println!("registered with collector");

    // tonic's stream_events requires an owned ('static) request stream, so we
    // can't feed it directly from `rx` (borrowed — it must survive this
    // function returning on error, so the caller can retry with it intact).
    // Bridge through a fresh per-attempt channel instead: `bridge_rx` is
    // owned and moved into the generator (satisfying 'static), while this
    // function keeps driving the outer `rx` and forwarding into `bridge_tx`.
    let (bridge_tx, mut bridge_rx) = tokio::sync::mpsc::channel::<KernelEvent>(256);
    let outbound = async_stream::stream! {
        while let Some(event) = bridge_rx.recv().await {
            yield event;
        }
    };

    let mut response = client.stream_events(outbound).await?.into_inner();

    loop {
        tokio::select! {
            biased;

            ack = response.message() => {
                match ack? {
                    Some(a) => {
                        if !a.ok {
                            eprintln!("collector rejected event");
                        }
                    }
                    // Server closed the stream — a connection failure, not a
                    // clean shutdown; retry.
                    None => anyhow::bail!("collector closed the event stream"),
                }
            }

            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(mut event) => {
                        event.node_id = node_id.to_string();
                        if bridge_tx.send(event).await.is_err() {
                            anyhow::bail!("outbound stream to collector closed");
                        }
                    }
                    // Every event-source Sender was dropped — there will
                    // never be another event. Close the outbound stream
                    // cleanly, drain remaining acks, then signal "done".
                    None => {
                        drop(bridge_tx);
                        while let Some(ack) = response.message().await? {
                            if !ack.ok {
                                eprintln!("collector rejected event");
                            }
                        }
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn keepalive_defaults_are_bounded_and_respect_collector_policy() {
        // The collector's EnforcementPolicy MinTime is 10s; pinging faster
        // gets the connection GOAWAY'd. Detection should stay under ~1 min.
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
        }

        #[tonic::async_trait]
        impl SentinelCollector for FakeCollector {
            async fn register(
                &self,
                _: Request<RegisterRequest>,
            ) -> std::result::Result<Response<RegisterResponse>, Status> {
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
                let s = async_stream::stream! {
                    while let Ok(Some(_)) = inbound.message().await {
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

        async fn freezing_proxy(
            upstream: std::net::SocketAddr,
            frozen: Arc<AtomicBool>,
        ) -> std::net::SocketAddr {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    let server = TcpStream::connect(upstream).await.unwrap();
                    let (cr, cw) = client.into_split();
                    let (sr, sw) = server.into_split();
                    tokio::spawn(pipe(cr, sw, frozen.clone()));
                    tokio::spawn(pipe(sr, cw, frozen.clone()));
                }
            });
            addr
        }

        #[tokio::test]
        async fn run_once_errors_when_collector_goes_silent_mid_stream() {
            // Real tonic collector behind a proxy that can go silent.
            let srv_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let srv_addr = srv_listener.local_addr().unwrap();
            let (got_tx, mut got_rx) = tokio::sync::mpsc::channel(1);
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(SentinelCollectorServer::new(FakeCollector {
                        got_event: got_tx,
                    }))
                    .serve_with_incoming(tokio_stream_from(srv_listener)),
            );
            let frozen = Arc::new(AtomicBool::new(false));
            let proxy_addr = freezing_proxy(srv_addr, frozen.clone()).await;

            let tuning = ConnTuning {
                connect_timeout: Duration::from_secs(5),
                // Deliberately long: the stream must be cut by keepalive,
                // not by the header timeout.
                rpc_timeout: Duration::from_secs(60),
                keepalive_interval: Duration::from_millis(200),
                keepalive_timeout: Duration::from_millis(300),
            };
            let (ev_tx, mut ev_rx) = tokio::sync::mpsc::channel::<KernelEvent>(16);
            let endpoint = format!("http://{proxy_addr}");
            let agent = tokio::spawn(async move {
                run_once(&endpoint, "n1", "", "h", None, &None, tuning, &mut ev_rx).await
            });

            ev_tx.send(KernelEvent::default()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), got_rx.recv())
                .await
                .expect("stream never got established")
                .unwrap();

            // Collector host "disappears": no FIN, no RST, no PING acks.
            frozen.store(true, Ordering::SeqCst);

            let res = tokio::time::timeout(Duration::from_secs(5), agent)
                .await
                .expect("run_once did not detect the half-open connection (keepalive missing?)")
                .unwrap();
            assert!(
                res.is_err(),
                "expected Err so the reconnect loop runs, got {res:?}"
            );
            drop(ev_tx);
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

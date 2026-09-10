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

/// Cap on how long any single RPC (register, or a message on the event
/// stream) may take once connected, so a collector that accepts the TCP
/// connection but then stalls can't hang the agent indefinitely either.
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

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
    rx: &mut Receiver<KernelEvent>,
) -> Result<()> {
    let mut builder = Endpoint::from_shared(endpoint.to_string())?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT);
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
}

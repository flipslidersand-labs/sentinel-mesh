use anyhow::Result;
use clap::Parser;
use uuid::Uuid;

pub mod pb {
    tonic::include_proto!("sentinel.v1");
}

mod events;
mod grpc;

#[derive(Parser)]
#[command(name = "sentinel-agent", about = "SentinelMesh eBPF agent")]
struct Args {
    /// Go Collector gRPC endpoint
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    collector: String,

    /// Node identifier (defaults to hostname)
    #[arg(long)]
    node_id: Option<String>,

    /// Generate mock events instead of loading real eBPF (for testing)
    #[arg(long, default_value_t = false)]
    mock: bool,

    /// Events per second in mock mode (1..=1000)
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..=1000))]
    mock_rate: u64,

    /// Region this node belongs to (falls back to $SENTINEL_REGION, then empty)
    #[arg(long, env = "SENTINEL_REGION")]
    region: Option<String>,

    /// Bearer token sent with every RPC (falls back to $SENTINEL_API_TOKEN, none = unauthenticated).
    /// Must be a per-agent token issued via `sentinel-collector token issue <node_id>` matching
    /// this agent's --node-id, not the collector's REST API token — the gRPC channel's per-agent
    /// token store doesn't recognize that one and will reject it (#183/#190, per ADR-005).
    #[arg(long, env = "SENTINEL_API_TOKEN")]
    grpc_token: Option<String>,

    /// CA certificate (PEM) to verify the collector's TLS cert when --collector uses https://;
    /// omit to use the system trust store (e.g. a publicly-signed cert)
    #[arg(long, env = "SENTINEL_GRPC_CA_CERT")]
    grpc_ca_cert: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let node_id = args.node_id.unwrap_or_else(|| {
        hostname::get()
            .map(|h| h.to_string_lossy().into_owned())
            .unwrap_or_else(|_| Uuid::new_v4().to_string())
    });

    let region = args.region.unwrap_or_default();

    println!(
        "sentinel-agent starting: node={node_id} region={region} collector={}",
        args.collector
    );

    let (tx, rx) = tokio::sync::mpsc::channel(256);

    // Start event source; keep the handle so its exit is not silent (#348)
    let source = if args.mock {
        println!("mode: mock ({} events/sec)", args.mock_rate);
        tokio::spawn(events::mock_source(tx, args.mock_rate))
    } else {
        #[cfg(feature = "ebpf")]
        {
            println!("mode: eBPF (requires CAP_BPF)");
            tokio::spawn(events::ebpf_source(tx))
        }
        #[cfg(not(feature = "ebpf"))]
        {
            eprintln!("error: compiled without --features ebpf; use --mock for testing");
            std::process::exit(1);
        }
    };

    if args.collector.starts_with("http://") {
        eprintln!("warning: connecting to collector over plaintext (http://) — use https:// once the collector has --grpc-tls-cert/--grpc-tls-key configured");
    }
    if args.grpc_token.is_none() {
        eprintln!(
            "warning: no --grpc-token/$SENTINEL_API_TOKEN set — RPCs will be unauthenticated"
        );
    }

    // Connect and stream to collector
    supervise_source(
        source,
        grpc::stream_to_collector(
            args.collector,
            node_id,
            region,
            args.grpc_token,
            args.grpc_ca_cert,
            rx,
        ),
    )
    .await
}

/// Run `stream` while watching the event source task. Sources run forever, so any
/// exit (load/attach failure, panic, early return) is an error: returning Err makes
/// the process exit non-zero so systemd `Restart=` can recover, instead of sending
/// heartbeats with zero events (#348).
async fn supervise_source<F>(mut source: tokio::task::JoinHandle<()>, stream: F) -> Result<()>
where
    F: std::future::Future<Output = Result<()>>,
{
    tokio::select! {
        // Sources run forever, so the stream ending with Ok(()) means the channel
        // closed because the source died (tx dropped): that is an error too.
        r = stream => match r {
            Ok(()) => Err(anyhow::anyhow!("event source closed the channel")),
            e => e,
        },
        j = &mut source => match j {
            Ok(()) => Err(anyhow::anyhow!("event source exited unexpectedly")),
            Err(e) => Err(anyhow::anyhow!("event source task failed: {e}")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_rate(v: &str) -> Result<Args, clap::Error> {
        Args::try_parse_from(["sentinel-agent", "--mock-rate", v])
    }

    #[tokio::test]
    async fn source_exit_is_an_error() {
        let source = tokio::spawn(async {});
        let r = supervise_source(source, std::future::pending::<Result<()>>()).await;
        assert!(r.unwrap_err().to_string().contains("event source"));
    }

    #[tokio::test]
    async fn source_exit_with_closed_channel_is_always_an_error() {
        // Source exit drops tx -> stream sees rx closed and returns Ok(()); both
        // select! branches become ready, so repeat to catch the race.
        for _ in 0..100 {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<u8>(1);
            let source = tokio::spawn(async move { drop(tx) });
            let stream = async move {
                rx.recv().await;
                Ok(())
            };
            assert!(supervise_source(source, stream).await.is_err());
        }
    }

    #[tokio::test]
    async fn source_panic_is_an_error() {
        let source = tokio::spawn(async { panic!("boom") });
        let r = supervise_source(source, std::future::pending::<Result<()>>()).await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn stream_result_wins_while_source_runs() {
        let source = tokio::spawn(std::future::pending::<()>());
        let r = supervise_source(source, async { Err(anyhow::anyhow!("stream down")) }).await;
        assert_eq!(r.unwrap_err().to_string(), "stream down");
    }

    #[test]
    fn mock_rate_rejects_out_of_range() {
        // 1001+ made 1000/rate == 0 and tokio::time::interval panicked (#271).
        for v in ["0", "1001", "5000", "-1"] {
            assert!(parse_rate(v).is_err(), "--mock-rate {v} must be rejected");
        }
    }

    #[test]
    fn mock_rate_accepts_bounds() {
        assert_eq!(parse_rate("1").unwrap().mock_rate, 1);
        assert_eq!(parse_rate("1000").unwrap().mock_rate, 1000);
    }
}

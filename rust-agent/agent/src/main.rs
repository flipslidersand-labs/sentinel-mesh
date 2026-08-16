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

    /// Events per second in mock mode
    #[arg(long, default_value_t = 2)]
    mock_rate: u64,

    /// Region this node belongs to (falls back to $SENTINEL_REGION, then empty)
    #[arg(long, env = "SENTINEL_REGION")]
    region: Option<String>,
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

    // Start event source
    if args.mock {
        println!("mode: mock ({} events/sec)", args.mock_rate);
        tokio::spawn(events::mock_source(tx, args.mock_rate));
    } else {
        #[cfg(feature = "ebpf")]
        {
            println!("mode: eBPF (requires CAP_BPF)");
            tokio::spawn(events::ebpf_source(tx));
        }
        #[cfg(not(feature = "ebpf"))]
        {
            eprintln!("error: compiled without --features ebpf; use --mock for testing");
            std::process::exit(1);
        }
    }

    // Connect and stream to collector
    grpc::stream_to_collector(args.collector, node_id, region, rx).await
}

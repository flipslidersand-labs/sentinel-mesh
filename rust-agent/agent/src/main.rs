use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(name = "sentinel-agent", about = "SentinelMesh eBPF agent")]
struct Args {
    /// Go Collector の gRPC エンドポイント
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    collector: String,

    /// このノードの識別子 (未指定時はホスト名)
    #[arg(long)]
    node_id: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let node_id = args
        .node_id
        .unwrap_or_else(|| hostname::get().unwrap_or_default().to_string_lossy().into());

    println!("sentinel-agent starting: node={node_id} collector={}", args.collector);
    // Phase 1: eBPF ロード + ring_buf ループをここに実装
    Ok(())
}

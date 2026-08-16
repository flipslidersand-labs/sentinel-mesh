use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tonic::transport::Channel;

use crate::pb::{
    sentinel_collector_client::SentinelCollectorClient, KernelEvent, RegisterRequest,
};

pub async fn stream_to_collector(
    endpoint: String,
    node_id: String,
    region: String,
    mut rx: Receiver<KernelEvent>,
) -> Result<()> {
    let channel = Channel::from_shared(endpoint.clone())?
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("connect to {endpoint}: {e}"))?;

    let mut client = SentinelCollectorClient::new(channel);

    // Register this node
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_else(|_| node_id.clone());

    let resp = client
        .register(RegisterRequest {
            node_id: node_id.clone(),
            hostname,
            ip: String::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            region,
        })
        .await?;

    if !resp.into_inner().ok {
        anyhow::bail!("registration rejected by collector");
    }
    println!("registered with collector");

    // Stream events
    let node_id_clone = node_id.clone();
    let outbound = async_stream::stream! {
        while let Some(mut event) = rx.recv().await {
            event.node_id = node_id_clone.clone();
            yield event;
        }
    };

    let mut response = client.stream_events(outbound).await?.into_inner();
    while let Some(ack) = response.message().await? {
        if !ack.ok {
            eprintln!("collector rejected event");
        }
    }

    Ok(())
}

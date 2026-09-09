use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::pb::{sentinel_collector_client::SentinelCollectorClient, KernelEvent, RegisterRequest};

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

pub async fn stream_to_collector(
    endpoint: String,
    node_id: String,
    region: String,
    token: Option<String>,
    ca_cert_path: Option<String>,
    mut rx: Receiver<KernelEvent>,
) -> Result<()> {
    let mut builder = Endpoint::from_shared(endpoint.clone())?;
    if endpoint.starts_with("https://") {
        let mut tls = ClientTlsConfig::new();
        if let Some(path) = &ca_cert_path {
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

    let auth_token = token
        .map(|t| MetadataValue::try_from(format!("Bearer {t}")))
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid token: {e}"))?;
    let mut client =
        SentinelCollectorClient::with_interceptor(channel, AuthInterceptor { token: auth_token });

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

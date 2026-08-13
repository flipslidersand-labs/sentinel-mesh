# sentinel-mesh

[![CI](https://github.com/flipslidersand/sentinel-mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/flipslidersand/sentinel-mesh/actions/workflows/ci.yml)

eBPF-based distributed observability platform for kernel-level system monitoring.

A **Rust Agent** collects kernel events via eBPF and streams them over gRPC to a **Go Collector**, which persists events in BadgerDB and exposes a REST API. Heartbeat tracking automatically marks unresponsive agents as inactive.

## Architecture

```text
┌─────────────────────────────────┐       ┌────────────────────────────────────┐
│  Rust Agent (sentinel-agent)    │       │  Go Collector (sentinel-collector) │
│                                 │       │                                    │
│  eBPF kernel program            │ gRPC  │  Receiver (gRPC server)            │
│  → event stream (tokio/tonic)  │──────▶│  → Store (BadgerDB)                │
│                                 │       │  → Registry (heartbeat tracking)   │
│  mock mode available (--mock)   │       │  → REST API (Gin, :8081)           │
└─────────────────────────────────┘       └────────────────────────────────────┘
```

## Requirements

- Rust 1.82+ (`cargo`)
- Go 1.22+ (`go`)
- `CAP_BPF` capability for real eBPF mode; mock mode works without it

## Build

```bash
# Rust Agent (mock mode — no kernel privileges required)
cd rust-agent/agent
cargo build --release

# Rust Agent (real eBPF mode — requires CAP_BPF)
cargo build --release --features ebpf

# Go Collector
cd go-collector
go build ./cmd/collector
```

## Quick Start

```bash
# Terminal 1: Start the Go Collector (with alerting rules)
cd go-collector
./collector serve --grpc-addr :50051 --http-addr :8081 --data-dir /tmp/sentinel-data --rules ../rules.yaml

# Terminal 2: Start the Rust Agent (mock mode)
cd rust-agent/agent
./sentinel-agent --mock --mock-rate 5
# → streams 5 mock kernel events/sec to localhost:50051

# Query the REST API
curl http://localhost:8081/api/events                        # list events (default limit 100)
curl "http://localhost:8081/api/events?node=web-01&limit=20"
curl http://localhost:8081/api/nodes                         # list registered agents + status
curl http://localhost:8081/api/stats                         # event counts by type
curl http://localhost:8081/api/alerts                        # list triggered alerts
curl http://localhost:8081/metrics                           # Prometheus metrics
curl http://localhost:8081/healthz                           # health check
```

## Agent CLI

```text
sentinel-agent [OPTIONS]

Options:
  --collector <URL>     gRPC endpoint of the Go Collector [default: http://127.0.0.1:50051]
  --node-id <ID>        Node identifier [default: hostname]
  --mock                Generate mock events instead of loading eBPF
  --mock-rate <N>       Events per second in mock mode [default: 2]
```

## Collector CLI

```text
sentinel-collector serve [OPTIONS]

Options:
  --grpc-addr <ADDR>         gRPC listen address [default: :50051]
  --http-addr <ADDR>         REST API listen address [default: :8081]
  --data-dir <PATH>          BadgerDB data directory [default: /tmp/sentinel-data]
  --heartbeat-timeout <DUR>  Inactivity before agent is marked inactive [default: 60s]
```

## REST API

| Endpoint          | Description                                                   |
| ----------------- | ------------------------------------------------------------- |
| `GET /healthz`    | Health check                                                  |
| `GET /api/events` | List events. Query params: `node=<id>`, `limit=<N>`           |
| `GET /api/nodes`  | List registered agents with `active` / `inactive` status      |
| `GET /api/stats`  | Event counts per event type                                   |
| `GET /api/alerts` | List triggered alerts. Query params: `node=<id>`, `limit=<N>` |

## Heartbeat Tracking

The collector runs a background goroutine that checks agent liveness every `heartbeat-timeout / 2`. An agent is marked `inactive` when its last gRPC message is older than `heartbeat-timeout`. Status is visible in `GET /api/nodes`.

## Directory Structure

```text
sentinel-mesh/
├── rust-agent/
│   ├── agent/               # Rust userspace agent (tokio + tonic gRPC client)
│   │   └── src/
│   │       ├── main.rs      # CLI entry, event routing
│   │       ├── events.rs    # mock_source / ebpf_source
│   │       └── grpc.rs      # tonic streaming to collector
│   └── agent-ebpf/          # eBPF kernel program (aya)
│       └── src/main.rs
├── go-collector/
│   ├── cmd/collector/       # CLI (cobra) — serve subcommand
│   └── internal/
│       ├── receiver/        # gRPC server (registers nodes, stores events)
│       ├── registry/        # Agent registry + heartbeat checker
│       ├── store/           # BadgerDB wrapper (events)
│       ├── exporter/        # Gin REST API router
│       └── pb/              # Generated protobuf/gRPC stubs
├── proto/
│   └── sentinel.v1.proto    # Event + AgentService proto definition
└── docs/
    ├── spec.md
    ├── tech-stack.md
    └── adr/
```

## Tech Stack

| Layer                       | Technology                            |
| --------------------------- | ------------------------------------- |
| Agent runtime               | Rust / tokio                          |
| eBPF                        | aya                                   |
| Agent → Collector transport | gRPC (tonic / google.golang.org/grpc) |
| Collector HTTP              | Gin                                   |
| Persistence                 | BadgerDB                              |
| Logging                     | go.uber.org/zap                       |
| CLI                         | clap (Rust) / cobra (Go)              |
| CI                          | GitHub Actions                        |

## Multi-Node Demo

Deploy Collector on one node and Agents on multiple nodes in a local network.

### Prerequisites

- SSH access to target nodes (passwordless key auth)
- Rust toolchain with `x86_64-unknown-linux-gnu` target (`rustup target add x86_64-unknown-linux-gnu`)
- Go 1.22+

### Quick Start (3 nodes)

```bash
# 1. Build all binaries (cross-compiled for Linux/amd64)
bash scripts/build.sh

# 2. Deploy Collector to MINIPC (systemd service)
bash scripts/install-collector.sh minipc

# 3. Deploy Agents to each node (mock mode, 3 events/sec)
bash scripts/deploy-agent.sh minipc  minipc  192.168.68.63:50051
bash scripts/deploy-agent.sh yuki    yuki    192.168.68.63:50051
bash scripts/deploy-agent.sh ds1     ds1     192.168.68.63:50051

# Or run everything at once (skips unreachable nodes automatically)
bash scripts/demo-multi-node.sh
```

### Verify

```bash
# All registered nodes (active/inactive)
curl http://192.168.68.63:8081/api/nodes

# Events from a specific node
curl "http://192.168.68.63:8081/api/events?node=yuki&limit=20"

# Dashboard
open http://192.168.68.63:8081
```

### Node Layout

| Role      | Host            | IP            | SSH alias      |
| --------- | --------------- | ------------- | -------------- |
| Collector | MINIPC          | 192.168.68.63 | `minipc`       |
| Agent     | YUKI-PRIVATE002 | 192.168.68.56 | `yuki-private` |
| Agent     | DS1HANAHANA     | 192.168.68.59 | `ds1`          |

### Switching to real eBPF mode

```bash
# On each agent host, replace --mock with CAP_BPF capability:
sudo setcap cap_bpf+eip /usr/local/bin/sentinel-agent
# Edit the systemd service to remove --mock and --mock-rate flags
sudo systemctl edit sentinel-agent-<NODE_ID>
sudo systemctl restart sentinel-agent-<NODE_ID>
```

## Status

| Phase | Description                                    | Status  |
| ----- | ---------------------------------------------- | ------- |
| 1     | Rust Agent — eBPF kernel program + mock source | ✅ Done |
| 2     | Proto definition + Go Collector gRPC server    | ✅ Done |
| 3     | BadgerDB store + Gin REST API                  | ✅ Done |
| 4     | Heartbeat timeout — stale agent detection      | ✅ Done |
| 5     | Alerting engine — rules.yaml + evaluation      | ✅ Done |
| 6     | OpenTelemetry metrics + distributed traces     | ✅ Done |
| 7     | Node filter on `GET /api/events`               | ✅ Done |
| 8     | Web UI dashboard (React + Vite)                | ✅ Done |
| 9     | Multi-node deploy scripts                      | ✅ Done |

## License

MIT

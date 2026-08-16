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

## Event Types

The agent collects kernel events via eBPF tracepoints and kprobes. Each event is tagged with a type:

| Type   | Trigger                       | Example                                  |
| ------ | ----------------------------- | ---------------------------------------- |
| `exec` | `sys_enter_execve` tracepoint | Process execution (binary, args)         |
| `tcp`  | `tcp_connect` kprobe          | TCP connection attempt (src/dst IP/port) |
| `file` | `sys_enter_openat` tracepoint | File open operation (path, flags)        |

In **mock mode** (`--mock`), events are synthetic and evenly distributed across types. In **eBPF mode** (requires `CAP_BPF`), events are captured from running kernel.

## Agent CLI

```text
sentinel-agent [OPTIONS]

Options:
  --collector <URL>     gRPC endpoint of the Go Collector [default: http://127.0.0.1:50051]
  --node-id <ID>        Node identifier [default: hostname]
  --mock                Generate mock events instead of loading eBPF
  --mock-rate <N>       Events per second in mock mode [default: 2]
  --region <NAME>       Region this node belongs to [env: SENTINEL_REGION]
```

## Collector CLI

```text
sentinel-collector serve [OPTIONS]

Options:
  --grpc-addr <ADDR>         gRPC listen address [default: :50051]
  --http-addr <ADDR>         REST API listen address [default: :8081]
  --data-dir <PATH>          BadgerDB data directory [default: /tmp/sentinel-data]
  --heartbeat-timeout <DUR>  Inactivity before agent is marked inactive [default: 60s]
  --region <NAME>            Default region for agents that register without one [default: default]

  # Aggregate mode (cross-region, read-only — no gRPC)
  --aggregate                Run as a cross-region aggregator
  --upstreams <region=url>   Region collectors to poll (repeatable)
  --poll-interval <DUR>      How often to poll upstreams [default: 10s]
```

## Multi-Region Aggregation

For geographically distributed deployments, run one collector per region (each with its own agents), then run a **cross-region aggregator** on top. The aggregator periodically polls each region collector's REST API and serves a unified, read-only view. Region collectors need no changes.

```bash
# Region collectors (one per region, agents register with --region)
./collector serve --region us-east --http-addr :8081   # on us-east host
./collector serve --region eu-west --http-addr :8081   # on eu-west host

# Aggregator (polls both, serves merged view + UI)
./collector serve --aggregate \
  --upstreams us-east=http://10.0.1.10:8081 \
  --upstreams eu-west=http://10.0.2.10:8081 \
  --http-addr :8080 --poll-interval 10s

curl http://localhost:8080/api/regions
# → [{"region":"eu-west","node_count":3,"active_count":3,"reachable":true},
#    {"region":"us-east","node_count":5,"active_count":4,"reachable":true}]
```

A region whose collector is unreachable is **isolated**: it is reported with `"reachable": false` and an `error`, while other regions continue to serve. The aggregator exposes `/api/nodes` (with `?region=`), `/api/events`, `/api/alerts`, and `/api/regions`.

## REST API

| Endpoint                 | Description                                                                            |
| ------------------------ | -------------------------------------------------------------------------------------- |
| `GET /healthz`           | Health check                                                                           |
| `GET /api/events`        | List events. Query params: `node=<id>`, `region=<name>`, `limit=<N>`                   |
| `GET /api/nodes`         | List registered agents with `active` / `inactive` status. Query param: `region=<name>` |
| `GET /api/regions`       | Per-region roll-up: `{region, node_count, active_count}`                               |
| `GET /api/stats`         | Event counts per event type                                                            |
| `GET /api/stats/windows` | Anomaly detector window stats. Aggregated by event type × window (1m/5m)               |
| `GET /api/alerts`        | List triggered alerts. Query params: `node=<id>`, `region=<name>`, `limit=<N>`         |

## Heartbeat Tracking

The collector runs a background goroutine that checks agent liveness every `heartbeat-timeout / 2`. An agent is marked `inactive` when its last gRPC message is older than `heartbeat-timeout`. Status is visible in `GET /api/nodes`.

## Anomaly Detection

The collector implements **sliding-window frequency-based anomaly detection**. Per-(node, event_type) counters are maintained over configured time windows (default: 1-minute and 5-minute). When event count exceeds a threshold within a window, an anomaly alert is fired and saved to the store.

### Default Windows

| Window | Threshold  | Behavior                           |
| ------ | ---------- | ---------------------------------- |
| 1m     | 30 events  | Alert if 30+ events/min per type   |
| 5m     | 100 events | Alert if 100+ events/5min per type |

### Query Window Stats

```bash
curl http://localhost:8081/api/stats/windows
# → {"exec":{"1m":4,"5m":12},"tcp":{"1m":0,"5m":2},"file":{"1m":1,"5m":3}}
```

Window thresholds are configured in `internal/anomaly/detector.go`; modify `DefaultWindows` to adjust sensitivity.

## Alert Notifications

Triggered alerts (from both the rules engine and the anomaly detector) can be forwarded to **Slack** and/or **email**. Notifications are configured entirely through environment variables so that secrets never enter the rules YAML, CLI flags, or version control. If no channel is configured, notification dispatch is a no-op.

| Variable                       | Channel | Description                                                                |
| ------------------------------ | ------- | -------------------------------------------------------------------------- |
| `SENTINEL_SLACK_WEBHOOK_URL`   | Slack   | Incoming-webhook URL. Enables Slack when set.                              |
| `SENTINEL_SMTP_ADDR`           | Email   | SMTP server `host:port`. Enables email when set.                           |
| `SENTINEL_SMTP_FROM`           | Email   | Sender address (required for email).                                       |
| `SENTINEL_SMTP_TO`             | Email   | Comma-separated recipients (required for email).                           |
| `SENTINEL_SMTP_USER`           | Email   | SMTP username. Omit for unauthenticated relays.                            |
| `SENTINEL_SMTP_PASSWORD`       | Email   | SMTP password (PLAIN auth, used only when user is set).                    |
| `SENTINEL_NOTIFY_MIN_SEVERITY` | Both    | Minimum severity to notify (default `warning`).                            |
| `SENTINEL_NOTIFY_COOLDOWN`     | Both    | Min gap between notifications per (rule, node); Go duration, default `5m`. |

```bash
# Example: Slack-only, notify on warning+ with a 2-minute cooldown
export SENTINEL_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/XXX/YYY/ZZZ"
export SENTINEL_NOTIFY_COOLDOWN=2m
./collector serve --rules ../rules.yaml
```

Severity ordering: `info`/`low` < `warning`/`warn`/`medium` < `high`/`critical`/`error`. The per-(rule, node) cooldown suppresses duplicate alert storms; a distinct node or rule notifies independently. Delivery is best-effort and runs off the event hot path — a failing channel is logged and never blocks event ingestion.

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

### Deploy Scripts

#### `scripts/build.sh`

Cross-compiles Rust and Go binaries for Linux/amd64 (even on macOS) and places output in `dist/`.

```bash
bash scripts/build.sh
# → dist/sentinel-agent, dist/sentinel-collector
```

#### `scripts/install-collector.sh`

Deploys the collector binary to a remote host and installs as a systemd service.

```bash
bash scripts/install-collector.sh <SSH_ALIAS> [collector_addr] [http_addr]
# Example:
bash scripts/install-collector.sh minipc :50051 :8081
```

#### `scripts/deploy-agent.sh`

Deploys an agent to a remote host, registers with the collector, and starts as a systemd service. Runs in mock mode by default.

```bash
bash scripts/deploy-agent.sh <SSH_ALIAS> <NODE_ID> <COLLECTOR_ADDR> [--ebpf]
# Example:
bash scripts/deploy-agent.sh yuki    yuki    192.168.68.63:50051
bash scripts/deploy-agent.sh ds1     ds1     192.168.68.63:50051 --ebpf
```

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
| 7     | Anomaly detection — sliding-window frequency   | ✅ Done |
| 8     | Web UI dashboard (React + Vite)                | ✅ Done |
| 9     | Multi-node deploy scripts + systemd service    | ✅ Done |

## License

MIT

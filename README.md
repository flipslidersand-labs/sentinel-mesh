# sentinel-mesh

[![CI](https://github.com/flipslidersand-labs/sentinel-mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/flipslidersand-labs/sentinel-mesh/actions/workflows/ci.yml)

Release type: `service`

> 日本語の概要は [下の「日本語」節](#日本語) にあります。

**Version:** v0.1.0 (latest tag). Changes since then: [CHANGELOG — Unreleased](CHANGELOG.md#unreleased).

**Docs:** [Specification](docs/spec.md) · [Data model](docs/data-model.md) · [Tech stack](docs/tech-stack.md) · [Implementation guide](docs/implementation-guide.md) · [ADRs](docs/adr/) · [CHANGELOG](CHANGELOG.md)

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
- Go 1.26+ (`go`)
- `CAP_BPF` capability for real eBPF mode; mock mode works without it

## Build

```bash
# Run from the repository root.

# Rust Agent (mock mode — no kernel privileges required)
# The crate is a workspace member, so the binary lands in rust-agent/target/release/sentinel-agent
(cd rust-agent && cargo build --release -p sentinel-agent)

# Rust Agent (real eBPF mode — requires CAP_BPF; see rust-agent/agent/Cargo.toml [features] for prerequisites)
(cd rust-agent && cargo build --release -p sentinel-agent --features ebpf)

# Go Collector (binary: go-collector/collector)
(cd go-collector && go build ./cmd/collector)
```

## Quick Start

```bash
# Terminal 1: Start the Go Collector (with alerting rules)
# --grpc-auth disabled: local demo only (gRPC defaults to `required`; see "gRPC TLS & Authentication")
cd go-collector
./collector serve --grpc-addr :50051 --http-addr :8081 --data-dir /tmp/sentinel-data --rules ../rules.yaml --grpc-auth disabled --http-auth disabled

# Terminal 2: Start the Rust Agent (mock mode), from the repository root
./rust-agent/target/release/sentinel-agent --mock --mock-rate 5
# → streams 5 mock kernel events/sec to localhost:50051

# Query the REST API
curl http://localhost:8081/api/events                        # list events (default limit 100)
curl "http://localhost:8081/api/events?node=web-01&limit=20"
curl http://localhost:8081/api/nodes                         # list registered agents + status
curl http://localhost:8081/api/stats                         # event counts by type
curl http://localhost:8081/api/alerts                        # list triggered alerts
curl -H "Authorization: Bearer $SENTINEL_API_TOKEN" http://localhost:8081/metrics   # Prometheus metrics (omit header with --http-auth disabled)
curl http://localhost:8081/healthz                           # health check
```

## Event Types

The agent collects kernel events via eBPF tracepoints and kprobes. Each event is tagged with a type:

| Type   | Trigger                       | Example                                  |
| ------ | ----------------------------- | ---------------------------------------- |
| `exec` | `sched_process_exec` tracepoint | Process execution (binary, args)         |
| `tcp`  | `tcp_connect` kprobe          | TCP connection attempt (src/dst IP/port; IPv4, IPv6 and v4-mapped; not yet verified on a real host) |
| `file` | `sys_enter_openat` + `sys_exit_openat` tracepoints | File open (path, ret from sys_exit_openat) |

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
  --grpc-token <TOKEN>  Bearer token sent with every RPC [env: SENTINEL_API_TOKEN]
  --grpc-ca-cert <PATH> CA cert (PEM) to verify a self-signed collector TLS cert [env: SENTINEL_GRPC_CA_CERT]
  --allow-insecure-token  Allow --grpc-token over plaintext http:// to a non-loopback collector (insecure; default: refused)
```

## Collector CLI

```text
sentinel-collector serve [OPTIONS]

Options:
  --grpc-addr <ADDR>         gRPC listen address [default: :50051]
  --http-addr <ADDR>         REST API listen address [default: :8081]
  --http-tls-cert <PATH>     REST API TLS certificate (PEM); requires --http-tls-key
  --http-tls-key <PATH>      REST API TLS private key (PEM); requires --http-tls-cert
                             (without both, the REST API and its bearer token are plaintext; a warning is logged)
  --data-dir <PATH>          BadgerDB data directory [default: /tmp/sentinel-data]
                             (the /tmp default is for development only; for production use scripts/install-collector.sh or set --data-dir explicitly)
  --heartbeat-timeout <DUR>  Inactivity before agent is marked inactive [default: 60s]
  --rules <PATH>             Alerting rules YAML file [default: none]
  --otel-endpoint <URL>      OTLP HTTP endpoint for distributed tracing, e.g. http://localhost:4318 [default: none]
  --static-dir <PATH>        Static UI directory [default: ./static]
  --cors-origins <ORIGINS>   Allowed CORS origins, comma-separated or repeated
                             [default: none = deny all cross-origin; bundled UI is same-origin]
  --region <NAME>            Default region for agents that register without one [default: default]
  --grpc-tls-cert <PATH>     gRPC server TLS certificate (PEM); requires --grpc-tls-key
  --grpc-tls-key <PATH>      gRPC server TLS private key (PEM); requires --grpc-tls-cert
  --grpc-auth <MODE>         gRPC auth: required | disabled [default: required]
  --http-auth <MODE>         REST auth: required | disabled [default: required]
                             (required: startup fails unless SENTINEL_API_TOKEN is set)

  # Aggregate mode (cross-region, read-only — no gRPC)
  --aggregate                Run as a cross-region aggregator
  --upstreams <region=url>   Region collectors to poll (repeatable)
  --poll-interval <DUR>      How often to poll upstreams [default: 10s]
                             (--upstreams/--poll-interval require --aggregate)
  --aggregate-allow-insecure-http
                             Allow plaintext http:// upstreams on non-loopback hosts
                             (default: rejected; loopback and https:// are always allowed)

  # In --aggregate mode, these normal-mode flags are ignored (a warning is logged
  # if set): --grpc-addr, --data-dir, --rules, --grpc-tls-cert, --grpc-tls-key,
  # --grpc-auth, --region. --http-addr, --http-auth, --static-dir and
  # --cors-origins still apply.
  # --otel-endpoint and --heartbeat-timeout are also not used in --aggregate
  # mode (no warning is logged).
```

```text
sentinel-collector token <issue|revoke|list> [OPTIONS]

Manages per-agent gRPC authentication tokens (see gRPC TLS & Authentication
below). `token revoke <node_id>` also works while the collector is running
(it is sent to `serve` over a local Unix socket, <data-dir>/admin/admin.sock).
`token issue` and `token list` work the same way. Only `token revoke --force`
requires the collector to be stopped first — BadgerDB takes an exclusive lock
on --data-dir, so it can't run alongside `serve` against the same directory.

Options:
  --data-dir <PATH>  BadgerDB data directory (must match the collector's --data-dir)
                     [default: /tmp/sentinel-data]

token issue <node_id>    Issue a new token, printed once (never stored in plaintext)
token revoke <node_id>   Revoke a token (also while serve is running); prompts for
                         confirmation unless --yes
token revoke --force <key> Delete a damaged (undecodable) record by the key shown in the
                         `token list` warning (e.g. agenttoken:<node_id>); no decoding
token list               List every node_id with issued time and active/revoked status
```

## Multi-Region Aggregation

For geographically distributed deployments, run one collector per region (each with its own agents), then run a **cross-region aggregator** on top. The aggregator periodically polls each region collector's REST API and serves a unified, read-only view. Region collectors need no changes.

```bash
# Region collectors (one per region, agents register with --region)
# (demo: --grpc-auth disabled; in production keep the default `required` and issue per-agent tokens)
./collector serve --region us-east --http-addr :8081 --grpc-auth disabled --http-auth disabled   # on us-east host
./collector serve --region eu-west --http-addr :8081 --grpc-auth disabled --http-auth disabled   # on eu-west host

# Aggregator (polls both, serves merged view + UI)
./collector serve --aggregate \
  --upstreams us-east=https://10.0.1.10:8081 \
  --upstreams eu-west=https://10.0.2.10:8081 \
  --http-addr :8080 --poll-interval 10s --http-auth disabled   # demo; production: set SENTINEL_API_TOKEN
# Region collectors need --http-tls-cert/--http-tls-key for https:// upstreams. Plaintext http://
# upstreams on non-loopback hosts are refused unless --aggregate-allow-insecure-http is passed.

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

### API Authentication & CORS

- **Authentication**: REST auth is controlled by `--http-auth=required|disabled` (default
  `required`), in both normal and `--aggregate` mode. With `required`, set `SENTINEL_API_TOKEN` to
  require `Authorization: Bearer <token>` on all `/api/*` endpoints (constant-time compared); if it
  is unset the collector **refuses to start**. Unauthenticated REST needs the explicit
  `--http-auth=disabled` (the collector then logs a warning) — only acceptable on a trusted
  network. `/healthz` remains open for probes; `/metrics` is behind the same Bearer auth as `/api/*`
  (Prometheus needs `authorization` credentials, or `--http-auth=disabled`).
  (Breaking change, #254: previously an unset token only logged a warning.)

  ```bash
  export SENTINEL_API_TOKEN="$(openssl rand -hex 32)"
  curl -H "Authorization: Bearer $SENTINEL_API_TOKEN" http://localhost:8081/api/nodes
  ```

  **If you set `SENTINEL_API_TOKEN`, you must also enter it in the bundled UI**: open the
  "API Token" widget in the top-right corner of the dashboard header and paste the same value.
  The UI keeps it in the browser tab's `sessionStorage` (not persisted: closing the tab or
  browser clears it, so you must enter it again next time) and sends it as `Authorization: Bearer
  <token>` on every `/api/*` request; without this step the dashboard itself gets `401`s from
  every endpoint it calls. A token left in `localStorage` by an older UI version is deleted, not reused.

- **Failed-auth throttling behind a reverse proxy**: failed authentication attempts are limited
  per source, keyed on the connection's remote IP (IPv4 /32, IPv6 /64); `X-Forwarded-For` is not
  trusted. Behind a reverse proxy or load balancer, all clients therefore share the proxy's single
  budget, so once one client has repeated failures, other clients' failed attempts are also rejected
  (REST: `429`, gRPC: `ResourceExhausted`). On both REST and gRPC a valid token is always accepted
  regardless of the budget; only clients that keep failing authentication are affected. Plan proxy
  placement accordingly.

- **CORS**: cross-origin access is **denied by default**. The bundled UI is served same-origin, so
  no CORS is needed in production. Pass `--cors-origins https://example.com` only when a separate
  front-end origin must call the API.

## gRPC TLS & Authentication

The gRPC endpoint (`--grpc-addr`, default `:50051`) that agents connect to is **plaintext by
default** (the collector logs a warning at startup); gRPC auth defaults to `--grpc-auth=required`
(per-agent tokens). On any network you don't fully trust, use both:

- **TLS**: pass `--grpc-tls-cert`/`--grpc-tls-key` (PEM) on the collector; agents connect with
  `--collector https://<host>:50051`, adding `--grpc-ca-cert <path>` if the cert isn't from a
  publicly trusted CA (e.g. self-signed).
- **Authentication**: each agent authenticates with its own **per-agent token**, not the REST
  API's shared `SENTINEL_API_TOKEN` (see [ADR-005](docs/adr/ADR-005-agent-auth-strategy.md) for
  why mTLS and a shared secret were both rejected in favor of this). Every RPC (`Register`,
  `StreamEvents`) must carry a matching `authorization: Bearer <token>` metadata entry, resolved
  against the per-agent token store; without it — or with a revoked/unknown token — the collector
  rejects the call with `Unauthenticated`. An authenticated agent also can't register or send
  events under a `node_id` other than the one its token was issued for.

gRPC auth is controlled by `--grpc-auth=required|disabled` (default `required`), independent of
`SENTINEL_API_TOKEN` (which now only gates the REST API). With `required`, the collector starts even
if no per-agent token has been issued yet, but rejects every RPC until one is (fail-closed).
Unauthenticated gRPC requires the explicit `--grpc-auth=disabled`.

> **Breaking change (#222):** previously gRPC was unauthenticated unless `SENTINEL_API_TOKEN` was set.
> The default is now `required`. **Migration:** either issue per-agent tokens (below) and pass them
> to agents, or keep the old behaviour explicitly with `SENTINEL_API_TOKEN=<token> sentinel-collector serve --grpc-auth=disabled`
> (`scripts/install-collector.sh <host> --grpc-auth disabled`). Existing deployments that ran without
> `SENTINEL_API_TOKEN` will otherwise see all agents rejected after upgrading.

```bash
# Collector: gRPC auth is --grpc-auth=required (the default; independent of the REST token).
# Issue a token for each agent before it can connect.
# token issue/list and a plain `token revoke <node_id>` also work while the collector runs
# (see below). Only `token revoke --force` requires the collector to be stopped first —
# BadgerDB takes an exclusive lock on --data-dir, so it can't run alongside `serve`.
export SENTINEL_API_TOKEN="$(openssl rand -hex 32)"  # required by --http-auth=required (default); gates the REST API only, NOT gRPC
# Run token commands as the service user (`sudo -u sentinel`): as root they would create
# root-owned files in --data-dir that the unprivileged `sentinel` service cannot open.
sudo -u sentinel sentinel-collector token issue web-01 --data-dir /var/lib/sentinel
# prints the token once — copy it now, it's never shown again
sentinel-collector serve --grpc-auth=required --grpc-tls-cert cert.pem --grpc-tls-key key.pem

# Agent: pass the per-agent token from `token issue`, matching --node-id.
./sentinel-agent --collector https://collector.internal:50051 --node-id web-01 \
  --grpc-ca-cert ca.pem --grpc-token "<token from 'token issue web-01'>"
```

Or deploy via `scripts/deploy-agent.sh`, which wires this up for you (see its `--help` for the
full per-agent-token workflow):

```bash
TOKEN=$(sudo -u sentinel sentinel-collector token issue web-01 --data-dir /var/lib/sentinel)
./scripts/deploy-agent.sh --tls --node-id web-01 --grpc-token "$TOKEN" \
  --collector collector.internal:50051 web-01-host
```

To revoke a compromised or decommissioned agent's access:

```bash
sudo -u sentinel sentinel-collector token revoke web-01 --data-dir /var/lib/sentinel
sudo -u sentinel sentinel-collector token list --data-dir /var/lib/sentinel   # confirm status
```

`token issue`, `token list` and `token revoke` work whether the collector is stopped or running, so a token can be rotated (revoke, then issue) without a restart; a token issued this way authenticates immediately. While `serve` runs, the command is sent to it over a local Unix socket (`<data-dir>/admin/admin.sock`; the `admin/` directory must be a real directory owned by the serving user with mode 0700, the socket is 0600). If `admin/` is not safe the collector does not open the socket (it logs a warning and keeps running), and the CLI refuses to use it instead of falling back silently. If the socket path is too long for a Unix socket, the collector warns and continues; issue, list and revoke then need the collector stopped. `token revoke --force` is never sent to a running collector. The issued plaintext token travels only over the socket and is never logged; the collector logs one line per issue/revoke with the action and `node_id` only. When `token list` is served by the running collector, the warning about undecodable records goes to the collector's log, not to the CLI's stderr.

With auth enabled, a stream that is already established with a revoked token is closed with `Unauthenticated` at the next liveness tick (within `heartbeat-timeout / 3`). If the token lookup fails transiently, the stream is kept and the check is retried on the next tick.

## Monitoring

`GET /metrics` (Bearer auth, see above) exposes Prometheus metrics. Alert rules are not shipped in this repository; evaluate them in your own Prometheus. Two counters are worth alerting on:

| Metric | Meaning |
| ------ | ------- |
| `sentinel_events_throttled_total` | Events dropped by the rate limit (2000 events/s per authenticated `node_id`; with auth disabled the bucket is per stream instead). Unlabelled on purpose (`node_id` would be high cardinality). The agent receives an `ACK_REASON_RATE_LIMITED` ack, but the collector logs only the first drop and then every 1000th drop per stream, so this counter is the reliable signal. |
| `sentinel_auth_failures_total` | Rejected gRPC authentication attempts, by `reason` (`missing_credentials`, `invalid_token`, `rate_limited`; the last is the per-source auth-failure budget, unrelated to the event rate limit above). The series appears only after the first failure. |

Sustained throttling means an agent (or a host being attacked) is producing events faster than the collector accepts them, so events are being lost. An example rule:

```yaml
groups:
  - name: sentinel-mesh
    rules:
      - alert: SentinelEventsThrottled
        # Tune the threshold to your baseline; any sustained drop means lost events.
        expr: rate(sentinel_events_throttled_total[5m]) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "sentinel-mesh collector is dropping events (per-node rate limit)"
```

## Heartbeat Tracking

The collector runs a background goroutine that checks agent liveness every `heartbeat-timeout / 2`. An agent is marked `inactive` when its last gRPC message is older than `heartbeat-timeout`. Status is visible in `GET /api/nodes`.

While a `StreamEvents` stream is open, the collector also records a heartbeat every `heartbeat-timeout / 3` even if no events arrive, so an idle but connected agent stays `active`. Once the stream closes, the agent becomes `inactive` after `heartbeat-timeout`. (With gRPC auth disabled, this starts after the first valid event.)

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
| `SENTINEL_SMTP_PASSWORD`       | Email   | SMTP password (PLAIN auth, used only when user is set). If the server does not advertise AUTH, sending fails (never sent unauthenticated).   |
| `SENTINEL_SMTP_ALLOW_PLAINTEXT`  | Email   | `true` allows non-loopback SMTP servers without STARTTLS (default: refused). Credentials are still never sent without STARTTLS to non-loopback hosts. |
| `SENTINEL_NOTIFY_MIN_SEVERITY` | Both    | Minimum severity to notify (default `warning`).                            |
| `SENTINEL_NOTIFY_COOLDOWN`     | Both    | Min gap between notifications per (rule, node); Go duration, default `5m`. |

```bash
# Example: Slack-only, notify on warning+ with a 2-minute cooldown
export SENTINEL_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/XXX/YYY/ZZZ"
export SENTINEL_NOTIFY_COOLDOWN=2m
./collector serve --rules ../rules.yaml --grpc-auth disabled --http-auth disabled   # demo; use per-agent tokens in production
```

Severity ordering: `info` < `warning` < `critical` (rules with any other severity fail to load). The per-(rule, node) cooldown suppresses duplicate alert storms; a distinct node or rule notifies independently. Delivery is best-effort and runs off the event hot path — a failing channel is logged and never blocks event ingestion.

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
│   ├── cmd/collector/       # CLI (cobra) — serve (main.go) and token (token.go) subcommands
│   └── internal/
│       ├── receiver/        # gRPC server (registers nodes, stores events, auth-failure/rate limiting)
│       ├── registry/        # Agent registry + heartbeat checker
│       ├── store/           # BadgerDB wrapper (events, per-agent tokens)
│       ├── exporter/        # Gin REST API router
│       ├── httpserver/      # Shared HTTP server base
│       ├── httpauth/        # REST API bearer-token auth
│       ├── aggregator/      # Cross-region aggregator (--aggregate)
│       ├── alerting/        # Alert rules engine (rules.yaml)
│       ├── anomaly/         # Anomaly detector
│       ├── notify/          # Alert notifications (Slack, email)
│       ├── otel/            # OpenTelemetry metrics and traces
│       └── pb/              # Generated protobuf/gRPC stubs
├── web/                     # Web UI (React + TypeScript + Vite)
├── proto/
│   └── sentinel.proto       # Event + AgentService proto definition
├── scripts/                 # build, install-collector, deploy-agent, demo-multi-node/-multi-region, lib-validate (shared helpers)
├── rules.yaml               # Sample alerting rules
├── test-integration.sh      # Integration test script
├── CHANGELOG.md
├── AGENTS.md
├── CLAUDE.md                # Build/test commands and pitfalls for coding agents
├── LICENSE
├── .github/                 # CI workflow, dependabot, PR template
├── errors/                  # Troubleshooting notes (rust/, tooling/)
└── docs/
    ├── spec.md
    ├── data-model.md
    ├── implementation-guide.md
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
- Go 1.26+

### Quick Start (3 nodes)

```bash
# 1. Build all binaries (cross-compiled for Linux/amd64)
bash scripts/build.sh

# 2. Deploy Collector to node-a (systemd service)
#    --grpc-auth disabled: demo with mock agents that have no per-agent token.
#    For production keep the default `required` and issue tokens (see "gRPC TLS & Authentication").
bash scripts/install-collector.sh node-a --grpc-auth disabled

# 3. Deploy Agents to each node (mock mode)
bash scripts/deploy-agent.sh --mock --collector 192.0.2.10:50051 node-a
bash scripts/deploy-agent.sh --mock --collector 192.0.2.10:50051 node-b
bash scripts/deploy-agent.sh --mock --collector 192.0.2.10:50051 node-c

# Or run everything at once (skips unreachable nodes automatically)
bash scripts/demo-multi-node.sh
```

### Verify

```bash
# All registered nodes (active/inactive)
curl http://192.0.2.10:8081/api/nodes

# Events from a specific node
curl "http://192.0.2.10:8081/api/events?node=node-b&limit=20"

# Dashboard
open http://192.0.2.10:8081
```

### Node Layout

Example layout for a 3-node home-lab deployment (substitute your own hosts/IPs):

| Role      | Host            | IP           | SSH alias      |
| --------- | --------------- | ------------ | -------------- |
| Collector | collector-node  | 192.0.2.10   | `node-a`       |
| Agent     | agent-node-1    | 192.0.2.11   | `node-b`       |
| Agent     | agent-node-2    | 192.0.2.12   | `node-c`       |

### Deploy Scripts

#### `scripts/build.sh`

Cross-compiles Rust and Go binaries for Linux/amd64 (even on macOS) and places output in `dist/`.

```bash
bash scripts/build.sh
# → dist/sentinel-agent, dist/sentinel-collector
```

#### `scripts/install-collector.sh`

Deploys the collector binary to a remote host and installs as a systemd service running as the
unprivileged `sentinel` user with systemd hardening (the data dir is chowned to it; TLS files
must be readable by `sentinel` and not under `/home`).

```bash
bash scripts/install-collector.sh <SSH_HOST> [OPTIONS]
# Options: --region, --grpc-addr, --http-addr, --data-dir, --grpc-tls-cert, --grpc-tls-key, --http-tls-cert, --http-tls-key, --grpc-auth, --http-auth
# Example (demo; --http-auth defaults to required and the unit crash-loops without SENTINEL_API_TOKEN):
bash scripts/install-collector.sh node-a --grpc-addr :50051 --http-addr :8081 --http-auth disabled
# Production: keep --http-auth required and put SENTINEL_API_TOKEN in a mode-600 EnvironmentFile
# via a systemd drop-in on the host (the script never writes the token), then restart the unit.
```

#### `scripts/deploy-agent.sh`

Deploys an agent to one or more remote hosts, registers with the collector, and starts as a
systemd service. Real eBPF mode is the default; pass `--mock` for a quick smoke test or hosts
without `CAP_BPF`. See `bash scripts/deploy-agent.sh --help` for the full option list.

```bash
bash scripts/deploy-agent.sh [OPTIONS] HOST...
# Example (real eBPF mode, node_id defaults to each host's own `hostname -s`):
bash scripts/deploy-agent.sh --collector 192.0.2.10:50051 node-b node-c
# Example (mock mode, explicit node_id — requires a single host):
bash scripts/deploy-agent.sh --mock --node-id web-01 --collector 192.0.2.10:50051 node-a
```

### Switching to real eBPF mode

Real eBPF mode is `deploy-agent.sh`'s default (`--mock` opts *into* mock mode, not out of it).
The systemd unit it installs runs as root, so no separate `CAP_BPF` grant is needed. The unit is
hardened (#580): `NoNewPrivileges`, `ProtectSystem=strict` (the agent writes no files, so no
`ReadWritePaths`), `ProtectHome=yes` (`read-only` if `--grpc-ca-cert` is under `/home`, `/root` or
`/run/user`), `PrivateTmp`, `ProtectControlGroups`, and `CapabilityBoundingSet=CAP_BPF CAP_PERFMON
CAP_SYS_ADMIN CAP_SYS_RESOURCE CAP_SYS_PTRACE` (kernel 5.8+; `CAP_SYS_ADMIN` because Debian/Ubuntu require it for `perf_event_open` when `perf_event_paranoid` > 2; `CAP_SYS_RESOURCE` for the memlock rlimit on kernel < 5.11; `CAP_SYS_PTRACE` for reading other processes' cwd; to be narrowed once verified on a real host). Directives that
could break eBPF (`ProtectKernel*`, `PrivateDevices`, `MemoryDenyWriteExecute`, `SystemCallFilter`)
are intentionally not set. Not yet verified on a real eBPF host. To switch an
already-deployed `--mock` host to real eBPF, redeploy without `--mock` — this overwrites the
`sentinel-agent.service` unit and restarts it:

```bash
bash scripts/deploy-agent.sh --collector 192.0.2.10:50051 <host>
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

---

## 日本語

### 概要

sentinel-mesh は、カーネルレベルのイベントを eBPF で集める分散観測基盤です。
**Rust Agent** が eBPF(またはモック)でカーネルイベント(exec / tcp / file)を収集して gRPC で送り、
**Go Collector** が BadgerDB に保存して REST API と Web UI(React)を提供します。
ハートビートが途絶えた Agent は自動で inactive になります(StreamEvents の接続中はイベントが無くても `heartbeat-timeout / 3` ごとに記録するため active のまま)。アラートルール(`rules.yaml`)、
スライディング窓による異常検知、Slack / メール通知、OpenTelemetry のメトリクス・トレース、
複数リージョンの集約(`--aggregate`)に対応しています。

### 構成

| 要素 | 役割 |
| --- | --- |
| `rust-agent/` | Rust 製 Agent(tokio + tonic)。`agent-ebpf/` は aya の eBPF プログラム |
| `go-collector/` | Go 製 Collector(cobra CLI: `serve` / `token`)。gRPC 受信・保存・REST API・アラート・異常検知・通知 |
| `web/` | Web UI(React + TypeScript + Vite) |
| `proto/` | gRPC の定義(`sentinel.proto`) |
| `scripts/` | ビルド・Collector の導入・Agent の配備・デモ用スクリプト |

### Agent の systemd unit のハードニング

`scripts/deploy-agent.sh` が生成する unit には `NoNewPrivileges`・`ProtectSystem=strict`・`ProtectHome=yes`
(`--grpc-ca-cert` が `/home` 等の下ならば `read-only`)・`PrivateTmp`・`ProtectControlGroups` と
`CapabilityBoundingSet=CAP_BPF CAP_PERFMON CAP_SYS_ADMIN CAP_SYS_RESOURCE CAP_SYS_PTRACE`(カーネル 5.8 以降。
`CAP_SYS_ADMIN` は Debian/Ubuntu で `perf_event_paranoid` が 2 超のとき `perf_event_open` に必要、`CAP_SYS_RESOURCE` は
カーネル 5.11 未満の memlock rlimit 用、`CAP_SYS_PTRACE` は他プロセスの cwd 取得用。実機で確認できたら絞ります)が入ります(#580)。Agent はファイルを書かないため `ReadWritePaths` は不要です。eBPF を壊しうる
`ProtectKernel*`・`PrivateDevices`・`MemoryDenyWriteExecute`・`SystemCallFilter` は意図的に付けていません。
実機(eBPF)での起動確認は未実施です。

### セットアップと動かし方

必要なもの: Rust 1.82+、Go 1.26+。実際の eBPF モードには `CAP_BPF` が要ります(モックモードは不要)。
ビルドとローカルでの起動手順は、上の英語の [Build](#build) と [Quick Start](#quick-start) を
そのまま使えます(リポジトリのルートで実行)。

注意: Collector は既定で gRPC・REST とも認証必須(`--grpc-auth=required` / `--http-auth=required`)です。
ローカルのデモでは `--grpc-auth disabled --http-auth disabled` を付けます。本番では Agent ごとに
`sentinel-collector token issue <node_id>` でトークンを発行し、REST には `SENTINEL_API_TOKEN` を設定します
(詳細は英語の [gRPC TLS & Authentication](#grpc-tls--authentication))。
`token issue` / `token list` / `token revoke <node_id>` は Collector の稼働中でも実行できます(ローカルの Unix ソケット
`<data-dir>/admin/admin.sock` 経由。`admin/` は実ディレクトリ・所有者が自分・0700 でなければなりません)。停止せずに
トークンをローテーション(revoke → issue)でき、発行したトークンは直ちに認証に使えます。平文トークンはソケット上だけを通り、
ログには出ません(Collector のログには action と `node_id` の 1 行だけ)。`token revoke --force` は従来どおり Collector を
停止してから実行します(BadgerDB の排他ロックのため)。

### ドキュメント

- 仕様: [docs/spec.md](docs/spec.md) / データモデル: [docs/data-model.md](docs/data-model.md)
- 技術スタック: [docs/tech-stack.md](docs/tech-stack.md) / 実装ガイド: [docs/implementation-guide.md](docs/implementation-guide.md)
- 設計判断の記録: [docs/adr/](docs/adr/) / 変更履歴: [CHANGELOG.md](CHANGELOG.md)(現在の版は v0.1.0、以降の変更は Unreleased)

### ライセンス

MIT

# Changelog

All notable changes to SentinelMesh are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **BREAKING (go-collector):** gRPC authentication is now controlled by the new
  `--grpc-auth=required|disabled` flag (default `required`) instead of being
  implicitly enabled by `SENTINEL_API_TOKEN` (#222). With `required` and no
  issued per-agent tokens the collector still starts but rejects every RPC.
  Migration: issue per-agent tokens (`sentinel-collector token issue <node_id>`)
  or pass `--grpc-auth=disabled` to keep the previous unauthenticated behaviour.
  `scripts/install-collector.sh` gained `--grpc-auth`.
- **BREAKING (go-collector):** REST API authentication is now controlled by the new
  `--http-auth=required|disabled` flag (default `required`), in both normal and
  `--aggregate` mode (#254). With `required` and `SENTINEL_API_TOKEN` unset the
  collector now exits at startup instead of serving the REST API unauthenticated
  with only a warning. Migration: set `SENTINEL_API_TOKEN` (and enter it in the
  bundled UI) or pass `--http-auth=disabled` to keep the previous unauthenticated
  behaviour. `scripts/install-collector.sh` gained `--http-auth`.
  `/metrics` is behind the same bearer auth as `/api/*` (only `/healthz` is open). The
  installer now fails (with the journal tail) if the unit is not active after start, and the
  README/`serve --help` examples were fixed so they start (#279).

## [0.1.0] - 2026-08-17

First tagged release. End-to-end eBPF-based host monitoring mesh, verified on
real hardware (YUKI WSL2 #57, DS1 WSL2 #59).

### Added

#### Agent (Rust)
- eBPF kernel programs loading real kernel events via `aya`
  (exec tracepoint, openat tracepoint, `tcp_connect` kprobe) with a mock mode
  fallback for testing.
- gRPC streaming client to the collector.
- `--region` flag / `SENTINEL_REGION` env for region tagging, sent on register.

#### Collector (Go)
- gRPC server + REST API ingesting kernel events into BadgerDB.
- Sliding-window anomaly detection.
- Heartbeat timeout — stale agents marked inactive (phase 4).
- Alerting engine with `rules.yaml` evaluation.
- Slack / Email alert notifications.
- Cross-region pull-based aggregation layer with `/api/regions` and region
  filtering on events.

#### Web UI (React)
- Dashboard with Nodes / Events / Alerts / Stats views.
- Event/alert filtering and time-range selection; node filter on `/api/events`.

#### Observability
- OpenTelemetry metrics and distributed traces.

#### Deployment
- Multi-node deploy scripts (`build.sh`, `deploy-agent.sh`) targeting
  MINIPC / YUKI / DS1 with real-eBPF mode, multi-host and region flags.
- `demo-multi-region.sh` demo.

### Verified
- Real eBPF load/attach confirmed on two hosts (YUKI, DS1), kernel 6.6.x WSL2,
  via `/proc/<pid>/fd` BPF prog/map/link inspection.

[0.1.0]: https://github.com/flipslidersand-labs/sentinel-mesh/releases/tag/v0.1.0

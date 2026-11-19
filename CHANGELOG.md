# Changelog

All notable changes to SentinelMesh are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **BREAKING (go-collector, #360):** `SENTINEL_API_TOKEN` must now be at least 32 bytes;
  the collector refuses to start with a shorter value (e.g. use `openssl rand -hex 32`).
  Rotate any shorter token before upgrading. This also applies with `--http-auth disabled`: a
  `SENTINEL_API_TOKEN` that is set but shorter than 32 bytes is a startup error.

### Security

- **go-collector:** REST API now throttles repeated failed bearer-auth attempts per source
  address (5 failures, refilling 5/min) and answers 429 with `Retry-After` (#360).

### Added

- **go-collector:** a node evicted from the registry while its stream stays
  connected is re-created on its next event (default region, empty hostname)
  instead of vanishing from `/api/nodes` until reconnect (#371).
- **go-collector:** `sentinel-collector token issue|revoke|list` subcommands and a
  per-agent token store in BadgerDB (#195, #198). gRPC auth resolves the agent
  identity (`node_id`) from the presented token (#196); see ADR-005 (#188).
- **Agent / deploy:** `--grpc-token` and `--grpc-ca-cert` (`SENTINEL_GRPC_CA_CERT`)
  for bearer-token auth and TLS verification against the collector, also accepted
  by `scripts/deploy-agent.sh` (#85). `deploy-agent.sh --grpc-token` is limited to
  a single host and requires `--node-id`, since a per-agent token is bound to one
  `node_id` (#200).
- **Web UI:** API Token widget; the bundled UI now sends the `Authorization`
  header (#144).
- gRPC auth-failure and rate-limit rejection metrics (#266).

### Changed

- **Agent:** a permanently rejected registration (`ok=false` or an
  InvalidArgument/Unauthenticated/PermissionDenied status) now logs the
  collector's reason and exits non-zero instead of retrying forever; transient
  errors still back off and retry (#380).
||||||| 167904e
- **Web UI:** a failed localStorage write now keeps the API token in memory for the
  current tab and the widget says "This tab only" instead of "Saved" (#377).
||||||| f4e74e3
- **go-collector:** `RuleSet.Validate` now rejects `gt`/`lt` conditions whose `value` is not
  numeric (e.g. `"10k"`), instead of loading a rule that can never fire (#375).
||||||| f4e74e3
- **Deploy:** `scripts/install-collector.sh` now runs the collector as the unprivileged
  system user `sentinel` with systemd hardening (`NoNewPrivileges`, `ProtectSystem=strict`,
  `ProtectHome`, `PrivateTmp`, ...; ports below 1024 stay bindable via
  `CAP_NET_BIND_SERVICE`) instead of root (#365). Migration: re-run the installer; it
  creates the user and chowns `--data-dir`. TLS cert/key given by `--grpc-tls-*` must be
  readable by `sentinel` and outside `/home`, otherwise the installer stops with a hint.

||||||| a460005
- **BREAKING (go-collector):** the SMTP notifier now requires STARTTLS for every
  non-loopback server, even without credentials; if the server does not advertise
  it the alert is not sent (previously sent in plaintext, open to downgrade) (#362).
  Migration: enable STARTTLS on the SMTP server, or set
  `SENTINEL_SMTP_ALLOW_PLAINTEXT=true` to keep the old behavior on a trusted network.
- **BREAKING (go-collector):** gRPC authentication is now controlled by the new
  `--grpc-auth=required|disabled` flag (default `required`) instead of being
  implicitly enabled by `SENTINEL_API_TOKEN` (#222). With `required` and no
  issued per-agent tokens the collector still starts but rejects every RPC.
  Migration: issue per-agent tokens (`sentinel-collector token issue <node_id>`)
  or pass `--grpc-auth=disabled` to keep the previous unauthenticated behaviour.
  `scripts/install-collector.sh` gained `--grpc-auth`.
- **BREAKING (go-collector):** REST API authentication is now controlled by the new
  `--http-auth=required|disabled` flag (default `required`), in both normal and
  `--aggregate` mode (#254, #278). With `required` and `SENTINEL_API_TOKEN` unset the
  collector now exits at startup instead of serving the REST API unauthenticated
  with only a warning. Migration: set `SENTINEL_API_TOKEN` (and enter it in the
  bundled UI) or pass `--http-auth=disabled` to keep the previous unauthenticated
  behaviour. `scripts/install-collector.sh` gained `--http-auth`.
  `/metrics` is behind the same bearer auth as `/api/*` (only `/healthz` is open). The
  installer now fails (with the journal tail) if the unit is not active after start, and the
  README/`serve --help` examples were fixed so they start (#279).
- **go-collector:** `TracesProvider.Close` no longer uses the signal-canceled context, so
  pending spans are flushed on SIGTERM (5s timeout) (#376).
||||||| 9c17729
- **go-collector:** anomaly window counts use a binary search over the sorted
  timestamps instead of a linear scan (#296).

### Fixed

- **agent:** `ok=false` acks from the collector are counted and logged as one
  summary line per 10 s instead of one line per event, so exceeding the per-node
  rate limit no longer floods the journal (#378).
- **go-collector:** `--otel-endpoint` for traces accepts a value with a scheme
  (e.g. `http://localhost:4318`) (#280).
- **go-collector:** a corrupted token record no longer breaks `token list`; it is
  skipped (#283); corrupted event/alert records are skipped (with one warning) by
  `store.New`, `ListEvents` and `ListAlerts` instead of failing them (#372).
- **Agent:** on collector disconnect the agent logs how many unacknowledged events
  were discarded (#281); mock-source events dropped by a full channel are counted
  and warned about (#284); the eBPF source uses the same drop counter (#291).
- **deploy:** `scripts/deploy-agent.sh` validates `--mock-rate` as an integer in
  1..1000 (#310).
- **go-collector:** alerting `eq`/`contains`/`regex` now stringify JSON numbers in plain decimal, so values of 1e6 or more (e.g. pid 1234567) no longer become `1.234567e+06` and match rules (#370).

### Fixed

- **go-collector:** `ListTokens` and `getAgentToken` no longer return results the
  background goroutine may still be writing after the context ends; they return
  nil on error (#382).

### Security

- **go-collector:** the aggregator rejects upstream URLs that contain userinfo
  (`user:pass@`) at startup, and its errors no longer echo the configured URL, so
  credentials cannot reach the logs (#364).
- Per-agent bearer tokens for the gRPC channel instead of a shared token
  (#195, #196, #198, ADR-005 / #188); token lookup is O(1) so valid tokens are not
  throttled by the auth-failure limiter (#238).
- gRPC server TLS + bearer authentication (#85); failed-auth RPCs are rate
  limited (#230).
- gRPC server limits: `MaxConcurrentStreams`, `MaxRecvMsgSize`, keepalive
  enforcement and a per-stream `StreamEvents` rate limit; notification dispatch
  is bounded (#98).
- `Register` validates `node_id` / `hostname` / `ip` / `version` / `region`
  before persisting (#128).
- `deploy-agent.sh` / `install-collector.sh` validate inputs interpolated into
  systemd units (command-injection fix, #87) and validate ssh/scp hosts, rejecting
  option-like hosts (#88).
- The aggregator sends the bearer token when polling upstream collectors, so
  cross-region aggregation keeps working with REST auth enabled (#179).
- HTTP responses (UI and API) now carry `Content-Security-Policy` (`default-src 'self'; frame-ancestors 'none'`),
  `X-Content-Type-Options: nosniff` and `Referrer-Policy: no-referrer` (#366).
- `/metrics` is gated behind bearer auth like `/api/*` (#180; see Changed, #279).
- `StreamEvents` validates `node_id` / `event_id` / payload strings (#166) and
  applies its rate limit before validation (#173); the limit is a shared bucket
  per authenticated `node_id` across streams (#277).
- The agent IP is derived from the gRPC peer, not a client-supplied field (#178).
- `deploy-agent.sh` no longer leaks the gRPC bearer token (#174); Slack send
  errors no longer include the webhook URL (#256).
- A negative `SENTINEL_NOTIFY_COOLDOWN` or an unknown `SENTINEL_NOTIFY_MIN_SEVERITY`
  now fails collector startup instead of notifying on every alert (#379).
||||||| f4e74e3
- gRPC `MaxRecvMsgSize` lowered from 4 MB to 8 KB, derived from the validated field
  limits (largest legitimate message is about 1.6 KB); oversized messages are
  rejected with `ResourceExhausted` (#361).
||||||| f4e74e3
- `demo-multi-region.sh` validates `SENTINEL_*_IP` (IPv4) and hosts before
  embedding them in the root aggregator unit, and passes `--` to ssh (#367).
||||||| a460005
- REST 500 responses return a fixed `internal error` message instead of the store
  error text (#297); the aggregator's `RegionStatus.error` carries a fixed
  classification instead of the raw upstream error (#306); the rate-limit log
  escapes and caps an unauthenticated client `node_id` (#307).

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

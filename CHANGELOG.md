# Changelog

All notable changes to SentinelMesh are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **rust-agent (#432):** EXEC events now carry `ppid` and `cmdline` (argv joined by spaces, ASCII-sanitized (bytes outside 0x20-0x7E become `?`) and capped at 256 bytes to match the collector limit), read in userspace from `/proc/<pid>/status` and `/proc/<pid>/cmdline`. Very short-lived processes that exit before the read still report `ppid` 0 / empty `cmdline`. Not verified on a real eBPF run.
- **rust-agent (eBPF, #419):** FILE events now carry the `openat` flags, and `op` is derived from them: `create` (O_CREAT), `write` (O_WRONLY/O_RDWR/O_TRUNC/O_APPEND) or `open` (read-only). Previously `op` was always `open`; `open` now means read-only opens only. Not yet verified on a real host (CAP_BPF, no `--mock`).
- **store (#452):** `SaveEvent` is now idempotent per `(node_id, event_id)` within the retention window, so resending an event after a timed-out Ack no longer stores it twice (events with an empty `event_id` are not deduplicated). Store operations are capped at 256 in flight, including writes still finishing after their caller timed out; beyond that they fail fast with `store.ErrBusy` (Ack `ok=false`) instead of piling up goroutines.
- **store (#466):** Reads (`ListEvents`/`ListAlerts`/token reads) now have their own in-flight cap (64), separate from writes (256), so reads stuck on a slow disk can no longer make `SaveEvent` fail with `ErrBusy` and stop ingestion. A conflicting `SaveEvent` no longer retries after its caller's context is done.
- **rust-agent (eBPF, #347):** EXEC events are now emitted from `sched/sched_process_exec` (successful execs only), so `comm` is the executed binary's name instead of the caller's (e.g. `curl`, not `bash`). Rules matching on `comm` (e.g. `bash_execution`) now fire when bash itself is executed, not for every child it spawns.
- **notify (#319):** the per-(rule, node) cooldown map now prunes expired keys, and the cooldown is released when every notifier fails so alerts are retried instead of being silenced for a full cooldown.
- **rust-agent (eBPF, #373):** FILE events are now emitted from `sys_exit_openat`, so `ret` carries the real result (negative errno for failed opens) instead of always 0.
- **rust-agent (eBPF, #369):** `tcp_connect` now checks `skc_family` and skips non-IPv4 sockets, so IPv6 connections are no longer recorded with bogus IPv4 addresses (IPv6 is not yet supported). Known limitation: dual-stack IPv6 sockets connecting to v4-mapped addresses are also skipped (tracked in #429).

### Changed

- **api/aggregator/exporter (#357):** route paths (`/api/nodes` etc.) and the region roll-up type now live in the new `internal/api` package, with `registry.StatusActive` / `registry.CountActive`, shared by the exporter and the aggregator; a contract test polls the real exporter router. No change to the REST responses.
- **BREAKING (rust-agent, #420):** the agent now refuses to start when `--grpc-token`/`$SENTINEL_API_TOKEN` is set but the collector is plaintext (`http://`) and not loopback (127.0.0.0/8, `::1`, `localhost`); it was a warning before. Use an `https://` collector, or pass the new `--allow-insecure-token` to keep the old behavior.
- **license:** declare `license = "MIT"` in the Rust agent manifests and `web/package.json` to match `LICENSE` (#328).
- **BREAKING (go-collector, #360):** `SENTINEL_API_TOKEN` must now be at least 32 bytes;
  the collector refuses to start with a shorter value (e.g. use `openssl rand -hex 32`).
  Rotate any shorter token before upgrading. This also applies with `--http-auth disabled`: a
  `SENTINEL_API_TOKEN` that is set but shorter than 32 bytes is a startup error.
- **BREAKING (go-collector, #327):** rule `severity` must now be exactly `info`, `warning` or
  `critical`. A rule with any other severity (empty, the former aliases
  `low`/`warn`/`medium`/`high`/`error`, or mixed case) is a load-time error and the collector
  does not start. `SENTINEL_NOTIFY_MIN_SEVERITY` likewise accepts only `info`/`warning`/`critical`
  (case and surrounding whitespace are still tolerated).
- **BREAKING (go-collector, #359):** `--aggregate` now rejects `http://` upstreams on
  non-loopback hosts (anything other than 127.0.0.0/8, `::1`, `localhost`), because the
  bearer token would be sent in plaintext. Migration: serve the region collectors over TLS
  (new `--http-tls-cert`/`--http-tls-key`) and use `https://` upstreams, or pass the new
  `--aggregate-allow-insecure-http` to keep plaintext on a trusted network.
- **scripts (#358):** `install-collector.sh` and `deploy-agent.sh` now stage uploads in a per-run
  remote `mktemp -d` directory (removed on exit) instead of fixed `/tmp` paths, and install binaries
  with `install -o root -g root -m 755` and static files with `rsync --no-links` as root:root.

### Security

- **rust-agent (#470):** the plaintext-token loopback check now also ends the collector authority at `#`, so `http://<remote>#@127.0.0.1:50051` is rejected instead of sending the token in plaintext to the remote host.
- **ci/rust-agent (#420):** pin all GitHub Actions in `ci.yml` to commit SHAs, set top-level `permissions: contents: read`, and hide `SENTINEL_API_TOKEN` from the agent's `--help` output (`hide_env_values`).
- **go-collector:** REST API now throttles repeated failed bearer-auth attempts per source
  address (5 failures, refilling 5/min) and answers 429 with `Retry-After` (#360).

### Added

- **CI:** `go mod tidy` consistency check for `go-collector` (fails if `go.mod`/`go.sum` would change) (#330).
- **go-collector:** `--http-tls-cert`/`--http-tls-key` serve the REST API (normal and
  `--aggregate` mode) over TLS; they must be given together. Without them the REST API stays
  plaintext and a warning is logged, as with gRPC (#359).
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
- **go-collector:** `sentinel-collector --help` has long descriptions, usage examples and a
  flag-consistency check (#113).
- **CI:** gitleaks secret scan (#14).

### Changed

- **Agent:** a permanently rejected registration (`ok=false` or an
  InvalidArgument/Unauthenticated/PermissionDenied status) now logs the
  collector's reason and exits non-zero instead of retrying forever; transient
  errors still back off and retry (#380).
- **Web UI:** a failed localStorage write now keeps the API token in memory for the
  current tab and the widget says "This tab only" instead of "Saved" (#377).
- **go-collector:** `RuleSet.Validate` now rejects `gt`/`lt` conditions whose `value` is not
  numeric (e.g. `"10k"`), instead of loading a rule that can never fire (#375).
- **Deploy:** `scripts/install-collector.sh` now runs the collector as the unprivileged
  system user `sentinel` with systemd hardening (`NoNewPrivileges`, `ProtectSystem=strict`,
  `ProtectHome`, `PrivateTmp`, ...; ports below 1024 stay bindable via
  `CAP_NET_BIND_SERVICE`) instead of root (#365). Migration: re-run the installer; it
  creates the user and chowns `--data-dir`. TLS cert/key given by `--grpc-tls-*` must be
  readable by `sentinel` and outside `/home`, otherwise the installer stops with a hint.
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
- **go-collector:** anomaly window counts use a binary search over the sorted
  timestamps instead of a linear scan (#296).
- **go-collector:** alerting `regex` conditions are compiled once when rules load (and an
  invalid pattern is a load-time error) instead of on every event (#355).
- **go-collector:** `/metrics` is served by the Prometheus client directly instead of the OTel
  bridge (#333). Metric names, labels, HELP and TYPE are unchanged, but the
  `otel_scope_name`/`otel_scope_schema_url`/`otel_scope_version` labels and the `target_info`
  gauge are gone; dashboards or alerts selecting on them need updating.

### Fixed

- **agent:** `ok=false` acks from the collector are counted and logged as one
  summary line per 10 s instead of one line per event, so exceeding the per-node
  rate limit no longer floods the journal (#378).
- **go-collector:** with `--cors-origins` set, CORS preflight now allows the `Authorization` header, so browsers can call the bearer-authenticated API cross-origin (#350).
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
- **go-collector (store):** BadgerDB entries get a retention TTL and `/api/stats` no longer scans
  every event (#77); value-log GC runs periodically (#149); the node-filter scan in
  `ListEvents`/`ListAlerts` is capped (#153); time keys are fixed-width so events within one
  second keep their order under `limit` (#250); `ListEvents`/`ListAlerts` no longer race after a
  context timeout (#351); unreadable token records are skipped so other tokens still
  authenticate (#242); `Stats()` stays in sync when `SaveEvent`'s context is cancelled
  mid-write (#325).
- **go-collector (shutdown):** gRPC/REST servers shut down gracefully on SIGINT/SIGTERM (#117),
  bounded against hung streams and store I/O (#155); REST server failures are propagated and the
  store closes only after the servers stop (#316, #318); the REST server is force-closed when
  graceful shutdown fails (#422).
- **go-collector (receiver):** store failures are reflected in the `Ack` and counted (#103);
  response headers are sent right after authentication (#217); payload values of 0 (e.g.
  `uid=0`) are kept (#249); unknown `EventType`s are rejected (#260).
- **go-collector (registry/heartbeat):** a node that heartbeats again returns to `active` (#79);
  inactive nodes are evicted after a while to bound memory (#105); `--heartbeat-timeout` of 0 or
  a negative value no longer panics (#147).
- **go-collector (alerting/anomaly):** `RuleSet.Validate()` is implemented and a missing
  `--rules` path is an error (#111); default rules use `dst_port` instead of `dport` and unknown
  field names are rejected (#248); idle anomaly keys are garbage-collected (#107); the anomaly
  detector no longer depends on the engine being non-nil (#150); anomaly alerts use a
  per-window cooldown instead of firing on every event (#422).
- **go-collector (REST/aggregator):** the `limit` query parameter is validated and capped (#102);
  the REST server has read/write/idle timeouts (#110); `--aggregate` warns about normal-mode-only
  flags (#152); the aggregator applies `limit`/`node`/`region` on `/api/events` and
  `/api/alerts` and returns newest first (#317); the `node` metric label's cardinality is
  bounded (#326).
- **go-collector (notify/otel):** `EmailNotifier` honours the dispatcher's context timeout (#109);
  SMTP to an IPv6 literal uses the right `PlainAuth` host (#246); spans are no longer leaked in
  the `StreamEvents` loop (#74).
- **agent:** reconnects to the collector with exponential backoff (#70, #73), reset once a
  connection is stable (#313); gRPC connect has a timeout (#108); HTTP/2 keepalive detects
  half-open connections (#231); the ring-buffer drain no longer blocks when full (#70) and the
  unaligned bytes→struct cast is gone (#69); eBPF-path `unwrap()`s are handled as errors (#118);
  non-UTF-8 `comm`/path bytes are no longer silently emptied (#104); `--mock-rate` is validated
  as 1..=1000 (#260, #271); the agent exits non-zero when its event source task ends (#348).
- **agent (eBPF):** TCP ports use the right byte order and the source address is filled in (#72);
  the `sys_enter_openat` filename pointer is read from the correct offset (#169).
- **Web UI:** API responses are type-checked instead of white-screening on unexpected JSON (#80);
  fetches time out and polls no longer overlap (#151); polling refetches on filter change and
  ignores stale responses (#320); the Alerts page no longer crashes on a null severity (#422).
- **scripts:** `deploy-agent.sh` refuses a non-mock deploy of a binary built without eBPF (#349).

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
- gRPC `MaxRecvMsgSize` lowered from 4 MB to 8 KB, derived from the validated field
  limits (largest legitimate message is about 1.6 KB); oversized messages are
  rejected with `ResourceExhausted` (#361).
- `demo-multi-region.sh` validates `SENTINEL_*_IP` (IPv4) and hosts before
  embedding them in the root aggregator unit, and passes `--` to ssh (#367).
- REST 500 responses return a fixed `internal error` message instead of the store
  error text (#297); the aggregator's `RegionStatus.error` carries a fixed
  classification instead of the raw upstream error (#306); the rate-limit log
  escapes and caps an unauthenticated client `node_id` (#307).
- Event keys no longer use the agent-supplied timestamp, so an agent cannot pin events to the
  front of `ListEvents` or overwrite other events via a timestamp collision (#78).
- eBPF: reserved ring-buffer records are zero-initialized so struct padding cannot leak kernel
  memory (#71).
- The aggregator no longer follows upstream redirects (SSRF) and caps upstream response bodies (#76).
- SMTP: `EmailNotifier` upgrades with STARTTLS and refuses to send when it is unsupported (#215);
  with credentials set, a server that does not advertise AUTH is an error (#247).
- `grpc` and `x/net` updated to versions without the known vulnerabilities, with the Go toolchain
  pinned (#252).
- `deploy-agent.sh` no longer prints the gRPC token value when input validation fails (#363).

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

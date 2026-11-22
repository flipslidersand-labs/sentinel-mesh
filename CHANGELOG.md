# Changelog

All notable changes to SentinelMesh are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **docs (#420):** README has a Monitoring section describing `sentinel_events_throttled_total` / `sentinel_auth_failures_total` and an example Prometheus alert rule for sustained per-node rate-limit drops (rules themselves are not shipped; evaluate them in your own Prometheus).
- **go-collector:** `token revoke --force <record key>` deletes a token record that cannot be decoded (and so cannot be revoked by node_id), using the key shown in the `token list` warning. `token list` still exits 0 when it skips such records (#276).
- **Agent / eBPF:** IPv6 TCP connections and IPv4 connections made through dual-stack
  AF_INET6 sockets (v4-mapped) are now emitted as TCP events; `TcpEvent` carries a family
  and 16-byte addresses (#429). Not yet verified on a real host (no CAP_BPF test): the
  eBPF object builds, but the verifier/runtime behaviour and the raw `sock_common`
  offsets (assume x86_64, CONFIG_NET_NS, CONFIG_IPV6) are untested.
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
- **CI (#312):** non-blocking weekly vulnerability scan workflow `vuln-scan.yml` (`govulncheck`, `cargo deny check advisories`, `npm audit`); findings are reported as warnings and in the job summary and the checks stay green. Do not add it to required checks. Not yet run on GitHub Actions at the time of the change.

### Changed

- **rust-agent (#255):** the self-made `include_bytes_aligned!` macro in `events.rs` is replaced by `aya::include_bytes_aligned!` (aya 0.13). The only difference is the embedded object's alignment (4096 → 32, which is what aya requires for ELF parsing); the eBPF object is still embedded at compile time ( `clippy --features ebpf` with the placeholder object passes).
- **go-collector (#500):** the pending-anomaly map (cap 1024) now evicts the oldest entry first instead of an arbitrary one; added tests for eviction, failed re-save and concurrent resend.
- **go-collector (#469):** removed test-only/unused code (`Store.SaveEvent`/`ListEvents`/`ListAlerts`, `runHTTPServerUntilDone`, `sendMailContext`, `Aggregator.Events`/`Alerts`, `authlimit.Limiter.Tracked`, the `receiver` authfail alias file); tests use the `*Dedup`/`*Where`/`*In` APIs. No behavior change.
- **go-collector:** `serve` now builds and starts the server in `run(ctx, cfg, logger)`; the logger and the signal-aware ctx are created in `RunE`, so `run` returns when ctx is cancelled. The `--grpc-addr` flag name is now a shared constant (no behavior change) (#265).
- **go-collector:** `validateKernelEvent` now reads per-type fields from the `eventtype` table instead of a type switch; behaviour unchanged (#353).
- **refactor (#421):** removed the unreachable heartbeat-timeout fallback in `registry.StartHeartbeatChecker` (caller validates), and bundled the per-connection parameters of the rust-agent gRPC session functions into one struct (no behavior change).
- **rust-agent (#477):** eBPF loader (`--features ebpf`) no longer uses deprecated APIs: `aya::Bpf` -> `aya::Ebpf`, `AsyncFd::new` -> `unsafe AsyncFd::register` (workspace tokio minimum raised to 1.54). Ring buffer ownership is unchanged. Not verified on a real eBPF run; compile/clippy/test only.
- **go-collector (#469):** anomaly detector keeps a fixed ring of 1-second counters per key (about 1.2KB for the default 5m window) instead of one timestamp per event; window counts now have 1-second precision, and maxDuration is computed once in New.
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
- **refactor (#255):** no behavior change: `store.Stats()` no longer returns an error; the `os.Stat` in `alerting/rules.go` is gone (an unreadable existing rules file now reports `rules file %q:`); the unused `case int` in alerting numeric conversion is removed (payload values are always float64); the aggregator `eventLimit` is a constant (still 100); the rust-agent send stream uses `ReceiverStream` (`async-stream` is now dev-only); the web UI builds queries through a shared `listPath`.
- **aggregator (#473):** events carry a collector-assigned `received_at` (omitted when empty; the store overwrites any agent-reported value), and the aggregator uses it to order region heads across regions, falling back to `timestamp` when it is absent (mixed versions, existing records, which have none until their TTL expires).
- **refactor (#353):** new `internal/eventtype` package is the single source for event kinds and payload schemas; the receiver and alerting use it, and the known alert fields now come from the payload structs' `json` tags instead of the pb descriptor. Payload JSON and error messages are unchanged; `validateKernelEvent` is not yet table-driven.
- **rust-agent (#331):** `tonic`/`tonic-build`/`prost` updated to the 0.14 series (`tonic-prost-build`, native roots enabled explicitly to keep the OS root certificates); `cargo deny check advisories` is clean afterwards. `aya` is unchanged. Not verified against a real TLS connection (in-process fake collector tests only).
- **refactor (#437):** removed the internal `store.Event`/`store.Alert` compatibility aliases; all code uses `model.Event`/`model.Alert` (no behavior or data-format change).

### Fixed

- **go-collector:** the node's region is now persisted (`node:<id>` in Badger, retention TTL) and stamped on stored events (`region`, omitted when empty), so a node re-created after eviction or a restart keeps its region and `/api/events?region=` still returns the events of evicted nodes; events stored before this change still match through the registry (#512, #468).
- **go-collector:** FILE/EXEC payload text (comm, cmdline, cwd, path) may now contain printable non-ASCII characters instead of the whole event being rejected, which let a single non-ASCII character in a file name evade detection; control characters, NUL, bidi/format characters and invalid UTF-8 are still rejected, and node_id/event_id/op stay ASCII-only (#251, #154).
- **go-collector:** aggregate mode now serves `/api/stats` (event counts summed over upstream regions) so the UI Stats page no longer 404s, and `/api/nodes` merges in a deterministic node_id order (#251).
- **go-collector/notify:** each notifier now gets its own send timeout instead of sharing one, so a slow notifier no longer causes the others to time out (#251).
- `scripts/install-collector.sh`: add `--http-tls-cert`/`--http-tls-key` for the REST API, and fail with exit 1 when only one of a TLS cert/key pair is given (gRPC or REST) instead of silently starting in plaintext (#254).
- **go-collector:** `StreamEvents` now rejects events whose oneof payload is missing or does not match the event type, instead of storing `payload:null` and counting it (#468).

- **go-collector:** events dropped by the per-node rate limit are now counted in the unlabelled `sentinel_events_throttled_total` metric instead of only a 1-in-1000 warn log (#420). No alert on it yet.
- **agent:** eBPF `ExecEvent.cwd` is now filled from `/proc/<pid>/cwd` (empty if the process already exited or is unreadable) (#422).
- **web:** Alerts severity filter now uses the same mapping as the badge (critical includes high/error), a missing `rule_id` no longer crashes the rule filter, and Events rows fall back to a composite React key when `event_id` is absent (#422).
- **go-collector:** an empty `node_id` is now rejected by Register, StreamEvents and `token issue` (an empty `event_id` is still accepted, as the store skips dedup for it) (#468).
- **go-collector:** `Store.Close` waits at most 10s for the GC goroutine and in-flight work, then logs a warning and closes the DB instead of hanging (#468).
- **CI:** the `rust-agent` job now runs `cargo clippy -p sentinel-agent --features ebpf --all-targets -- -D warnings` (with an empty placeholder for the embedded eBPF object, no nightly/bpf-linker), so the `ebpf` feature code is linted; fixed the one `type_complexity` warning it surfaced in a grpc test (#477).
- **go-collector:** `serve` now logs a warning when `--data-dir` is under `/tmp` (the unchanged default); README notes the default is for development only (#506). Newly created data directories are already mode 0700 (pinned by a test).
- **web:** the Nodes page keeps showing the last fetched table and shows the error as a banner when a later poll fails; only a first-load error replaces the table (#254).
- **go-collector:** `--upstreams` now rejects a duplicate region name at startup instead of silently ignoring one of the entries (#254).
- **Collector:** `token issue <node_id>` now rejects node_ids that Register would always refuse (non-printable-ASCII or over 256 bytes) instead of issuing an unusable token (#254).
- **Collector (#491):** an anomaly alert whose save failed is now saved when the agent resends the same event, without double-counting it in the detector. The pending alert is kept in memory only (lost on restart).
- **rust-agent (#241):** events taken from the queue but not yet acked are now kept (up to 2048 events or 2 MiB) and re-sent first after a reconnect (at-least-once), instead of being discarded. Re-sent events can reach the collector twice; it dedups by `event_id` within its retention window. The queue does not survive process exit. Events acked `ok=false` are not re-sent unless the collector marks them retryable (#488).
- **proto/collector/rust-agent (#488):** `EventAck` gains `reason` (`AckReason`: `INVALID`, `RATE_LIMITED`, `RETRYABLE`). The collector sets `RETRYABLE` when saving an event or alert fails, `INVALID` for validation / node_id mismatch, `RATE_LIMITED` for throttling. The agent re-sends only `RETRYABLE` events, at the back of the queue with exponential backoff, up to 5 times, then drops them with a warning. A reason-less `ok=false` (older collector) is still not re-sent. Wire-compatible in both directions (new field 2).
- **rust-agent (#432):** EXEC events now carry `ppid` and `cmdline` (argv joined by spaces, ASCII-sanitized (bytes outside 0x20-0x7E become `?`) and capped at 256 bytes to match the collector limit), read in userspace from `/proc/<pid>/status` and `/proc/<pid>/cmdline`. Very short-lived processes that exit before the read still report `ppid` 0 / empty `cmdline`. Not verified on a real eBPF run.
- **rust-agent (eBPF, #419):** FILE events now carry the `openat` flags, and `op` is derived from them: `create` (O_CREAT), `write` (O_WRONLY/O_RDWR/O_TRUNC/O_APPEND) or `open` (read-only). Previously `op` was always `open`; `open` now means read-only opens only. Not yet verified on a real host (CAP_BPF, no `--mock`).
- **store (#452):** `SaveEvent` is now idempotent per `(node_id, event_id)` within the retention window, so resending an event after a timed-out Ack no longer stores it twice (events with an empty `event_id` are not deduplicated). Store operations are capped at 256 in flight, including writes still finishing after their caller timed out; beyond that they fail fast with `store.ErrBusy` (Ack `ok=false`) instead of piling up goroutines.
- **store (#466):** Reads (`ListEvents`/`ListAlerts`/token reads) now have their own in-flight cap (64), separate from writes (256), so reads stuck on a slow disk can no longer make `SaveEvent` fail with `ErrBusy` and stop ingestion. A conflicting `SaveEvent` no longer retries after its caller's context is done.
- **rust-agent (eBPF, #347):** EXEC events are now emitted from `sched/sched_process_exec` (successful execs only), so `comm` is the executed binary's name instead of the caller's (e.g. `curl`, not `bash`). Rules matching on `comm` (e.g. `bash_execution`) now fire when bash itself is executed, not for every child it spawns.
- **notify (#319):** the per-(rule, node) cooldown map now prunes expired keys, and the cooldown is released when every notifier fails so alerts are retried instead of being silenced for a full cooldown.
- **rust-agent (eBPF, #373):** FILE events are now emitted from `sys_exit_openat`, so `ret` carries the real result (negative errno for failed opens) instead of always 0.
- **rust-agent (eBPF, #369):** `tcp_connect` now checks `skc_family` and skips non-IPv4 sockets, so IPv6 connections are no longer recorded with bogus IPv4 addresses (IPv6 is not yet supported). Known limitation: dual-stack IPv6 sockets connecting to v4-mapped addresses are also skipped (tracked in #429).
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
- **go-collector:** `ListTokens` and `getAgentToken` no longer return results the
  background goroutine may still be writing after the context ends; they return
  nil on error (#382).
- **go-collector (#260):** a node whose `StreamEvents` stream is open but idle no longer goes `inactive`: while the stream is open the collector records a heartbeat every `heartbeat-timeout / 3` (only for already-registered nodes; with auth disabled it starts after the first valid event). After the stream closes the node goes `inactive` once `heartbeat-timeout` has passed.
- **aggregator (#468, partial):** per-region events are merged with a k-way merge that keeps each upstream's order (collector receive order); the agent-reported `timestamp` is only used to compare region heads. Requests with `node` or a `limit` above the 100-entry cache are fetched directly from the region's upstream (`limit` capped at 1000), falling back to the node-filtered cache if the upstream fails. Other items of #468 remain open.
- **go-collector (#457):** a re-sent duplicate event (same `node_id`/`event_id`) no longer re-runs alert evaluation, anomaly detection or event metrics; the Ack stays `ok=true`. Adds `store.SaveEventDedup`; `SaveEvent` keeps its signature.
- **go-collector (#324):** the REST `region` filter is now applied inside the store scan, so `limit` counts matching records, and an unregistered region returns `[]` without scanning the store. If matches lie beyond the scan cap (`maxNodeFilterScan`), fewer than `limit` results are returned. The aggregator's upstream fetch is a separate path and is unchanged.
- **go-collector (#494):** `token issue/revoke/list` now return flag lookup errors (via `flagReader`) instead of silently using an empty `--data-dir` or `--yes` value.
- **web:** the Events table no longer produces duplicate React keys when events share an `event_id` (across nodes or before server-side dedup); rows are keyed by `node_id`, `event_id` and row index (#422, partial).

### Security

- **deploy-agent (#420):** `scripts/deploy-agent.sh` now refuses `--grpc-token` without `--tls` when the collector is not loopback (`localhost`, `127.0.0.0/8`), before anything is uploaded, matching the agent's own startup check.
- **go-collector (#252):** bump `go` in `go.mod` from 1.26.10 to 1.26.11 to pick up the standard-library `net/http` fix for GO-2026-6859; `govulncheck` now reports 0 affected vulnerabilities.
- **rust-agent (#470):** the plaintext-token loopback check now also ends the collector authority at `#`, so `http://<remote>#@127.0.0.1:50051` is rejected instead of sending the token in plaintext to the remote host.
- **ci/rust-agent (#420):** pin all GitHub Actions in `ci.yml` to commit SHAs, set top-level `permissions: contents: read`, and hide `SENTINEL_API_TOKEN` from the agent's `--help` output (`hide_env_values`).
- **go-collector:** REST API now throttles repeated failed bearer-auth attempts per source
  address (5 failures, refilling 5/min) and answers 429 with `Retry-After` (#360).
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
- **go-collector (#482):** with gRPC auth enabled, an established `StreamEvents` stream whose bearer token has been revoked is now closed with `Unauthenticated` ("token revoked") at the next liveness tick (within `heartbeat-timeout / 3`); its node stops heartbeating and goes `inactive` after the heartbeat timeout. A transient token-lookup error (e.g. `ErrBusy`) does not close the stream; it is retried on the next tick (fail-open). With auth disabled there is no token to re-check, so a stream keeps its node alive until it disconnects.

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

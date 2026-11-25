# CLAUDE.md

Rules shared across repositories: see [AGENTS.md](AGENTS.md). Do not copy them here.

## Build / test (same commands as CI, `.github/workflows/ci.yml`)

- Go collector: `cd go-collector && go build ./... && go vet ./... && go test -race ./...`
  (CI coverage threshold 52%; `internal/pb` and `internal/otel` count as 0% in CI, so local `go tool cover` reads higher)
- Rust agent: `cd rust-agent && cargo check --workspace && cargo test --workspace`
  (binary: `rust-agent/target/release/sentinel-agent`; `agent-ebpf` is NOT a workspace member and needs nightly + bpf-linker)
- Web UI: `cd web && npm ci && npm run lint && npm test && npm run build`
- Integration: build the collector and `cargo build --release -p sentinel-agent`, then `./test-integration.sh` (mock mode only)

## Pitfalls

- `sentinel-collector token issue|list` and `token revoke --force` must run with the collector stopped: BadgerDB holds an exclusive lock on `--data-dir`. A plain `token revoke <node_id>` also works while `serve` runs (delegated over `<data-dir>/admin/admin.sock`, #572).
- Collector defaults are `--grpc-auth=required` and `--http-auth=required`; local demos need `--grpc-auth disabled --http-auth disabled`.
- This is a PUBLIC repo: no real IPs, hostnames, tokens, or personal data in code, docs, tests, or commit messages.
- Verify "not yet fixed" claims against `git show origin/master:<path>`, not the shared working tree, which may lag behind
  (see `errors/tooling/` for past false positives).
- CI `runs-on` uses `${{ vars.GATE_RUNNER }}` (public repo, no default). Do not hard-code runner labels.
- Generated code in `go-collector/internal/pb/` comes from `proto/sentinel.proto`; do not edit it by hand.

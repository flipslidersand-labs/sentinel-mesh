# SentinelMesh — 仕様書

## 概要

各 Linux ノードで Rust 製 eBPF Agent を動作させ、Go 製 Control Plane で統合管理する
分散観測プラットフォーム。

## 実装状況の凡例

- 【実装済み】: コードに存在することを確認済み
- 【未実装】: 現時点のコードに存在しない（仕様・将来案）

## アーキテクチャ

現状の実装は Go Collector → BadgerDB（ADR-004）。ClickHouse は将来案であり【未実装】
（コード・go.mod に ClickHouse への参照は無い）。

```
Linux Node A ─ Rust eBPF Agent ┐
Linux Node B ─ Rust eBPF Agent ├→ Go Collector → BadgerDB → API / Alert
Linux Node C ─ Rust eBPF Agent ┘
(将来案: BadgerDB に代えて ClickHouse)
```

## Rust Agent の責務

- eBPF プログラムでカーネルイベント取得（exec / TCP / ファイルオープン）
- ローカル集約とバッファリング
- gRPC ストリームで Go Collector へ送信
- リソース使用量を一定以下に制限【未実装】（Agent に rlimit / cgroup 等の制限処理は無い）

## Go Collector の責務

- Agent 登録・管理 (gRPC)
- イベント受信・正規化
- BadgerDB への保存【実装済み】（ClickHouse は将来案・【未実装】）
- ルール配信 API【未実装】（`proto/sentinel.proto` の RPC は Register / StreamEvents のみ、`/api/rules` も無い。アラートルールは Collector 内の alerting エンジンが評価する）
- アラート生成【実装済み】（`go-collector/internal/alerting`）
- OpenTelemetry Trace/Metrics 出力
- REST API (イベント一覧、クラスタ状態)

## MVP スコープ

- 1 台の Linux Agent で exec + TCP イベントを取得
- Go Collector へ gRPC で送信
- イベント一覧 API (`GET /api/events`)
- 基本集計 (`GET /api/stats`)

## 検出対象イベント

eBPF プログラムは `sys_enter_execve` / `sys_enter_openat` tracepoint と `tcp_connect` kprobe の 3 種のみ
（`rust-agent/agent-ebpf/src/main.rs`）。proto の EventType も EXEC / TCP / FILE のみ。

| カテゴリ       | イベント種別          | 状況     |
| -------------- | --------------------- | -------- |
| プロセス       | execve                | 実装済み |
| プロセス       | exit                  | 未実装   |
| ネットワーク   | TCP connect           | 実装済み |
| ネットワーク   | TCP accept, DNS query | 未実装   |
| ファイル       | open (openat)         | 実装済み |
| ファイル       | unlink, rename        | 未実装   |
| システムコール | 遅延 (>10ms)          | 未実装   |

## データフロー

```
eBPF Program (kernel)
  ↓ ring_buf
Rust Collector (user-space)
  ↓ batch / compress
Go Collector (gRPC stream)
  ↓ normalize
Storage (BadgerDB。ClickHouse は将来案・未実装)
  ↓
REST API
```

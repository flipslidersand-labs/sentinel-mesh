# SentinelMesh — 仕様書

## 概要

各 Linux ノードで Rust 製 eBPF Agent を動作させ、Go 製 Control Plane で統合管理する
分散観測プラットフォーム。

## アーキテクチャ

```
Linux Node A ─ Rust eBPF Agent ┐
Linux Node B ─ Rust eBPF Agent ├→ Go Collector → ClickHouse → API / Alert
Linux Node C ─ Rust eBPF Agent ┘
```

## Rust Agent の責務

- eBPF プログラムでカーネルイベント取得（exec / TCP / ファイルオープン）
- ローカル集約とバッファリング
- gRPC ストリームで Go Collector へ送信
- リソース使用量を一定以下に制限

## Go Collector の責務

- Agent 登録・管理 (gRPC)
- イベント受信・正規化
- ClickHouse または BadgerDB への保存
- ルール配信 API
- アラート生成
- OpenTelemetry Trace/Metrics 出力
- REST API (イベント一覧、クラスタ状態)

## MVP スコープ

- 1 台の Linux Agent で exec + TCP イベントを取得
- Go Collector へ gRPC で送信
- イベント一覧 API (`GET /api/events`)
- 基本集計 (`GET /api/stats`)

## 検出対象イベント

| カテゴリ       | イベント種別                  |
| -------------- | ----------------------------- |
| プロセス       | execve, exit                  |
| ネットワーク   | TCP connect/accept, DNS query |
| ファイル       | open, unlink, rename          |
| システムコール | 遅延 (>10ms)                  |

## データフロー

```
eBPF Program (kernel)
  ↓ ring_buf
Rust Collector (user-space)
  ↓ batch / compress
Go Collector (gRPC stream)
  ↓ normalize
Storage (BadgerDB / ClickHouse)
  ↓
REST API
```

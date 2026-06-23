# ADR-002: Agent → Collector 通信に gRPC 双方向ストリームを使う

- 日付: 2026-06-24
- ステータス: 承認済み

## 背景

Agent からのイベントは高頻度 (数百/秒) であり、Collector からのルール配信も
リアルタイムで行いたい。REST ポーリングでは遅延と過剰リクエストが問題になる。

## 決定

`proto/sentinel.proto` に双方向ストリーム RPC を定義し、
単一コネクションでイベント送信とルール受信を同時に行う。

```protobuf
service SentinelCollector {
  rpc Connect(stream KernelEvent) returns (stream ControlMessage);
}
```

## 理由

- 接続確立コストが 1 回で済む
- Collector → Agent のルール配信を同一チャネルで実現できる
- tonic (Rust) / grpc-go (Go) で実装が対称的

## トレードオフ

- ストリームの再接続ロジックが必要（指数バックオフ）
- HTTP/2 フロー制御を意識する必要がある

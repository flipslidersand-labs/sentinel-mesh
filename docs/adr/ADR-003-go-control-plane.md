# ADR-003: Control Plane を Go で実装する

- 日付: 2026-06-24
- ステータス: 承認済み

## 背景

Control Plane は複数 Agent からの同時接続を管理し、REST API・アラートエンジン・
OTel 出力を並行処理する必要がある。

## 決定

Go + goroutine/channel で Control Plane を実装する。

## 理由

- goroutine による軽量な同時接続管理 (Agent 数 × ストリーム)
- grpc-go が gRPC 双方向ストリームのファーストクラスサポート
- gin による REST API の簡潔な実装
- OTel Go SDK が成熟している

## トレードオフ

- Rust Agent と Go Collector で言語が分かれるため、proto ファイルを
  共通仕様として厳密に管理する必要がある
- GC 停止が極まれにレイテンシスパイクを起こす可能性がある（観測用途では許容範囲）

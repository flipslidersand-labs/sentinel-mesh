# SentinelMesh — 技術スタック

## Rust Agent

| 用途              | クレート           | バージョン |
| ----------------- | ------------------ | ---------- |
| eBPF              | aya                | 0.12       |
| eBPF カーネル     | aya-bpf            | 0.12       |
| 非同期            | tokio (full)       | 1          |
| gRPC クライアント | tonic              | 0.11       |
| Protobuf          | prost              | 0.12       |
| シリアライズ      | serde + serde_json | 1          |
| エラー            | anyhow             | 1          |
| CLI               | clap (derive)      | 4          |

## Go Collector

| 用途          | パッケージ                     | バージョン |
| ------------- | ------------------------------ | ---------- |
| gRPC サーバー | google.golang.org/grpc         | 1.63.0     |
| Protobuf      | google.golang.org/protobuf     | 1.34.0     |
| HTTP ルーター | github.com/gin-gonic/gin       | 1.10.0     |
| KV ストア     | github.com/dgraph-io/badger/v4 | 4.2.0      |
| ログ          | go.uber.org/zap                | 1.27.0     |
| OTel          | go.opentelemetry.io/otel       | 1.24.0     |
| CLI           | github.com/spf13/cobra         | 1.8.0      |
| 設定          | github.com/spf13/viper         | 1.19.0     |

## 通信プロトコル

- Agent → Collector: gRPC streaming (bidirectional)
- Collector → クライアント: REST (JSON) + gRPC
- Proto 定義: `proto/sentinel.proto` (共有)

# SentinelMesh — 技術スタック

バージョンは固定表を持たない。正は次のファイル（直接依存のみ記載）。

- Go Collector: `go-collector/go.mod`
- Rust Agent: `rust-agent/Cargo.toml`（workspace.dependencies）、`rust-agent/agent/Cargo.toml`、`rust-agent/agent-ebpf/Cargo.toml`

## Rust Agent

| 用途               | クレート                     | 備考                                      |
| ------------------ | ---------------------------- | ----------------------------------------- |
| eBPF（ユーザ空間） | aya                          | `rust-agent/agent/Cargo.toml`（optional） |
| eBPF（カーネル）   | aya-ebpf                     | `rust-agent/agent-ebpf/Cargo.toml`        |
| 非同期             | tokio (full)                 |                                           |
| gRPC クライアント  | tonic / tonic-build          |                                           |
| Protobuf           | prost                        |                                           |
| シリアライズ       | serde + serde_json           |                                           |
| エラー             | anyhow                       |                                           |
| CLI                | clap (derive, env)           |                                           |
| その他             | hostname, uuid, async-stream |                                           |

## Go Collector

| 用途          | パッケージ                                     |
| ------------- | ---------------------------------------------- |
| gRPC サーバー | google.golang.org/grpc                         |
| Protobuf      | google.golang.org/protobuf                     |
| HTTP ルーター | github.com/gin-gonic/gin（+ gin-contrib/cors） |
| KV ストア     | github.com/dgraph-io/badger/v4                 |
| ログ          | go.uber.org/zap                                |
| OTel          | go.opentelemetry.io/otel                       |
| メトリクス    | github.com/prometheus/client_golang            |
| CLI           | github.com/spf13/cobra                         |
| ID            | github.com/google/uuid                         |

設定は cobra のフラグで行う（viper は使用していない）。

## 通信プロトコル

- Agent → Collector: gRPC streaming (bidirectional)
- Collector → クライアント: REST (JSON)。gRPC は Agent との通信 (`Register` / `StreamEvents`) のみ
- Proto 定義: `proto/sentinel.proto` (共有)

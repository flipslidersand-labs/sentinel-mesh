# SentinelMesh — データモデル

## KernelEvent (共通構造)

```protobuf
message KernelEvent {
  string  event_id   = 1;  // UUID v4
  string  node_id    = 2;  // Agent ノード識別子
  int64   timestamp  = 3;  // Unix nanoseconds
  EventType type     = 4;
  oneof payload {
    ExecEvent   exec   = 10;
    TcpEvent    tcp    = 11;
    FileEvent   file   = 12;
  }
}

enum EventType {
  EXEC    = 0;
  TCP     = 1;
  FILE    = 2;
}
```

## ExecEvent

```protobuf
message ExecEvent {
  uint32 pid     = 1;
  uint32 ppid    = 2;
  string comm    = 3;  // プロセス名 (16 bytes)
  string cmdline = 4;
  string cwd     = 5;
  uint32 uid     = 6;
}
```

## TcpEvent

```protobuf
message TcpEvent {
  uint32 pid        = 1;
  string comm       = 2;
  string src_ip     = 3;
  uint32 src_port   = 4;
  string dst_ip     = 5;
  uint32 dst_port   = 6;
  string direction  = 7;  // "connect" | "accept"
}
```

## FileEvent

```protobuf
message FileEvent {
  uint32 pid      = 1;
  string comm     = 2;
  string path     = 3;
  string op       = 4;  // "open" | "unlink" | "rename"
  int32  ret      = 5;  // システムコール戻り値
}
```

## AgentNode (Go Collector 内 BadgerDB)

```json
{
  "node_id": "node-abc123",
  "hostname": "worker-01",
  "ip": "10.0.0.5",
  "version": "0.1.0",
  "region": "us-east",
  "registered": "2026-01-01T00:00:00Z",
  "last_seen": "2026-01-01T01:00:00Z",
  "status": "active"
}
```

## AlertRule (YAML 設定)

リポジトリ直下の `rules.yaml` が実スキーマ。実装は `go-collector/internal/alerting/engine.go`。

```yaml
rules:
  - id: tcp_to_suspicious_port
    description: Alert on connections to high ports
    event_type: tcp          # exec | tcp | file | *(すべて)
    severity: warning
    message: "TCP connection to non-standard port detected"
    conditions:              # すべて満たしたときに発火(AND)
      - field: dst_port      # イベント payload のキー名。存在しないキーは起動時に警告が出る
        operator: gt
        value: "10000"       # 値は常に文字列で書く
```

- サポートする `operator`: `eq`(文字列一致)、`contains`(部分一致)、`regex`(正規表現)、`gt` / `lt`(数値比較)。rules.yaml に未知の operator があると、起動時の検証(`RuleSet.Validate`)でエラーになり Collector は起動しない
- 頻度・件数の検知(旧例の `window` / `count`)は rules.yaml では表現できない。`go-collector/internal/anomaly` が別機構で担う

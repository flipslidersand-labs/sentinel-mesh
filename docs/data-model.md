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
  "registered": "2026-01-01T00:00:00Z",
  "last_seen": "2026-01-01T01:00:00Z",
  "status": "active"
}
```

## AlertRule (YAML 設定)

```yaml
rules:
  - name: suspicious_outbound
    type: tcp
    condition:
      dst_port: [4444, 6666, 9001]
    action: alert
  - name: mass_exec
    type: exec
    window: 10s
    condition:
      count: "> 50"
    action: alert
```

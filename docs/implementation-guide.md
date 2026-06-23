# SentinelMesh — 実装ガイド

## Phase 1: Rust Agent MVP (2週)

1. aya-ebpf で exec_tp (sys_enter_execve) をアタッチ
2. ring_buf でユーザースペースへイベント転送
3. TCP connect の kprobe 追加
4. JSON 出力で動作確認

## Phase 2: gRPC 通信 (1週)

1. `proto/sentinel.proto` を定義
2. tonic で Agent 側 streaming client 実装
3. Go Collector 側 gRPC server 実装
4. バイナリメッセージ確認

## Phase 3: Go Collector REST API (1週)

1. gin で `/api/events` GET 実装
2. `/api/nodes` GET (登録済み Agent 一覧)
3. `/api/stats` GET (種別ごとのカウント)
4. BadgerDB にイベントを保存

## Phase 4: Agent 登録・管理 (1週)

1. Agent 起動時に Register RPC を呼ぶ
2. Collector が AgentNode を BadgerDB に保存
3. Heartbeat (30s interval)
4. 切断時に status: inactive へ更新

## Phase 5: アラートエンジン (1週)

1. `rules.yaml` をロード
2. イベント受信ごとにルール評価
3. 条件一致で zap.Warn ログ + `/api/alerts` へ記録

## Phase 6: OpenTelemetry 出力 (3日)

1. Collector に OTel SDK 追加
2. イベント件数を Metrics として公開
3. アラート発火を Trace Span として記録

## Phase 7: 複数ノード対応 (1週)

1. node_id を hostname + UUID で生成
2. Collector 側でノード別イベントを区別して保存
3. `/api/events?node=xxx` でフィルタリング

## 注意点

- eBPF のロードには `CAP_BPF` または root が必要
- Ubuntu 22.04 以降 (BTF 対応カーネル) を推奨
- `xtask build-ebpf` でカーネル側バイナリを先にビルド
- Rust ツールチェーンは `bpf-linker` インストール必須

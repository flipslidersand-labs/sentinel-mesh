# ADR-004: イベント保存に BadgerDB を使う（Phase 1）

- 日付: 2026-06-24
- ステータス: 承認済み

## 背景

MVP では外部サービスへの依存を最小化しつつ、イベントを永続化して
API から参照できる状態にしたい。

## 決定

Phase 1〜4 では BadgerDB (組み込み KV) を使用する。
Phase 5 以降は ClickHouse への移行を検討する。

## 理由

- Go バイナリに組み込めるため Docker Compose 不要でローカル動作
- LSM-Tree ベースで高速な書き込みに適する
- StreamRail (G1) で同様の判断をしており知見が流用できる

## トレードオフ

- 複雑な集計クエリには向かない（ClickHouse への移行動機）
- BadgerDB の GC を定期実行しないとディスク増加

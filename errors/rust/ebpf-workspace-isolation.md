---
title: "eBPF クレートをワークスペース外でビルドする際の workspace 衝突"
tags: [rust, cargo, workspace, ebpf]
severity: medium
date: "2026-08-16"
---

## 症状

`cargo +nightly build --target bpfel-unknown-none` を agent-ebpf/ で実行すると:

```
error: current package believes it's in a workspace when it's not:
current:   .../agent-ebpf/Cargo.toml
workspace: .../rust-agent/Cargo.toml
```

## 原因

cargo は親ディレクトリを再帰的にスキャンして workspace root を探す。
`rust-agent/Cargo.toml` が `[workspace]` を持つため、子ディレクトリの
agent-ebpf が勝手にその workspace に属すると誤認される。
agent-ebpf は `bpfel-unknown-none` ターゲット向けに単独ビルドが必要なため
workspace メンバーにできない。

## 解決策

`agent-ebpf/Cargo.toml` に空の `[workspace]` テーブルを追加して独立させる:

```toml
[[bin]]
name = "sentinel-agent-ebpf"
path = "src/main.rs"

[workspace]   # ← これを追加
```

## 予防

eBPF カーネルコードを別ターゲットでビルドするクレートは必ず `[workspace]` を付ける。
aya プロジェクトの推奨構成もこのパターン。

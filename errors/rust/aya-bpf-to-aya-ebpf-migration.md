---
title: "aya-bpf 0.1 → aya-ebpf 0.2 移行: crates.io 掲載廃止 + API 変更"
tags: [rust, ebpf, aya]
severity: high
date: "2026-08-16"
---

## 症状

`cargo build --target bpfel-unknown-none` が以下のエラーで失敗:

```
error: no matching package named `aya-bpf` found
```

または `#[kprobe(name = "...")]` に対して:

```
error: invalid argument
error: macro expansion ignores `{` and any tokens following
note: the usage of `kprobe!` is likely invalid in item context
```

## 原因

1. `aya-bpf` は crates.io に登録されていない（git 依存のみ）。
   crates.io 掲載版は `aya-ebpf = "0.2"` に改名されている。
2. aya-ebpf 0.2 ��は `#[kprobe(name = "prog_name")]` の `name` 引数が廃止。
   BPF プログラム名は Rust 関数名から自動導出される。

## 解決策

```toml
# Cargo.toml (agent-ebpf)
[dependencies]
aya-ebpf = "0.2"   # aya-bpf = "0.1" から変更
```

```rust
// use aya_bpf → use aya_ebpf に一括置換
use aya_ebpf::{...};

// kprobe の name 引数を削除
#[kprobe]           // #[kprobe(name = "sentinel_tcp_connect")] → これだけ
pub fn sentinel_tcp_connect(ctx: ProbeContext) -> u32 { ... }
```

## 予防

- `aya-bpf` の依存を記載するなら `git = "https://github.com/aya-rs/aya"` のみ有効。
  crates.io からは `aya-ebpf` を使う。
- `#[kprobe]` / `#[tracepoint]` の引数は aya-ebpf バージョンで変わるため、
  リリースノートを確認する。

---
title: "include_bytes! に ../.. を含むパスが解決されない"
tags: [rust, include_bytes, path]
severity: medium
date: "2026-08-16"
---

## 症状

```
error: couldn't read `.../agent/../../target/bpfel-unknown-none/debug/sentinel-agent-ebpf`:
  No such file or directory
```

ファイルは `rust-agent/target/bpfel-unknown-none/debug/sentinel-agent-ebpf` に存在するが
`include_bytes!` が解決できない。

## 原因

`CARGO_MANIFEST_DIR` = `rust-agent/agent` で、コードは:
```rust
concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/...")
```
と記述されていたが、`agent` から `../..` は **rust-agent の 2 つ上** になり、
`sentinel-mesh/target/` を探してしまう誤りだった。
`agent` は workspace root `rust-agent` の直下にあるため `..` は 1 つで足りる。

## 解決策

```rust
// 誤
"/../../target/bpfel-unknown-none/debug/sentinel-agent-ebpf"
// 正
"/../target/bpfel-unknown-none/debug/sentinel-agent-ebpf"
```

## 予防

`CARGO_MANIFEST_DIR` からの相対パスは、ディレクトリ階層を手で数えてから書く。
`ls -la "$(path)"` でシェルから事前検証するのが確実。

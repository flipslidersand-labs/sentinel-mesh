---
title: "ローカルGoツールチェインにcovdata欠如→go test ./... -coverprofileがCIと異なる(過大な)値を出す"
tags: [go, coverage, toolchain, ci-local-mismatch]
severity: high
date: "2026-09-15"
---

## 症状

`go test ./... -coverprofile=x.out` の集計値（`go tool cover -func | grep total:`）が
ローカルでは 72.9% と出たが、CI（qa-workflows/go-test.yml、同一コマンド + `-race -covermode=atomic`）
では 57.9% だった。この値を信じて `coverage-threshold: 65` を設定した PR が CI で fail した
（PR #186、sentinel-mesh）。

## 原因

ローカルの `go` コマンドは `/usr/local/go/bin/go`（実体 go1.23.1）で、`go.mod` の
`go 1.25.0` 要求により GOTOOLCHAIN=auto がモジュールキャッシュから go1.25.0 toolchain を
自動ダウンロードして委譲実行していた。このダウンロード済み toolchain
（`~/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.0.../pkg/tool/linux_amd64/`）に
**`covdata` バイナリが欠けていた**（`/usr/local/go/pkg/tool/linux_amd64/` には存在するのに、
ダウンロード版には無い）。

`covdata` は Go 1.20+ のクロスパッケージカバレッジ集計（`go test ./...` で複数パッケージを
同一 `-coverprofile` に集約する際、テストファイルが無いパッケージ（今回は生成コードの
`internal/pb`、`internal/otel`）も 0% として profile に算入する機能）に必要。これが欠けると
該当パッケージが profile から**サイレントに除外**される（`go: no such tool "covdata"` という
stderr 警告は出るが、`go test` 自体は他パッケージの結果を "ok" として返し続け、終了コードは
0 のまま）。結果、分母（総 statement 数）が小さくなり、実際より高いカバレッジ率が出る。

## 解決策

- カバレッジ閾値のようなCI必須条件に関わる数値は、**必ずCI実測値で検証する**。ローカルの
  `go tool cover -func` 値を鵜呑みにしない。
- ローカルで正確に再現したい場合は `GOTOOLCHAIN=local` を使わず、`covdata` を含む完全な
  toolchain（`/usr/local/go` 等、`go version` の実体を `go env GOROOT` で確認）を使う。
  `go env GOROOT` がモジュールキャッシュ配下（`.../toolchain@.../`）を指している場合は要注意。
- `go test ./... -coverprofile` 実行時に stderr へ `go: no such tool "covdata"` が出ていないか
  必ず確認する（出力に紛れて見落としやすい）。

## 予防

- カバレッジ関連の PR（閾値変更・段階的引き上げ運用ルールを含む issue、例: #141）を作業する
  agent/subagent のプロンプトに「ローカル測定値だけでなく CI 実行結果（Actions のログ、
  `gh pr checks`）で最終確認すること」を明記する。
- 生成コード（protobuf 等）や計装コード（otel setup）など「意図的にテストが無い」パッケージが
  存在するモジュールでは、ローカル/CI 間のこの種の乖離が起きやすいと想定しておく。

---
title: "コードレビューsubagentが共有ワークツリーの古いローカルcheckoutを見て誤検知する"
tags: [git-worktree, code-review, subagent, false-positive]
severity: medium
date: "2026-09-13"
---

## 症状

「新しいissueをスカウトして」と一般目的subagentに依頼したところ、6件中3件が
「すでにPRでマージ済みの修正がまだ入っていない」という誤検知だった
（web/src/api.tsのトークン未送信、CI timeout-minutes未設定、reusable workflow
の`@main`直参照）。実際はorigin/masterには全て修正が入っていた。

## 原因

`~/projects`配下のメイン作業ツリーは複数セッション・複数worktreeで共有されており、
自セッションが直近でmergeしたPRの内容も、他セッションが動かした別ブランチの内容も、
ローカルの`git status`上のファイルには反映されない（このセッション中もローカルHEADは
14コミット遅れていた）。subagentはファイルシステム上のパスを素直にReadするため、
古い内容を「現在のコード」として検証してしまう。

## 解決策

スカウト系subagentのプロンプトに以下を必須で明記する:

```
git fetch origin master
git log --oneline -1 origin/master   # ローカルHEADと比較
git show origin/master:<path>        # ファイル内容は必ずこれで読む（作業ツリー直読み禁止）
```

呼び出し側（親セッション）でも、subagentが返した「未修正」という主張を鵜呑みにせず、
issue化する前に自分で`git show origin/master:<path>`を1回実行して二重検証する
（このセッションでは実際にこの二重検証で3件を却下できた）。

## 予防

- 「バグを探して」系のsubagentプロンプトには、対象リポが共有worktree環境かどうかに
  関わらず、常に`git fetch`＋`git show origin/<branch>:<path>`での検証手順をテンプレ化
  して埋め込む。
- 「findingが本当に未修正か」の検証コストは、誤ってissueを量産するコストより
  はるかに低い。

# リアルタイムRunner

このRunnerは、定期的に **データ取り込み → 特徴量更新 → 取引提案** を自動で回し、Order Intent を作成します。  
**実行（approve + execute）は自動化されません。**

## 使い方
```bash
uv run trade-agent run --strategy news_overlay
```

### 長時間運用の例
- ターミナルを閉じない運用: `tmux` / `screen` を使う
- 簡易バックグラウンド: `nohup` で実行（ログは別途保存）

## 推奨インターバルの目安
| 足 | market_poll_seconds | news_poll_seconds | propose_poll_seconds |
|---|---:|---:|---:|
| 1m | 30〜60 | 120〜300 | 60 |
| 5m | 60〜120 | 300〜600 | 120 |
| 15m | 120〜300 | 300〜900 | 300 |

`propose_cooldown_seconds` は重複提案を抑制するため 300 秒程度から開始がおすすめです。

## 状態ファイル
Runner は `data/runner_state.json` に以下を更新します:
- 最終成功時刻（market/news/propose）
- 最終エラー時刻と概要
- 反復カウンタ

CLI or Web UI の監査ログと合わせて、動作確認に利用してください。

## systemd サンプル（参考）
```ini
[Unit]
Description=News-aware Crypto Agent Runner
After=network.target

[Service]
Type=simple
WorkingDirectory=/path/to/news-aware-crypto-agent
ExecStart=/usr/bin/env bash -lc 'uv run trade-agent run --strategy news_overlay'
Restart=always

[Install]
WantedBy=multi-user.target
```

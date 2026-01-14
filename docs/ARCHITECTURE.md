# Architecture

## Non-negotiables (from README/AGENTS)
- Spot-only trading. No leverage, margin, futures, or shorting.
- Default execution is paper (dry-run) and requires human approval.
- Live trading requires BOTH env `I_UNDERSTAND_LIVE_TRADING=true` and config `trading.i_understand_live_trading: true`.
- API keys are never stored in the repo; load from env or `.env`.
- News text is untrusted; only normalized fields and derived features are used.
- Risk gates must remain enforced (kill switch, daily loss limit, order limits, cooldown).
- CLI commands must remain: `status`, `ingest`, `propose`, `approve`, `execute`, `backtest`, `report`.

## High-level pipeline
1. **Ingest**
   - Fetch OHLCV candles incrementally (since last timestamp with a small overlap). If the exchange lacks `fetchOHLCV`, build candles from `fetchTrades` when available.
   - Optional orderbook snapshots via ccxt.
   - Fetch RSS articles, normalize, and deduplicate.
2. **Feature extraction**
   - Convert normalized news to safe features: sentiment score, keyword flags, source weights.
3. **Propose**
   - Strategy generates a `TradePlan` (baseline or news overlay).
   - Risk engine enforces limits and adjusts size if needed.
4. **Intent store**
   - Create a signed `OrderIntent` (canonical JSON + SHA256).
   - Persist intent and audit log.
5. **Approval**
   - Store approval with intent hash.
6. **Execute**
   - Paper: simulate fills with deterministic RNG + orderbook snapshot.
   - Live: ccxt limit order with timeout + cancel.
7. **Report**
   - Compute metrics and output JSON/CSV equity curve.

## Module map (implemented vs partial)
- `src/trade_agent/main.py`: CLI orchestration for all commands. **Implemented**.
- `src/trade_agent/config.py`: YAML config loader + defaults. **Implemented**.
- `src/trade_agent/db.py`: SQLite schema + CRUD helpers + audit logs. **Implemented**.
- `src/trade_agent/exchange.py`: ccxt adapter (spot, rate limit enforced). **Implemented**.
- `src/trade_agent/news/rss.py`: RSS ingestion + dedup by URL/title hash. **Implemented**.
- `src/trade_agent/news/normalize.py`: Normalization + safe text. **Implemented**.
- `src/trade_agent/news/features.py`: Sentiment + keyword flags + source weighting. **Implemented**.
- `src/trade_agent/strategies/baseline.py`: SMA + momentum baseline. **Implemented**.
- `src/trade_agent/strategies/news_overlay.py`: Sentiment overlay on baseline. **Implemented**.
- `src/trade_agent/risk.py`: Risk gates (kill switch, daily loss, cooldown, max orders). **Implemented**.
- `src/trade_agent/intent.py`: OrderIntent signing + expiry. **Implemented**.
- `src/trade_agent/paper.py`: Deterministic fill simulation. **Implemented**.
- `src/trade_agent/executor.py`: Paper + live execution; live uses limit orders with maker emulation if `postOnly` is unsupported. **Implemented** (no market fallback).
  - Maker emulation uses configurable `trading.maker_emulation` settings.
- `src/trade_agent/backtest.py`: Candle + news latency backtest. **Implemented**.
- `src/trade_agent/metrics.py`: PnL metrics + reports. **Implemented**.
- `src/trade_agent/apps/cli.py`: CLI entrypoint (status/ingest/propose/approve/execute/backtest/report). **Implemented**.
- `src/trade_agent/apps/web.py`: FastAPI Web UI backend + static asset serving. **Implemented**.
- `src/trade_agent/services/`: Shared service layer for CLI/Web (ingest/propose/approve/execute/report). **Implemented**.
- `legacy/streamlit_app.py`: Legacy Streamlit UI (optional). **Implemented**.

## Data model / storage approach (SQLite)
- `candles`: OHLCV per `symbol`, `timeframe`, `ts` + `source`, `ingested_at`.
- `orderbook_snapshots`: best bid/ask snapshots for paper fill simulation.
- `news_articles`: normalized RSS entries with `guid`, `summary`, `published_at`, `observed_at`, `raw_payload_hash`; dedup by `url` and `title_hash`.
- `news_features`: sentiment/keywords/source weight per article + `feature_version`.
- `feature_rows`: aggregated feature vectors per `symbol` and `ts`, with `news_window_start/end`.
- `order_intents`: proposed/approved/expired intents with canonical JSON + hash + `order_type`, `time_in_force`, `rationale_features_ref`.
- `approvals`: stored approval hash + timestamp + `approved_by`.
- `executions`: execution attempts (paper/live) with status, `fee`, `slippage_model`, and details.
- `orders`: order records keyed by `order_id` with raw exchange/paper payloads.
- `fills`: executed fills.
- `trade_results`: realized PnL and metadata.
- `daily_stats`: derived day-level stats (orders_count, realized_pnl).
- `reports`: metrics JSON + equity curve path per run.
- `audit_logs`: event trail for ingest/propose/approve/execute/report/backtest.

## Timing model
- **Market data**: `candles.ts` is exchange time in ms; `ingested_at` is local UTC (observed_at).
- **Orderbook**: `orderbook_snapshots.ts` is exchange time in ms; `ingested_at` is local UTC.
- **News**: `published_at` from RSS (UTC normalized); `observed_at` is local UTC (ingest time).
- **Features**: `extracted_at` / `computed_at` are local UTC.
- **Latency guard**: proposals/backtests only use news with `observed_at <= ts - news_latency_seconds` and lookback windows bound by `published_at`.

## Safety gates
- **Paper default**: config defaults to paper + dry run.
- **Human approval**: `trading.require_approval` must be satisfied unless autopilot constraints pass.
- **Approve+Execute**: a convenience flow that still records approval and enforces the same gates.
- **Live double-consent**: both env and config flags are required for live execution.
- **Long-only**: sells are only allowed when a spot position exists; otherwise proposals become hold.
- **Risk limits**: kill switch, daily loss proxy, order count, cooldown, position sizing, notional caps.
- **Spot only**: no leverage, margin, futures, or shorts.

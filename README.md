# News-Aware Crypto Spot Trading Agent (MVP)

Local-first research framework for a single user. It ingests market data and news, turns news into safe features, proposes trade plans under strict risk limits, and requires human approval by default. This is **not** a promise of profitability.

## Key Safety Notes
- Spot-only (no leverage, margin, or futures).
- API keys are never stored in the repo. Use env vars or `.env`.
- Live trading requires **both** `I_UNDERSTAND_LIVE_TRADING=true` and config `i_understand_live_trading: true`.
- Default mode is paper (dry run) and requires approval.

## Setup (uv)
```bash
brew install uv
uv venv
uv pip install -e '.[dev]'
cp config.example.yaml config.yaml
```

Create `.env` with your exchange keys (do not commit):
```
EXCHANGE_API_KEY=...
EXCHANGE_API_SECRET=...
EXCHANGE_API_PASSWORD=...  # if required
I_UNDERSTAND_LIVE_TRADING=false
```

## CLI Overview
```bash
uv run trade-agent status
uv run trade-agent ingest --orderbook
uv run trade-agent propose --strategy news_overlay
uv run trade-agent approve <intent_id>
uv run trade-agent execute --mode paper
uv run trade-agent backtest --from 2024-01-01 --to 2024-02-01
uv run trade-agent report
```

## UI (Streamlit)
Install UI extras and launch:
```bash
uv pip install -e '.[dev,ui]'
uv run streamlit run streamlit_app.py
```

### Commands
- `status` : Check exchange + news readiness.
- `ingest` : Fetch candles and RSS news, compute features.
- `propose` : Generate a Trade Plan and store an Order Intent.
- `approve <intent_id>` : Approve with the configured phrase.
- `execute` : Execute approved intents in paper or live mode.
- `backtest` : Run on stored candles/news with latency.
- `report` : Save metrics and equity curve to `data/reports/`.

## Configuration
Edit `config.yaml`:
- `exchange.name`: ccxt exchange id (spot only).
- `trading.symbol_whitelist`: allowed symbols.
- `trading.maker_emulation`: maker-style price padding when `postOnly` is unavailable.
- `risk.*`: position, loss, and rate limits.
- `news.rss_urls`: feeds to ingest.
- `paper.*`: deterministic simulation settings.

Note: If the exchange does not support `fetchOHLCV`, the app falls back to building candles from `fetchTrades` when available.

## Testing
```bash
uv run pytest
```

## Disclaimer
This project is a research/experimentation framework. Use at your own risk.

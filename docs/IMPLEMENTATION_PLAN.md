# Implementation Plan

## Scope alignment with README
This plan preserves the current CLI surface (`status`, `ingest`, `propose`, `approve`, `execute`, `backtest`, `report`) and keeps spot-only trading, default paper mode, and human approval as non-negotiables.

## Current state summary
- Core pipeline (ingest → features → propose → intent → approve → execute → report) is implemented and covered by tests.
- Live execution supports limit orders with timeouts, but maker emulation/cancel-replace and market fallback are not implemented.
- Autopilot logic exists but has no CLI/UI toggles beyond config and is intentionally constrained.

## Prioritized tasks

### P0 — Safety & execution correctness
1) **Maker emulation for exchanges without postOnly**
   - Implement cancel/replace or price-padding logic when `postOnly` is not supported.
    - Acceptance criteria:
      - `execute --mode live` places a maker-style order even when exchange lacks `postOnly`.
      - Orders are canceled after timeout and recorded in `executions` with status.
    - Status: implemented with best bid/ask padding and execution detail logging.

2) **Live-mode guardrail tests**
   - Enforce double-consent (env + config) and add unit tests for `dry_run`, consent, and missing credentials.
   - Acceptance criteria:
     - Tests assert live execution is rejected unless both flags are true and credentials exist.
   - Status: implemented.

### P1 — Data ingestion reliability
3) **Incremental candle ingestion**
   - Track last candle timestamp per symbol/timeframe and request `since` to avoid redundant pulls.
   - Acceptance criteria:
     - Repeated `ingest` does not refetch overlapping candles beyond a small overlap window.

4) **News ingestion robustness**
   - Add basic feed error reporting and per-feed stats.
   - Acceptance criteria:
     - `ingest` returns per-feed counts and errors without failing the whole run.

### P1 — Strategy/risk visibility
5) **Risk explanation logging**
   - Log explicit risk rejections/adjustments into audit logs.
   - Acceptance criteria:
     - Each proposal records whether size was adjusted and why.

6) **Report enrichment**
   - Include trade list CSV with timestamps, side, price, size, fee, and pnl.
   - Acceptance criteria:
     - `report` produces a trade CSV alongside existing JSON/CSV outputs.

### P2 — UI and operator UX
7) **UI dashboards**
   - Add UI sections for latest intents, approvals, executions, and a basic equity curve chart.
   - Acceptance criteria:
     - Streamlit shows recent intents/executions and renders equity curve from `report` data.

8) **Config validation UX**
   - Validate required config keys at startup with user-friendly errors.
   - Acceptance criteria:
     - Missing keys produce a structured error without stack traces.

## Test status
- `uv run pytest` (current) passes locally with 4 tests.

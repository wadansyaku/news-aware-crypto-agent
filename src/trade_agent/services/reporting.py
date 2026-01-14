from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from trade_agent.backtest import run_backtest
from trade_agent.config import AppSettings
from trade_agent.metrics import compute_metrics, save_report, save_trade_csv
from trade_agent.schemas import ReportRecord
from trade_agent.store import SQLiteStore


def backtest(
    settings: AppSettings,
    store: SQLiteStore,
    start: str,
    end: str,
    strategy: str,
    symbol: Optional[str] = None,
) -> dict[str, Any]:
    if strategy not in {"baseline", "news_overlay"}:
        raise ValueError("invalid strategy")
    symbol = symbol or settings.trading.symbol_whitelist[0]
    timeframe = settings.trading.timeframes[0]
    output_dir = str(Path(settings.app.data_dir) / "reports")

    result = run_backtest(store, settings, symbol, timeframe, start, end, strategy, output_dir)
    metrics_payload = result.metrics.__dict__.copy()
    metrics_payload["strategy"] = strategy
    store.save_report_record(
        ReportRecord(
            run_id=str(uuid.uuid4()),
            period=f"{start}:{end}",
            metrics=metrics_payload,
            equity_curve_path=result.metrics_path_csv,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
    )
    store.log_event("backtest", {"strategy": strategy, "report_json": result.metrics_path_json})

    summary_text = Path(result.metrics_path_summary).read_text(encoding="utf-8")
    return {
        "report_json": result.metrics_path_json,
        "equity_csv": result.metrics_path_csv,
        "summary_txt": result.metrics_path_summary,
        "summary": summary_text,
        "metrics": metrics_payload,
        "equity": result.equity,
        "trades": result.trades,
    }


def report(
    settings: AppSettings,
    store: SQLiteStore,
    mode: Optional[str] = None,
) -> dict[str, Any]:
    if mode and mode not in {"paper", "live"}:
        raise ValueError("invalid mode")

    trades = store.load_trades(mode)
    metrics, equity = compute_metrics(trades, capital_jpy=settings.risk.capital_jpy)
    output_dir = str(Path(settings.app.data_dir) / "reports")
    paths = save_report(metrics, equity, output_dir, f"report_{mode or 'all'}")
    trade_details = store.load_trade_details(mode)
    trade_csv = save_trade_csv(trade_details, output_dir, f"report_{mode or 'all'}")
    store.save_report_record(
        ReportRecord(
            run_id=str(uuid.uuid4()),
            period=mode or "all",
            metrics=metrics.__dict__,
            equity_curve_path=paths["csv"],
            created_at=datetime.now(timezone.utc).isoformat(),
        )
    )
    store.log_event(
        "report",
        {"mode": mode, "report_json": paths["json"], "summary_txt": paths["summary"]},
    )
    return {
        "metrics": metrics.__dict__,
        "equity": equity,
        "trades": trade_details,
        "paths": {**paths, "trades": trade_csv},
    }


def analytics(settings: AppSettings, store: SQLiteStore, mode: Optional[str] = None) -> dict[str, Any]:
    if mode and mode not in {"paper", "live"}:
        raise ValueError("invalid mode")
    trades = store.load_trades(mode)
    metrics, equity = compute_metrics(trades, capital_jpy=settings.risk.capital_jpy)
    trade_details = store.load_trade_details(mode)
    return {"metrics": metrics.__dict__, "equity": equity, "trades": trade_details}

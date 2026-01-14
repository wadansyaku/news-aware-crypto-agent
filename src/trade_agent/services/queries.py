from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from trade_agent.config import AppSettings
from trade_agent.store import SQLiteStore


def list_intents(store: SQLiteStore, limit: int = 20) -> list[dict[str, Any]]:
    return store.list_intents(limit)


def list_audit_logs(store: SQLiteStore, event: Optional[str] = None, limit: int = 50) -> list[dict[str, Any]]:
    logs = []
    for row in store.list_audit_logs(event=event, limit=limit):
        data = json.loads(row["data_json"]) if row["data_json"] else {}
        logs.append({"ts": row["ts"], "event": row["event"], "data": data})
    return logs


def position(settings: AppSettings, store: SQLiteStore, symbol: Optional[str] = None) -> dict[str, Any]:
    sym = symbol or settings.trading.symbol_whitelist[0]
    size, avg = store.get_position_state(sym)
    last_execution = store.get_last_execution_time()
    return {"symbol": sym, "size": size, "avg_price": avg, "last_execution": last_execution}


def position_overview(
    settings: AppSettings, store: SQLiteStore, symbol: Optional[str] = None
) -> dict[str, Any]:
    sym = symbol or settings.trading.symbol_whitelist[0]
    size, avg = store.get_position_state(sym)
    timeframe = settings.trading.timeframes[0]
    latest = store.get_latest_candle(sym, timeframe)
    current_price = float(latest["close"]) if latest else 0.0
    price_ts = int(latest["ts"]) if latest else None

    unrealized = (current_price - avg) * size if size > 0 else 0.0
    return_pct = (current_price - avg) / avg if size > 0 and avg > 0 else 0.0
    exposure = (
        (current_price * size) / settings.risk.capital_jpy
        if current_price > 0 and settings.risk.capital_jpy > 0
        else 0.0
    )
    open_ts = store.get_position_open_time(sym)

    return {
        "symbol": sym,
        "size": size,
        "avg_price": avg,
        "current_price": current_price,
        "current_price_ts": price_ts,
        "unrealized_pnl": unrealized,
        "return_pct": return_pct,
        "exposure_pct": exposure,
        "position_opened_at": open_ts,
    }


def list_backtest_reports(store: SQLiteStore, limit: int = 50) -> list[dict[str, Any]]:
    reports = []
    for row in store.list_reports(limit=limit):
        metrics = json.loads(row["metrics_json"]) if row["metrics_json"] else {}
        reports.append(
            {
                "run_id": row["run_id"],
                "period": row["period"],
                "metrics": metrics,
                "equity_curve_path": row["equity_curve_path"],
                "created_at": row["created_at"],
            }
        )
    return reports


def latest_news_with_features(store: SQLiteStore, limit: int = 50) -> list[dict[str, Any]]:
    rows = store.list_latest_news_with_features(limit=limit)
    items = []
    for row in rows:
        keyword_flags = {}
        if row["keyword_flags"]:
            try:
                keyword_flags = json.loads(row["keyword_flags"])
            except json.JSONDecodeError:
                keyword_flags = {}
        items.append(
            {
                "id": row["id"],
                "title": row["title"],
                "source": row["source"],
                "url": row["url"],
                "published_at": row["published_at"],
                "observed_at": row["observed_at"],
                "sentiment": float(row["sentiment"]) if row["sentiment"] is not None else 0.0,
                "source_weight": float(row["source_weight"]) if row["source_weight"] is not None else 1.0,
                "keyword_flags": keyword_flags,
                "language": row["language"] if "language" in row.keys() else "",
            }
        )
    return items


def sentiment_timeline(store: SQLiteStore, hours: int = 24) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = store.list_news_features_since(cutoff.isoformat())
    buckets: dict[str, dict[str, float]] = {}
    for row in rows:
        observed = datetime.fromisoformat(row["observed_at"])
        bucket = observed.replace(minute=0, second=0, microsecond=0).isoformat()
        sentiment = float(row["sentiment"] or 0.0)
        weight = float(row["source_weight"] or 1.0)
        entry = buckets.setdefault(bucket, {"sum": 0.0, "count": 0.0})
        entry["sum"] += sentiment * weight
        entry["count"] += 1.0

    timeline = []
    for bucket in sorted(buckets.keys()):
        entry = buckets[bucket]
        avg = entry["sum"] / entry["count"] if entry["count"] > 0 else 0.0
        timeline.append({"bucket": bucket, "avg_sentiment": avg, "count": int(entry["count"])})
    return timeline

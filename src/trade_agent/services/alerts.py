from __future__ import annotations

from typing import Any

from trade_agent.config import AppSettings
from trade_agent.schemas import utc_now_iso
from trade_agent.store import SQLiteStore

ALLOWED_CONDITIONS = {"above", "below", "change_pct"}


def build_price_snapshot(
    settings: AppSettings, store: SQLiteStore
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    timeframe = settings.trading.timeframes[0] if settings.trading.timeframes else "1m"
    items: list[dict[str, Any]] = []
    price_map: dict[str, dict[str, Any]] = {}
    for sym in settings.trading.symbol_whitelist:
        recent = store.list_recent_candles(sym, timeframe, limit=2)
        price = None
        change_pct = None
        ts = None
        if recent:
            latest = recent[0]
            price = float(latest["close"])
            ts = int(latest["ts"])
            if len(recent) > 1:
                prev_close = float(recent[1]["close"])
                if prev_close > 0:
                    change_pct = (price - prev_close) / prev_close
        item = {"symbol": sym, "price": price, "change_pct": change_pct, "ts": ts}
        items.append(item)
        price_map[sym] = item
    return items, price_map


def create_alert(store: SQLiteStore, symbol: str, condition: str, threshold: float) -> dict[str, Any]:
    condition = condition.strip().lower()
    if condition not in ALLOWED_CONDITIONS:
        raise ValueError(f"unsupported condition: {condition}")
    if threshold <= 0:
        raise ValueError("threshold must be positive")
    alert_id = store.create_alert(symbol, condition, float(threshold), created_at=utc_now_iso())
    return {
        "id": alert_id,
        "symbol": symbol,
        "condition": condition,
        "threshold": float(threshold),
        "enabled": 1,
    }


def list_alerts(
    store: SQLiteStore,
    current_prices: dict[str, dict[str, Any]] | None = None,
    enabled_only: bool = False,
) -> list[dict[str, Any]]:
    items = []
    for row in store.list_alerts(enabled_only=enabled_only):
        symbol = row["symbol"]
        item = {
            "id": int(row["id"]),
            "symbol": symbol,
            "condition": row["condition"],
            "threshold": float(row["threshold"]),
            "enabled": int(row["enabled"]),
            "triggered_at": row["triggered_at"],
            "created_at": row["created_at"],
        }
        if current_prices and symbol in current_prices:
            info = current_prices[symbol]
            item["current_price"] = info.get("price")
            item["change_pct"] = info.get("change_pct")
            item["price_ts"] = info.get("ts")
        items.append(item)
    return items


def check_alerts(
    store: SQLiteStore, current_prices: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    triggered: list[dict[str, Any]] = []
    rows = store.list_alerts(enabled_only=True)
    now = utc_now_iso()
    for row in rows:
        symbol = row["symbol"]
        info = current_prices.get(symbol)
        if not info or info.get("price") is None:
            continue
        price = float(info["price"])
        threshold = float(row["threshold"])
        condition = row["condition"]
        match = False
        if condition == "above":
            match = price >= threshold
        elif condition == "below":
            match = price <= threshold
        elif condition == "change_pct":
            change_pct = info.get("change_pct")
            if change_pct is None:
                continue
            match = abs(float(change_pct)) * 100 >= threshold
        if match:
            store.update_alert_triggered(int(row["id"]), triggered_at=now, enabled=0)
            payload = {
                "id": int(row["id"]),
                "symbol": symbol,
                "condition": condition,
                "threshold": threshold,
                "triggered_at": now,
                "current_price": price,
                "change_pct": info.get("change_pct"),
            }
            store.log_event("alert_triggered", payload)
            triggered.append(payload)
    return triggered


def watchlist(settings: AppSettings, store: SQLiteStore) -> dict[str, Any]:
    items, _ = build_price_snapshot(settings, store)
    timeframe = settings.trading.timeframes[0] if settings.trading.timeframes else "1m"
    return {"items": items, "timeframe": timeframe, "as_of": utc_now_iso()}

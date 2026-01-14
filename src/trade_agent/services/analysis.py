from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from trade_agent.config import AppSettings
from trade_agent.metrics import compute_metrics
from trade_agent.store import SQLiteStore


def _parse_iso(value: str, end: bool = False) -> str:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if len(text) == 10:
        dt = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
        if end:
            dt = dt + timedelta(days=1) - timedelta(microseconds=1)
        return dt.isoformat()
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _normalize_range(start: str | None, end: str | None) -> tuple[str | None, str | None]:
    start_iso = _parse_iso(start, end=False) if start else None
    end_iso = _parse_iso(end, end=True) if end else None
    return start_iso, end_iso


def _iter_days(start_date: datetime, end_date: datetime) -> list[str]:
    days = []
    cursor = start_date.date()
    end_day = end_date.date()
    while cursor <= end_day:
        days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return days


def _daily_series(
    trades: Iterable[dict[str, Any]],
    start_iso: str | None,
    end_iso: str | None,
) -> list[dict[str, Any]]:
    pnl_by_day: dict[str, float] = defaultdict(float)
    min_dt: datetime | None = None
    max_dt: datetime | None = None
    for trade in trades:
        ts = trade.get("created_at")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(str(ts))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        day = dt.date().isoformat()
        pnl_by_day[day] += float(trade.get("pnl_jpy", 0.0))
        min_dt = dt if min_dt is None else min(min_dt, dt)
        max_dt = dt if max_dt is None else max(max_dt, dt)

    if start_iso:
        min_dt = datetime.fromisoformat(start_iso)
    if end_iso:
        max_dt = datetime.fromisoformat(end_iso)
    if min_dt is None or max_dt is None:
        return []

    days = _iter_days(min_dt, max_dt)
    running = 0.0
    series = []
    for day in days:
        pnl = pnl_by_day.get(day, 0.0)
        running += pnl
        series.append({"day": day, "pnl_jpy": pnl, "equity": running})
    return series


def _fetch_internal_fills(
    store: SQLiteStore,
    mode: str | None,
    symbol: str | None,
    start_iso: str | None,
    end_iso: str | None,
) -> list[dict[str, Any]]:
    query = (
        "SELECT f.fill_id, f.exec_id, f.symbol, f.side, f.size, f.price, f.fee, f.fee_currency, f.ts, "
        "e.mode, e.intent_id, e.executed_at, "
        "oi.strategy, oi.price as intent_price, oi.size as intent_size, oi.side as intent_side "
        "FROM fills f "
        "JOIN executions e ON f.exec_id = e.exec_id "
        "JOIN order_intents oi ON e.intent_id = oi.intent_id "
        "WHERE 1=1"
    )
    params: list[Any] = []
    if mode:
        query += " AND e.mode = ?"
        params.append(mode)
    if symbol:
        query += " AND f.symbol = ?"
        params.append(symbol)
    if start_iso:
        query += " AND f.ts >= ?"
        params.append(start_iso)
    if end_iso:
        query += " AND f.ts <= ?"
        params.append(end_iso)
    query += " ORDER BY f.ts ASC"
    cur = store.conn.execute(query, params)
    return [dict(row) for row in cur.fetchall()]


def _fees_in_jpy(
    fee: float, fee_currency: str | None, base_currency: str, symbol: str, price: float
) -> float:
    if not fee_currency or fee_currency == base_currency:
        return fee
    if "/" in symbol:
        base, quote = symbol.split("/", 1)
        if quote == base_currency and fee_currency == base:
            return fee * price
    return fee


def _trades_from_fills(
    settings: AppSettings, rows: Iterable[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, float]]:
    positions: dict[str, dict[str, float]] = {}
    trades: list[dict[str, Any]] = []
    pnl_by_intent: dict[str, float] = defaultdict(float)

    for row in rows:
        symbol = row.get("symbol")
        if not symbol:
            continue
        side = str(row.get("side") or "").lower()
        if side not in {"buy", "sell"}:
            continue
        size = float(row.get("size") or 0.0)
        price = float(row.get("price") or 0.0)
        fee = float(row.get("fee") or 0.0)
        fee_currency = row.get("fee_currency")
        fee_jpy = _fees_in_jpy(
            fee, str(fee_currency) if fee_currency else "", settings.trading.base_currency, symbol, price
        )

        pos = positions.setdefault(symbol, {"size": 0.0, "cost": 0.0})
        if side == "buy":
            pos["cost"] += price * size + fee_jpy
            pos["size"] += size
            continue

        if pos["size"] <= 0:
            continue
        avg_cost = pos["cost"] / pos["size"] if pos["size"] > 0 else 0.0
        pnl = (price - avg_cost) * size - fee_jpy
        pos["cost"] -= avg_cost * size
        pos["size"] -= size

        intent_id = row.get("intent_id") or ""
        pnl_by_intent[intent_id] += pnl
        trades.append(
            {
                "intent_id": intent_id,
                "created_at": row.get("ts"),
                "mode": row.get("mode"),
                "symbol": symbol,
                "side": side,
                "size": size,
                "price": price,
                "fee_jpy": fee_jpy,
                "pnl_jpy": pnl,
                "notional_jpy": price * size,
                "strategy": row.get("strategy") or "",
            }
        )
    return trades, pnl_by_intent


def _strategy_stats(trades: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, float]] = {}
    for trade in trades:
        strat = trade.get("strategy") or "unknown"
        entry = stats.setdefault(
            strat,
            {"count": 0.0, "wins": 0.0, "losses": 0.0, "pnl": 0.0},
        )
        pnl = float(trade.get("pnl_jpy") or 0.0)
        entry["count"] += 1
        entry["pnl"] += pnl
        if pnl > 0:
            entry["wins"] += 1
        elif pnl < 0:
            entry["losses"] += 1
    result = []
    for strat, entry in stats.items():
        count = entry["count"]
        wins = entry["wins"]
        losses = entry["losses"]
        result.append(
            {
                "strategy": strat,
                "count": int(count),
                "win_rate": wins / count if count else 0.0,
                "avg_pnl": entry["pnl"] / count if count else 0.0,
                "total_pnl": entry["pnl"],
                "wins": int(wins),
                "losses": int(losses),
            }
        )
    return sorted(result, key=lambda x: x["total_pnl"], reverse=True)


def internal_performance(
    settings: AppSettings,
    store: SQLiteStore,
    mode: str | None = None,
    symbol: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    start_iso, end_iso = _normalize_range(start, end)
    rows = _fetch_internal_fills(store, mode, symbol, start_iso, end_iso)
    trades, _ = _trades_from_fills(settings, rows)
    metrics, equity = compute_metrics(
        trades,
        capital_jpy=settings.risk.capital_jpy,
        start_at=start_iso,
        end_at=end_iso,
    )
    daily = _daily_series(trades, start_iso, end_iso)
    return {
        "metrics": metrics.__dict__,
        "equity": equity,
        "daily": daily,
        "trades": trades,
        "strategy_stats": _strategy_stats(trades),
        "range": {"start": start_iso, "end": end_iso},
    }


def intent_outcomes(
    settings: AppSettings,
    store: SQLiteStore,
    mode: str | None = None,
    symbol: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    start_iso, end_iso = _normalize_range(start, end)
    base_query = (
        "SELECT oi.intent_id, oi.created_at, oi.status as intent_status, oi.strategy, "
        "oi.symbol, oi.side, oi.size as intent_size, oi.price as intent_price, "
        "oi.confidence, e.exec_id, e.executed_at, e.mode as exec_mode, e.status as exec_status "
        "FROM order_intents oi "
        "LEFT JOIN executions e ON e.intent_id = oi.intent_id "
        "WHERE 1=1"
    )
    params: list[Any] = []
    if mode:
        base_query += " AND oi.mode = ?"
        params.append(mode)
    if symbol:
        base_query += " AND oi.symbol = ?"
        params.append(symbol)
    if start_iso:
        base_query += " AND oi.created_at >= ?"
        params.append(start_iso)
    if end_iso:
        base_query += " AND oi.created_at <= ?"
        params.append(end_iso)
    base_query += " ORDER BY oi.created_at DESC"
    rows = store.conn.execute(base_query, params).fetchall()

    exec_ids = [row["exec_id"] for row in rows if row["exec_id"]]
    fills_by_exec: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if exec_ids:
        placeholders = ",".join("?" for _ in exec_ids)
        cur = store.conn.execute(
            f"SELECT exec_id, size, price, fee FROM fills WHERE exec_id IN ({placeholders})",
            exec_ids,
        )
        for fill in cur.fetchall():
            fills_by_exec[fill["exec_id"]].append(dict(fill))

    fill_rows = _fetch_internal_fills(store, mode, symbol, start_iso, end_iso)
    _, pnl_by_intent = _trades_from_fills(settings, fill_rows)

    items = []
    slippages = []
    fill_ratios = []
    wins = losses = 0
    for row in rows:
        intent_id = row["intent_id"]
        intent_price = float(row["intent_price"] or 0.0)
        intent_size = float(row["intent_size"] or 0.0)
        exec_id = row["exec_id"]
        fills = fills_by_exec.get(exec_id, []) if exec_id else []
        filled_size = sum(float(f.get("size") or 0.0) for f in fills)
        filled_notional = sum(float(f.get("size") or 0.0) * float(f.get("price") or 0.0) for f in fills)
        avg_price = (filled_notional / filled_size) if filled_size > 0 else 0.0
        fee_total = sum(float(f.get("fee") or 0.0) for f in fills)
        fill_ratio = (filled_size / intent_size) if intent_size > 0 else 0.0
        slippage_bps = None
        side = str(row["side"] or "").lower()
        if filled_size > 0 and intent_price > 0 and side in {"buy", "sell"}:
            if side == "buy":
                slippage_bps = (avg_price - intent_price) / intent_price * 10000
            else:
                slippage_bps = (intent_price - avg_price) / intent_price * 10000
            slippages.append(slippage_bps)
        if fill_ratio > 0:
            fill_ratios.append(fill_ratio)

        pnl = pnl_by_intent.get(intent_id)
        outcome = "open"
        if pnl is not None:
            if pnl > 0:
                outcome = "win"
                wins += 1
            elif pnl < 0:
                outcome = "loss"
                losses += 1
            else:
                outcome = "flat"

        items.append(
            {
                "intent_id": intent_id,
                "created_at": row["created_at"],
                "strategy": row["strategy"],
                "symbol": row["symbol"],
                "side": row["side"],
                "intent_size": intent_size,
                "intent_price": intent_price,
                "confidence": float(row["confidence"] or 0.0),
                "exec_id": exec_id,
                "exec_status": row["exec_status"],
                "exec_mode": row["exec_mode"],
                "filled_size": filled_size,
                "avg_price": avg_price,
                "fee_total": fee_total,
                "fill_ratio": fill_ratio,
                "slippage_bps": slippage_bps,
                "pnl_jpy": pnl,
                "outcome": outcome,
                "intent_status": row["intent_status"],
            }
        )

    total = len(items)
    summary = {
        "total": total,
        "executed": sum(1 for item in items if item.get("exec_id")),
        "filled": sum(1 for item in items if item.get("fill_ratio", 0) > 0),
        "wins": wins,
        "losses": losses,
        "hit_rate": wins / (wins + losses) if (wins + losses) else 0.0,
        "avg_slippage_bps": sum(slippages) / len(slippages) if slippages else 0.0,
        "avg_fill_ratio": sum(fill_ratios) / len(fill_ratios) if fill_ratios else 0.0,
    }
    return {"summary": summary, "items": items}


def external_summary(
    settings: AppSettings,
    store: SQLiteStore,
    symbol: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    start_iso, end_iso = _normalize_range(start, end)
    exchange = settings.exchange.name
    rows = store.list_external_trades_between(exchange, start_iso, end_iso, symbol)

    trades = []
    positions: dict[str, dict[str, float]] = {}
    for row in rows:
        side = str(row["side"] or "").lower()
        if side not in {"buy", "sell"}:
            continue
        row_symbol = row["symbol"]
        size = float(row["amount"] or 0.0)
        price = float(row["price"] or 0.0)
        fee = float(row["fee"] or 0.0)
        fee_currency = row["fee_currency"]
        fee_jpy = _fees_in_jpy(
            fee, fee_currency, settings.trading.base_currency, row_symbol, price
        )

        pos = positions.setdefault(row_symbol, {"size": 0.0, "cost": 0.0})
        if side == "buy":
            pos["cost"] += price * size + fee_jpy
            pos["size"] += size
            continue
        if pos["size"] <= 0:
            continue
        avg_cost = pos["cost"] / pos["size"] if pos["size"] > 0 else 0.0
        pnl = (price - avg_cost) * size - fee_jpy
        pos["cost"] -= avg_cost * size
        pos["size"] -= size
        trades.append(
            {
                "created_at": row["ts"],
                "symbol": row_symbol,
                "side": side,
                "size": size,
                "price": price,
                "fee_jpy": fee_jpy,
                "pnl_jpy": pnl,
                "notional_jpy": price * size,
            }
        )

    balances = store.list_latest_external_balances(exchange)
    price_map: dict[str, float] = {}
    for sym in settings.trading.symbol_whitelist:
        if "/" not in sym:
            continue
        base, quote = sym.split("/", 1)
        if quote != settings.trading.base_currency:
            continue
        candle = store.get_latest_candle(sym, settings.trading.timeframes[0])
        if candle:
            price_map[base] = float(candle["close"])

    balance_rows = []
    total_value = 0.0
    for row in balances:
        currency = row["currency"]
        value_jpy = None
        if currency == settings.trading.base_currency:
            value_jpy = float(row["total"])
        elif currency in price_map:
            value_jpy = float(row["total"]) * price_map[currency]
        if value_jpy is not None:
            total_value += value_jpy
        balance_rows.append(
            {
                "currency": currency,
                "total": float(row["total"]),
                "free": float(row["free"]),
                "used": float(row["used"]),
                "value_jpy": value_jpy,
                "ts": row["ts"],
            }
        )

    capital = total_value if total_value > 0 else settings.risk.capital_jpy
    metrics, equity = compute_metrics(
        trades,
        capital_jpy=capital,
        start_at=start_iso,
        end_at=end_iso,
    )
    daily = _daily_series(trades, start_iso, end_iso)
    return {
        "balances": balance_rows,
        "total_value_jpy": total_value,
        "metrics": metrics.__dict__,
        "equity": equity,
        "daily": daily,
        "trades": trades,
        "range": {"start": start_iso, "end": end_iso},
    }

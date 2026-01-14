from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable

from trade_agent.config import AppSettings
from trade_agent.exchange import build_exchange, has_credentials
from trade_agent.schemas import ensure_utc_iso, sha256_hex, utc_now_iso
from trade_agent.store import SQLiteStore


def _trade_uid(exchange: str, trade: dict[str, Any]) -> str:
    trade_id = trade.get("id") or trade.get("order")
    if trade_id:
        return f"{exchange}:{trade_id}"
    payload = {
        "symbol": trade.get("symbol"),
        "side": trade.get("side"),
        "price": trade.get("price"),
        "amount": trade.get("amount"),
        "timestamp": trade.get("timestamp"),
        "datetime": trade.get("datetime"),
    }
    return f"{exchange}:{sha256_hex(json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True))}"


def _iso_to_ms(value: str) -> int | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def _trade_ts_iso(trade: dict[str, Any]) -> str:
    ts = trade.get("datetime")
    if ts:
        normalized = ensure_utc_iso(str(ts))
        if normalized:
            return normalized
    ms = trade.get("timestamp")
    if ms is not None:
        try:
            dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
            return dt.isoformat()
        except (ValueError, OSError):
            pass
    return utc_now_iso()


def _iter_symbols(settings: AppSettings, symbols: Iterable[str] | None) -> list[str]:
    if symbols:
        return list(symbols)
    return list(settings.trading.symbol_whitelist)


def ingest_external(
    settings: AppSettings,
    store: SQLiteStore,
    symbols: Iterable[str] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    if not has_credentials(settings.exchange):
        raise ValueError("missing API credentials")

    exchange_client = build_exchange(settings.exchange)
    exchange_client.load_markets()
    exchange_name = settings.exchange.name

    balance = exchange_client.fetch_balance()
    ts_iso = None
    if balance.get("datetime"):
        ts_iso = ensure_utc_iso(str(balance.get("datetime")))
    if ts_iso is None and balance.get("timestamp") is not None:
        try:
            ts_iso = datetime.fromtimestamp(
                int(balance.get("timestamp")) / 1000, tz=timezone.utc
            ).isoformat()
        except (ValueError, OSError):
            ts_iso = None
    if ts_iso is None:
        ts_iso = utc_now_iso()
    totals = balance.get("total") or {}
    free = balance.get("free") or {}
    used = balance.get("used") or {}
    raw_balance = json.dumps(balance, separators=(",", ":"), sort_keys=True)
    balance_rows = 0
    for currency, total in totals.items():
        store.save_external_balance(
            exchange=exchange_name,
            currency=str(currency),
            total=float(total or 0.0),
            free=float(free.get(currency) or 0.0),
            used=float(used.get(currency) or 0.0),
            ts=ts_iso,
            raw_json=raw_balance,
        )
        balance_rows += 1

    trade_rows = 0
    errors: list[dict[str, str]] = []
    for symbol in _iter_symbols(settings, symbols):
        since_ms = None
        latest_ts = store.get_latest_external_trade_ts(exchange_name, symbol)
        if latest_ts:
            since_ms = _iso_to_ms(latest_ts)
            if since_ms is not None:
                since_ms += 1
        try:
            trades = exchange_client.fetch_my_trades(symbol, since=since_ms, limit=limit)
        except Exception as exc:  # noqa: BLE001
            errors.append({"symbol": symbol, "error": str(exc)})
            continue
        for trade in trades:
            trade_uid = _trade_uid(exchange_name, trade)
            fee = trade.get("fee") or {}
            fee_cost = float(fee.get("cost") or 0.0)
            fee_currency = str(
                fee.get("currency") or settings.trading.base_currency or ""
            )
            price = float(trade.get("price") or 0.0)
            amount = float(trade.get("amount") or 0.0)
            cost = float(trade.get("cost") or (price * amount))
            side = str(trade.get("side") or "unknown").lower()
            ts_iso = _trade_ts_iso(trade)
            raw_trade = json.dumps(trade, separators=(",", ":"), sort_keys=True)
            inserted = store.save_external_trade(
                trade_uid=trade_uid,
                exchange=exchange_name,
                trade_id=str(trade.get("id")) if trade.get("id") else None,
                symbol=str(trade.get("symbol") or symbol),
                side=side,
                price=price,
                amount=amount,
                cost=cost,
                fee=fee_cost,
                fee_currency=fee_currency,
                ts=ts_iso,
                raw_json=raw_trade,
            )
            if inserted:
                trade_rows += 1

    result = {
        "exchange": exchange_name,
        "balances": balance_rows,
        "trades": trade_rows,
        "errors": errors,
    }
    store.log_event("external_ingest", result)
    return result

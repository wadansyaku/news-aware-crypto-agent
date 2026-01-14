from __future__ import annotations

from typing import Any

from trade_agent.config import AppSettings, resolve_db_path
from trade_agent.exchange import build_exchange, check_public_connection
from trade_agent.news.rss import fetch_entries


def get_config_snapshot(settings: AppSettings) -> dict[str, Any]:
    return {
        "exchange": settings.exchange.name,
        "symbols": settings.trading.symbol_whitelist,
        "timeframes": settings.trading.timeframes,
        "mode": settings.trading.mode,
        "dry_run": settings.trading.dry_run,
        "require_approval": settings.trading.require_approval,
        "approval_phrase": settings.trading.approval_phrase,
        "kill_switch": settings.trading.kill_switch,
        "autopilot_enabled": settings.autopilot.enabled,
        "i_understand_live_trading": settings.trading.i_understand_live_trading,
        "risk": {
            "capital_jpy": settings.risk.capital_jpy,
            "max_loss_jpy_per_day": settings.risk.max_loss_jpy_per_day,
            "max_orders_per_day": settings.risk.max_orders_per_day,
            "cooldown_minutes": settings.risk.cooldown_minutes,
            "cooldown_bypass_pct": settings.risk.cooldown_bypass_pct,
        },
        "runner": {
            "market_poll_seconds": settings.runner.market_poll_seconds,
            "news_poll_seconds": settings.runner.news_poll_seconds,
            "propose_poll_seconds": settings.runner.propose_poll_seconds,
            "propose_cooldown_seconds": settings.runner.propose_cooldown_seconds,
            "orderbook": settings.runner.orderbook,
            "jitter_seconds": settings.runner.jitter_seconds,
            "max_backoff_seconds": settings.runner.max_backoff_seconds,
        },
    }


def get_status(settings: AppSettings) -> dict[str, Any]:
    exchange_client = build_exchange(settings.exchange)
    exchange_ok, exchange_msg = check_public_connection(exchange_client)
    caps = exchange_client.exchange.has
    ohlcv_source = (
        "fetchOHLCV"
        if caps.get("fetchOHLCV")
        else "fetchTrades"
        if caps.get("fetchTrades")
        else "unavailable"
    )

    news_ok = False
    news_msg = "not configured"
    if settings.news.rss_urls:
        try:
            entries = fetch_entries(settings.news.rss_urls[:1])
            news_ok = True
            news_msg = f"ok ({len(entries)} entries)"
        except Exception as exc:  # noqa: BLE001
            news_msg = f"error: {exc}"

    return {
        "exchange": {"ok": exchange_ok, "message": exchange_msg},
        "exchange_capabilities": {
            "fetchOHLCV": bool(caps.get("fetchOHLCV")),
            "fetchTrades": bool(caps.get("fetchTrades")),
            "fetchTime": bool(caps.get("fetchTime")),
            "postOnly": bool(caps.get("postOnly")),
            "ohlcv_source": ohlcv_source,
        },
        "news": {"ok": news_ok, "message": news_msg},
        "db_path": resolve_db_path(settings),
        "config": get_config_snapshot(settings),
    }

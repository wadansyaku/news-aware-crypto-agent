from __future__ import annotations

from typing import Any

from trade_agent.config import AppSettings
from trade_agent.services import queries
from trade_agent.store import SQLiteStore


def get_portfolio(settings: AppSettings, store: SQLiteStore) -> dict[str, Any]:
    positions = []
    total_value = 0.0
    total_pnl = 0.0

    for sym in settings.trading.symbol_whitelist:
        pos = queries.position_overview(settings, store, sym)
        current_price = float(pos.get("current_price") or 0.0)
        size = float(pos.get("size") or 0.0)
        position_value = current_price * size
        pos["position_value"] = position_value
        positions.append(pos)
        total_value += position_value
        total_pnl += float(pos.get("unrealized_pnl") or 0.0)

    return {"positions": positions, "total_value": total_value, "total_pnl": total_pnl}

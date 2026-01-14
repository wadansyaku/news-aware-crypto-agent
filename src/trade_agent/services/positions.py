from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from trade_agent.config import AppSettings
from trade_agent.intent import TradePlan, from_plan
from trade_agent.risk import evaluate_plan
from trade_agent.store import SQLiteStore


@dataclass
class ClosePositionParams:
    symbol: Optional[str] = None
    mode: str = "paper"


def close_position(
    settings: AppSettings, store: SQLiteStore, params: ClosePositionParams
) -> dict[str, Any]:
    if params.mode not in {"paper", "live"}:
        raise ValueError("invalid mode")

    symbol = params.symbol or settings.trading.symbol_whitelist[0]
    size, avg = store.get_position_state(symbol)
    if size <= 0:
        return {"status": "rejected", "reason": "no position to close"}

    timeframe = settings.trading.timeframes[0]
    latest = store.get_latest_candle(symbol, timeframe)
    if not latest:
        raise ValueError("no latest price available")

    price = float(latest["close"])
    plan = TradePlan(
        symbol=symbol,
        side="sell",
        size=size,
        price=price,
        confidence=1.0,
        rationale="manual close position",
        strategy="manual_close",
    )

    risk_result = evaluate_plan(
        store,
        plan,
        settings.risk,
        settings.trading,
        current_position=size,
    )
    store.log_event(
        "risk_check",
        {
            "symbol": plan.symbol,
            "strategy": plan.strategy,
            "side": plan.side,
            "status": "approved" if risk_result.approved else "rejected",
            "reason": risk_result.reason,
            "original_size": size,
            "adjusted_size": risk_result.plan.size if risk_result.plan else 0.0,
        },
    )

    if not risk_result.approved or not risk_result.plan:
        return {"status": "rejected", "reason": risk_result.reason}

    intent = from_plan(
        risk_result.plan,
        mode=params.mode,
        expiry_seconds=settings.trading.intent_expiry_seconds,
        rationale_features_ref=None,
    )
    store.save_order_intent(intent)
    store.log_event(
        "propose",
        {"intent_id": intent.intent_id, "symbol": intent.symbol, "side": intent.side},
    )

    return {
        "status": "proposed",
        "intent_id": intent.intent_id,
        "side": intent.side,
        "size": intent.size,
        "price": intent.price,
        "strategy": intent.strategy,
        "confidence": intent.confidence,
        "rationale": intent.rationale,
        "expires_at": intent.expires_at,
    }

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import sqlite3

from trade_agent import db
from trade_agent.config import RiskConfig, TradingConfig
from trade_agent.intent import TradePlan


@dataclass
class RiskResult:
    approved: bool
    reason: str
    plan: Optional[TradePlan] = None


@dataclass
class RiskState:
    daily_pnl: float
    daily_orders: int
    last_exec_time: datetime | None
    unrealized_pnl: float = 0.0


def _utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def evaluate_plan(
    conn: sqlite3.Connection,
    plan: TradePlan,
    risk: RiskConfig,
    trading: TradingConfig,
    current_position: float | None = None,
    state: RiskState | None = None,
    now: datetime | None = None,
) -> RiskResult:
    if plan.side == "hold":
        return RiskResult(approved=False, reason="no trade", plan=plan)

    if trading.kill_switch:
        return RiskResult(approved=False, reason="kill switch enabled")

    if plan.symbol not in trading.symbol_whitelist:
        return RiskResult(approved=False, reason="symbol not whitelisted")

    if plan.size <= 0 or plan.price <= 0:
        return RiskResult(approved=False, reason="invalid size or price")

    if state is None:
        day = _utc_day()
        realized_pnl = db.get_daily_pnl(conn, day)
        daily_orders = db.get_daily_execution_count(conn, day)
        last_exec = db.get_last_execution_time(conn)
        last_exec_time = datetime.fromisoformat(last_exec) if last_exec else None
        position, avg_cost = db.get_position_state(conn, plan.symbol)
        unrealized_pnl = (plan.price - avg_cost) * position if position > 0 else 0.0
    else:
        realized_pnl = state.daily_pnl
        daily_orders = state.daily_orders
        last_exec_time = state.last_exec_time
        unrealized_pnl = state.unrealized_pnl

    loss_proxy = realized_pnl + min(unrealized_pnl, 0.0)
    if loss_proxy <= -abs(risk.max_loss_jpy_per_day):
        return RiskResult(approved=False, reason="daily loss limit reached")

    if daily_orders >= risk.max_orders_per_day:
        return RiskResult(approved=False, reason="max orders per day reached")

    current_time = now or datetime.now(timezone.utc)
    if last_exec_time:
        cooldown = last_exec_time + timedelta(minutes=risk.cooldown_minutes)
        if current_time < cooldown:
            return RiskResult(approved=False, reason="cooldown active")

    position = current_position if current_position is not None else db.get_position_size(conn, plan.symbol)

    if plan.side == "sell" and position <= 0:
        if trading.long_only:
            hold_plan = TradePlan.hold(
                symbol=plan.symbol,
                strategy=plan.strategy,
                rationale="long-only: no position to sell",
            )
            return RiskResult(approved=False, reason="long-only: no position to sell", plan=hold_plan)
        return RiskResult(approved=False, reason="no position to sell")

    max_position_size = (risk.capital_jpy * risk.max_position_pct) / plan.price
    size = plan.size

    if plan.side == "buy" and position + size > max_position_size:
        size = max(max_position_size - position, 0.0)

    notional = size * plan.price
    notional_cap = min(risk.max_order_notional_jpy, risk.max_loss_jpy_per_trade)
    if notional > notional_cap:
        size = notional_cap / plan.price
        notional = size * plan.price

    if size <= 0:
        return RiskResult(approved=False, reason="size reduced to zero")

    adjusted = TradePlan(
        symbol=plan.symbol,
        side=plan.side,
        size=size,
        price=plan.price,
        confidence=plan.confidence,
        rationale=plan.rationale,
        strategy=plan.strategy,
    )
    return RiskResult(approved=True, reason="ok", plan=adjusted)

from __future__ import annotations

from trade_agent.config import MakerEmulationConfig, RiskConfig, TradingConfig
from trade_agent.intent import TradePlan
from trade_agent.risk import evaluate_plan
from trade_agent.store import SQLiteStore


def _make_trading(kill_switch: bool) -> TradingConfig:
    return TradingConfig(
        mode="paper",
        dry_run=True,
        require_approval=True,
        approval_phrase="I APPROVE",
        kill_switch=kill_switch,
        i_understand_live_trading=False,
        long_only=True,
        symbol_whitelist=["BTC/JPY"],
        base_currency="JPY",
        timeframes=["1m"],
        candle_limit=100,
        order_timeout_seconds=10,
        post_only=True,
        intent_expiry_seconds=300,
        maker_emulation=MakerEmulationConfig(buffer_bps=0.1, use_tick=True),
    )


def test_kill_switch_blocks() -> None:
    store = SQLiteStore(":memory:")
    risk = RiskConfig(
        capital_jpy=100000,
        max_position_pct=1.0,
        max_order_notional_jpy=50000,
        max_loss_jpy_per_trade=50000,
        max_loss_jpy_per_day=100000,
        max_orders_per_day=10,
        cooldown_minutes=0,
    )
    plan = TradePlan(
        symbol="BTC/JPY",
        side="buy",
        size=1.0,
        price=1000.0,
        confidence=0.5,
        rationale="test",
        strategy="baseline",
    )
    result = evaluate_plan(store, plan, risk, _make_trading(kill_switch=True))
    assert not result.approved
    assert result.reason == "kill switch enabled"


def test_notional_cap_adjusts_size() -> None:
    store = SQLiteStore(":memory:")
    risk = RiskConfig(
        capital_jpy=100000,
        max_position_pct=1.0,
        max_order_notional_jpy=5000,
        max_loss_jpy_per_trade=4000,
        max_loss_jpy_per_day=100000,
        max_orders_per_day=10,
        cooldown_minutes=0,
    )
    plan = TradePlan(
        symbol="BTC/JPY",
        side="buy",
        size=10.0,
        price=1000.0,
        confidence=0.5,
        rationale="test",
        strategy="baseline",
    )
    result = evaluate_plan(store, plan, risk, _make_trading(kill_switch=False))
    assert result.approved
    assert result.plan is not None
    assert result.plan.size == 4.0

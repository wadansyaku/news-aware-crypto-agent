from __future__ import annotations

from pathlib import Path

from trade_agent.config import load_config
from trade_agent.intent import OrderIntent
from trade_agent.schemas import ExecutionRecord, FillRecord
from trade_agent.services import analysis
from trade_agent.store import SQLiteStore


def make_settings(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("", encoding="utf-8")
    return load_config(str(config_path))


def _intent(intent_id: str, created_at: str, side: str, price: float) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        created_at=created_at,
        symbol="BTC/JPY",
        side=side,
        size=1.0,
        price=price,
        order_type="limit",
        time_in_force="GTC",
        strategy="baseline",
        confidence=0.7,
        rationale="test",
        rationale_features_ref=None,
        expires_at="2024-01-01T02:00:00+00:00",
        mode="paper",
    )


def test_internal_performance_and_intent_outcomes(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    store = SQLiteStore(":memory:")

    buy_intent = _intent("intent-buy", "2024-01-01T00:00:00+00:00", "buy", 100.0)
    store.save_order_intent(buy_intent)
    store.save_execution(
        ExecutionRecord(
            exec_id="exec-buy",
            intent_id=buy_intent.intent_id,
            intent_hash=buy_intent.hash(),
            executed_at="2024-01-01T00:00:00+00:00",
            mode="paper",
            status="filled",
            fee=0.0,
            slippage_model="paper",
            details={},
        )
    )
    store.save_fill(
        FillRecord(
            fill_id="fill-buy",
            exec_id="exec-buy",
            symbol="BTC/JPY",
            side="buy",
            size=1.0,
            price=100.0,
            fee=0.0,
            fee_currency="JPY",
            ts="2024-01-01T00:00:00+00:00",
        )
    )

    sell_intent = _intent("intent-sell", "2024-01-01T01:00:00+00:00", "sell", 110.0)
    store.save_order_intent(sell_intent)
    store.save_execution(
        ExecutionRecord(
            exec_id="exec-sell",
            intent_id=sell_intent.intent_id,
            intent_hash=sell_intent.hash(),
            executed_at="2024-01-01T01:00:00+00:00",
            mode="paper",
            status="filled",
            fee=0.0,
            slippage_model="paper",
            details={},
        )
    )
    store.save_fill(
        FillRecord(
            fill_id="fill-sell",
            exec_id="exec-sell",
            symbol="BTC/JPY",
            side="sell",
            size=1.0,
            price=110.0,
            fee=0.0,
            fee_currency="JPY",
            ts="2024-01-01T01:00:00+00:00",
        )
    )

    perf = analysis.internal_performance(settings, store, mode="paper", symbol="BTC/JPY")
    assert perf["metrics"]["total_pnl"] == 10.0
    assert len(perf["trades"]) == 1
    assert perf["daily"][0]["pnl_jpy"] == 10.0

    outcomes = analysis.intent_outcomes(settings, store, mode="paper", symbol="BTC/JPY")
    assert outcomes["summary"]["wins"] == 1
    assert outcomes["summary"]["losses"] == 0
    intent_map = {item["intent_id"]: item for item in outcomes["items"]}
    assert intent_map["intent-sell"]["outcome"] == "win"
    assert intent_map["intent-sell"]["fill_ratio"] == 1.0

    store.close()

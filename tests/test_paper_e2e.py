from __future__ import annotations

from pathlib import Path

from trade_agent.config import load_config, resolve_db_path
from trade_agent.executor import execute_intent
from trade_agent.intent import TradePlan, from_plan
from trade_agent.store import SQLiteStore


def test_paper_execution_deterministic(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
app:
  data_dir: {data_dir}
  db_path: {db_path}
trading:
  require_approval: true
  approval_phrase: "I APPROVE"
  symbol_whitelist:
    - "BTC/JPY"
risk:
  capital_jpy: 100000
  max_position_pct: 1.0
  max_order_notional_jpy: 100000
  max_loss_jpy_per_trade: 100000
  max_loss_jpy_per_day: 100000
  max_orders_per_day: 10
  cooldown_minutes: 0
paper:
  seed: 1
  slippage_bps: 0
  fee_bps: 0
  fill_probability: 1.0
  spread_bps: 0
""".format(
            data_dir=tmp_path.as_posix(), db_path=(tmp_path / "test.db").as_posix()
        ),
        encoding="utf-8",
    )

    settings = load_config(str(config_path))
    store = SQLiteStore(resolve_db_path(settings))

    plan = TradePlan(
        symbol="BTC/JPY",
        side="buy",
        size=1.0,
        price=105.0,
        confidence=0.7,
        rationale="test",
        strategy="baseline",
    )
    intent = from_plan(plan, mode="paper", expiry_seconds=300)
    store.save_order_intent(intent)
    store.save_approval_phrase(intent.intent_id, intent.hash(), "I APPROVE", "test")

    store.save_orderbook_snapshot(
        symbol="BTC/JPY",
        bid=99.0,
        ask=101.0,
        bid_size=1.0,
        ask_size=1.0,
        ts=0,
    )

    result = execute_intent(store, intent.intent_id, settings, mode="paper")
    assert result.status == "filled"

    fills = store.list_fills(symbol="BTC/JPY", limit=1)
    assert fills[0]["price"] == 101.0
    store.close()

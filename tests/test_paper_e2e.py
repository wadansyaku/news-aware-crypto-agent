from __future__ import annotations

from pathlib import Path

from trade_agent import db
from trade_agent.config import load_config, resolve_db_path
from trade_agent.executor import execute_intent
from trade_agent.intent import TradePlan, from_plan


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
    conn = db.connect(resolve_db_path(settings))
    db.init_db(conn)

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
    db.insert_order_intent(conn, intent.as_record())
    db.insert_approval(conn, intent.intent_id, intent.hash(), "I APPROVE")

    db.insert_orderbook_snapshot(
        conn,
        symbol="BTC/JPY",
        bid=99.0,
        ask=101.0,
        bid_size=1.0,
        ask_size=1.0,
        ts=0,
    )

    result = execute_intent(conn, intent.intent_id, settings, mode="paper")
    assert result.status == "filled"

    row = conn.execute("SELECT price FROM fills").fetchone()
    assert row["price"] == 101.0

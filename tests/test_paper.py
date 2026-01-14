from __future__ import annotations

import random

from trade_agent.config import PaperConfig
from trade_agent.intent import OrderIntent
from trade_agent.paper import OrderbookSnapshot, simulate_fill


def _make_intent(price: float) -> OrderIntent:
    return OrderIntent(
        intent_id="intent-1",
        created_at="2024-01-01T00:00:00+00:00",
        symbol="BTC/JPY",
        side="buy",
        size=1.0,
        price=price,
        order_type="limit",
        time_in_force="GTC",
        strategy="baseline",
        confidence=0.7,
        rationale="test",
        rationale_features_ref=None,
        expires_at="2024-01-01T00:15:00+00:00",
        mode="paper",
    )


def test_simulate_fill_slippage_buy() -> None:
    intent = _make_intent(price=105.0)
    orderbook = OrderbookSnapshot(bid=99.0, ask=100.0, bid_size=1.0, ask_size=1.0, ts="t")
    config = PaperConfig(
        seed=1,
        slippage_bps=100,
        fee_bps=0,
        fill_probability=1.0,
        spread_bps=0,
    )
    fill = simulate_fill(intent, orderbook, config, random.Random(1))
    assert fill.filled is True
    assert fill.price == 101.0


def test_simulate_fill_deterministic_rng() -> None:
    intent = _make_intent(price=95.0)
    orderbook = OrderbookSnapshot(bid=99.0, ask=100.0, bid_size=1.0, ask_size=1.0, ts="t")
    config = PaperConfig(
        seed=1,
        slippage_bps=0,
        fee_bps=0,
        fill_probability=0.5,
        spread_bps=0,
    )
    fill1 = simulate_fill(intent, orderbook, config, random.Random(123))
    fill2 = simulate_fill(intent, orderbook, config, random.Random(123))
    assert fill1.filled == fill2.filled

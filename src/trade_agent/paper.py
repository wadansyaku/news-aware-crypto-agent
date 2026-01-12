from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from trade_agent.config import PaperConfig
from trade_agent.intent import OrderIntent


@dataclass
class OrderbookSnapshot:
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    ts: str


@dataclass
class PaperFill:
    filled: bool
    price: float
    size: float
    fee: float
    fee_currency: str
    status: str
    message: str


def estimate_orderbook_from_price(price: float, spread_bps: float) -> OrderbookSnapshot:
    half_spread = price * (spread_bps / 10000) / 2
    bid = max(price - half_spread, 0.0)
    ask = price + half_spread
    ts = datetime.now(timezone.utc).isoformat()
    return OrderbookSnapshot(bid=bid, ask=ask, bid_size=1.0, ask_size=1.0, ts=ts)


def simulate_fill(
    intent: OrderIntent,
    orderbook: OrderbookSnapshot,
    config: PaperConfig,
    rng: random.Random,
) -> PaperFill:
    slippage = config.slippage_bps / 10000
    fee_rate = config.fee_bps / 10000

    if intent.side not in {"buy", "sell"}:
        return PaperFill(
            filled=False,
            price=0.0,
            size=0.0,
            fee=0.0,
            fee_currency="JPY",
            status="rejected",
            message="invalid side",
        )

    if intent.side == "buy":
        if intent.price >= orderbook.ask:
            fill_price = min(intent.price, orderbook.ask * (1 + slippage))
            if fill_price > intent.price:
                return PaperFill(False, 0.0, 0.0, 0.0, "JPY", "open", "limit too low")
            notional = fill_price * intent.size
            fee = notional * fee_rate
            return PaperFill(True, fill_price, intent.size, fee, "JPY", "filled", "crossed spread")
    else:
        if intent.price <= orderbook.bid:
            fill_price = max(intent.price, orderbook.bid * (1 - slippage))
            if fill_price < intent.price:
                return PaperFill(False, 0.0, 0.0, 0.0, "JPY", "open", "limit too high")
            notional = fill_price * intent.size
            fee = notional * fee_rate
            return PaperFill(True, fill_price, intent.size, fee, "JPY", "filled", "crossed spread")

    if rng.random() <= config.fill_probability:
        fill_price = intent.price
        notional = fill_price * intent.size
        fee = notional * fee_rate
        return PaperFill(True, fill_price, intent.size, fee, "JPY", "filled", "probabilistic fill")

    return PaperFill(False, 0.0, 0.0, 0.0, "JPY", "open", "not filled")


def build_rng(config: PaperConfig) -> random.Random:
    return random.Random(config.seed)

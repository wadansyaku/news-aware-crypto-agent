from __future__ import annotations

from statistics import mean
from typing import Sequence

from trade_agent.config import RiskConfig, StrategyBaselineConfig
from trade_agent.intent import TradePlan


def _close(candle: dict) -> float:
    return float(candle["close"]) if isinstance(candle, dict) else float(candle["close"])


def generate_plan(
    symbol: str,
    candles: Sequence[dict],
    risk: RiskConfig,
    config: StrategyBaselineConfig,
) -> TradePlan:
    if len(candles) < max(config.sma_period, config.momentum_lookback) + 1:
        return TradePlan.hold(symbol=symbol, strategy="baseline", rationale="insufficient data")

    closes = [_close(c) for c in candles]
    sma_window = closes[-config.sma_period :]
    sma_value = mean(sma_window)
    current = closes[-1]
    momentum = current - closes[-1 - config.momentum_lookback]

    base_notional = risk.capital_jpy * config.base_position_pct
    size = base_notional / current if current > 0 else 0.0
    confidence = 0.55

    if current > sma_value and momentum > 0:
        rationale = f"price>{sma_value:.2f}, momentum={momentum:.2f}"
        return TradePlan(
            symbol=symbol,
            side="buy",
            size=size,
            price=current,
            confidence=confidence,
            rationale=rationale,
            strategy="baseline",
        )

    if current < sma_value and momentum < 0:
        rationale = f"price<{sma_value:.2f}, momentum={momentum:.2f}"
        return TradePlan(
            symbol=symbol,
            side="sell",
            size=size,
            price=current,
            confidence=confidence,
            rationale=rationale,
            strategy="baseline",
        )

    return TradePlan.hold(
        symbol=symbol,
        strategy="baseline",
        rationale=f"no signal (price={current:.2f}, sma={sma_value:.2f})",
    )

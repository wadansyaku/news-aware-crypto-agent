from __future__ import annotations

from typing import Sequence

from trade_agent.config import RiskConfig, StrategyBaselineConfig, StrategyNewsOverlayConfig
from trade_agent.intent import TradePlan
from trade_agent.news.features import aggregate_sentiment
from trade_agent.strategies.baseline import generate_plan as baseline_plan


def generate_plan(
    symbol: str,
    candles: Sequence[dict],
    news_features: list[dict],
    risk: RiskConfig,
    baseline_cfg: StrategyBaselineConfig,
    overlay_cfg: StrategyNewsOverlayConfig,
) -> TradePlan:
    base = baseline_plan(symbol, candles, risk, baseline_cfg)
    if base.side == "hold":
        return base

    sentiment = aggregate_sentiment(news_features)
    size = base.size
    confidence = base.confidence
    rationale = base.rationale

    if sentiment >= overlay_cfg.sentiment_boost_threshold:
        size *= overlay_cfg.boost_multiplier
        confidence = min(0.95, confidence + 0.1)
        rationale = f"{rationale}; sentiment boost {sentiment:.2f}"
    elif sentiment <= overlay_cfg.sentiment_cut_threshold:
        size *= overlay_cfg.cut_multiplier
        confidence = max(0.1, confidence - 0.1)
        rationale = f"{rationale}; sentiment cut {sentiment:.2f}"

    return TradePlan(
        symbol=base.symbol,
        side=base.side,
        size=size,
        price=base.price,
        confidence=confidence,
        rationale=rationale,
        strategy="news_overlay",
    )

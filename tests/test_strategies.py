from __future__ import annotations

from trade_agent.config import RiskConfig, StrategyBaselineConfig, StrategyNewsOverlayConfig
from trade_agent.strategies import baseline, news_overlay


def _risk() -> RiskConfig:
    return RiskConfig(
        capital_jpy=100000,
        max_position_pct=1.0,
        max_order_notional_jpy=100000,
        max_loss_jpy_per_trade=100000,
        max_loss_jpy_per_day=100000,
        max_orders_per_day=10,
        cooldown_minutes=0,
    )


def test_baseline_buy_signal() -> None:
    candles = [{"close": v} for v in [100, 101, 102, 103]]
    cfg = StrategyBaselineConfig(sma_period=3, momentum_lookback=2, base_position_pct=0.1)
    plan = baseline.generate_plan("BTC/JPY", candles, _risk(), cfg)
    assert plan.side == "buy"


def test_baseline_sell_signal() -> None:
    candles = [{"close": v} for v in [103, 102, 101, 100]]
    cfg = StrategyBaselineConfig(sma_period=3, momentum_lookback=2, base_position_pct=0.1)
    plan = baseline.generate_plan("BTC/JPY", candles, _risk(), cfg)
    assert plan.side == "sell"


def test_news_overlay_boosts_size() -> None:
    candles = [{"close": v} for v in [100, 101, 102, 103]]
    base_cfg = StrategyBaselineConfig(sma_period=3, momentum_lookback=2, base_position_pct=0.1)
    overlay_cfg = StrategyNewsOverlayConfig(
        sentiment_boost_threshold=0.2,
        sentiment_cut_threshold=-0.2,
        boost_multiplier=1.5,
        cut_multiplier=0.5,
    )
    news_features = [{"sentiment": 0.5, "source_weight": 1.0}]
    plan = news_overlay.generate_plan(
        "BTC/JPY", candles, news_features, _risk(), base_cfg, overlay_cfg
    )
    assert plan.side == "buy"
    assert plan.size > 0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Sequence

from trade_agent.config import AppSettings
from trade_agent.metrics import Metrics, compute_metrics, save_report
from trade_agent.news.features import aggregate_feature_vector
from trade_agent.risk import RiskState, evaluate_plan
from trade_agent.schemas import FeatureRow
from trade_agent.store import SQLiteStore
from trade_agent.strategies import baseline, news_overlay


@dataclass
class BacktestResult:
    metrics_path_json: str
    metrics_path_csv: str
    metrics_path_summary: str
    metrics: Metrics
    equity: list[float]
    trades: list[dict]


def _iso_to_ms(ts: str) -> int:
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def _collect_news_features(
    store: SQLiteStore, start_iso: str, end_iso: str, latency_seconds: int
) -> list[dict]:
    features = store.list_news_features_window(
        start_iso=start_iso,
        end_iso=end_iso,
        observed_cutoff=end_iso,
        limit=100000,
    )
    enriched = []
    for row in features:
        published = datetime.fromisoformat(row["published_at"])
        observed = datetime.fromisoformat(row["observed_at"])
        available_at = max(observed, published + timedelta(seconds=latency_seconds))
        enriched.append(
            {
                "sentiment": float(row["sentiment"]),
                "source_weight": float(row["source_weight"]),
                "published_at": row["published_at"],
                "observed_at": row["observed_at"],
                "available_at": available_at.isoformat(),
            }
        )
    return sorted(enriched, key=lambda row: row["available_at"])


def _filter_recent_news(features: Sequence[dict], cutoff: datetime, lookback_hours: int) -> list[dict]:
    start = cutoff - timedelta(hours=lookback_hours)
    return [
        f
        for f in features
        if start <= datetime.fromisoformat(f["published_at"]) <= cutoff
    ]


def run_backtest(
    store: SQLiteStore,
    settings: AppSettings,
    symbol: str,
    timeframe: str,
    start: str,
    end: str,
    strategy_name: str,
    output_dir: str,
) -> BacktestResult:
    start_ms = _iso_to_ms(f"{start}T00:00:00+00:00")
    end_ms = _iso_to_ms(f"{end}T23:59:59+00:00")
    candles = store.list_candles_between(symbol, timeframe, start_ms, end_ms)
    if not candles:
        raise ValueError("no candles in range")

    news_features_all = _collect_news_features(
        store,
        _ms_to_iso(start_ms),
        _ms_to_iso(end_ms),
        settings.news.news_latency_seconds,
    )
    news_idx = 0
    available_news: list[dict] = []

    position = 0.0
    avg_cost = 0.0
    trades: list[dict] = []
    current_day: str | None = None
    state = RiskState(daily_pnl=0.0, daily_orders=0, last_exec_time=None)

    for idx in range(len(candles)):
        candle = candles[idx]
        ts = int(candle["ts"])
        current_time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        while news_idx < len(news_features_all):
            available_at = datetime.fromisoformat(news_features_all[news_idx]["available_at"])
            if available_at <= current_time:
                available_news.append(news_features_all[news_idx])
                news_idx += 1
            else:
                break

        lookback_news = _filter_recent_news(
            available_news, current_time, settings.news.sentiment_lookback_hours
        )
        feature_vector = aggregate_feature_vector(lookback_news)
        store.save_feature_row(
            FeatureRow(
                symbol=symbol,
                ts=ts,
                features=feature_vector,
                feature_version="news_v1",
                computed_at=current_time.isoformat(),
                news_window_start=(
                    current_time - timedelta(hours=settings.news.sentiment_lookback_hours)
                ).isoformat(),
                news_window_end=current_time.isoformat(),
            )
        )

        candle_slice = candles[: idx + 1]
        if strategy_name == "baseline":
            plan = baseline.generate_plan(
                symbol, candle_slice, settings.risk, settings.strategies.baseline
            )
        else:
            plan = news_overlay.generate_plan(
                symbol,
                candle_slice,
                lookback_news,
                settings.risk,
                settings.strategies.baseline,
                settings.strategies.news_overlay,
            )

        day_key = current_time.date().isoformat()
        if current_day != day_key:
            current_day = day_key
            state = RiskState(daily_pnl=0.0, daily_orders=0, last_exec_time=None)

        price = float(candle["close"])
        state.unrealized_pnl = (price - avg_cost) * position if position > 0 else 0.0

        risk_result = evaluate_plan(
            store,
            plan,
            settings.risk,
            settings.trading,
            current_position=position,
            state=state,
            now=current_time,
        )
        if not risk_result.approved or not risk_result.plan:
            continue

        plan = risk_result.plan
        fee_bps = (
            settings.backtest.taker_fee_bps
            if settings.backtest.assume_taker
            else settings.backtest.maker_fee_bps
        )
        fee_rate = fee_bps / 10000
        slippage = settings.backtest.slippage_bps / 10000
        if plan.side == "buy" and position <= 0:
            size = plan.size
            exec_price = price * (1 + slippage)
            fee = exec_price * size * fee_rate
            total_cost = exec_price * size + fee
            avg_cost = (avg_cost * position + total_cost) / (position + size) if position + size > 0 else 0.0
            position += size
            state.daily_orders += 1
            state.last_exec_time = current_time
            trades.append(
                {
                    "symbol": symbol,
                    "side": "buy",
                    "size": size,
                    "price": exec_price,
                    "pnl_jpy": 0.0,
                    "notional_jpy": exec_price * size,
                    "fee_jpy": fee,
                    "created_at": current_time.isoformat(),
                }
            )
        elif plan.side == "sell" and position > 0:
            size = min(plan.size, position)
            exec_price = price * (1 - slippage)
            fee = exec_price * size * fee_rate
            pnl = (exec_price - avg_cost) * size - fee
            position -= size
            if position <= 0:
                position = 0.0
                avg_cost = 0.0
            state.daily_orders += 1
            state.daily_pnl += pnl
            state.last_exec_time = current_time
            trades.append(
                {
                    "symbol": symbol,
                    "side": "sell",
                    "size": size,
                    "price": exec_price,
                    "pnl_jpy": pnl,
                    "notional_jpy": exec_price * size,
                    "fee_jpy": fee,
                    "created_at": current_time.isoformat(),
                }
            )

    metrics, equity = compute_metrics(
        trades,
        capital_jpy=settings.risk.capital_jpy,
        start_at=f"{start}T00:00:00+00:00",
        end_at=f"{end}T23:59:59+00:00",
    )
    paths = save_report(metrics, equity, output_dir, f"backtest_{strategy_name}")
    return BacktestResult(
        metrics_path_json=paths["json"],
        metrics_path_csv=paths["csv"],
        metrics_path_summary=paths["summary"],
        metrics=metrics,
        equity=equity,
        trades=trades,
    )

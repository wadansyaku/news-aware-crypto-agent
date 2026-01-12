from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Sequence

import sqlite3

from trade_agent import db
from trade_agent.config import AppSettings
from trade_agent.metrics import compute_metrics, save_report
from trade_agent.risk import RiskState, evaluate_plan
from trade_agent.strategies import baseline, news_overlay


@dataclass
class BacktestResult:
    metrics_path_json: str
    metrics_path_csv: str


def _iso_to_ms(ts: str) -> int:
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def _collect_news_features(
    conn: sqlite3.Connection, start_iso: str, end_iso: str
) -> list[dict]:
    cur = conn.execute(
        """
        SELECT nf.sentiment, nf.source_weight, na.published_at
        FROM news_features nf
        JOIN news_articles na ON nf.article_id = na.id
        WHERE na.published_at >= ? AND na.published_at <= ?
        ORDER BY na.published_at ASC
        """,
        (start_iso, end_iso),
    )
    features = []
    for row in cur.fetchall():
        features.append(
            {
                "sentiment": float(row["sentiment"]),
                "source_weight": float(row["source_weight"]),
                "published_at": row["published_at"],
            }
        )
    return features


def _filter_recent_news(features: Sequence[dict], cutoff: datetime, lookback_hours: int) -> list[dict]:
    start = cutoff - timedelta(hours=lookback_hours)
    return [
        f
        for f in features
        if start <= datetime.fromisoformat(f["published_at"]) <= cutoff
    ]


def run_backtest(
    conn: sqlite3.Connection,
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
    candles = db.list_candles_between(conn, symbol, timeframe, start_ms, end_ms)
    if not candles:
        raise ValueError("no candles in range")

    news_features_all = _collect_news_features(conn, _ms_to_iso(start_ms), _ms_to_iso(end_ms))
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
        cutoff = current_time - timedelta(seconds=settings.news.news_latency_seconds)

        while news_idx < len(news_features_all):
            published = datetime.fromisoformat(news_features_all[news_idx]["published_at"])
            if published <= cutoff:
                available_news.append(news_features_all[news_idx])
                news_idx += 1
            else:
                break

        lookback_news = _filter_recent_news(
            available_news, cutoff, settings.news.sentiment_lookback_hours
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
            conn,
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
        fee_rate = settings.backtest.fee_bps / 10000
        if plan.side == "buy" and position <= 0:
            size = plan.size
            fee = price * size * fee_rate
            total_cost = price * size + fee
            avg_cost = (avg_cost * position + total_cost) / (position + size) if position + size > 0 else 0.0
            position += size
            state.daily_orders += 1
            state.last_exec_time = current_time
            trades.append({"pnl_jpy": 0.0, "notional_jpy": price * size, "fee_jpy": fee})
        elif plan.side == "sell" and position > 0:
            size = min(plan.size, position)
            fee = price * size * fee_rate
            pnl = (price - avg_cost) * size - fee
            position -= size
            if position <= 0:
                position = 0.0
                avg_cost = 0.0
            state.daily_orders += 1
            state.daily_pnl += pnl
            state.last_exec_time = current_time
            trades.append({"pnl_jpy": pnl, "notional_jpy": price * size, "fee_jpy": fee})

    metrics, equity = compute_metrics(trades)
    paths = save_report(metrics, equity, output_dir, f"backtest_{strategy_name}")
    return BacktestResult(metrics_path_json=paths["json"], metrics_path_csv=paths["csv"])

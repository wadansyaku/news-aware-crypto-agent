from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import typer

from trade_agent import db
from trade_agent.backtest import run_backtest
from trade_agent.config import AppSettings, ensure_data_dir, load_config, resolve_db_path
from trade_agent.exchange import build_exchange, check_public_connection
from trade_agent.executor import execute_intent
from trade_agent.intent import TradePlan, from_plan
from trade_agent.metrics import (
    compute_metrics,
    load_trade_details_from_db,
    load_trades_from_db,
    save_report,
    save_trade_csv,
)
from trade_agent.news.features import extract_features
from trade_agent.news.normalize import NormalizedNews
from trade_agent.news.rss import fetch_entries, ingest_rss
from trade_agent.risk import evaluate_plan
from trade_agent.strategies import baseline, news_overlay

app = typer.Typer(help="News-aware crypto spot trading agent")


def _load_settings(config_path: str) -> AppSettings:
    settings = load_config(config_path)
    ensure_data_dir(settings)
    return settings


def _get_conn(settings: AppSettings) -> sqlite3.Connection:
    path = resolve_db_path(settings)
    conn = db.connect(path)
    db.init_db(conn)
    return conn


def _recent_news(conn: sqlite3.Connection, settings: AppSettings) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=settings.news.news_latency_seconds)
    rows = db.list_news_features(conn, published_before=cutoff.isoformat())
    start = cutoff - timedelta(hours=settings.news.sentiment_lookback_hours)
    features = []
    for row in rows:
        published = datetime.fromisoformat(row["published_at"])
        if published >= start:
            features.append(
                {
                    "sentiment": float(row["sentiment"]),
                    "source_weight": float(row["source_weight"]),
                }
            )
    return features


def _normalize_from_row(row: sqlite3.Row) -> NormalizedNews:
    return NormalizedNews(
        title=row["title"],
        url=row["url"],
        source=row["source"],
        published_at=row["published_at"],
        ingested_at=row["ingested_at"],
        title_hash=row["title_hash"],
    )


def _timeframe_ms(exchange_client: object, timeframe: str) -> int | None:
    try:
        seconds = exchange_client.exchange.parse_timeframe(timeframe)
        return int(seconds * 1000)
    except Exception:  # noqa: BLE001
        return None


@app.command()
def status(config: str = typer.Option("config.yaml", help="Path to config.yaml")) -> None:
    settings = _load_settings(config)
    conn = _get_conn(settings)
    exchange_client = build_exchange(settings.exchange)
    exchange_ok, exchange_msg = check_public_connection(exchange_client)

    news_ok = False
    news_msg = "not configured"
    if settings.news.rss_urls:
        try:
            entries = fetch_entries(settings.news.rss_urls[:1])
            news_ok = True
            news_msg = f"ok ({len(entries)} entries)"
        except Exception as exc:  # noqa: BLE001
            news_msg = f"error: {exc}"

    typer.echo(
        json.dumps(
            {
                "exchange": {"ok": exchange_ok, "message": exchange_msg},
                "news": {"ok": news_ok, "message": news_msg},
                "db_path": resolve_db_path(settings),
            },
            indent=2,
        )
    )
    conn.close()


@app.command()
def ingest(
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    symbol: Optional[str] = typer.Option(None, help="Symbol to ingest"),
    orderbook: bool = typer.Option(False, help="Ingest orderbook snapshot"),
) -> None:
    settings = _load_settings(config)
    conn = _get_conn(settings)
    exchange_client = build_exchange(settings.exchange)

    symbols = [symbol] if symbol else settings.trading.symbol_whitelist
    total_candles = 0
    ingest_errors = []
    for sym in symbols:
        for timeframe in settings.trading.timeframes:
            since = None
            last_ts = db.get_latest_candle_ts(conn, sym, timeframe)
            frame_ms = _timeframe_ms(exchange_client, timeframe)
            if last_ts is not None and frame_ms:
                since = max(last_ts - frame_ms, 0)
            try:
                candles = exchange_client.fetch_candles(
                    sym, timeframe=timeframe, limit=settings.trading.candle_limit, since=since
                )
                total_candles += db.insert_candles(conn, sym, timeframe, candles)
            except Exception as exc:  # noqa: BLE001
                ingest_errors.append(
                    {"symbol": sym, "timeframe": timeframe, "error": str(exc)}
                )

        if orderbook:
            try:
                ob = exchange_client.fetch_orderbook(sym)
                bid = float(ob["bids"][0][0]) if ob.get("bids") else 0.0
                ask = float(ob["asks"][0][0]) if ob.get("asks") else 0.0
                bid_size = float(ob["bids"][0][1]) if ob.get("bids") else 0.0
                ask_size = float(ob["asks"][0][1]) if ob.get("asks") else 0.0
                ts = int(
                    ob.get("timestamp") or int(datetime.now(timezone.utc).timestamp() * 1000)
                )
                db.insert_orderbook_snapshot(conn, sym, bid, ask, bid_size, ask_size, ts)
            except Exception as exc:  # noqa: BLE001
                ingest_errors.append({"symbol": sym, "orderbook": True, "error": str(exc)})

    news_stats = ingest_rss(conn, settings.news.rss_urls) if settings.news.rss_urls else {}
    articles = db.list_articles_without_features(conn)
    for row in articles:
        normalized = _normalize_from_row(row)
        features = extract_features(
            normalized, settings.news.keyword_flags, settings.news.source_weights
        )
        db.insert_news_features(
            conn,
            article_id=int(row["id"]),
            sentiment=features.sentiment,
            keyword_flags=features.keyword_flags,
            source_weight=features.source_weight,
            language=features.language,
        )

    db.log_event(
        conn,
        "ingest",
        {
            "candles": total_candles,
            "news": news_stats,
            "features_added": len(articles),
            "errors": ingest_errors,
        },
    )
    typer.echo(
        json.dumps(
            {
                "candles": total_candles,
                "news": news_stats,
                "features_added": len(articles),
                "errors": ingest_errors,
            },
            indent=2,
        )
    )
    conn.close()


@app.command()
def propose(
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    symbol: Optional[str] = typer.Option(None, help="Symbol to trade"),
    strategy: str = typer.Option("baseline", help="baseline or news_overlay"),
    mode: str = typer.Option("paper", help="paper or live"),
    refresh: bool = typer.Option(False, help="Refresh candles from exchange"),
) -> None:
    settings = _load_settings(config)
    conn = _get_conn(settings)
    exchange_client = build_exchange(settings.exchange)

    symbol = symbol or settings.trading.symbol_whitelist[0]
    timeframe = settings.trading.timeframes[0]

    if refresh:
        candles = exchange_client.fetch_candles(
            symbol, timeframe=timeframe, limit=settings.trading.candle_limit
        )
        db.insert_candles(conn, symbol, timeframe, candles)

    rows = db.fetch_candles(conn, symbol, timeframe, settings.trading.candle_limit)
    if not rows:
        typer.echo("no candles available; run ingest first")
        conn.close()
        raise typer.Exit(code=1)

    news_features = _recent_news(conn, settings)

    if strategy == "baseline":
        plan = baseline.generate_plan(
            symbol, rows, settings.risk, settings.strategies.baseline
        )
    else:
        plan = news_overlay.generate_plan(
            symbol,
            rows,
            news_features,
            settings.risk,
            settings.strategies.baseline,
            settings.strategies.news_overlay,
        )

    if plan.side in {"buy", "sell"} and settings.trading.post_only:
        if not exchange_client.exchange.has.get("postOnly"):
            try:
                ob = exchange_client.fetch_orderbook(symbol)
                if plan.side == "buy" and ob.get("bids"):
                    bid = float(ob["bids"][0][0])
                    plan = TradePlan(
                        symbol=plan.symbol,
                        side=plan.side,
                        size=plan.size,
                        price=min(plan.price, bid),
                        confidence=plan.confidence,
                        rationale=f"{plan.rationale}; maker price at bid",
                        strategy=plan.strategy,
                    )
                elif plan.side == "sell" and ob.get("asks"):
                    ask = float(ob["asks"][0][0])
                    plan = TradePlan(
                        symbol=plan.symbol,
                        side=plan.side,
                        size=plan.size,
                        price=max(plan.price, ask),
                        confidence=plan.confidence,
                        rationale=f"{plan.rationale}; maker price at ask",
                        strategy=plan.strategy,
                    )
            except Exception:  # noqa: BLE001
                pass

    original_size = plan.size
    risk_result = evaluate_plan(conn, plan, settings.risk, settings.trading)
    adjusted_size = risk_result.plan.size if risk_result.plan else 0.0
    db.log_event(
        conn,
        "risk_check",
        {
            "symbol": plan.symbol,
            "strategy": plan.strategy,
            "side": plan.side,
            "status": "approved" if risk_result.approved else "rejected",
            "reason": risk_result.reason,
            "original_size": original_size,
            "adjusted_size": adjusted_size,
        },
    )
    if not risk_result.approved or not risk_result.plan:
        typer.echo(json.dumps({"status": "rejected", "reason": risk_result.reason}, indent=2))
        conn.close()
        return

    plan = risk_result.plan
    intent = from_plan(plan, mode=mode, expiry_seconds=settings.trading.intent_expiry_seconds)
    db.insert_order_intent(conn, intent.as_record())
    db.log_event(
        conn,
        "propose",
        {"intent_id": intent.intent_id, "symbol": intent.symbol, "side": intent.side},
    )

    typer.echo(
        json.dumps(
            {
                "intent_id": intent.intent_id,
                "hash": intent.hash(),
                "side": intent.side,
                "size": intent.size,
                "price": intent.price,
                "strategy": intent.strategy,
                "confidence": intent.confidence,
                "rationale": intent.rationale,
                "expires_at": intent.expires_at,
            },
            indent=2,
        )
    )
    conn.close()


@app.command()
def approve(
    intent_id: str,
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    phrase: Optional[str] = typer.Option(None, help="Approval phrase"),
) -> None:
    settings = _load_settings(config)
    conn = _get_conn(settings)
    record = db.get_order_intent(conn, intent_id)
    if not record:
        typer.echo("intent not found")
        conn.close()
        raise typer.Exit(code=1)

    phrase = phrase or typer.prompt("Approval phrase")
    if phrase.strip() != settings.trading.approval_phrase:
        typer.echo("approval phrase mismatch")
        conn.close()
        raise typer.Exit(code=1)

    db.insert_approval(conn, intent_id, record["intent_hash"], phrase.strip())
    db.update_order_intent_status(conn, intent_id, "approved")
    db.log_event(conn, "approve", {"intent_id": intent_id})
    typer.echo(json.dumps({"status": "approved", "intent_id": intent_id}, indent=2))
    conn.close()


@app.command()
def execute(
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    intent_id: Optional[str] = typer.Option(None, help="Intent ID"),
    mode: str = typer.Option("paper", help="paper or live"),
) -> None:
    settings = _load_settings(config)
    conn = _get_conn(settings)

    if not intent_id:
        record = db.get_latest_intent(conn, status="approved")
        if not record:
            record = db.get_latest_intent(conn, status="proposed")
        if not record:
            typer.echo("no pending intent")
            conn.close()
            raise typer.Exit(code=1)
        intent_id = record["intent_id"]

    exchange_client = build_exchange(settings.exchange) if mode == "live" else None
    result = execute_intent(conn, intent_id, settings, mode, exchange_client=exchange_client)
    db.log_event(conn, "execute", {"intent_id": intent_id, "status": result.status})
    typer.echo(json.dumps(result.__dict__, indent=2))
    conn.close()


@app.command()
def backtest(
    start: str = typer.Option(..., "--from", help="Start date YYYY-MM-DD"),
    end: str = typer.Option(..., "--to", help="End date YYYY-MM-DD"),
    strategy: str = typer.Option("baseline", help="baseline or news_overlay"),
    symbol: Optional[str] = typer.Option(None, help="Symbol"),
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
) -> None:
    settings = _load_settings(config)
    conn = _get_conn(settings)
    symbol = symbol or settings.trading.symbol_whitelist[0]
    timeframe = settings.trading.timeframes[0]
    output_dir = str(Path(settings.app.data_dir) / "reports")
    result = run_backtest(conn, settings, symbol, timeframe, start, end, strategy, output_dir)
    db.log_event(
        conn,
        "backtest",
        {"strategy": strategy, "report_json": result.metrics_path_json},
    )
    typer.echo(
        json.dumps(
            {
                "report_json": result.metrics_path_json,
                "equity_csv": result.metrics_path_csv,
            },
            indent=2,
        )
    )
    conn.close()


@app.command()
def report(
    config: str = typer.Option("config.yaml", help="Path to config.yaml"),
    mode: Optional[str] = typer.Option(None, help="Filter by mode"),
) -> None:
    settings = _load_settings(config)
    conn = _get_conn(settings)
    trades = load_trades_from_db(conn, mode)
    metrics, equity = compute_metrics(trades)
    output_dir = str(Path(settings.app.data_dir) / "reports")
    paths = save_report(metrics, equity, output_dir, f"report_{mode or 'all'}")
    trade_details = load_trade_details_from_db(conn, mode)
    trade_csv = save_trade_csv(trade_details, output_dir, f"report_{mode or 'all'}")
    db.log_event(
        conn,
        "report",
        {"mode": mode, "report_json": paths["json"], "trade_csv": trade_csv},
    )
    typer.echo(
        json.dumps({"metrics": metrics.__dict__, "paths": {**paths, "trades": trade_csv}}, indent=2)
    )
    conn.close()


if __name__ == "__main__":
    app()

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import streamlit as st

from trade_agent import db
from trade_agent.backtest import run_backtest
from trade_agent.config import AppSettings, ensure_data_dir, load_config, resolve_db_path
from trade_agent.exchange import build_exchange, check_public_connection
from trade_agent.executor import execute_intent
from trade_agent.intent import TradePlan, from_plan
from trade_agent.metrics import compute_metrics, load_trades_from_db, save_report
from trade_agent.news.features import extract_features
from trade_agent.news.normalize import NormalizedNews
from trade_agent.news.rss import fetch_entries, ingest_rss
from trade_agent.risk import evaluate_plan
from trade_agent.strategies import baseline, news_overlay

st.set_page_config(page_title="トレードエージェント UI", layout="wide")

st.title("トレードエージェント UI")
st.caption("ローカル中心の研究用アプリです。収益性は保証しません。")
st.warning("現物のみ。デフォルトで人の承認が必要です。自己責任で利用してください。")


def _load_settings(path: str) -> AppSettings:
    settings = load_config(path)
    ensure_data_dir(settings)
    return settings


def _get_conn(settings: AppSettings) -> sqlite3.Connection:
    path = resolve_db_path(settings)
    conn = db.connect(path)
    db.init_db(conn)
    return conn


def _normalize_from_row(row: sqlite3.Row) -> NormalizedNews:
    return NormalizedNews(
        title=row["title"],
        url=row["url"],
        source=row["source"],
        published_at=row["published_at"],
        ingested_at=row["ingested_at"],
        title_hash=row["title_hash"],
    )


def _recent_news(conn: sqlite3.Connection, settings: AppSettings) -> list[dict[str, Any]]:
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


def _list_intents(conn: sqlite3.Connection, limit: int = 20) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT intent_id, created_at, status, symbol, side, size, price, strategy, confidence
        FROM order_intents
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [dict(row) for row in cur.fetchall()]


st.sidebar.header("設定")
config_path = st.sidebar.text_input("config.yaml のパス", "config.yaml")

try:
    settings = _load_settings(config_path)
    db_path = resolve_db_path(settings)
    st.sidebar.write(f"DB: {db_path}")
except FileNotFoundError:
    st.sidebar.error("config.yaml が見つかりません")
    st.stop()


with st.expander("ステータス", expanded=True):
    if st.button("ステータス確認"):
        exchange_client = build_exchange(settings.exchange)
        exchange_ok, exchange_msg = check_public_connection(exchange_client)
        exchange_msg = f"OK ({exchange_msg})" if exchange_ok else f"エラー: {exchange_msg}"
        news_ok = False
        news_msg = "未設定"
        if settings.news.rss_urls:
            try:
                entries = fetch_entries(settings.news.rss_urls[:1])
                news_ok = True
                news_msg = f"OK ({len(entries)}件)"
            except Exception as exc:  # noqa: BLE001
                news_msg = f"エラー: {exc}"
        st.json(
            {
                "exchange": {"ok": exchange_ok, "message": exchange_msg},
                "news": {"ok": news_ok, "message": news_msg},
                "db_path": db_path,
            }
        )


with st.expander("取り込み", expanded=False):
    symbol = st.selectbox("銘柄", settings.trading.symbol_whitelist)
    orderbook = st.checkbox("板スナップショットを取得", value=True)
    if st.button("今すぐ取り込み"):
        conn = _get_conn(settings)
        exchange_client = build_exchange(settings.exchange)
        total_candles = 0
        for timeframe in settings.trading.timeframes:
            candles = exchange_client.fetch_candles(
                symbol, timeframe=timeframe, limit=settings.trading.candle_limit
            )
            total_candles += db.insert_candles(conn, symbol, timeframe, candles)

        if orderbook:
            ob = exchange_client.fetch_orderbook(symbol)
            bid = float(ob["bids"][0][0]) if ob.get("bids") else 0.0
            ask = float(ob["asks"][0][0]) if ob.get("asks") else 0.0
            bid_size = float(ob["bids"][0][1]) if ob.get("bids") else 0.0
            ask_size = float(ob["asks"][0][1]) if ob.get("asks") else 0.0
            ts = int(ob.get("timestamp") or int(datetime.now(timezone.utc).timestamp() * 1000))
            db.insert_orderbook_snapshot(conn, symbol, bid, ask, bid_size, ask_size, ts)

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

        result = {
            "candles": total_candles,
            "news": news_stats,
            "features_added": len(articles),
        }
        db.log_event(conn, "ingest", result)
        st.json(result)
        conn.close()


with st.expander("提案", expanded=False):
    strategy_labels = {"ベースライン": "baseline", "ニュース・オーバーレイ": "news_overlay"}
    mode_labels = {"ペーパー": "paper", "ライブ": "live"}
    strategy_label = st.selectbox("戦略", list(strategy_labels.keys()))
    mode_label = st.selectbox("モード", list(mode_labels.keys()))
    strategy = strategy_labels[strategy_label]
    mode = mode_labels[mode_label]
    refresh = st.checkbox("ローソク足を再取得", value=False)
    if st.button("提案を生成"):
        conn = _get_conn(settings)
        exchange_client = build_exchange(settings.exchange)
        timeframe = settings.trading.timeframes[0]

        if refresh:
            candles = exchange_client.fetch_candles(
                symbol, timeframe=timeframe, limit=settings.trading.candle_limit
            )
            db.insert_candles(conn, symbol, timeframe, candles)

        rows = db.fetch_candles(conn, symbol, timeframe, settings.trading.candle_limit)
        if not rows:
            st.error("ローソク足がありません。先に取り込みを実行してください。")
            conn.close()
        else:
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

            risk_result = evaluate_plan(conn, plan, settings.risk, settings.trading)
            if not risk_result.approved or not risk_result.plan:
                st.json({"status": "rejected", "reason": risk_result.reason})
            else:
                plan = risk_result.plan
                intent = from_plan(
                    plan, mode=mode, expiry_seconds=settings.trading.intent_expiry_seconds
                )
                db.insert_order_intent(conn, intent.as_record())
                db.log_event(
                    conn,
                    "propose",
                    {"intent_id": intent.intent_id, "symbol": intent.symbol, "side": intent.side},
                )
                st.json(
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
                    }
                )
            conn.close()


with st.expander("承認", expanded=False):
    conn = _get_conn(settings)
    intents = _list_intents(conn)
    conn.close()
    st.json(intents)
    if intents:
        intent_id = st.selectbox("承認するインテント", [i["intent_id"] for i in intents])
        phrase = st.text_input("承認フレーズ", type="password")
        if st.button("承認する"):
            conn = _get_conn(settings)
            record = db.get_order_intent(conn, intent_id)
            if not record:
                st.error("インテントが見つかりません")
            elif phrase.strip() != settings.trading.approval_phrase:
                st.error("承認フレーズが一致しません")
            else:
                db.insert_approval(conn, intent_id, record["intent_hash"], phrase.strip())
                db.update_order_intent_status(conn, intent_id, "approved")
                db.log_event(conn, "approve", {"intent_id": intent_id})
                st.success("承認しました")
            conn.close()


with st.expander("実行", expanded=False):
    conn = _get_conn(settings)
    intents = _list_intents(conn)
    conn.close()
    if intents:
        intent_id = st.selectbox("実行するインテント", [i["intent_id"] for i in intents])
        exec_mode_label = st.selectbox("実行モード", ["ペーパー", "ライブ"], key="exec_mode")
        mode = "paper" if exec_mode_label == "ペーパー" else "live"
        if st.button("実行する"):
            conn = _get_conn(settings)
            exchange_client = build_exchange(settings.exchange) if mode == "live" else None
            result = execute_intent(conn, intent_id, settings, mode, exchange_client=exchange_client)
            db.log_event(conn, "execute", {"intent_id": intent_id, "status": result.status})
            st.json(result.__dict__)
            conn.close()


with st.expander("バックテスト", expanded=False):
    start_date = st.date_input("開始日")
    end_date = st.date_input("終了日")
    bt_strategy_label = st.selectbox(
        "バックテスト戦略",
        ["ベースライン", "ニュース・オーバーレイ"],
        key="bt",
    )
    strategy = "baseline" if bt_strategy_label == "ベースライン" else "news_overlay"
    if st.button("バックテスト実行"):
        conn = _get_conn(settings)
        timeframe = settings.trading.timeframes[0]
        output_dir = str(Path(settings.app.data_dir) / "reports")
        result = run_backtest(
            conn,
            settings,
            symbol,
            timeframe,
            start_date.isoformat(),
            end_date.isoformat(),
            strategy,
            output_dir,
        )
        db.log_event(
            conn,
            "backtest",
            {"strategy": strategy, "report_json": result.metrics_path_json},
        )
        st.json({"report_json": result.metrics_path_json, "equity_csv": result.metrics_path_csv})
        conn.close()


with st.expander("レポート", expanded=False):
    report_mode_label = st.selectbox("モード", ["全て", "ペーパー", "ライブ"], key="report")
    mode = (
        "all"
        if report_mode_label == "全て"
        else "paper"
        if report_mode_label == "ペーパー"
        else "live"
    )
    if st.button("レポート作成"):
        conn = _get_conn(settings)
        trades = load_trades_from_db(conn, None if mode == "all" else mode)
        metrics, equity = compute_metrics(trades)
        output_dir = str(Path(settings.app.data_dir) / "reports")
        paths = save_report(metrics, equity, output_dir, f"report_{mode}")
        db.log_event(conn, "report", {"mode": mode, "report_json": paths["json"]})
        st.json({"metrics": metrics.__dict__, "paths": paths})
        conn.close()

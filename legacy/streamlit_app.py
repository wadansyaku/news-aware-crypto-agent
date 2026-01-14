from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import streamlit as st

from trade_agent.backtest import run_backtest
from trade_agent.config import AppSettings, ensure_data_dir, load_config, resolve_db_path
from trade_agent.exchange import build_exchange, check_public_connection
from trade_agent.executor import execute_intent
from trade_agent.intent import TradePlan, from_plan
from trade_agent.metrics import compute_metrics, save_report, save_trade_csv
from trade_agent.news.features import aggregate_feature_vector, extract_features
from trade_agent.news.rss import fetch_entries, ingest_rss
from trade_agent.risk import evaluate_plan
from trade_agent.schemas import FeatureRow, NewsItem, ReportRecord
from trade_agent.store import SQLiteStore
from trade_agent.strategies import baseline, news_overlay


st.set_page_config(page_title="トレードエージェント UI", layout="wide", page_icon="📈")


def _apply_theme() -> None:
    st.markdown(
        """
<style>
.stApp {
  background: radial-gradient(1200px circle at 10% 10%, #f5f1e8 0%, #eef5ff 35%, #fbfbf7 70%),
              linear-gradient(180deg, #f8f6ef 0%, #f9fbff 100%);
  color: #101828;
  font-family: "Hiragino Sans", "Hiragino Kaku Gothic ProN", "Noto Sans JP", "Yu Gothic", sans-serif;
}
.block-container {
  padding-top: 2rem;
  padding-bottom: 4rem;
}
.section-title {
  font-weight: 700;
  letter-spacing: 0.02em;
  margin-bottom: 0.3rem;
}
.hero {
  background: rgba(255, 255, 255, 0.75);
  border: 1px solid #e4e7ec;
  border-radius: 20px;
  padding: 1.5rem 2rem;
  box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
}
.hero h1 {
  margin-bottom: 0.2rem;
  font-size: 2rem;
}
.hero p {
  margin: 0.2rem 0;
  color: #475467;
}
.card {
  background: rgba(255, 255, 255, 0.85);
  border: 1px solid #e4e7ec;
  border-radius: 16px;
  padding: 1rem 1.2rem;
  box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
}
.card h4 {
  margin: 0 0 0.4rem 0;
  color: #475467;
  font-size: 0.9rem;
  font-weight: 600;
}
.card p {
  margin: 0;
  font-size: 1.1rem;
  font-weight: 600;
}
.status-ok {
  color: #039855;
}
.status-warn {
  color: #f79009;
}
.status-bad {
  color: #d92d20;
}
.small-note {
  color: #667085;
  font-size: 0.85rem;
}
.stButton>button {
  border-radius: 12px;
  padding: 0.6rem 1.2rem;
}
</style>
""",
        unsafe_allow_html=True,
    )


_apply_theme()


st.markdown(
    """
<div class="hero">
  <h1>ニュース連動型・暗号資産トレードエージェント</h1>
  <p>ローカル中心の研究用アプリです。収益性は保証しません。</p>
  <p class="small-note">現物のみ / デフォルトで人の承認が必要 / リスク制限は常に適用</p>
</div>
""",
    unsafe_allow_html=True,
)


# -----------------------------
# Helpers
# -----------------------------

def _load_settings(path: str) -> AppSettings:
    settings = load_config(path)
    ensure_data_dir(settings)
    return settings


def _get_store(settings: AppSettings) -> SQLiteStore:
    return SQLiteStore(resolve_db_path(settings))


def _timeframe_ms(exchange_client: object, timeframe: str) -> int | None:
    try:
        seconds = exchange_client.exchange.parse_timeframe(timeframe)
        return int(seconds * 1000)
    except Exception:  # noqa: BLE001
        return None


def _approved_by() -> str:
    return os.getenv("USER") or os.getenv("USERNAME") or "local"


def _news_item_from_row(row: sqlite3.Row) -> NewsItem:
    return NewsItem(
        title=row["title"],
        source_url=row["url"],
        source_name=row["source"],
        guid=row["guid"] if "guid" in row.keys() else None,
        summary=row["summary"] if "summary" in row.keys() else "",
        published_at=row["published_at"],
        observed_at=row["observed_at"] if "observed_at" in row.keys() else row["ingested_at"],
        raw_payload_hash=row["raw_payload_hash"] if "raw_payload_hash" in row.keys() else "",
        title_hash=row["title_hash"],
    )


def _recent_news(
    store: SQLiteStore, settings: AppSettings
) -> tuple[list[dict[str, Any]], str, str]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=settings.news.sentiment_lookback_hours)
    features = store.list_news_features_window(
        start_iso=start.isoformat(),
        end_iso=now.isoformat(),
        observed_cutoff=now.isoformat(),
    )
    usable: list[dict[str, Any]] = []
    for row in features:
        published = datetime.fromisoformat(row["published_at"])
        observed = datetime.fromisoformat(row["observed_at"])
        available_at = max(
            observed, published + timedelta(seconds=settings.news.news_latency_seconds)
        )
        if available_at <= now and published >= start:
            usable.append(
                {"sentiment": float(row["sentiment"]), "source_weight": float(row["source_weight"])}
            )
    return usable, start.isoformat(), now.isoformat()


def _summarize_audit(row: sqlite3.Row) -> dict[str, Any]:
    data = json.loads(row["data_json"]) if row["data_json"] else {}
    return {
        "ts": row["ts"],
        "event": row["event"],
        "symbol": data.get("symbol", ""),
        "side": data.get("side", ""),
        "status": data.get("status", data.get("mode", "")),
        "reason": data.get("reason", data.get("message", "")),
        "original_size": data.get("original_size", ""),
        "adjusted_size": data.get("adjusted_size", ""),
    }


def _result_box(title: str, payload: dict[str, Any], kind: str = "info") -> None:
    if kind == "success":
        st.success(title)
    elif kind == "error":
        st.error(title)
    else:
        st.info(title)
    with st.expander("詳細を見る"):
        st.json(payload)


# -----------------------------
# Sidebar
# -----------------------------

st.sidebar.header("設定")
config_path = st.sidebar.text_input("config.yaml のパス", "config.yaml")

try:
    settings = _load_settings(config_path)
    db_path = resolve_db_path(settings)
except FileNotFoundError:
    st.sidebar.error("config.yaml が見つかりません")
    st.stop()

symbol = st.sidebar.selectbox("対象銘柄", settings.trading.symbol_whitelist)
timeframe = st.sidebar.selectbox("時間足", settings.trading.timeframes)

st.sidebar.markdown("---")
st.sidebar.subheader("安全状態")
kill_switch = "有効" if settings.trading.kill_switch else "無効"
approval_required = "必須" if settings.trading.require_approval else "任意"
mode = settings.trading.mode
st.sidebar.write(f"モード: {mode}")
st.sidebar.write(f"承認: {approval_required}")
st.sidebar.write(f"キルスイッチ: {kill_switch}")

show_phrase = st.sidebar.checkbox("承認フレーズを表示")
if show_phrase:
    st.sidebar.code(settings.trading.approval_phrase)

st.sidebar.markdown("---")
with st.sidebar.expander("取引所切替ガイド", expanded=False):
    st.markdown(
        """
- `fetchOHLCV` 対応の取引所だと安定
- 例: `binance`, `kraken`, `bitstamp`（利用可能地域は要確認）
- 変更: `config.yaml` の `exchange.name` と `trading.symbol_whitelist`
- JPYペアが無い場合は USDT ペアへ切替
"""
    )

st.sidebar.caption(f"DB: {db_path}")


# -----------------------------
# Status + Metrics
# -----------------------------

def _check_status(settings: AppSettings) -> dict[str, Any]:
    exchange_client = build_exchange(settings.exchange)
    exchange_ok, exchange_msg = check_public_connection(exchange_client)
    caps = exchange_client.exchange.has
    ohlcv_source = (
        "fetchOHLCV"
        if caps.get("fetchOHLCV")
        else "fetchTrades"
        if caps.get("fetchTrades")
        else "unavailable"
    )
    news_ok = False
    news_msg = "未設定"
    if settings.news.rss_urls:
        try:
            entries = fetch_entries(settings.news.rss_urls[:1])
            news_ok = True
            news_msg = f"OK ({len(entries)}件)"
        except Exception as exc:  # noqa: BLE001
            news_msg = f"エラー: {exc}"
    return {
        "exchange": {
            "ok": exchange_ok,
            "message": "OK" if exchange_ok else "エラー",
            "detail": exchange_msg,
        },
        "exchange_capabilities": {
            "fetchOHLCV": bool(caps.get("fetchOHLCV")),
            "fetchTrades": bool(caps.get("fetchTrades")),
            "fetchTime": bool(caps.get("fetchTime")),
            "postOnly": bool(caps.get("postOnly")),
            "ohlcv_source": ohlcv_source,
        },
        "news": {"ok": news_ok, "message": news_msg},
        "db_path": resolve_db_path(settings),
    }


# -----------------------------
# UI Tabs
# -----------------------------

tabs = st.tabs(["ダッシュボード", "取引フロー", "バックテスト/レポート", "監査/履歴"])


with tabs[0]:
    st.markdown("### システム状態")
    status = st.session_state.get("status_result")
    if st.button("ステータスを更新"):
        status = _check_status(settings)
        st.session_state["status_result"] = status

    if status:
        cols = st.columns(3)
        exchange_label = "🟢" if status["exchange"]["ok"] else "🔴"
        news_label = "🟢" if status["news"]["ok"] else "🔴"
        cols[0].metric("取引所", f"{exchange_label} {status['exchange']['message']}")
        cols[1].metric("ニュース", f"{news_label} {status['news']['message']}")
        cols[2].metric("DB", Path(status["db_path"]).name)
        with st.expander("詳細"):
            st.json(status)
    else:
        st.info("まだステータス確認が実行されていません。")

    store = _get_store(settings)
    pos_size, avg_cost = store.get_position_state(symbol)
    day = datetime.now(timezone.utc).date().isoformat()
    daily_pnl = store.get_daily_pnl(day)
    intents = store.list_intents(limit=10)
    fills = store.list_fills(symbol=symbol, limit=1000)
    trades = store.load_trades(None)
    store.close()

    st.markdown("### ポジション & 損益")
    metrics_cols = st.columns(3)
    metrics_cols[0].metric("現在のポジション", f"{pos_size:.6f}")
    metrics_cols[1].metric("平均コスト", f"{avg_cost:.2f}")
    metrics_cols[2].metric("当日損益 (UTC)", f"{daily_pnl:.2f} JPY")

    position = 0.0
    position_series = []
    for row in fills:
        size = float(row["size"])
        position += size if row["side"] == "buy" else -size
        position_series.append({"ts": row["ts"], "position": position})

    chart_cols = st.columns(2)
    if position_series:
        chart_cols[0].line_chart(position_series, x="ts", y="position")
    else:
        chart_cols[0].info("ポジション履歴はまだありません。")

    _, equity = compute_metrics(trades, capital_jpy=settings.risk.capital_jpy)
    if equity:
        chart_cols[1].line_chart(
            [{"step": idx + 1, "equity": value} for idx, value in enumerate(equity)],
            x="step",
            y="equity",
        )
    else:
        chart_cols[1].info("PnL履歴はまだありません。")

    st.markdown("### 最近のインテント")
    if intents:
        st.dataframe(intents, use_container_width=True)
    else:
        st.info("インテント履歴がありません。")


with tabs[1]:
    st.markdown("### 取引フロー")
    st.caption("上から順に進めると迷わず実行できます。")

    st.markdown("#### 1. ステータス確認")
    if st.button("ステータス確認", key="flow_status"):
        st.session_state["status_result"] = _check_status(settings)
        _result_box("ステータス更新完了", st.session_state["status_result"], kind="success")

    st.markdown("#### 2. 取り込み")
    with st.form("ingest_form"):
        orderbook = st.checkbox("板スナップショットを取得", value=True)
        submitted = st.form_submit_button("今すぐ取り込み")
    if submitted:
        store = _get_store(settings)
        exchange_client = build_exchange(settings.exchange)
        total_candles = 0
        ingest_errors = []
        source = f"ccxt:{settings.exchange.name}"
        for tf in settings.trading.timeframes:
            since = None
            last_ts = store.get_latest_candle_ts(symbol, tf)
            frame_ms = _timeframe_ms(exchange_client, tf)
            if last_ts is not None and frame_ms:
                since = max(last_ts - frame_ms, 0)
            try:
                candles = exchange_client.fetch_candles(
                    symbol, timeframe=tf, limit=settings.trading.candle_limit, since=since
                )
                total_candles += store.save_candles(symbol, tf, candles, source=source)
            except Exception as exc:  # noqa: BLE001
                ingest_errors.append({"symbol": symbol, "timeframe": tf, "error": str(exc)})

        if orderbook:
            try:
                ob = exchange_client.fetch_orderbook(symbol)
                bid = float(ob["bids"][0][0]) if ob.get("bids") else 0.0
                ask = float(ob["asks"][0][0]) if ob.get("asks") else 0.0
                bid_size = float(ob["bids"][0][1]) if ob.get("bids") else 0.0
                ask_size = float(ob["asks"][0][1]) if ob.get("asks") else 0.0
                ts = int(
                    ob.get("timestamp") or int(datetime.now(timezone.utc).timestamp() * 1000)
                )
                store.save_orderbook_snapshot(symbol, bid, ask, bid_size, ask_size, ts)
            except Exception as exc:  # noqa: BLE001
                ingest_errors.append({"symbol": symbol, "orderbook": True, "error": str(exc)})

        news_stats: dict[str, Any] = {}
        if settings.news.rss_urls:
            items, news_stats = ingest_rss(settings.news.rss_urls)
            inserted_total = 0
            feed_inserted: dict[str, int] = {}
            for item, feed_url in items:
                if store.save_news_item(item) is not None:
                    inserted_total += 1
                    feed_inserted[feed_url] = feed_inserted.get(feed_url, 0) + 1
            news_stats["inserted"] = inserted_total
            for url, meta in news_stats.get("feeds", {}).items():
                meta["inserted"] = feed_inserted.get(url, 0)

        feature_version = "news_v1"
        articles = store.list_articles_without_features(feature_version=feature_version)
        for row in articles:
            normalized = _news_item_from_row(row)
            features = extract_features(
                normalized, settings.news.keyword_flags, settings.news.source_weights
            )
            store.save_news_features(
                article_id=int(row["id"]),
                sentiment=features.sentiment,
                keyword_flags=features.keyword_flags,
                source_weight=features.source_weight,
                language=features.language,
                feature_version=feature_version,
            )

        result = {
            "candles": total_candles,
            "news": news_stats,
            "features_added": len(articles),
            "errors": ingest_errors,
        }
        store.log_event("ingest", result)
        store.close()
        st.session_state["last_ingest"] = result
        _result_box("取り込み完了", result, kind="success")

    st.markdown("#### 3. 提案")
    with st.form("propose_form"):
        strategy_label = st.selectbox(
            "戦略", ["ベースライン", "ニュース・オーバーレイ"], index=0
        )
        mode_label = st.selectbox("モード", ["ペーパー", "ライブ"], index=0)
        refresh = st.checkbox("ローソク足を再取得", value=False)
        submitted = st.form_submit_button("提案を生成")

    if submitted:
        store = _get_store(settings)
        exchange_client = build_exchange(settings.exchange)
        source = f"ccxt:{settings.exchange.name}"

        if refresh:
            candles = exchange_client.fetch_candles(
                symbol, timeframe=timeframe, limit=settings.trading.candle_limit
            )
            store.save_candles(symbol, timeframe, candles, source=source)

        rows = store.fetch_candles(symbol, timeframe, settings.trading.candle_limit)
        if not rows:
            store.close()
            st.error("ローソク足がありません。先に取り込みを実行してください。")
        else:
            pos_size, avg_cost = store.get_position_state(symbol)
            if pos_size > 0:
                st.info(f"現在のポジション: {pos_size:.6f} / 平均コスト: {avg_cost:.2f}")
            else:
                st.info("現在のポジション: なし（ロングオンリー）")

            news_features, window_start, window_end = _recent_news(store, settings)
            latest_ts = int(rows[-1]["ts"])
            feature_vector = aggregate_feature_vector(news_features)
            store.save_feature_row(
                FeatureRow(
                    symbol=symbol,
                    ts=latest_ts,
                    features=feature_vector,
                    feature_version="news_v1",
                    computed_at=datetime.now(timezone.utc).isoformat(),
                    news_window_start=window_start,
                    news_window_end=window_end,
                )
            )
            features_ref = f"{symbol}:{latest_ts}:news_v1"

            strategy = "baseline" if strategy_label == "ベースライン" else "news_overlay"
            mode = "paper" if mode_label == "ペーパー" else "live"

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
            risk_result = evaluate_plan(
                store, plan, settings.risk, settings.trading, current_position=pos_size
            )
            adjusted_size = risk_result.plan.size if risk_result.plan else 0.0
            store.log_event(
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
                store.close()
                if risk_result.plan and risk_result.plan.side == "hold":
                    _result_box(
                        "ホールド（取引なし）",
                        {
                            "status": "hold",
                            "reason": risk_result.reason,
                            "rationale": risk_result.plan.rationale,
                        },
                        kind="info",
                    )
                else:
                    _result_box(
                        "リスク条件で却下されました",
                        {"status": "rejected", "reason": risk_result.reason},
                        kind="error",
                    )
            else:
                plan = risk_result.plan
                intent = from_plan(
                    plan,
                    mode=mode,
                    expiry_seconds=settings.trading.intent_expiry_seconds,
                    rationale_features_ref=features_ref,
                )
                store.save_order_intent(intent)
                store.log_event(
                    "propose",
                    {"intent_id": intent.intent_id, "symbol": intent.symbol, "side": intent.side},
                )
                payload = {
                    "intent_id": intent.intent_id,
                    "hash": intent.hash(),
                    "side": intent.side,
                    "size": intent.size,
                    "price": intent.price,
                    "strategy": intent.strategy,
                    "confidence": intent.confidence,
                    "rationale": intent.rationale,
                    "features_ref": features_ref,
                    "expires_at": intent.expires_at,
                }
                store.close()
                _result_box("提案を作成しました", payload, kind="success")

    st.markdown("#### 4. 承認")
    store = _get_store(settings)
    intents = store.list_intents(limit=20)
    store.close()
    if intents:
        with st.form("approve_form"):
            intent_id = st.selectbox("承認するインテント", [i["intent_id"] for i in intents])
            phrase = st.text_input("承認フレーズ", type="password")
            submitted = st.form_submit_button("承認する")
        if submitted:
            store = _get_store(settings)
            record = store.get_order_intent(intent_id)
            if not record:
                store.close()
                st.error("インテントが見つかりません")
            elif phrase.strip() != settings.trading.approval_phrase:
                store.close()
                st.error("承認フレーズが一致しません")
            else:
                store.save_approval_phrase(
                    intent_id,
                    record["intent_hash"],
                    phrase.strip(),
                    _approved_by(),
                )
                store.update_order_intent_status(intent_id, "approved")
                store.log_event("approve", {"intent_id": intent_id})
                store.close()
                st.success("承認しました")
    else:
        st.info("承認できるインテントがありません。")

    st.markdown("#### 5. 実行")
    store = _get_store(settings)
    intents = store.list_intents(limit=20)
    store.close()
    if intents:
        with st.form("execute_form"):
            intent_id = st.selectbox("実行するインテント", [i["intent_id"] for i in intents])
            exec_mode_label = st.selectbox("実行モード", ["ペーパー", "ライブ"], index=0)
            submitted = st.form_submit_button("実行する")
        if submitted:
            store = _get_store(settings)
            mode = "paper" if exec_mode_label == "ペーパー" else "live"
            exchange_client = build_exchange(settings.exchange) if mode == "live" else None
            result = execute_intent(store, intent_id, settings, mode, exchange_client=exchange_client)
            store.log_event("execute", {"intent_id": intent_id, "status": result.status})
            store.close()
            _result_box("実行結果", result.__dict__, kind="success" if result.status == "filled" else "info")
    else:
        st.info("実行できるインテントがありません。")


with tabs[2]:
    st.markdown("### バックテスト")
    with st.form("backtest_form"):
        start_date = st.date_input("開始日")
        end_date = st.date_input("終了日")
        bt_strategy_label = st.selectbox(
            "戦略", ["ベースライン", "ニュース・オーバーレイ"], index=0
        )
        submitted = st.form_submit_button("バックテスト実行")

    if submitted:
        store = _get_store(settings)
        strategy = "baseline" if bt_strategy_label == "ベースライン" else "news_overlay"
        output_dir = str(Path(settings.app.data_dir) / "reports")
        result = run_backtest(
            store,
            settings,
            symbol,
            timeframe,
            start_date.isoformat(),
            end_date.isoformat(),
            strategy,
            output_dir,
        )
        store.save_report_record(
            ReportRecord(
                run_id=str(uuid.uuid4()),
                period=f"{start_date.isoformat()}:{end_date.isoformat()}",
                metrics=json.loads(Path(result.metrics_path_json).read_text(encoding="utf-8")),
                equity_curve_path=result.metrics_path_csv,
                created_at=datetime.now(timezone.utc).isoformat(),
            )
        )
        store.log_event("backtest", {"strategy": strategy, "report_json": result.metrics_path_json})
        store.close()
        _result_box(
            "バックテスト完了",
            {
                "report_json": result.metrics_path_json,
                "equity_csv": result.metrics_path_csv,
                "summary_txt": result.metrics_path_summary,
            },
            kind="success",
        )

    st.markdown("### レポート")
    with st.form("report_form"):
        report_mode_label = st.selectbox("モード", ["全て", "ペーパー", "ライブ"], index=0)
        submitted = st.form_submit_button("レポート作成")

    if submitted:
        store = _get_store(settings)
        mode = (
            None
            if report_mode_label == "全て"
            else "paper"
            if report_mode_label == "ペーパー"
            else "live"
        )
        trades = store.load_trades(mode)
        metrics, equity = compute_metrics(trades, capital_jpy=settings.risk.capital_jpy)
        output_dir = str(Path(settings.app.data_dir) / "reports")
        prefix = f"report_{mode or 'all'}"
        paths = save_report(metrics, equity, output_dir, prefix)
        trade_details = store.load_trade_details(mode)
        trade_csv = save_trade_csv(trade_details, output_dir, prefix)
        store.save_report_record(
            ReportRecord(
                run_id=str(uuid.uuid4()),
                period=report_mode_label,
                metrics=metrics.__dict__,
                equity_curve_path=paths["csv"],
                created_at=datetime.now(timezone.utc).isoformat(),
            )
        )
        store.log_event(
            "report",
            {"mode": mode, "report_json": paths["json"], "trade_csv": trade_csv, "summary_txt": paths["summary"]},
        )
        store.close()
        _result_box(
            "レポート作成完了",
            {"metrics": metrics.__dict__, "paths": {**paths, "trades": trade_csv}},
            kind="success",
        )


with tabs[3]:
    st.markdown("### 監査ログ")
    store = _get_store(settings)
    logs = store.list_audit_logs(limit=200)
    intents = store.list_intents(limit=50)
    fills = store.list_fills(symbol=symbol, limit=200)
    store.close()

    events = sorted({row["event"] for row in logs})
    filter_event = st.selectbox("イベント", ["全て"] + events)
    if filter_event != "全て":
        logs = [row for row in logs if row["event"] == filter_event]
    summary = [_summarize_audit(row) for row in logs]
    st.dataframe(summary, use_container_width=True)

    st.markdown("### インテント履歴")
    if intents:
        st.dataframe(intents, use_container_width=True)
    else:
        st.info("インテント履歴がありません。")

    st.markdown("### 約定履歴")
    if fills:
        st.dataframe([dict(row) for row in fills], use_container_width=True)
    else:
        st.info("約定履歴がありません。")

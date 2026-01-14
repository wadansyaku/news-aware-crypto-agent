from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from trade_agent.config import AppSettings
from trade_agent.exchange import build_exchange
from trade_agent.news.features import extract_features
from trade_agent.news.rss import ingest_rss
from trade_agent.schemas import NewsItem
from trade_agent.store import SQLiteStore


@dataclass
class IngestParams:
    symbol: str | None = None
    orderbook: bool = False
    news_only: bool = False
    features_only: bool = False
    market_only: bool = False


def _timeframe_ms(exchange_client: object, timeframe: str) -> int | None:
    try:
        seconds = exchange_client.exchange.parse_timeframe(timeframe)
        return int(seconds * 1000)
    except Exception:  # noqa: BLE001
        return None


def _news_item_from_row(row: Any) -> NewsItem:
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


def ingest(settings: AppSettings, store: SQLiteStore, params: IngestParams) -> dict[str, Any]:
    if params.news_only and params.features_only:
        raise ValueError("cannot use news_only and features_only together")
    if params.market_only and (params.news_only or params.features_only):
        raise ValueError("cannot combine market_only with news_only/features_only")

    exchange_client = build_exchange(settings.exchange)
    symbols = [params.symbol] if params.symbol else settings.trading.symbol_whitelist
    total_candles = 0
    ingest_errors = []
    source = f"ccxt:{settings.exchange.name}"

    do_market = params.market_only or not (params.news_only or params.features_only)
    do_news = params.news_only or not (params.features_only or params.market_only)
    do_features = params.features_only or not (params.news_only or params.market_only)

    if do_market:
        for sym in symbols:
            for timeframe in settings.trading.timeframes:
                since = None
                last_ts = store.get_latest_candle_ts(sym, timeframe)
                frame_ms = _timeframe_ms(exchange_client, timeframe)
                if last_ts is not None and frame_ms:
                    since = max(last_ts - frame_ms, 0)
                try:
                    candles = exchange_client.fetch_candles(
                        sym,
                        timeframe=timeframe,
                        limit=settings.trading.candle_limit,
                        since=since,
                    )
                    total_candles += store.save_candles(sym, timeframe, candles, source=source)
                except Exception as exc:  # noqa: BLE001
                    ingest_errors.append({"symbol": sym, "timeframe": timeframe, "error": str(exc)})

            if params.orderbook:
                try:
                    ob = exchange_client.fetch_orderbook(sym)
                    bid = float(ob["bids"][0][0]) if ob.get("bids") else 0.0
                    ask = float(ob["asks"][0][0]) if ob.get("asks") else 0.0
                    bid_size = float(ob["bids"][0][1]) if ob.get("bids") else 0.0
                    ask_size = float(ob["asks"][0][1]) if ob.get("asks") else 0.0
                    ts = int(
                        ob.get("timestamp") or int(datetime.now(timezone.utc).timestamp() * 1000)
                    )
                    store.save_orderbook_snapshot(sym, bid, ask, bid_size, ask_size, ts)
                except Exception as exc:  # noqa: BLE001
                    ingest_errors.append({"symbol": sym, "orderbook": True, "error": str(exc)})

    news_stats: dict[str, Any] = {}
    if do_news and settings.news.rss_urls:
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

    features_added = 0
    if do_features:
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
        features_added = len(articles)

    result = {
        "candles": total_candles,
        "news": news_stats,
        "features_added": features_added,
        "errors": ingest_errors,
    }
    store.log_event("ingest", result)
    return result

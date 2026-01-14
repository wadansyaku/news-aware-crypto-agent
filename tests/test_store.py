from __future__ import annotations

from trade_agent.intent import OrderIntent
from trade_agent.schemas import NewsItem, sha256_hex
from trade_agent.store import SQLiteStore


def test_candle_dedupe() -> None:
    store = SQLiteStore(":memory:")
    candles = [[1700000000000, 100.0, 110.0, 90.0, 105.0, 1.0]]
    inserted_first = store.save_candles("BTC/JPY", "1m", candles, source="test")
    inserted_second = store.save_candles("BTC/JPY", "1m", candles, source="test")
    assert inserted_first == 1
    assert inserted_second == 0
    store.close()


def test_news_item_dedupe() -> None:
    store = SQLiteStore(":memory:")
    title = "Test News"
    item = NewsItem(
        source_url="https://example.com/news/1",
        source_name="example",
        guid="guid-1",
        title=title,
        summary="summary",
        published_at="2024-01-01T00:00:00+00:00",
        observed_at="2024-01-01T01:00:00+00:00",
        raw_payload_hash="payload",
        title_hash=sha256_hex(title),
    )
    first_id = store.save_news_item(item)
    second_id = store.save_news_item(item)
    assert first_id is not None
    assert second_id is None
    store.close()


def test_news_item_utc_normalization() -> None:
    store = SQLiteStore(":memory:")
    title = "UTC Test"
    item = NewsItem(
        source_url="https://example.com/news/utc",
        source_name="example",
        guid="guid-utc",
        title=title,
        summary="summary",
        published_at="2024-01-01T00:00:00",
        observed_at="2024-01-01T09:00:00+09:00",
        raw_payload_hash="payload",
        title_hash=sha256_hex(title),
    )
    store.save_news_item(item)
    row = store.conn.execute(
        "SELECT published_at, observed_at FROM news_articles WHERE url = ?",
        (item.source_url,),
    ).fetchone()
    assert row is not None
    assert str(row["published_at"]).endswith("+00:00")
    assert str(row["observed_at"]).endswith("+00:00")
    assert str(row["observed_at"]).startswith("2024-01-01T00:00:00")
    store.close()


def test_order_intent_idempotent() -> None:
    store = SQLiteStore(":memory:")
    intent = OrderIntent(
        intent_id="intent-1",
        created_at="2024-01-01T00:00:00+00:00",
        symbol="BTC/JPY",
        side="buy",
        size=0.1,
        price=5000000.0,
        order_type="limit",
        time_in_force="GTC",
        strategy="baseline",
        confidence=0.7,
        rationale="test",
        rationale_features_ref="feat-1",
        expires_at="2024-01-01T00:15:00+00:00",
        mode="paper",
    )
    first = store.save_order_intent(intent)
    second = store.save_order_intent(intent)
    assert first is True
    assert second is False
    store.close()

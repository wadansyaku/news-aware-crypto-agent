from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from trade_agent.config import load_config, resolve_db_path
from trade_agent.store import SQLiteStore
from trade_agent.backtest import run_backtest


def _ms(ts: str) -> int:
    return int(datetime.fromisoformat(ts).timestamp() * 1000)


def test_backtest_with_synthetic_news(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
app:
  data_dir: {tmp_path.as_posix()}
  db_path: { (tmp_path / 'test.db').as_posix() }
trading:
  symbol_whitelist:
    - "BTC/JPY"
risk:
  capital_jpy: 100000
  max_position_pct: 1.0
  max_order_notional_jpy: 100000
  max_loss_jpy_per_trade: 100000
  max_loss_jpy_per_day: 100000
  max_orders_per_day: 100
  cooldown_minutes: 0
strategies:
  baseline:
    sma_period: 2
    momentum_lookback: 1
    base_position_pct: 0.2
backtest:
  maker_fee_bps: 0
  taker_fee_bps: 0
  slippage_bps: 0
  assume_taker: true
news:
  news_latency_seconds: 60
""",
        encoding="utf-8",
    )
    settings = load_config(str(config_path))
    store = SQLiteStore(resolve_db_path(settings))

    candles = []
    for idx, close in enumerate([100, 101, 102, 103, 102, 101, 100, 99, 98, 97]):
        ts = _ms(f"2024-01-01T00:0{idx}:00+00:00")
        candles.append([ts, close, close, close, close, 1.0])
    store.save_candles("BTC/JPY", "1m", candles, source="test")

    # Manually insert a news item and features.
    from trade_agent.schemas import NewsItem, sha256_hex

    news_item = NewsItem(
        source_url="https://example.com/news/1",
        source_name="example",
        guid="guid-1",
        title="Test News",
        summary="summary",
        published_at="2024-01-01T00:00:00+00:00",
        observed_at="2024-01-01T00:01:30+00:00",
        raw_payload_hash="payload",
        title_hash=sha256_hex("Test News"),
    )
    article_id = store.save_news_item(news_item)
    assert article_id is not None
    store.save_news_features(
        article_id=article_id,
        sentiment=0.3,
        keyword_flags={"test": True},
        source_weight=1.0,
        language="en",
        feature_version="news_v1",
    )

    output_dir = str(Path(settings.app.data_dir) / "reports")
    result = run_backtest(
        store,
        settings,
        "BTC/JPY",
        "1m",
        "2024-01-01",
        "2024-01-01",
        "baseline",
        output_dir,
    )
    assert Path(result.metrics_path_json).exists()
    assert Path(result.metrics_path_csv).exists()
    assert Path(result.metrics_path_summary).exists()
    store.close()

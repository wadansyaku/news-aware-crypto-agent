from __future__ import annotations

from pathlib import Path

from trade_agent.news.rss import ingest_rss
from trade_agent.store import SQLiteStore


def test_rss_ingest_fixture(tmp_path: Path) -> None:
    fixture = Path(__file__).parent / "fixtures" / "rss_sample.xml"
    items, stats = ingest_rss([fixture.as_posix()])
    assert stats["total"] == 2
    assert fixture.as_posix() in stats["feeds"]

    store = SQLiteStore(":memory:")
    inserted = 0
    for item, _ in items:
        if store.save_news_item(item) is not None:
            inserted += 1
    assert inserted == 2

    inserted_again = 0
    for item, _ in items:
        if store.save_news_item(item) is not None:
            inserted_again += 1
    assert inserted_again == 0
    store.close()

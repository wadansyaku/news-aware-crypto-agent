from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import feedparser

from trade_agent import db
from trade_agent.news.normalize import normalize_entry


def _source_from_feed(feed: dict[str, Any], url: str) -> str:
    title = str(feed.get("title", "")).strip().lower()
    if title:
        return re.sub(r"\s+", "_", title)
    return urlparse(url).netloc.replace(".", "_").lower()


def fetch_entries(urls: list[str]) -> list[tuple[dict[str, Any], str]]:
    entries: list[tuple[dict[str, Any], str]] = []
    for url in urls:
        parsed = feedparser.parse(url)
        source = _source_from_feed(parsed.feed, url)
        for entry in parsed.entries:
            entries.append((entry, source))
    return entries


def ingest_rss(conn: sqlite3.Connection, urls: list[str]) -> dict[str, int]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    entries = fetch_entries(urls)
    inserted = 0
    for entry, source in entries:
        normalized = normalize_entry(entry, source=source, ingested_at=ingested_at)
        article_id = db.insert_news_article(
            conn,
            url=normalized.url,
            title=normalized.title,
            source=normalized.source,
            published_at=normalized.published_at,
            title_hash=normalized.title_hash,
        )
        if article_id is not None:
            inserted += 1
    return {"total": len(entries), "inserted": inserted}

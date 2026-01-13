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


def ingest_rss(conn: sqlite3.Connection, urls: list[str]) -> dict[str, Any]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    stats: dict[str, Any] = {"total": 0, "inserted": 0, "feeds": {}, "errors": []}
    for url in urls:
        try:
            parsed = feedparser.parse(url)
        except Exception as exc:  # noqa: BLE001
            stats["errors"].append({"url": url, "error": str(exc)})
            continue

        if getattr(parsed, "bozo", False) and getattr(parsed, "bozo_exception", None):
            stats["errors"].append(
                {"url": url, "error": str(parsed.bozo_exception), "bozo": True}
            )

        source = _source_from_feed(parsed.feed, url)
        feed_total = 0
        feed_inserted = 0
        for entry in parsed.entries:
            feed_total += 1
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
                feed_inserted += 1

        stats["feeds"][url] = {
            "source": source,
            "total": feed_total,
            "inserted": feed_inserted,
            "bozo": bool(getattr(parsed, "bozo", False)),
        }
        stats["total"] += feed_total
        stats["inserted"] += feed_inserted
    return stats

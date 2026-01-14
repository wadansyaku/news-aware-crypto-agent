from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import feedparser

from trade_agent.news.normalize import normalize_entry
from trade_agent.schemas import NewsItem


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


def ingest_rss(urls: list[str]) -> tuple[list[tuple[NewsItem, str]], dict[str, Any]]:
    observed_at = datetime.now(timezone.utc).isoformat()
    stats: dict[str, Any] = {"total": 0, "feeds": {}, "errors": []}
    items: list[tuple[NewsItem, str]] = []
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
        for entry in parsed.entries:
            feed_total += 1
            normalized = normalize_entry(entry, source=source, observed_at=observed_at)
            items.append((normalized, url))

        stats["feeds"][url] = {
            "source": source,
            "total": feed_total,
            "bozo": bool(getattr(parsed, "bozo", False)),
        }
        stats["total"] += feed_total
    return items, stats

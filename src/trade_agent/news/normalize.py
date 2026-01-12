from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")


@dataclass
class NormalizedNews:
    title: str
    url: str
    source: str
    published_at: str
    ingested_at: str
    title_hash: str


def safe_text(text: str) -> str:
    text = text.strip()
    text = _CONTROL_CHARS.sub(" ", text)
    return " ".join(text.split())


def _parse_published(entry: dict[str, Any]) -> datetime:
    if entry.get("published_parsed"):
        return datetime(*entry["published_parsed"][:6], tzinfo=timezone.utc)
    if entry.get("published"):
        try:
            parsed = parsedate_to_datetime(entry["published"])
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:  # noqa: BLE001
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


def normalize_entry(entry: dict[str, Any], source: str, ingested_at: str) -> NormalizedNews:
    title = safe_text(str(entry.get("title", "")))
    url = safe_text(str(entry.get("link", "")))
    published = _parse_published(entry)
    title_hash = hashlib.sha256(title.encode("utf-8")).hexdigest()
    return NormalizedNews(
        title=title,
        url=url,
        source=source,
        published_at=published.isoformat(),
        ingested_at=ingested_at,
        title_hash=title_hash,
    )

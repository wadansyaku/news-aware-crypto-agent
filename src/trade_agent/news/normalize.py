from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

from trade_agent.schemas import NewsItem, ensure_utc_iso

_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")


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


def _raw_payload_hash(entry: dict[str, Any]) -> str:
    try:
        payload = json.dumps(entry, sort_keys=True, default=str)
    except Exception:  # noqa: BLE001
        payload = repr(entry)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_entry(entry: dict[str, Any], source: str, observed_at: str) -> NewsItem:
    title = safe_text(str(entry.get("title", "")))
    url = safe_text(str(entry.get("link", "")))
    summary = safe_text(str(entry.get("summary") or entry.get("description") or ""))
    guid_raw = entry.get("id") or entry.get("guid") or entry.get("link") or ""
    guid = safe_text(str(guid_raw)) or None
    published = _parse_published(entry)
    title_hash = hashlib.sha256(title.encode("utf-8")).hexdigest()
    observed_iso = ensure_utc_iso(observed_at, default_to_now=True) or published.isoformat()
    return NewsItem(
        title=title,
        source_url=url,
        source_name=source,
        guid=guid,
        summary=summary,
        published_at=published.isoformat(),
        observed_at=observed_iso,
        raw_payload_hash=_raw_payload_hash(entry),
        title_hash=title_hash,
    )

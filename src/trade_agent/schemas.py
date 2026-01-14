from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_utc_iso(value: str | datetime | None, default_to_now: bool = False) -> str | None:
    if value is None:
        return utc_now_iso() if default_to_now else None
    if isinstance(value, datetime):
        dt = value
    else:
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return utc_now_iso() if default_to_now else None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


@dataclass
class Candle:
    symbol: str
    timeframe: str
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str
    ingested_at: str


@dataclass
class NewsItem:
    source_url: str
    source_name: str
    guid: str | None
    title: str
    summary: str
    published_at: str | None
    observed_at: str
    raw_payload_hash: str
    title_hash: str


@dataclass
class FeatureRow:
    symbol: str
    ts: int
    features: dict[str, float]
    feature_version: str
    computed_at: str
    news_window_start: str
    news_window_end: str


@dataclass
class OrderIntentRecord:
    intent_id: str
    created_at: str
    strategy: str
    symbol: str
    side: str
    order_type: str
    qty: float
    limit_price: float | None
    rationale_features_ref: str | None
    status: str
    intent_hash: str
    intent_json: str
    confidence: float
    rationale: str
    expires_at: str
    time_in_force: str
    mode: str


@dataclass
class ApprovalRecord:
    intent_id: str
    intent_hash: str
    approved_at: str
    approved_by: str
    approval_phrase_hash: str


@dataclass
class ExecutionRecord:
    exec_id: str
    intent_id: str
    intent_hash: str
    executed_at: str
    mode: str
    status: str
    fee: float
    slippage_model: str
    details: dict[str, Any]


@dataclass
class FillRecord:
    fill_id: str
    exec_id: str
    symbol: str
    side: str
    size: float
    price: float
    fee: float
    fee_currency: str
    ts: str


@dataclass
class ReportRecord:
    run_id: str
    period: str
    metrics: dict[str, Any]
    equity_curve_path: str
    created_at: str

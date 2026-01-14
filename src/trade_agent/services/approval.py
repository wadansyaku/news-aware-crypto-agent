from __future__ import annotations

from dataclasses import dataclass

from trade_agent.config import AppSettings
from trade_agent.store import SQLiteStore


@dataclass
class ApprovalResult:
    status: str
    intent_id: str


def approve_intent(
    settings: AppSettings,
    store: SQLiteStore,
    intent_id: str,
    phrase: str,
    approved_by: str,
) -> ApprovalResult:
    record = store.get_order_intent(intent_id)
    if not record:
        raise ValueError("intent not found")

    clean_phrase = phrase.strip() if phrase else ""
    if not clean_phrase:
        clean_phrase = settings.trading.approval_phrase
    if clean_phrase != settings.trading.approval_phrase:
        raise ValueError("approval phrase mismatch")

    store.save_approval_phrase(intent_id, record["intent_hash"], clean_phrase, approved_by)
    store.update_order_intent_status(intent_id, "approved")
    store.log_event("approve", {"intent_id": intent_id})
    return ApprovalResult(status="approved", intent_id=intent_id)

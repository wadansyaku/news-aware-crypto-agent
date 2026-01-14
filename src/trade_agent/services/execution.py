from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from trade_agent.config import AppSettings
from trade_agent.executor import ExecutionResult, execute_intent
from trade_agent.exchange import build_exchange
from trade_agent.store import SQLiteStore
from trade_agent.services.approval import approve_intent, ApprovalResult


@dataclass
class ApprovalExecutionResult:
    approval: ApprovalResult
    execution: ExecutionResult


def execute(
    settings: AppSettings,
    store: SQLiteStore,
    intent_id: Optional[str] = None,
    mode: str = "paper",
) -> ExecutionResult:
    if mode not in {"paper", "live"}:
        raise ValueError("invalid mode")

    if not intent_id:
        record = store.get_latest_intent(status="approved")
        if not record:
            record = store.get_latest_intent(status="proposed")
        if not record:
            raise ValueError("no pending intent")
        intent_id = record["intent_id"]

    exchange_client = build_exchange(settings.exchange) if mode == "live" else None
    result = execute_intent(store, intent_id, settings, mode, exchange_client=exchange_client)
    store.log_event("execute", {"intent_id": intent_id, "status": result.status})
    return result


def approve_and_execute(
    settings: AppSettings,
    store: SQLiteStore,
    intent_id: str,
    phrase: str,
    approved_by: str,
    mode: str = "paper",
) -> ApprovalExecutionResult:
    approval = approve_intent(settings, store, intent_id, phrase, approved_by)
    execution = execute(settings, store, intent_id=intent_id, mode=mode)
    store.log_event(
        "approve_execute",
        {"intent_id": intent_id, "mode": mode, "status": execution.status},
    )
    return ApprovalExecutionResult(approval=approval, execution=execution)

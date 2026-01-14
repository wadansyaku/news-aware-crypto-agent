from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass
class TradePlan:
    symbol: str
    side: str
    size: float
    price: float
    confidence: float
    rationale: str
    strategy: str

    @classmethod
    def hold(cls, symbol: str, strategy: str, rationale: str) -> "TradePlan":
        return cls(
            symbol=symbol,
            side="hold",
            size=0.0,
            price=0.0,
            confidence=0.0,
            rationale=rationale,
            strategy=strategy,
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class OrderIntent:
    intent_id: str
    created_at: str
    symbol: str
    side: str
    size: float
    price: float
    order_type: str
    time_in_force: str
    strategy: str
    confidence: float
    rationale: str
    rationale_features_ref: str | None
    expires_at: str
    mode: str

    def to_dict(self) -> dict[str, object]:
        return {
            "intent_id": self.intent_id,
            "created_at": self.created_at,
            "symbol": self.symbol,
            "side": self.side,
            "size": self.size,
            "price": self.price,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "strategy": self.strategy,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "rationale_features_ref": self.rationale_features_ref,
            "expires_at": self.expires_at,
            "mode": self.mode,
        }

    def canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)

    def hash(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    def as_record(self) -> dict[str, object]:
        return {
            "intent_id": self.intent_id,
            "created_at": self.created_at,
            "intent_json": self.canonical_json(),
            "intent_hash": self.hash(),
            "status": "proposed",
            "expires_at": self.expires_at,
            "strategy": self.strategy,
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "size": self.size,
            "price": self.price,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "rationale_features_ref": self.rationale_features_ref,
            "mode": self.mode,
        }


def from_plan(
    plan: TradePlan, mode: str, expiry_seconds: int, rationale_features_ref: str | None = None
) -> OrderIntent:
    now = _utc_now()
    return OrderIntent(
        intent_id=str(uuid.uuid4()),
        created_at=now.isoformat(),
        symbol=plan.symbol,
        side=plan.side,
        size=float(plan.size),
        price=float(plan.price),
        order_type="limit",
        time_in_force="GTC",
        strategy=plan.strategy,
        confidence=float(plan.confidence),
        rationale=plan.rationale,
        rationale_features_ref=rationale_features_ref,
        expires_at=(now + timedelta(seconds=expiry_seconds)).isoformat(),
        mode=mode,
    )


def intent_expired(intent: OrderIntent) -> bool:
    expires = datetime.fromisoformat(intent.expires_at)
    return _utc_now() >= expires

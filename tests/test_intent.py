from __future__ import annotations

import hashlib

from trade_agent.intent import OrderIntent


def test_canonical_json_and_hash() -> None:
    intent = OrderIntent(
        intent_id="test-id",
        created_at="2024-01-01T00:00:00+00:00",
        symbol="BTC/JPY",
        side="buy",
        size=0.1,
        price=5000000.0,
        order_type="limit",
        time_in_force="GTC",
        strategy="baseline",
        confidence=0.7,
        rationale="test",
        rationale_features_ref="feat-1",
        expires_at="2024-01-01T00:15:00+00:00",
        mode="paper",
    )
    expected_json = (
        "{"
        '"confidence":0.7,'
        '"created_at":"2024-01-01T00:00:00+00:00",'
        '"expires_at":"2024-01-01T00:15:00+00:00",'
        '"intent_id":"test-id",'
        '"mode":"paper",'
        '"order_type":"limit",'
        '"price":5000000.0,'
        '"rationale":"test",'
        '"rationale_features_ref":"feat-1",'
        '"side":"buy",'
        '"size":0.1,'
        '"strategy":"baseline",'
        '"symbol":"BTC/JPY",'
        '"time_in_force":"GTC"'
        "}"
    )
    assert intent.canonical_json() == expected_json
    expected_hash = hashlib.sha256(expected_json.encode("utf-8")).hexdigest()
    assert intent.hash() == expected_hash

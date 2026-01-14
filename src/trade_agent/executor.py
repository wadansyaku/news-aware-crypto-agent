from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from trade_agent.config import AppSettings
from trade_agent.exchange import ExchangeClient, has_credentials
from trade_agent.intent import OrderIntent, TradePlan, intent_expired
from trade_agent.paper import OrderbookSnapshot, build_rng, estimate_orderbook_from_price, simulate_fill
from trade_agent.risk import evaluate_plan
from trade_agent.schemas import ExecutionRecord, FillRecord
from trade_agent.store import SQLiteStore


@dataclass
class ExecutionResult:
    status: str
    message: str
    exec_id: Optional[str] = None


def _intent_from_record(record: sqlite3.Row) -> OrderIntent:
    payload = json.loads(record["intent_json"])
    return OrderIntent(
        intent_id=payload["intent_id"],
        created_at=payload["created_at"],
        symbol=payload["symbol"],
        side=payload["side"],
        size=float(payload["size"]),
        price=float(payload["price"]),
        order_type=payload.get("order_type", "limit"),
        time_in_force=payload.get("time_in_force", "GTC"),
        strategy=payload["strategy"],
        confidence=float(payload["confidence"]),
        rationale=payload["rationale"],
        rationale_features_ref=payload.get("rationale_features_ref"),
        expires_at=payload["expires_at"],
        mode=payload["mode"],
    )


def _approval_ok(store: SQLiteStore, intent_id: str, intent_hash: str) -> bool:
    approval = store.get_approval(intent_id)
    if not approval:
        return False
    return approval["intent_hash"] == intent_hash


def _autopilot_ok(settings: AppSettings, intent: OrderIntent) -> bool:
    if not settings.autopilot.enabled:
        return False
    if intent.symbol not in settings.autopilot.symbol_whitelist:
        return False
    notional = intent.price * intent.size
    if notional > settings.autopilot.max_order_notional_jpy:
        return False
    if settings.risk.max_loss_jpy_per_trade > settings.autopilot.max_loss_jpy_per_trade:
        return False
    if intent.confidence < settings.autopilot.min_confidence:
        return False
    return True


def _price_tick(exchange: object, symbol: str) -> float | None:
    try:
        market = exchange.market(symbol)
        precision = market.get("precision", {}).get("price")
        if isinstance(precision, int) and precision >= 0:
            return 10 ** (-precision)
    except Exception:  # noqa: BLE001
        return None
    return None


def _emulate_post_only_price(
    exchange_client: ExchangeClient,
    intent: OrderIntent,
    buffer_bps: float,
    use_tick: bool,
) -> tuple[float, dict[str, object]]:
    details: dict[str, object] = {
        "maker_emulation": True,
        "requested_price": intent.price,
        "buffer_bps": buffer_bps,
        "use_tick": use_tick,
    }
    try:
        exchange_client.load_markets()
        orderbook = exchange_client.fetch_orderbook(intent.symbol)
    except Exception as exc:  # noqa: BLE001
        details["maker_emulation_error"] = str(exc)
        return intent.price, details

    bid = float(orderbook["bids"][0][0]) if orderbook.get("bids") else 0.0
    ask = float(orderbook["asks"][0][0]) if orderbook.get("asks") else 0.0
    tick = _price_tick(exchange_client.exchange, intent.symbol) if use_tick else None
    base = bid or ask or intent.price
    buffer = base * (buffer_bps / 10000)

    price = intent.price
    if intent.side == "buy" and bid > 0:
        price = min(price, bid)
        if ask > 0 and price >= ask:
            pad = tick if tick is not None else buffer
            price = max(bid - pad, 0.0)
    elif intent.side == "sell" and ask > 0:
        price = max(price, ask)
        if bid > 0 and price <= bid:
            pad = tick if tick is not None else buffer
            price = ask + pad

    details.update({"best_bid": bid, "best_ask": ask, "tick_size": tick, "placed_price": price})
    return price, details


def _record_order(
    store: SQLiteStore,
    order_id: str,
    exec_id: str,
    intent: OrderIntent,
    mode: str,
    status: str,
    price: float,
    raw: dict[str, object],
    created_at: str,
) -> None:
    store.save_order(
        order_id=order_id,
        exec_id=exec_id,
        intent_id=intent.intent_id,
        created_at=created_at,
        mode=mode,
        symbol=intent.symbol,
        side=intent.side,
        order_type=intent.order_type,
        time_in_force=intent.time_in_force,
        size=float(intent.size),
        price=float(price),
        status=status,
        raw=raw,
    )


def execute_intent(
    store: SQLiteStore,
    intent_id: str,
    settings: AppSettings,
    mode: str,
    exchange_client: ExchangeClient | None = None,
) -> ExecutionResult:
    record = store.get_order_intent(intent_id)
    if not record:
        return ExecutionResult(status="error", message="intent not found")

    intent = _intent_from_record(record)
    intent_hash = record["intent_hash"]
    if intent.hash() != intent_hash:
        return ExecutionResult(status="rejected", message="intent hash mismatch")
    if intent_expired(intent):
        store.update_order_intent_status(intent_id, "expired")
        return ExecutionResult(status="rejected", message="intent expired")

    if settings.trading.require_approval and not _autopilot_ok(settings, intent):
        if not _approval_ok(store, intent.intent_id, intent_hash):
            return ExecutionResult(status="rejected", message="approval required")

    plan = TradePlan(
        symbol=intent.symbol,
        side=intent.side,
        size=float(intent.size),
        price=float(intent.price),
        confidence=float(intent.confidence),
        rationale=intent.rationale,
        strategy=intent.strategy,
    )
    risk_result = evaluate_plan(
        store,
        plan,
        settings.risk,
        settings.trading,
        current_position=store.get_position_size(intent.symbol),
    )
    if not risk_result.approved or not risk_result.plan:
        store.update_order_intent_status(intent.intent_id, "rejected")
        return ExecutionResult(status="rejected", message=f"risk rejected: {risk_result.reason}")
    if abs(risk_result.plan.size - intent.size) > 1e-9:
        store.update_order_intent_status(intent.intent_id, "rejected")
        return ExecutionResult(status="rejected", message="risk adjustment required; re-propose")

    if mode == "live":
        if settings.trading.dry_run:
            return ExecutionResult(status="rejected", message="dry_run enabled")
        ack_env = os.getenv("I_UNDERSTAND_LIVE_TRADING", "").lower() == "true"
        if not (settings.trading.i_understand_live_trading and ack_env):
            return ExecutionResult(status="rejected", message="live trading not acknowledged")
        if not has_credentials(settings.exchange):
            return ExecutionResult(status="rejected", message="missing API credentials")

    exec_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    if mode == "paper":
        rng = build_rng(settings.paper)
        snapshot = store.get_latest_orderbook_snapshot(intent.symbol)
        if snapshot:
            orderbook = OrderbookSnapshot(
                bid=float(snapshot["bid"]),
                ask=float(snapshot["ask"]),
                bid_size=float(snapshot["bid_size"]),
                ask_size=float(snapshot["ask_size"]),
                ts=now,
            )
        else:
            orderbook = estimate_orderbook_from_price(intent.price, settings.paper.spread_bps)

        fill = simulate_fill(intent, orderbook, settings.paper, rng)
        _record_order(
            store,
            order_id=exec_id,
            exec_id=exec_id,
            intent=intent,
            mode=mode,
            status=fill.status,
            price=fill.price if fill.filled else intent.price,
            raw={
                "message": fill.message,
                "filled": fill.filled,
                "orderbook": orderbook.__dict__,
                "slippage_bps": settings.paper.slippage_bps,
            },
            created_at=now,
        )
        store.save_execution(
            ExecutionRecord(
                exec_id=exec_id,
                intent_id=intent.intent_id,
                intent_hash=intent_hash,
                executed_at=now,
                mode=mode,
                status=fill.status,
                fee=fill.fee if fill.filled else 0.0,
                slippage_model="paper_v1",
                details={"message": fill.message},
            )
        )
        if fill.filled:
            fill_id = str(uuid.uuid4())
            store.save_fill(
                FillRecord(
                    fill_id=fill_id,
                    exec_id=exec_id,
                    symbol=intent.symbol,
                    side=intent.side,
                    size=fill.size,
                    price=fill.price,
                    fee=fill.fee,
                    fee_currency=fill.fee_currency,
                    ts=now,
                )
            )
            pnl = 0.0
            notional = fill.price * fill.size
            if intent.side == "sell":
                _, avg_cost = store.get_position_state(intent.symbol)
                pnl = (fill.price - avg_cost) * fill.size - fill.fee
            store.save_trade_result(
                trade_id=str(uuid.uuid4()),
                intent_id=intent.intent_id,
                pnl_jpy=pnl,
                mode=mode,
                meta={
                    "fill_price": fill.price,
                    "size": fill.size,
                    "notional": notional,
                    "fee": fill.fee,
                },
            )
        store.update_order_intent_status(intent.intent_id, fill.status)
        return ExecutionResult(status=fill.status, message=fill.message, exec_id=exec_id)

    if mode == "live":
        if exchange_client is None:
            return ExecutionResult(status="error", message="exchange client missing")
        try:
            order_price = intent.price
            details: dict[str, object] = {"requested_price": intent.price, "maker_emulation": False}
            if settings.trading.post_only and not exchange_client.exchange.has.get("postOnly"):
                order_price, emulation_details = _emulate_post_only_price(
                    exchange_client,
                    intent,
                    settings.trading.maker_emulation.buffer_bps,
                    settings.trading.maker_emulation.use_tick,
                )
                details.update(emulation_details)
            order = exchange_client.create_limit_order(
                intent.symbol, intent.side, intent.size, order_price, settings.trading.post_only
            )
            order_id = order.get("id")
            deadline = time.time() + settings.trading.order_timeout_seconds
            status = "open"
            filled = 0.0
            avg_price = 0.0
            while time.time() < deadline:
                info = exchange_client.fetch_order(order_id, intent.symbol)
                status = info.get("status", status)
                filled = float(info.get("filled") or 0.0)
                avg_price = float(info.get("average") or info.get("price") or intent.price)
                if status in {"closed", "filled"}:
                    break
                time.sleep(1)
            if status not in {"closed", "filled"}:
                exchange_client.cancel_order(order_id, intent.symbol)
                status = "canceled"
            _record_order(
                store,
                order_id=str(order_id or exec_id),
                exec_id=exec_id,
                intent=intent,
                mode=mode,
                status=status,
                price=order_price,
                raw={
                    "order": order,
                    "filled": filled,
                    "avg_price": avg_price,
                    **details,
                },
                created_at=now,
            )
            store.save_execution(
                ExecutionRecord(
                    exec_id=exec_id,
                    intent_id=intent.intent_id,
                    intent_hash=intent_hash,
                    executed_at=now,
                    mode=mode,
                    status=status,
                    fee=0.0,
                    slippage_model="exchange",
                    details={
                        "order_id": order_id,
                        "filled": filled,
                        "avg_price": avg_price,
                        **details,
                    },
                )
            )
            if filled > 0:
                store.save_fill(
                    FillRecord(
                        fill_id=str(uuid.uuid4()),
                        exec_id=exec_id,
                        symbol=intent.symbol,
                        side=intent.side,
                        size=filled,
                        price=avg_price,
                        fee=0.0,
                        fee_currency=settings.trading.base_currency,
                        ts=now,
                    )
                )
            store.update_order_intent_status(intent.intent_id, status)
            return ExecutionResult(status=status, message="live execution", exec_id=exec_id)
        except Exception as exc:  # noqa: BLE001
            _record_order(
                store,
                order_id=exec_id,
                exec_id=exec_id,
                intent=intent,
                mode=mode,
                status="error",
                price=intent.price,
                raw={"error": str(exc)},
                created_at=now,
            )
            store.save_execution(
                ExecutionRecord(
                    exec_id=exec_id,
                    intent_id=intent.intent_id,
                    intent_hash=intent_hash,
                    executed_at=now,
                    mode=mode,
                    status="error",
                    fee=0.0,
                    slippage_model="exchange",
                    details={"error": str(exc)},
                )
            )
            store.update_order_intent_status(intent.intent_id, "error")
            return ExecutionResult(status="error", message=str(exc), exec_id=exec_id)

    return ExecutionResult(status="error", message="unknown mode")

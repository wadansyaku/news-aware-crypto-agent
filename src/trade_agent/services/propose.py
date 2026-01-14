from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from trade_agent.config import AppSettings
from trade_agent.exchange import build_exchange
from trade_agent.intent import TradePlan, from_plan
from trade_agent.news.features import aggregate_feature_vector
from trade_agent.risk import evaluate_plan
from trade_agent.schemas import FeatureRow
from trade_agent.store import SQLiteStore
from trade_agent.strategies import baseline, news_overlay


@dataclass
class ProposeParams:
    symbol: str | None = None
    strategy: str = "baseline"
    mode: str = "paper"
    refresh: bool = False


@dataclass
class ProposalCandidate:
    status: str
    plan: TradePlan | None
    features_ref: str | None
    reason: str | None = None


def _recent_news(
    store: SQLiteStore, settings: AppSettings, as_of: datetime | None = None
) -> tuple[list[dict[str, float]], str, str]:
    now = as_of or datetime.now(timezone.utc)
    start = now - timedelta(hours=settings.news.sentiment_lookback_hours)
    features = store.list_news_features_window(
        start_iso=start.isoformat(),
        end_iso=now.isoformat(),
        observed_cutoff=now.isoformat(),
    )
    usable = []
    for row in features:
        published = datetime.fromisoformat(row["published_at"])
        observed = datetime.fromisoformat(row["observed_at"])
        available_at = max(
            observed, published + timedelta(seconds=settings.news.news_latency_seconds)
        )
        if available_at <= now and published >= start:
            usable.append(
                {
                    "sentiment": float(row["sentiment"]),
                    "source_weight": float(row["source_weight"]),
                }
            )
    return usable, start.isoformat(), now.isoformat()


def prepare_proposal(
    settings: AppSettings, store: SQLiteStore, params: ProposeParams
) -> ProposalCandidate:
    if params.strategy not in {"baseline", "news_overlay"}:
        raise ValueError("invalid strategy")
    if params.mode not in {"paper", "live"}:
        raise ValueError("invalid mode")

    exchange_client = build_exchange(settings.exchange)
    symbol = params.symbol or settings.trading.symbol_whitelist[0]
    timeframe = settings.trading.timeframes[0]
    source = f"ccxt:{settings.exchange.name}"

    if params.refresh:
        candles = exchange_client.fetch_candles(
            symbol, timeframe=timeframe, limit=settings.trading.candle_limit
        )
        store.save_candles(symbol, timeframe, candles, source=source)

    rows = store.fetch_candles(symbol, timeframe, settings.trading.candle_limit)
    if not rows:
        raise ValueError("no candles available; run ingest first")

    latest_ts = int(rows[-1]["ts"])
    news_features, window_start, window_end = _recent_news(store, settings)
    feature_version = "news_v1"
    feature_vector = aggregate_feature_vector(news_features)
    feature_row = FeatureRow(
        symbol=symbol,
        ts=latest_ts,
        features=feature_vector,
        feature_version=feature_version,
        computed_at=datetime.now(timezone.utc).isoformat(),
        news_window_start=window_start,
        news_window_end=window_end,
    )
    store.save_feature_row(feature_row)
    features_ref = f"{symbol}:{latest_ts}:{feature_version}"

    if params.strategy == "baseline":
        plan = baseline.generate_plan(symbol, rows, settings.risk, settings.strategies.baseline)
    else:
        plan = news_overlay.generate_plan(
            symbol,
            rows,
            news_features,
            settings.risk,
            settings.strategies.baseline,
            settings.strategies.news_overlay,
        )

    current_position = store.get_position_size(symbol)
    if plan.side in {"buy", "sell"} and settings.trading.post_only:
        if not exchange_client.exchange.has.get("postOnly"):
            try:
                ob = exchange_client.fetch_orderbook(symbol)
                if plan.side == "buy" and ob.get("bids"):
                    bid = float(ob["bids"][0][0])
                    plan = TradePlan(
                        symbol=plan.symbol,
                        side=plan.side,
                        size=plan.size,
                        price=min(plan.price, bid),
                        confidence=plan.confidence,
                        rationale=f"{plan.rationale}; maker price at bid",
                        strategy=plan.strategy,
                    )
                elif plan.side == "sell" and ob.get("asks"):
                    ask = float(ob["asks"][0][0])
                    plan = TradePlan(
                        symbol=plan.symbol,
                        side=plan.side,
                        size=plan.size,
                        price=max(plan.price, ask),
                        confidence=plan.confidence,
                        rationale=f"{plan.rationale}; maker price at ask",
                        strategy=plan.strategy,
                    )
            except Exception:  # noqa: BLE001
                pass

    original_size = plan.size
    risk_result = evaluate_plan(
        store, plan, settings.risk, settings.trading, current_position=current_position
    )
    adjusted_size = risk_result.plan.size if risk_result.plan else 0.0
    store.log_event(
        "risk_check",
        {
            "symbol": plan.symbol,
            "strategy": plan.strategy,
            "side": plan.side,
            "status": "approved" if risk_result.approved else "rejected",
            "reason": risk_result.reason,
            "original_size": original_size,
            "adjusted_size": adjusted_size,
        },
    )

    if not risk_result.approved or not risk_result.plan:
        if risk_result.plan and risk_result.plan.side == "hold":
            return ProposalCandidate(
                status="hold",
                plan=risk_result.plan,
                features_ref=features_ref,
                reason=risk_result.reason,
            )
        return ProposalCandidate(
            status="rejected", plan=None, features_ref=features_ref, reason=risk_result.reason
        )

    return ProposalCandidate(status="proposed", plan=risk_result.plan, features_ref=features_ref)


def finalize_proposal(
    settings: AppSettings,
    store: SQLiteStore,
    candidate: ProposalCandidate,
    params: ProposeParams,
) -> dict[str, Any]:
    if candidate.status != "proposed" or not candidate.plan:
        if candidate.status == "hold" and candidate.plan:
            return {
                "status": "hold",
                "reason": candidate.reason,
                "rationale": candidate.plan.rationale,
            }
        return {"status": "rejected", "reason": candidate.reason}

    intent = from_plan(
        candidate.plan,
        mode=params.mode,
        expiry_seconds=settings.trading.intent_expiry_seconds,
        rationale_features_ref=candidate.features_ref,
    )
    store.save_order_intent(intent)
    store.log_event(
        "propose",
        {"intent_id": intent.intent_id, "symbol": intent.symbol, "side": intent.side},
    )

    return {
        "intent_id": intent.intent_id,
        "hash": intent.hash(),
        "side": intent.side,
        "size": intent.size,
        "price": intent.price,
        "strategy": intent.strategy,
        "confidence": intent.confidence,
        "rationale": intent.rationale,
        "features_ref": candidate.features_ref,
        "expires_at": intent.expires_at,
    }


def propose(settings: AppSettings, store: SQLiteStore, params: ProposeParams) -> dict[str, Any]:
    candidate = prepare_proposal(settings, store, params)
    return finalize_proposal(settings, store, candidate, params)

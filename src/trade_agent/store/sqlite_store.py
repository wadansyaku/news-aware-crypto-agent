from __future__ import annotations

import sqlite3
from typing import Any, Iterable, Sequence

from trade_agent import db, metrics
from trade_agent.intent import OrderIntent
from trade_agent.schemas import (
    ApprovalRecord,
    ExecutionRecord,
    FeatureRow,
    FillRecord,
    NewsItem,
    ReportRecord,
    ensure_utc_iso,
    sha256_hex,
    utc_now_iso,
)


class SQLiteStore:
    def __init__(self, db_path: str) -> None:
        self.conn = db.connect(db_path)
        db.init_db(self.conn)

    def close(self) -> None:
        self.conn.close()

    def save_candles(
        self, symbol: str, timeframe: str, candles: Iterable[list[Any]], source: str
    ) -> int:
        return db.insert_candles(self.conn, symbol, timeframe, candles, source=source)

    def fetch_candles(
        self, symbol: str, timeframe: str, limit: int, since_ts: int | None = None
    ) -> list[sqlite3.Row]:
        return db.fetch_candles(self.conn, symbol, timeframe, limit, since_ts=since_ts)

    def list_candles_between(
        self, symbol: str, timeframe: str, start_ts: int, end_ts: int
    ) -> list[sqlite3.Row]:
        return db.list_candles_between(self.conn, symbol, timeframe, start_ts, end_ts)

    def list_feature_rows_between(
        self, symbol: str, start_ts: int, end_ts: int, feature_version: str = "news_v1"
    ) -> list[sqlite3.Row]:
        return db.list_feature_rows_between(
            self.conn, symbol, start_ts, end_ts, feature_version=feature_version
        )

    def get_latest_candle_ts(self, symbol: str, timeframe: str) -> int | None:
        return db.get_latest_candle_ts(self.conn, symbol, timeframe)

    def save_orderbook_snapshot(
        self,
        symbol: str,
        bid: float,
        ask: float,
        bid_size: float,
        ask_size: float,
        ts: int,
    ) -> None:
        db.insert_orderbook_snapshot(self.conn, symbol, bid, ask, bid_size, ask_size, ts)

    def get_latest_orderbook_snapshot(self, symbol: str) -> sqlite3.Row | None:
        return db.get_latest_orderbook_snapshot(self.conn, symbol)

    def save_news_item(self, item: NewsItem) -> int | None:
        published_at = ensure_utc_iso(item.published_at)
        observed_at = ensure_utc_iso(item.observed_at, default_to_now=True)
        if published_at is None:
            published_at = observed_at
        return db.insert_news_article(
            self.conn,
            url=item.source_url,
            title=item.title,
            source=item.source_name,
            published_at=published_at,
            title_hash=item.title_hash,
            guid=item.guid,
            summary=item.summary,
            observed_at=observed_at,
            raw_payload_hash=item.raw_payload_hash,
        )

    def save_news_items(self, items: Sequence[NewsItem]) -> int:
        inserted = 0
        for item in items:
            if self.save_news_item(item) is not None:
                inserted += 1
        return inserted

    def list_articles_without_features(
        self, limit: int = 200, feature_version: str = "news_v1"
    ) -> list[sqlite3.Row]:
        return db.list_articles_without_features(self.conn, limit=limit, feature_version=feature_version)

    def save_news_features(
        self,
        article_id: int,
        sentiment: float,
        keyword_flags: dict[str, bool],
        source_weight: float,
        language: str,
        feature_version: str = "news_v1",
    ) -> None:
        db.insert_news_features(
            self.conn,
            article_id=article_id,
            sentiment=sentiment,
            keyword_flags=keyword_flags,
            source_weight=source_weight,
            language=language,
            feature_version=feature_version,
        )

    def list_news_features_window(
        self, start_iso: str, end_iso: str, observed_cutoff: str, limit: int = 500
    ) -> list[dict[str, Any]]:
        rows = db.list_news_features_window(
            self.conn,
            start_iso=start_iso,
            end_iso=end_iso,
            observed_cutoff=observed_cutoff,
            limit=limit,
        )
        return [
            {
                "sentiment": float(row["sentiment"]),
                "source_weight": float(row["source_weight"]),
                "published_at": row["published_at"],
                "observed_at": row["observed_at"],
            }
            for row in rows
        ]

    def list_news_items_between(self, start_iso: str, end_iso: str) -> list[sqlite3.Row]:
        return db.list_news_items_between(self.conn, start_iso, end_iso)

    def list_latest_news_with_features(self, limit: int = 50) -> list[sqlite3.Row]:
        return db.list_latest_news_with_features(self.conn, limit=limit)

    def list_news_features_since(self, since_iso: str) -> list[sqlite3.Row]:
        return db.list_news_features_since(self.conn, since_iso)

    def save_feature_row(self, row: FeatureRow) -> int:
        return db.insert_feature_row(self.conn, row)

    def save_order_intent(self, intent: OrderIntent) -> bool:
        return db.insert_order_intent(self.conn, intent.as_record())

    def update_order_intent_status(self, intent_id: str, status: str) -> None:
        db.update_order_intent_status(self.conn, intent_id, status)

    def get_order_intent(self, intent_id: str) -> sqlite3.Row | None:
        return db.get_order_intent(self.conn, intent_id)

    def get_latest_intent(self, status: str = "proposed") -> sqlite3.Row | None:
        return db.get_latest_intent(self.conn, status=status)

    def save_approval(self, record: ApprovalRecord) -> None:
        db.insert_approval(
            self.conn,
            intent_id=record.intent_id,
            intent_hash=record.intent_hash,
            approval_phrase_hash=record.approval_phrase_hash,
            approved_by=record.approved_by,
            approved_at=record.approved_at,
        )

    def save_approval_phrase(
        self, intent_id: str, intent_hash: str, phrase: str, approved_by: str
    ) -> None:
        phrase_hash = sha256_hex(phrase)
        record = ApprovalRecord(
            intent_id=intent_id,
            intent_hash=intent_hash,
            approved_at=utc_now_iso(),
            approved_by=approved_by,
            approval_phrase_hash=phrase_hash,
        )
        self.save_approval(record)

    def get_approval(self, intent_id: str) -> sqlite3.Row | None:
        return db.get_approval(self.conn, intent_id)

    def save_execution(self, record: ExecutionRecord) -> None:
        db.insert_execution(
            self.conn,
            exec_id=record.exec_id,
            intent_id=record.intent_id,
            intent_hash=record.intent_hash,
            mode=record.mode,
            status=record.status,
            details=record.details,
            fee=record.fee,
            slippage_model=record.slippage_model,
            executed_at=record.executed_at,
        )

    def save_order(
        self,
        order_id: str,
        exec_id: str,
        intent_id: str,
        created_at: str,
        mode: str,
        symbol: str,
        side: str,
        order_type: str,
        time_in_force: str,
        size: float,
        price: float,
        status: str,
        raw: dict[str, Any],
    ) -> None:
        db.insert_order(
            self.conn,
            order_id=order_id,
            exec_id=exec_id,
            intent_id=intent_id,
            created_at=created_at,
            mode=mode,
            symbol=symbol,
            side=side,
            order_type=order_type,
            time_in_force=time_in_force,
            size=size,
            price=price,
            status=status,
            raw=raw,
        )

    def save_fill(self, record: FillRecord) -> None:
        db.insert_fill(
            self.conn,
            fill_id=record.fill_id,
            exec_id=record.exec_id,
            symbol=record.symbol,
            side=record.side,
            size=record.size,
            price=record.price,
            fee=record.fee,
            fee_currency=record.fee_currency,
            ts=record.ts,
        )

    def save_trade_result(
        self, trade_id: str, intent_id: str, pnl_jpy: float, mode: str, meta: dict[str, Any]
    ) -> None:
        db.insert_trade_result(self.conn, trade_id, intent_id, pnl_jpy, mode, meta)

    def save_report_record(self, record: ReportRecord) -> None:
        db.insert_report(self.conn, record)

    def log_event(self, event: str, data: dict[str, Any]) -> None:
        db.log_event(self.conn, event, data)

    def list_audit_logs(
        self, event: str | None = None, limit: int = 100
    ) -> list[sqlite3.Row]:
        return db.list_audit_logs(self.conn, event=event, limit=limit)

    def get_daily_pnl(self, day: str) -> float:
        return db.get_daily_pnl(self.conn, day)

    def get_daily_execution_count(self, day: str) -> int:
        return db.get_daily_execution_count(self.conn, day)

    def get_last_execution_time(self) -> str | None:
        return db.get_last_execution_time(self.conn)

    def get_position_size(self, symbol: str) -> float:
        return db.get_position_size(self.conn, symbol)

    def get_position_state(self, symbol: str) -> tuple[float, float]:
        return db.get_position_state(self.conn, symbol)

    def get_position_open_time(self, symbol: str) -> str | None:
        return db.get_position_open_time(self.conn, symbol)

    def get_latest_candle(self, symbol: str, timeframe: str) -> sqlite3.Row | None:
        return db.get_latest_candle(self.conn, symbol, timeframe)

    def list_recent_candles(
        self, symbol: str, timeframe: str, limit: int = 2
    ) -> list[sqlite3.Row]:
        return db.list_recent_candles(self.conn, symbol, timeframe, limit=limit)

    def list_fills(self, symbol: str | None = None, limit: int = 1000) -> list[sqlite3.Row]:
        return db.list_fills(self.conn, symbol=symbol, limit=limit)

    def save_external_balance(
        self,
        exchange: str,
        currency: str,
        total: float,
        free: float,
        used: float,
        ts: str,
        raw_json: str,
    ) -> None:
        db.insert_external_balance(
            self.conn,
            exchange=exchange,
            currency=currency,
            total=total,
            free=free,
            used=used,
            ts=ts,
            raw_json=raw_json,
        )

    def save_external_trade(
        self,
        trade_uid: str,
        exchange: str,
        trade_id: str | None,
        symbol: str,
        side: str,
        price: float,
        amount: float,
        cost: float,
        fee: float,
        fee_currency: str,
        ts: str,
        raw_json: str,
    ) -> bool:
        return db.insert_external_trade(
            self.conn,
            trade_uid=trade_uid,
            exchange=exchange,
            trade_id=trade_id,
            symbol=symbol,
            side=side,
            price=price,
            amount=amount,
            cost=cost,
            fee=fee,
            fee_currency=fee_currency,
            ts=ts,
            raw_json=raw_json,
        )

    def list_external_trades_between(
        self,
        exchange: str,
        start_iso: str | None,
        end_iso: str | None,
        symbol: str | None = None,
    ) -> list[sqlite3.Row]:
        return db.list_external_trades_between(
            self.conn,
            exchange=exchange,
            start_iso=start_iso,
            end_iso=end_iso,
            symbol=symbol,
        )

    def get_latest_external_trade_ts(
        self, exchange: str, symbol: str | None = None
    ) -> str | None:
        return db.get_latest_external_trade_ts(self.conn, exchange=exchange, symbol=symbol)

    def list_latest_external_balances(self, exchange: str) -> list[sqlite3.Row]:
        return db.list_latest_external_balances(self.conn, exchange=exchange)

    def load_trades(self, mode: str | None = None) -> list[dict[str, Any]]:
        return metrics.load_trades_from_db(self.conn, mode)

    def load_trade_details(self, mode: str | None = None) -> list[dict[str, Any]]:
        return metrics.load_trade_details_from_db(self.conn, mode)

    def list_intents(self, limit: int = 20) -> list[dict[str, Any]]:
        cur = self.conn.execute(
            """
            SELECT intent_id, created_at, status, symbol, side, size, price, strategy, confidence
            FROM order_intents
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]

    def list_reports(self, limit: int = 50) -> list[sqlite3.Row]:
        return db.list_reports(self.conn, limit=limit)

    def create_alert(
        self, symbol: str, condition: str, threshold: float, created_at: str | None = None
    ) -> int:
        return db.insert_alert(self.conn, symbol, condition, threshold, created_at=created_at)

    def list_alerts(self, enabled_only: bool = False) -> list[sqlite3.Row]:
        return db.list_alerts(self.conn, enabled_only=enabled_only)

    def delete_alert(self, alert_id: int) -> None:
        db.delete_alert(self.conn, alert_id)

    def update_alert_triggered(
        self, alert_id: int, triggered_at: str | None = None, enabled: int = 0
    ) -> None:
        db.update_alert_triggered(self.conn, alert_id, triggered_at=triggered_at, enabled=enabled)

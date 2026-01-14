from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable

from trade_agent.schemas import FeatureRow, ReportRecord


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row["name"] == column for row in cur.fetchall())


def _ensure_column(
    conn: sqlite3.Connection, table: str, column: str, definition: str, default: Any | None = None
) -> None:
    if _column_exists(conn, table, column):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    if default is not None:
        conn.execute(
            f"UPDATE {table} SET {column} = ? WHERE {column} IS NULL OR {column} = ''",
            (default,),
    )


def _ensure_index(
    conn: sqlite3.Connection,
    name: str,
    table: str,
    columns: str,
    unique: bool = False,
    required_columns: list[str] | None = None,
) -> None:
    if required_columns:
        for col in required_columns:
            if not _column_exists(conn, table, col):
                return
    kind = "UNIQUE INDEX" if unique else "INDEX"
    conn.execute(f"CREATE {kind} IF NOT EXISTS {name} ON {table}({columns})")


def _iso_day(ts: str) -> str:
    if len(ts) >= 10:
        return ts[:10]
    try:
        return datetime.fromisoformat(ts).date().isoformat()
    except ValueError:
        return datetime.now(timezone.utc).date().isoformat()


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS candles (
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            ts INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            source TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            PRIMARY KEY (symbol, timeframe, ts)
        );

        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            symbol TEXT NOT NULL,
            ts INTEGER NOT NULL,
            bid REAL NOT NULL,
            ask REAL NOT NULL,
            bid_size REAL NOT NULL,
            ask_size REAL NOT NULL,
            ingested_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS news_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            source TEXT NOT NULL,
            guid TEXT,
            summary TEXT,
            published_at TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            raw_payload_hash TEXT,
            title_hash TEXT NOT NULL,
            UNIQUE (url),
            UNIQUE (title_hash)
        );

        CREATE TABLE IF NOT EXISTS news_features (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER NOT NULL,
            sentiment REAL NOT NULL,
            keyword_flags TEXT NOT NULL,
            source_weight REAL NOT NULL,
            language TEXT NOT NULL,
            feature_version TEXT NOT NULL,
            extracted_at TEXT NOT NULL,
            FOREIGN KEY (article_id) REFERENCES news_articles(id)
        );

        CREATE TABLE IF NOT EXISTS order_intents (
            intent_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            intent_json TEXT NOT NULL,
            intent_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            time_in_force TEXT NOT NULL,
            size REAL NOT NULL,
            price REAL NOT NULL,
            confidence REAL NOT NULL,
            rationale TEXT NOT NULL,
            rationale_features_ref TEXT,
            mode TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS approvals (
            intent_id TEXT PRIMARY KEY,
            intent_hash TEXT NOT NULL,
            approved_at TEXT NOT NULL,
            approved_by TEXT NOT NULL,
            approval_phrase_hash TEXT NOT NULL,
            approval_phrase TEXT NOT NULL,
            FOREIGN KEY (intent_id) REFERENCES order_intents(intent_id)
        );

        CREATE TABLE IF NOT EXISTS executions (
            exec_id TEXT PRIMARY KEY,
            intent_id TEXT NOT NULL,
            intent_hash TEXT NOT NULL,
            executed_at TEXT NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            fee REAL NOT NULL,
            slippage_model TEXT NOT NULL,
            details_json TEXT NOT NULL,
            FOREIGN KEY (intent_id) REFERENCES order_intents(intent_id)
        );

        CREATE TABLE IF NOT EXISTS fills (
            fill_id TEXT PRIMARY KEY,
            exec_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            size REAL NOT NULL,
            price REAL NOT NULL,
            fee REAL NOT NULL,
            fee_currency TEXT NOT NULL,
            ts TEXT NOT NULL,
            FOREIGN KEY (exec_id) REFERENCES executions(exec_id)
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            exec_id TEXT NOT NULL,
            intent_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            mode TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            time_in_force TEXT NOT NULL,
            size REAL NOT NULL,
            price REAL NOT NULL,
            status TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            FOREIGN KEY (intent_id) REFERENCES order_intents(intent_id)
        );

        CREATE TABLE IF NOT EXISTS trade_results (
            trade_id TEXT PRIMARY KEY,
            intent_id TEXT NOT NULL,
            pnl_jpy REAL NOT NULL,
            created_at TEXT NOT NULL,
            mode TEXT NOT NULL,
            meta_json TEXT NOT NULL,
            FOREIGN KEY (intent_id) REFERENCES order_intents(intent_id)
        );

        CREATE TABLE IF NOT EXISTS daily_stats (
            day TEXT PRIMARY KEY,
            orders_count INTEGER NOT NULL,
            realized_pnl REAL NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS feature_rows (
            symbol TEXT NOT NULL,
            ts INTEGER NOT NULL,
            feature_version TEXT NOT NULL,
            features_json TEXT NOT NULL,
            computed_at TEXT NOT NULL,
            news_window_start TEXT NOT NULL,
            news_window_end TEXT NOT NULL,
            PRIMARY KEY (symbol, ts, feature_version)
        );

        CREATE TABLE IF NOT EXISTS reports (
            run_id TEXT PRIMARY KEY,
            period TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            equity_curve_path TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            event TEXT NOT NULL,
            data_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            condition TEXT NOT NULL,
            threshold REAL NOT NULL,
            enabled INTEGER DEFAULT 1,
            triggered_at TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS external_balances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT NOT NULL,
            currency TEXT NOT NULL,
            total REAL NOT NULL,
            free REAL NOT NULL,
            used REAL NOT NULL,
            ts TEXT NOT NULL,
            raw_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS external_trades (
            trade_uid TEXT PRIMARY KEY,
            exchange TEXT NOT NULL,
            trade_id TEXT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            amount REAL NOT NULL,
            cost REAL NOT NULL,
            fee REAL NOT NULL,
            fee_currency TEXT NOT NULL,
            ts TEXT NOT NULL,
            raw_json TEXT NOT NULL
        );
        """
    )

    _ensure_column(conn, "candles", "source", "TEXT NOT NULL DEFAULT 'exchange'", "exchange")
    _ensure_column(conn, "news_articles", "guid", "TEXT")
    _ensure_column(conn, "news_articles", "summary", "TEXT")
    _ensure_column(conn, "news_articles", "observed_at", "TEXT NOT NULL DEFAULT ''", utc_now_iso())
    _ensure_column(conn, "news_articles", "raw_payload_hash", "TEXT")
    conn.execute(
        """
        UPDATE news_articles
        SET observed_at = ingested_at
        WHERE observed_at IS NULL OR observed_at = ''
        """
    )
    _ensure_column(
        conn, "news_features", "feature_version", "TEXT NOT NULL DEFAULT 'news_v1'", "news_v1"
    )
    _ensure_column(conn, "order_intents", "order_type", "TEXT NOT NULL DEFAULT 'limit'", "limit")
    _ensure_column(conn, "order_intents", "time_in_force", "TEXT NOT NULL DEFAULT 'GTC'", "GTC")
    _ensure_column(conn, "order_intents", "rationale_features_ref", "TEXT")
    _ensure_column(conn, "approvals", "approved_by", "TEXT NOT NULL DEFAULT 'local'", "local")
    _ensure_column(conn, "approvals", "approval_phrase_hash", "TEXT NOT NULL DEFAULT ''", "")
    _ensure_column(conn, "executions", "fee", "REAL NOT NULL DEFAULT 0", 0.0)
    _ensure_column(conn, "executions", "slippage_model", "TEXT NOT NULL DEFAULT ''", "")

    _ensure_index(conn, "idx_candles_symbol_timeframe_ts", "candles", "symbol, timeframe, ts")
    _ensure_index(conn, "idx_news_published_at", "news_articles", "published_at")
    _ensure_index(
        conn,
        "idx_news_observed_at",
        "news_articles",
        "observed_at",
        required_columns=["observed_at"],
    )
    _ensure_index(
        conn,
        "idx_news_features_article_version",
        "news_features",
        "article_id, feature_version",
        unique=True,
        required_columns=["feature_version"],
    )
    _ensure_index(conn, "idx_feature_rows_symbol_ts", "feature_rows", "symbol, ts")
    _ensure_index(conn, "idx_executions_intent_id", "executions", "intent_id")
    _ensure_index(conn, "idx_fills_symbol", "fills", "symbol")
    _ensure_index(conn, "idx_orders_intent_id", "orders", "intent_id")
    _ensure_index(conn, "idx_daily_stats_day", "daily_stats", "day", unique=True)
    _ensure_index(conn, "idx_alerts_symbol", "alerts", "symbol")
    _ensure_index(conn, "idx_external_trades_symbol_ts", "external_trades", "symbol, ts")
    _ensure_index(conn, "idx_external_trades_ts", "external_trades", "ts")
    _ensure_index(conn, "idx_external_balances_exchange_ts", "external_balances", "exchange, ts")
    conn.commit()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def insert_candles(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    candles: Iterable[list[Any]],
    source: str = "exchange",
) -> int:
    ingested_at = utc_now_iso()
    before = conn.total_changes
    rows = [
        (
            symbol,
            timeframe,
            int(c[0]),
            float(c[1]),
            float(c[2]),
            float(c[3]),
            float(c[4]),
            float(c[5]),
            source,
            ingested_at,
        )
        for c in candles
    ]
    conn.executemany(
        """
        INSERT OR IGNORE INTO candles
        (symbol, timeframe, ts, open, high, low, close, volume, source, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return conn.total_changes - before


def fetch_candles(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    limit: int,
    since_ts: int | None = None,
) -> list[sqlite3.Row]:
    query = (
        "SELECT * FROM candles WHERE symbol = ? AND timeframe = ? "
        + ("AND ts >= ? " if since_ts is not None else "")
        + "ORDER BY ts ASC LIMIT ?"
    )
    params: list[Any] = [symbol, timeframe]
    if since_ts is not None:
        params.append(since_ts)
    params.append(limit)
    cur = conn.execute(query, params)
    return cur.fetchall()


def insert_orderbook_snapshot(
    conn: sqlite3.Connection,
    symbol: str,
    bid: float,
    ask: float,
    bid_size: float,
    ask_size: float,
    ts: int,
) -> None:
    conn.execute(
        """
        INSERT INTO orderbook_snapshots
        (symbol, ts, bid, ask, bid_size, ask_size, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (symbol, ts, bid, ask, bid_size, ask_size, utc_now_iso()),
    )
    conn.commit()


def insert_news_article(
    conn: sqlite3.Connection,
    url: str,
    title: str,
    source: str,
    published_at: str,
    title_hash: str,
    guid: str | None = None,
    summary: str | None = None,
    observed_at: str | None = None,
    raw_payload_hash: str | None = None,
) -> int | None:
    observed_at = observed_at or utc_now_iso()
    try:
        cur = conn.execute(
            """
            INSERT INTO news_articles
            (url, title, source, guid, summary, published_at, observed_at, ingested_at, raw_payload_hash, title_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                url,
                title,
                source,
                guid,
                summary,
                published_at,
                observed_at,
                observed_at,
                raw_payload_hash,
                title_hash,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    except sqlite3.IntegrityError:
        return None


def insert_news_features(
    conn: sqlite3.Connection,
    article_id: int,
    sentiment: float,
    keyword_flags: dict[str, bool],
    source_weight: float,
    language: str,
    feature_version: str = "news_v1",
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO news_features
        (article_id, sentiment, keyword_flags, source_weight, language, feature_version, extracted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            article_id,
            sentiment,
            json.dumps(keyword_flags, separators=(",", ":"), sort_keys=True),
            source_weight,
            language,
            feature_version,
            utc_now_iso(),
        ),
    )
    conn.commit()


def insert_feature_row(conn: sqlite3.Connection, row: FeatureRow) -> int:
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO feature_rows
        (symbol, ts, feature_version, features_json, computed_at, news_window_start, news_window_end)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row.symbol,
            row.ts,
            row.feature_version,
            json.dumps(row.features, separators=(",", ":"), sort_keys=True),
            row.computed_at,
            row.news_window_start,
            row.news_window_end,
        ),
    )
    conn.commit()
    return conn.total_changes - before


def insert_order_intent(conn: sqlite3.Connection, intent: dict[str, Any]) -> bool:
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO order_intents
        (intent_id, created_at, intent_json, intent_hash, status, expires_at, strategy,
         symbol, side, order_type, time_in_force, size, price, confidence, rationale,
         rationale_features_ref, mode)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            intent["intent_id"],
            intent["created_at"],
            intent["intent_json"],
            intent["intent_hash"],
            intent["status"],
            intent["expires_at"],
            intent["strategy"],
            intent["symbol"],
            intent["side"],
            intent.get("order_type", "limit"),
            intent.get("time_in_force", "GTC"),
            intent["size"],
            intent["price"],
            intent["confidence"],
            intent["rationale"],
            intent.get("rationale_features_ref"),
            intent["mode"],
        ),
    )
    conn.commit()
    return (conn.total_changes - before) > 0


def update_order_intent_status(conn: sqlite3.Connection, intent_id: str, status: str) -> None:
    conn.execute(
        "UPDATE order_intents SET status = ? WHERE intent_id = ?",
        (status, intent_id),
    )
    conn.commit()


def get_order_intent(conn: sqlite3.Connection, intent_id: str) -> sqlite3.Row | None:
    cur = conn.execute("SELECT * FROM order_intents WHERE intent_id = ?", (intent_id,))
    return cur.fetchone()


def get_latest_intent(conn: sqlite3.Connection, status: str = "proposed") -> sqlite3.Row | None:
    cur = conn.execute(
        "SELECT * FROM order_intents WHERE status = ? ORDER BY created_at DESC LIMIT 1",
        (status,),
    )
    return cur.fetchone()


def insert_approval(
    conn: sqlite3.Connection,
    intent_id: str,
    intent_hash: str,
    approval_phrase_hash: str,
    approved_by: str,
    approved_at: str | None = None,
    approval_phrase: str | None = None,
) -> None:
    approved_at = approved_at or utc_now_iso()
    if approval_phrase is None:
        approval_phrase = approval_phrase_hash
    conn.execute(
        """
        INSERT OR REPLACE INTO approvals
        (intent_id, intent_hash, approved_at, approved_by, approval_phrase_hash, approval_phrase)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (intent_id, intent_hash, approved_at, approved_by, approval_phrase_hash, approval_phrase),
    )
    conn.commit()


def get_approval(conn: sqlite3.Connection, intent_id: str) -> sqlite3.Row | None:
    cur = conn.execute("SELECT * FROM approvals WHERE intent_id = ?", (intent_id,))
    return cur.fetchone()


def insert_execution(
    conn: sqlite3.Connection,
    exec_id: str,
    intent_id: str,
    intent_hash: str,
    mode: str,
    status: str,
    details: dict[str, Any],
    fee: float = 0.0,
    slippage_model: str = "",
    executed_at: str | None = None,
) -> None:
    executed_at = executed_at or utc_now_iso()
    conn.execute(
        """
        INSERT INTO executions
        (exec_id, intent_id, intent_hash, executed_at, mode, status, fee, slippage_model, details_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            exec_id,
            intent_id,
            intent_hash,
            executed_at,
            mode,
            status,
            fee,
            slippage_model,
            json.dumps(details, separators=(",", ":"), sort_keys=True),
        ),
    )
    conn.commit()
    upsert_daily_stats(conn, day=_iso_day(executed_at), orders_delta=1, realized_delta=0.0)


def insert_fill(
    conn: sqlite3.Connection,
    fill_id: str,
    exec_id: str,
    symbol: str,
    side: str,
    size: float,
    price: float,
    fee: float,
    fee_currency: str,
    ts: str,
) -> None:
    conn.execute(
        """
        INSERT INTO fills
        (fill_id, exec_id, symbol, side, size, price, fee, fee_currency, ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (fill_id, exec_id, symbol, side, size, price, fee, fee_currency, ts),
    )
    conn.commit()


def insert_trade_result(
    conn: sqlite3.Connection,
    trade_id: str,
    intent_id: str,
    pnl_jpy: float,
    mode: str,
    meta: dict[str, Any],
) -> None:
    created_at = utc_now_iso()
    conn.execute(
        """
        INSERT INTO trade_results
        (trade_id, intent_id, pnl_jpy, created_at, mode, meta_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id,
            intent_id,
            pnl_jpy,
            created_at,
            mode,
            json.dumps(meta, separators=(",", ":"), sort_keys=True),
        ),
    )
    conn.commit()
    upsert_daily_stats(conn, day=_iso_day(created_at), orders_delta=0, realized_delta=pnl_jpy)


def insert_order(
    conn: sqlite3.Connection,
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
    conn.execute(
        """
        INSERT OR REPLACE INTO orders
        (order_id, exec_id, intent_id, created_at, mode, symbol, side, order_type,
         time_in_force, size, price, status, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            order_id,
            exec_id,
            intent_id,
            created_at,
            mode,
            symbol,
            side,
            order_type,
            time_in_force,
            size,
            price,
            status,
            json.dumps(raw, separators=(",", ":"), sort_keys=True),
        ),
    )
    conn.commit()


def upsert_daily_stats(
    conn: sqlite3.Connection, day: str, orders_delta: int, realized_delta: float
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO daily_stats (day, orders_count, realized_pnl, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(day) DO UPDATE SET
            orders_count = orders_count + ?,
            realized_pnl = realized_pnl + ?,
            updated_at = ?
        """,
        (
            day,
            orders_delta,
            realized_delta,
            now,
            orders_delta,
            realized_delta,
            now,
        ),
    )
    conn.commit()


def insert_report(conn: sqlite3.Connection, record: ReportRecord) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO reports
        (run_id, period, metrics_json, equity_curve_path, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            record.run_id,
            record.period,
            json.dumps(record.metrics, separators=(",", ":"), sort_keys=True),
            record.equity_curve_path,
            record.created_at,
        ),
    )
    conn.commit()


def log_event(conn: sqlite3.Connection, event: str, data: dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO audit_logs (ts, event, data_json) VALUES (?, ?, ?)",
        (
            utc_now_iso(),
            event,
            json.dumps(data, separators=(",", ":"), sort_keys=True),
        ),
    )
    conn.commit()


def list_audit_logs(
    conn: sqlite3.Connection, event: str | None = None, limit: int = 100
) -> list[sqlite3.Row]:
    query = "SELECT * FROM audit_logs"
    params: list[Any] = []
    if event:
        query += " WHERE event = ?"
        params.append(event)
    query += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    cur = conn.execute(query, params)
    return cur.fetchall()


def insert_alert(
    conn: sqlite3.Connection,
    symbol: str,
    condition: str,
    threshold: float,
    created_at: str | None = None,
) -> int:
    created_at = created_at or utc_now_iso()
    cur = conn.execute(
        """
        INSERT INTO alerts (symbol, condition, threshold, enabled, created_at)
        VALUES (?, ?, ?, 1, ?)
        """,
        (symbol, condition, threshold, created_at),
    )
    conn.commit()
    return int(cur.lastrowid)


def list_alerts(conn: sqlite3.Connection, enabled_only: bool = False) -> list[sqlite3.Row]:
    query = "SELECT * FROM alerts"
    params: list[Any] = []
    if enabled_only:
        query += " WHERE enabled = 1"
    query += " ORDER BY created_at DESC"
    cur = conn.execute(query, params)
    return cur.fetchall()


def delete_alert(conn: sqlite3.Connection, alert_id: int) -> None:
    conn.execute("DELETE FROM alerts WHERE id = ?", (alert_id,))
    conn.commit()


def update_alert_triggered(
    conn: sqlite3.Connection, alert_id: int, triggered_at: str | None = None, enabled: int = 0
) -> None:
    triggered_at = triggered_at or utc_now_iso()
    conn.execute(
        "UPDATE alerts SET triggered_at = ?, enabled = ? WHERE id = ?",
        (triggered_at, enabled, alert_id),
    )
    conn.commit()


def get_daily_execution_count(conn: sqlite3.Connection, day: str) -> int:
    cur = conn.execute(
        """
        SELECT COUNT(*) as cnt FROM executions
        WHERE executed_at LIKE ?
        """,
        (f"{day}%",),
    )
    row = cur.fetchone()
    return int(row["cnt"]) if row else 0


def get_last_execution_time(conn: sqlite3.Connection) -> str | None:
    cur = conn.execute(
        "SELECT executed_at FROM executions ORDER BY executed_at DESC LIMIT 1"
    )
    row = cur.fetchone()
    return str(row["executed_at"]) if row else None


def get_position_size(conn: sqlite3.Connection, symbol: str) -> float:
    cur = conn.execute("SELECT side, size FROM fills WHERE symbol = ?", (symbol,))
    position = 0.0
    for row in cur.fetchall():
        if row["side"] == "buy":
            position += float(row["size"])
        elif row["side"] == "sell":
            position -= float(row["size"])
    return position


def get_position_state(conn: sqlite3.Connection, symbol: str) -> tuple[float, float]:
    cur = conn.execute(
        "SELECT side, size, price, fee FROM fills WHERE symbol = ? ORDER BY ts ASC",
        (symbol,),
    )
    size = 0.0
    cost_total = 0.0
    for row in cur.fetchall():
        fill_size = float(row["size"])
        fill_price = float(row["price"])
        fee = float(row["fee"])
        if row["side"] == "buy":
            cost_total += fill_price * fill_size + fee
            size += fill_size
        else:
            if size <= 0:
                continue
            avg_cost = cost_total / size if size > 0 else 0.0
            cost_total -= avg_cost * fill_size
            size -= fill_size
    avg_cost = cost_total / size if size > 0 else 0.0
    return size, avg_cost


def get_latest_orderbook_snapshot(conn: sqlite3.Connection, symbol: str) -> sqlite3.Row | None:
    cur = conn.execute(
        """
        SELECT * FROM orderbook_snapshots
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (symbol,),
    )
    return cur.fetchone()


def get_daily_pnl(conn: sqlite3.Connection, day: str) -> float:
    cur = conn.execute(
        """
        SELECT COALESCE(SUM(pnl_jpy), 0) as total FROM trade_results
        WHERE created_at LIKE ?
        """,
        (f"{day}%",),
    )
    row = cur.fetchone()
    return float(row["total"]) if row else 0.0


def list_news_features(
    conn: sqlite3.Connection, published_before: str, limit: int = 200
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT nf.*, na.published_at, na.observed_at FROM news_features nf
        JOIN news_articles na ON nf.article_id = na.id
        WHERE na.published_at <= ?
        ORDER BY na.published_at DESC
        LIMIT ?
        """,
        (published_before, limit),
    )
    return cur.fetchall()


def list_latest_news_with_features(conn: sqlite3.Connection, limit: int = 50) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT na.id, na.title, na.source, na.url, na.published_at, na.observed_at,
               nf.sentiment, nf.keyword_flags, nf.source_weight, nf.language
        FROM news_articles na
        LEFT JOIN news_features nf ON nf.article_id = na.id
        ORDER BY na.observed_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


def list_news_features_since(conn: sqlite3.Connection, since_iso: str) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT nf.sentiment, nf.source_weight, na.observed_at
        FROM news_features nf
        JOIN news_articles na ON nf.article_id = na.id
        WHERE na.observed_at >= ?
        ORDER BY na.observed_at ASC
        """,
        (since_iso,),
    )
    return cur.fetchall()


def list_reports(conn: sqlite3.Connection, limit: int = 50) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT run_id, period, metrics_json, equity_curve_path, created_at
        FROM reports
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


def get_latest_candle(conn: sqlite3.Connection, symbol: str, timeframe: str) -> sqlite3.Row | None:
    cur = conn.execute(
        """
        SELECT ts, close FROM candles
        WHERE symbol = ? AND timeframe = ?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (symbol, timeframe),
    )
    return cur.fetchone()


def list_recent_candles(
    conn: sqlite3.Connection, symbol: str, timeframe: str, limit: int = 2
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT ts, close FROM candles
        WHERE symbol = ? AND timeframe = ?
        ORDER BY ts DESC
        LIMIT ?
        """,
        (symbol, timeframe, limit),
    )
    return cur.fetchall()


def get_position_open_time(conn: sqlite3.Connection, symbol: str) -> str | None:
    cur = conn.execute(
        """
        SELECT side, size, ts FROM fills
        WHERE symbol = ?
        ORDER BY ts ASC
        """,
        (symbol,),
    )
    size = 0.0
    open_ts: str | None = None
    for row in cur.fetchall():
        side = row["side"]
        fill_size = float(row["size"])
        if side == "buy":
            if size <= 0:
                open_ts = row["ts"]
            size += fill_size
        elif side == "sell":
            size -= fill_size
            if size <= 0:
                size = 0.0
                open_ts = None
    return open_ts


def list_news_features_window(
    conn: sqlite3.Connection,
    start_iso: str,
    end_iso: str,
    observed_cutoff: str,
    limit: int = 500,
    feature_version: str = "news_v1",
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT nf.sentiment, nf.source_weight, na.published_at, na.observed_at
        FROM news_features nf
        JOIN news_articles na ON nf.article_id = na.id
        WHERE na.published_at >= ? AND na.published_at <= ?
          AND na.observed_at <= ?
          AND nf.feature_version = ?
        ORDER BY na.published_at ASC
        LIMIT ?
        """,
        (start_iso, end_iso, observed_cutoff, feature_version, limit),
    )
    return cur.fetchall()


def list_news_features_since(
    conn: sqlite3.Connection, published_after: str
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT nf.*, na.published_at, na.observed_at FROM news_features nf
        JOIN news_articles na ON nf.article_id = na.id
        WHERE na.published_at >= ?
        ORDER BY na.published_at ASC
        """,
        (published_after,),
    )
    return cur.fetchall()


def list_news_items_between(
    conn: sqlite3.Connection, start_iso: str, end_iso: str
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT * FROM news_articles
        WHERE published_at >= ? AND published_at <= ?
        ORDER BY published_at ASC
        """,
        (start_iso, end_iso),
    )
    return cur.fetchall()


def list_candles_between(
    conn: sqlite3.Connection, symbol: str, timeframe: str, start_ts: int, end_ts: int
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT * FROM candles
        WHERE symbol = ? AND timeframe = ? AND ts >= ? AND ts <= ?
        ORDER BY ts ASC
        """,
        (symbol, timeframe, start_ts, end_ts),
    )
    return cur.fetchall()


def list_feature_rows_between(
    conn: sqlite3.Connection,
    symbol: str,
    start_ts: int,
    end_ts: int,
    feature_version: str = "news_v1",
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT * FROM feature_rows
        WHERE symbol = ? AND feature_version = ? AND ts >= ? AND ts <= ?
        ORDER BY ts ASC
        """,
        (symbol, feature_version, start_ts, end_ts),
    )
    return cur.fetchall()


def get_latest_candle_ts(conn: sqlite3.Connection, symbol: str, timeframe: str) -> int | None:
    cur = conn.execute(
        "SELECT MAX(ts) as max_ts FROM candles WHERE symbol = ? AND timeframe = ?",
        (symbol, timeframe),
    )
    row = cur.fetchone()
    if row and row["max_ts"] is not None:
        return int(row["max_ts"])
    return None


def list_articles_without_features(
    conn: sqlite3.Connection, limit: int = 200, feature_version: str = "news_v1"
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT na.* FROM news_articles na
        LEFT JOIN news_features nf
            ON na.id = nf.article_id AND nf.feature_version = ?
        WHERE nf.id IS NULL
        ORDER BY na.published_at ASC
        LIMIT ?
        """,
        (feature_version, limit),
    )
    return cur.fetchall()


def list_fills(
    conn: sqlite3.Connection, symbol: str | None = None, limit: int = 1000
) -> list[sqlite3.Row]:
    query = "SELECT * FROM fills"
    params: list[Any] = []
    if symbol:
        query += " WHERE symbol = ?"
        params.append(symbol)
    query += " ORDER BY ts ASC LIMIT ?"
    params.append(limit)
    cur = conn.execute(query, params)
    return cur.fetchall()


def insert_external_balance(
    conn: sqlite3.Connection,
    exchange: str,
    currency: str,
    total: float,
    free: float,
    used: float,
    ts: str,
    raw_json: str,
) -> None:
    conn.execute(
        """
        INSERT INTO external_balances
        (exchange, currency, total, free, used, ts, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (exchange, currency, total, free, used, ts, raw_json),
    )
    conn.commit()


def insert_external_trade(
    conn: sqlite3.Connection,
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
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO external_trades
        (trade_uid, exchange, trade_id, symbol, side, price, amount, cost, fee, fee_currency, ts, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_uid,
            exchange,
            trade_id,
            symbol,
            side,
            price,
            amount,
            cost,
            fee,
            fee_currency,
            ts,
            raw_json,
        ),
    )
    conn.commit()
    return (conn.total_changes - before) > 0


def list_external_trades_between(
    conn: sqlite3.Connection,
    exchange: str,
    start_iso: str | None,
    end_iso: str | None,
    symbol: str | None = None,
) -> list[sqlite3.Row]:
    query = "SELECT * FROM external_trades WHERE exchange = ?"
    params: list[Any] = [exchange]
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    if start_iso:
        query += " AND ts >= ?"
        params.append(start_iso)
    if end_iso:
        query += " AND ts <= ?"
        params.append(end_iso)
    query += " ORDER BY ts ASC"
    cur = conn.execute(query, params)
    return cur.fetchall()


def get_latest_external_trade_ts(
    conn: sqlite3.Connection, exchange: str, symbol: str | None = None
) -> str | None:
    query = "SELECT MAX(ts) as max_ts FROM external_trades WHERE exchange = ?"
    params: list[Any] = [exchange]
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    cur = conn.execute(query, params)
    row = cur.fetchone()
    return str(row["max_ts"]) if row and row["max_ts"] else None


def list_latest_external_balances(
    conn: sqlite3.Connection, exchange: str
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT b.*
        FROM external_balances b
        JOIN (
            SELECT currency, MAX(ts) AS max_ts
            FROM external_balances
            WHERE exchange = ?
            GROUP BY currency
        ) latest
          ON latest.currency = b.currency AND latest.max_ts = b.ts
        WHERE b.exchange = ?
        ORDER BY b.currency ASC
        """,
        (exchange, exchange),
    )
    return cur.fetchall()

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


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
            published_at TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
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
            size REAL NOT NULL,
            price REAL NOT NULL,
            confidence REAL NOT NULL,
            rationale TEXT NOT NULL,
            mode TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS approvals (
            intent_id TEXT PRIMARY KEY,
            intent_hash TEXT NOT NULL,
            approved_at TEXT NOT NULL,
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

        CREATE TABLE IF NOT EXISTS trade_results (
            trade_id TEXT PRIMARY KEY,
            intent_id TEXT NOT NULL,
            pnl_jpy REAL NOT NULL,
            created_at TEXT NOT NULL,
            mode TEXT NOT NULL,
            meta_json TEXT NOT NULL,
            FOREIGN KEY (intent_id) REFERENCES order_intents(intent_id)
        );

        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            event TEXT NOT NULL,
            data_json TEXT NOT NULL
        );
        """
    )
    conn.commit()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def insert_candles(
    conn: sqlite3.Connection, symbol: str, timeframe: str, candles: Iterable[list[Any]]
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
            ingested_at,
        )
        for c in candles
    ]
    conn.executemany(
        """
        INSERT OR IGNORE INTO candles
        (symbol, timeframe, ts, open, high, low, close, volume, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
) -> int | None:
    try:
        cur = conn.execute(
            """
            INSERT INTO news_articles
            (url, title, source, published_at, ingested_at, title_hash)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (url, title, source, published_at, utc_now_iso(), title_hash),
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
) -> None:
    conn.execute(
        """
        INSERT INTO news_features
        (article_id, sentiment, keyword_flags, source_weight, language, extracted_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            article_id,
            sentiment,
            json.dumps(keyword_flags, separators=(",", ":"), sort_keys=True),
            source_weight,
            language,
            utc_now_iso(),
        ),
    )
    conn.commit()


def insert_order_intent(conn: sqlite3.Connection, intent: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO order_intents
        (intent_id, created_at, intent_json, intent_hash, status, expires_at, strategy,
         symbol, side, size, price, confidence, rationale, mode)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            intent["size"],
            intent["price"],
            intent["confidence"],
            intent["rationale"],
            intent["mode"],
        ),
    )
    conn.commit()


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
    conn: sqlite3.Connection, intent_id: str, intent_hash: str, approval_phrase: str
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO approvals
        (intent_id, intent_hash, approved_at, approval_phrase)
        VALUES (?, ?, ?, ?)
        """,
        (intent_id, intent_hash, utc_now_iso(), approval_phrase),
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
) -> None:
    conn.execute(
        """
        INSERT INTO executions
        (exec_id, intent_id, intent_hash, executed_at, mode, status, details_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            exec_id,
            intent_id,
            intent_hash,
            utc_now_iso(),
            mode,
            status,
            json.dumps(details, separators=(",", ":"), sort_keys=True),
        ),
    )
    conn.commit()


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
            utc_now_iso(),
            mode,
            json.dumps(meta, separators=(",", ":"), sort_keys=True),
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
        SELECT nf.*, na.published_at FROM news_features nf
        JOIN news_articles na ON nf.article_id = na.id
        WHERE na.published_at <= ?
        ORDER BY na.published_at DESC
        LIMIT ?
        """,
        (published_before, limit),
    )
    return cur.fetchall()


def list_news_features_since(
    conn: sqlite3.Connection, published_after: str
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT nf.*, na.published_at FROM news_features nf
        JOIN news_articles na ON nf.article_id = na.id
        WHERE na.published_at >= ?
        ORDER BY na.published_at ASC
        """,
        (published_after,),
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


def get_latest_candle_ts(conn: sqlite3.Connection, symbol: str, timeframe: str) -> int | None:
    cur = conn.execute(
        "SELECT MAX(ts) as max_ts FROM candles WHERE symbol = ? AND timeframe = ?",
        (symbol, timeframe),
    )
    row = cur.fetchone()
    if row and row["max_ts"] is not None:
        return int(row["max_ts"])
    return None


def list_articles_without_features(conn: sqlite3.Connection, limit: int = 200) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT na.* FROM news_articles na
        LEFT JOIN news_features nf ON na.id = nf.article_id
        WHERE nf.id IS NULL
        ORDER BY na.published_at ASC
        LIMIT ?
        """,
        (limit,),
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

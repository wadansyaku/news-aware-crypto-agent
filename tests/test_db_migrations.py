from __future__ import annotations

import sqlite3

from trade_agent import db


def test_init_db_adds_observed_at() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE news_articles (
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
        """
    )
    db.init_db(conn)
    cols = [row["name"] for row in conn.execute("PRAGMA table_info(news_articles)").fetchall()]
    assert "observed_at" in cols

    tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master").fetchall()}
    assert "orders" in tables
    assert "daily_stats" in tables
    conn.close()

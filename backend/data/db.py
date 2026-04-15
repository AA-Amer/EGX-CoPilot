"""
backend/data/db.py

SQLite interface — the single source of truth for all price data.
yfinance is only called by price_collector.py; everything else reads here.

DB path: os.getenv('DB_PATH', 'D:/SQLLite/egx_copilot.db')

Tables
------
prices          OHLCV per ticker per date (UNIQUE on ticker+date)
collection_log  record of each price_collector run
positions       open/closed trade records for portfolio tracking
"""
from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_DB_DEFAULT = "D:/SQLLite/egx_copilot.db"


def _db_path() -> str:
    return os.getenv("DB_PATH", _DB_DEFAULT)


def get_connection() -> sqlite3.Connection:
    """
    Open and return a new SQLite connection to the configured DB file.
    Creates parent directories if they don't exist.
    Caller is responsible for closing (or using as context manager).
    """
    path = _db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # safe for concurrent reads
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ── Schema ────────────────────────────────────────────────────────────────────

_DDL_PRICES = """
CREATE TABLE IF NOT EXISTS prices (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker  TEXT    NOT NULL,
    date    TEXT    NOT NULL,          -- YYYY-MM-DD
    open    REAL,
    high    REAL,
    low     REAL,
    close   REAL    NOT NULL,
    volume  INTEGER,
    source  TEXT    DEFAULT 'yfinance',
    UNIQUE(ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_prices_ticker_date
    ON prices (ticker, date DESC);
"""

_DDL_COLLECTION_LOG = """
CREATE TABLE IF NOT EXISTS collection_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at       TEXT    NOT NULL,     -- ISO-8601 datetime
    tickers_ok   INTEGER DEFAULT 0,
    tickers_fail INTEGER DEFAULT 0,
    notes        TEXT                  -- JSON: {failed:[...], skipped:[...]}
);
"""

_DDL_POSITIONS = """
CREATE TABLE IF NOT EXISTS positions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker       TEXT    NOT NULL,
    wallet       TEXT    NOT NULL,     -- 'swing' | 'long_term'
    entry_date   TEXT    NOT NULL,
    entry_price  REAL    NOT NULL,
    shares       INTEGER NOT NULL,
    target_price REAL,
    stop_price   REAL,
    status       TEXT    DEFAULT 'open',   -- 'open' | 'closed'
    closed_date  TEXT,
    closed_price REAL,
    notes        TEXT
);
"""

_DDL_MANUAL_PRICES = """
CREATE TABLE IF NOT EXISTS manual_prices (
    ticker     TEXT NOT NULL,
    date       TEXT NOT NULL,          -- YYYY-MM-DD
    close      REAL NOT NULL,
    entered_by TEXT DEFAULT 'manual'
);
"""

_initialized = False   # module-level flag: persists across Streamlit reruns


def init_db() -> None:
    """
    Create all 4 tables if they do not already exist.
    Safe to call repeatedly — uses CREATE TABLE IF NOT EXISTS throughout.
    """
    global _initialized
    if _initialized:
        return

    conn = get_connection()
    try:
        conn.executescript(_DDL_PRICES)
        conn.executescript(_DDL_COLLECTION_LOG)
        conn.executescript(_DDL_POSITIONS)
        conn.executescript(_DDL_MANUAL_PRICES)
        conn.commit()
        logger.info("init_db: schema ready at %s", _db_path())
    finally:
        conn.close()

    _initialized = True


# ── Queries ───────────────────────────────────────────────────────────────────

def get_prices(ticker: str, limit: int = 120) -> pd.DataFrame:
    """
    Return the last N rows for a ticker, sorted ascending by date.

    Columns: Open, High, Low, Close, Volume (capitalized).
    Index:   date as pandas Timestamp.
    Returns empty DataFrame if ticker has no rows.
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT date, open, high, low, close, volume
            FROM   prices
            WHERE  ticker = ?
            ORDER  BY date DESC
            LIMIT  ?
            """,
            conn,
            params=(ticker.upper(), limit),
        )
    except Exception as exc:
        logger.error("get_prices(%s): %s", ticker, exc)
        return pd.DataFrame()
    finally:
        conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")
    df.columns = [c.capitalize() for c in df.columns]   # Open High Low Close Volume
    return df


def get_all_tickers() -> list[str]:
    """Return distinct ticker names present in the prices table, sorted A–Z."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT DISTINCT ticker FROM prices ORDER BY ticker"
        ).fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


def get_latest_price(ticker: str) -> tuple[Optional[float], Optional[str]]:
    """
    Return (close_price, date_string) for the most recent row.
    Returns (None, None) if ticker has no data.
    """
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT close, date
            FROM   prices
            WHERE  ticker = ?
            ORDER  BY date DESC
            LIMIT  1
            """,
            (ticker.upper(),),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None, None
    return float(row["close"]), str(row["date"])


def get_db_summary() -> dict[str, dict]:
    """
    Return a summary of each ticker's data coverage.

    Returns:
        {
            "SWDY": {
                "rows":         120,
                "from_date":    "2023-10-01",
                "to_date":      "2024-04-15",
                "latest_close": 45.50,
            },
            ...
        }
    """
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT
                s.ticker,
                s.row_count,
                s.from_date,
                s.to_date,
                p.close AS latest_close
            FROM (
                SELECT
                    ticker,
                    COUNT(*)  AS row_count,
                    MIN(date) AS from_date,
                    MAX(date) AS to_date
                FROM prices
                GROUP BY ticker
            ) s
            JOIN prices p
              ON p.ticker = s.ticker
             AND p.date   = s.to_date
            ORDER BY s.ticker
            """
        ).fetchall()
    except Exception as exc:
        logger.error("get_db_summary: %s", exc)
        return {}
    finally:
        conn.close()

    return {
        r["ticker"]: {
            "rows":         r["row_count"],
            "from_date":    r["from_date"],
            "to_date":      r["to_date"],
            "latest_close": round(float(r["latest_close"]), 2),
        }
        for r in rows
    }


def get_manual_entries(limit: int = 20) -> pd.DataFrame:
    """
    Return the most recent rows from manual_prices.

    Columns: ticker, date, close, entered_by, rowid.
    Sorted by date DESC, rowid DESC (newest first).
    Returns empty DataFrame if the table has no entries.
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT rowid as rowid, ticker, date, close, entered_by
            FROM   manual_prices
            ORDER  BY date DESC, rowid DESC
            LIMIT  ?
            """,
            conn,
            params=(limit,),
        )
    except Exception as exc:
        logger.error("get_manual_entries: %s", exc)
        return pd.DataFrame()
    finally:
        conn.close()
    return df

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

_DB_DEFAULT = "data\egx_copilot.db"


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

_DDL_LT_TRANSACTIONS = """
CREATE TABLE IF NOT EXISTS lt_transactions (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    date                     DATE    NOT NULL,
    category                 TEXT    NOT NULL,
    ticker                   TEXT,
    quantity                 REAL,
    fulfillment_price        REAL,
    fees                     REAL    DEFAULT 0,
    dividend_tax             REAL    DEFAULT 0,
    actual_price_per_share   REAL,
    total_amount             REAL,
    year                     INTEGER,
    quarter                  TEXT,
    fx_rate                  REAL,
    usd_value                REAL,
    net_wallet_impact        REAL,
    external_capital_impact  REAL,
    notes                    TEXT
);
"""

_DDL_LT_POSITIONS = """
CREATE TABLE IF NOT EXISTS lt_positions (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker            TEXT    UNIQUE NOT NULL,
    total_shares      REAL    DEFAULT 0,
    total_cost_net    REAL    DEFAULT 0,
    weighted_avg_cost REAL    DEFAULT 0,
    realized_pl       REAL    DEFAULT 0,
    dividends_net     REAL    DEFAULT 0,
    status            TEXT    DEFAULT 'Open',
    last_updated      DATE
);
"""

_DDL_LT_SIGNALS = """
CREATE TABLE IF NOT EXISTS lt_signals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date                DATE    NOT NULL,
    ticker                  TEXT    NOT NULL,
    avg_cost                REAL,
    price                   REAL,
    signal                  TEXT,
    action                  TEXT,
    score                   INTEGER,
    position_size_pct       REAL,
    current_allocation_pct  REAL,
    recommended_shares      INTEGER,
    recommended_capital     REAL,
    suggested_buy_price     REAL,
    profit_pct              REAL,
    sell_price              REAL,
    fib_zone                TEXT,
    swing_high              REAL,
    swing_low               REAL,
    target_1m               REAL,
    target_6m               REAL,
    target_12m              REAL,
    exp_return_1m           REAL,
    exp_return_6m           REAL,
    exp_return_12m          REAL,
    forecast_confidence     TEXT,
    description             TEXT,
    deploy_pct              REAL DEFAULT 0,
    deploy_label            TEXT,
    deploy_note             TEXT,
    UNIQUE(run_date, ticker)
);
"""

_DDL_LT_PURIFICATION = """
CREATE TABLE IF NOT EXISTS lt_purification (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker               TEXT    NOT NULL,
    year                 INTEGER,
    quarter              TEXT,
    quarter_start        DATE,
    quarter_end          DATE,
    daily_haram_rate     REAL,
    share_days           REAL,
    purification_amount  REAL,
    purification_rounded REAL,
    status               TEXT,
    paid_amount          REAL    DEFAULT 0,
    outstanding          REAL    DEFAULT 0,
    quarter_closed       TEXT    DEFAULT 'N',
    UNIQUE(ticker, year, quarter)
);
"""

_DDL_INFLATION_DATA = """
CREATE TABLE IF NOT EXISTS inflation_data (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    month_year       TEXT    UNIQUE NOT NULL,
    headline_mom     REAL,
    cumulative_index REAL,
    cumulative_pct   REAL
);
"""

_initialized = False   # module-level flag: persists across Streamlit reruns


def init_lt_tables() -> None:
    """
    Create all 5 Long-Term Wallet tables if they do not already exist.
    Safe to call repeatedly — uses CREATE TABLE IF NOT EXISTS throughout.
    """
    conn = get_connection()
    try:
        conn.executescript(_DDL_LT_TRANSACTIONS)
        conn.executescript(_DDL_LT_POSITIONS)
        conn.executescript(_DDL_LT_SIGNALS)
        conn.executescript(_DDL_LT_PURIFICATION)
        conn.executescript(_DDL_INFLATION_DATA)
        conn.commit()
        logger.info("init_lt_tables: LT schema ready at %s", _db_path())
    finally:
        conn.close()

    # Migrate lt_signals: add columns if they don't exist yet
    conn = get_connection()
    try:
        for col, typedef in [
            ("deploy_pct",    "REAL DEFAULT 0"),
            ("deploy_label",  "TEXT"),
            ("deploy_note",   "TEXT"),
            ("deploy_tier",   "TEXT"),
            ("enhanced_json", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE lt_signals ADD COLUMN {col} {typedef}")
                conn.commit()
            except Exception:
                pass  # column already exists
    finally:
        conn.close()


def init_db() -> None:
    """
    Create all tables (core + LT wallet) if they do not already exist.
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                ticker          TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                yahoo_code      TEXT,
                sector          TEXT,
                market          TEXT DEFAULT 'EGX',
                shariah         INTEGER DEFAULT 1,
                active          INTEGER DEFAULT 1,
                notes           TEXT,
                added_at        TEXT DEFAULT (datetime('now'))
            )
        """)

        conn.executemany("""
            INSERT OR IGNORE INTO watchlist
            (ticker, name, yahoo_code, sector, market, shariah, active)
            VALUES (?,?,?,?,?,?,?)
        """, [
            ('MPCI', 'Misr Phosphate',           'MPCI.CA', 'Materials',   'EGX', 1, 1),
            ('AMOC', 'Alexandria Mineral Oils',  'AMOC.CA', 'Energy',      'EGX', 1, 1),
            ('ORWE', 'Oriental Weavers',          'ORWE.CA', 'Consumer',    'EGX', 1, 1),
            ('MICH', 'Misr Chemical Industries', 'MICH.CA', 'Materials',   'EGX', 1, 1),
            ('ORAS', 'Orascom Construction',      'ORAS.CA', 'Industrials', 'EGX', 1, 1),
            ('OLFI', 'Olympic Group',             'OLFI.CA', 'Consumer',    'EGX', 1, 1),
            ('SUGR', 'Delta Sugar',               'SUGR.CA', 'Consumer',    'EGX', 1, 1),
            ('SWDY', 'Elsewedy Electric',         'SWDY.CA', 'Industrials', 'EGX', 1, 1),
        ])

        conn.commit()
        logger.info("init_db: schema ready at %s", _db_path())
    finally:
        conn.close()

    init_lt_tables()
    from backend.data.fundamental_db import init_fundamental_tables
    init_fundamental_tables()
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


def get_prev_close(ticker: str) -> Optional[float]:
    """
    Return the second-most-recent closing price for a ticker (i.e. yesterday's close),
    or None if fewer than 2 rows exist.
    """
    try:
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT close FROM prices WHERE ticker=? ORDER BY date DESC LIMIT 2",
                (ticker.upper(),),
            ).fetchall()
        finally:
            conn.close()
        return float(rows[1][0]) if len(rows) >= 2 else None
    except Exception:
        return None


def get_last_collection_time() -> str:
    """
    Return the latest run_at timestamp from collection_log as a string,
    or an empty string if no collection has been run yet.
    Used for the auto-refresh polling mechanism.
    """
    try:
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT run_at FROM collection_log ORDER BY run_at DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        return row[0] if row else ""
    except Exception:
        return ""

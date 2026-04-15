"""
backend/data/fetcher.py

Read-only data access layer — all price data comes from SQLite.
yfinance is NOT called here; use price_collector.py for that.

Public API
----------
fetch_tickers(limit)    {ticker: DataFrame} for config universe
get_latest_price(ticker) (close_price, date_string) from DB
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_tickers(limit: int = 120) -> dict[str, pd.DataFrame]:
    """
    Load the last N rows of OHLCV data for every ticker in the config universe.

    Reads from SQLite via db.get_prices() — no network calls.
    Logs a warning for any ticker that has no data yet (run collect_today()
    or load historical data from Investing.com to populate the DB).

    Args:
        limit: Maximum number of rows per ticker (most-recent, ascending order).

    Returns:
        {short_ticker: DataFrame(Open, High, Low, Close, Volume)}
        Missing tickers are omitted from the result dict.
    """
    from backend.data.config_loader import load_config
    from backend.data.db import get_prices

    cfg = load_config()
    universe: list[str] = cfg["tickers"]["universe"]
    result: dict[str, pd.DataFrame] = {}

    for ticker in universe:
        df = get_prices(ticker, limit=limit)
        if df.empty:
            logger.warning(
                "fetch_tickers: no data in DB for %s — "
                "run price_collector.collect_today() to populate",
                ticker,
            )
            continue
        result[ticker] = df

    return result


def get_latest_price(ticker: str) -> tuple[Optional[float], Optional[str]]:
    """
    Return (close_price, date_string) for the most recent DB row.
    Returns (None, None) if no data exists for this ticker.
    """
    from backend.data.db import get_latest_price as _db_latest
    return _db_latest(ticker)

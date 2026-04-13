"""
backend/data/fetcher.py

yfinance wrapper for EGX (Egyptian Exchange).
Reads the ticker universe from config.json.
EGX tickers use the .CA suffix on yfinance (e.g. SWDY.CA).
Stooq via pandas-datareader is the automatic fallback for single-ticker history.

Public API:
    resolve_symbol(ticker)              short name → yfinance symbol
    fetch_tickers(period, interval)     batch OHLCV for the full config universe
    get_latest_price(ticker)            most recent close + timestamp
    get_price_history(ticker, ...)      single-ticker OHLCV with Stooq fallback
    get_quote(ticker)                   latest quote snapshot dict
    get_news(ticker, max_items)         recent news via yfinance
    get_ticker_info(ticker)             raw yfinance .info dict
    get_egx30_index(period)             ^EGX30 index OHLCV
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

from backend.data.config_loader import load_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ticker symbol overrides
# ---------------------------------------------------------------------------
# Most EGX tickers are simply SHORT.CA on yfinance.
# A handful use ISIN-derived symbols — those are listed here as overrides.
# resolve_symbol() falls back to SHORT.CA for anything not in this dict.

_SYMBOL_OVERRIDES: dict[str, str] = {
    "ORAS": "EGS95001C011.CA",
    "AMOC": "EGS380P1C010.CA",
    "MICH": "EGS38211C016.CA",
    "SWDY": "EGS3G0Z1C014.CA",
    "MPCI": "EGS38351C010.CA",
    "ORWE": "EGS33041C012.CA",
    "SUGR": "EGS30201C015.CA",
    "ABUK": "EGS38191C010.CA",
    "OLFI": "EGS30AL1C012.CA",
}

EGX30_SYMBOL = "^EGX30"


def resolve_symbol(ticker: str) -> str:
    """
    Map a short EGX ticker to its yfinance symbol.

    Lookup order:
      1. _SYMBOL_OVERRIDES (ISIN-based exceptions)
      2. If the input already has a dot (e.g. "COMI.CA"), return as-is
      3. Append ".CA" — the universal EGX suffix on yfinance
    """
    ticker = ticker.upper().strip()
    if ticker in _SYMBOL_OVERRIDES:
        return _SYMBOL_OVERRIDES[ticker]
    if "." in ticker:
        return ticker
    return f"{ticker}.CA"


# ---------------------------------------------------------------------------
# Batch fetch — primary entry point for the scanner
# ---------------------------------------------------------------------------

def fetch_tickers(
    period: str = "6mo",
    interval: str = "1d",
) -> dict[str, pd.DataFrame]:
    """
    Download OHLCV data for every ticker in the config universe.

    - Reads tickers.universe from config.json
    - Maps each to its yfinance .CA symbol
    - Runs a single batched yf.download() call
    - Validates each ticker has ≥ 30 rows; drops and warns on failures
    - Returns {short_ticker: DataFrame(Open, High, Low, Close, Volume)}

    Args:
        period:   yfinance period string (e.g. "6mo", "1y").
        interval: Bar size (e.g. "1d", "1wk").

    Returns:
        Dict of valid DataFrames keyed by short ticker name.
        Empty dict if the download fails entirely.
    """
    cfg = load_config()
    universe: list[str] = cfg["tickers"]["universe"]
    symbol_map: dict[str, str] = {t.upper(): resolve_symbol(t) for t in universe}
    symbols = list(symbol_map.values())

    logger.info("fetch_tickers: downloading %d symbols — %s", len(symbols), symbols)

    try:
        raw = yf.download(
            tickers=symbols,
            period=period,
            interval=interval,
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as exc:
        logger.error("fetch_tickers: yf.download failed: %s", exc)
        return {}

    result: dict[str, pd.DataFrame] = {}

    for short, sym in symbol_map.items():
        try:
            # yf.download returns a flat DataFrame when only one symbol is passed
            df = raw if len(symbols) == 1 else raw[sym]
            df = df[["Open", "High", "Low", "Close", "Volume"]].dropna(how="all")

            # Normalise timezone — strip to naive Cairo local time
            if not df.empty and hasattr(df.index, "tz") and df.index.tz is not None:
                df.index = df.index.tz_convert("Africa/Cairo").tz_localize(None)

            row_count = len(df)
            if row_count < 30:
                logger.warning(
                    "fetch_tickers: dropping %s (%s) — only %d rows (need ≥ 30)",
                    short, sym, row_count,
                )
                continue

            result[short] = df

        except KeyError:
            logger.warning(
                "fetch_tickers: dropping %s (%s) — not found in download response",
                short, sym,
            )
        except Exception as exc:
            logger.warning("fetch_tickers: dropping %s (%s) — %s", short, sym, exc)

    logger.info(
        "fetch_tickers: %d/%d tickers valid after validation",
        len(result), len(universe),
    )
    return result


# ---------------------------------------------------------------------------
# Latest price snapshot
# ---------------------------------------------------------------------------

def get_latest_price(ticker: str) -> dict:
    """
    Return the most recent close price and its timestamp for a single ticker.

    Returns:
        {"ticker": str, "symbol": str, "price": float, "timestamp": str}
        {"ticker": str, "symbol": str, "error": str}  on failure
    """
    symbol = resolve_symbol(ticker)
    try:
        hist = yf.Ticker(symbol).history(period="2d", interval="1d", auto_adjust=True)
        if hist.empty:
            return {"ticker": ticker, "symbol": symbol, "error": "no data"}

        ts = hist.index[-1]
        if hasattr(ts, "tz") and ts.tz is not None:
            ts = ts.tz_convert("Africa/Cairo").tz_localize(None)

        return {
            "ticker": ticker,
            "symbol": symbol,
            "price": round(float(hist["Close"].iloc[-1]), 2),
            "timestamp": str(ts),
        }
    except Exception as exc:
        logger.error("get_latest_price failed for %s: %s", ticker, exc)
        return {"ticker": ticker, "symbol": symbol, "error": str(exc)}


# ---------------------------------------------------------------------------
# Single-ticker history (with Stooq fallback)
# ---------------------------------------------------------------------------

def get_price_history(
    ticker: str,
    period: str = "6mo",
    interval: str = "1d",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch OHLCV history for a single EGX ticker.
    Tries yfinance first; falls back to Stooq on empty result.

    Returns:
        DataFrame(Open, High, Low, Close, Volume) or empty DataFrame.
    """
    symbol = resolve_symbol(ticker)
    df = _fetch_yfinance(symbol, period=period, interval=interval, start=start, end=end)
    if df.empty:
        logger.warning("yfinance empty for %s — trying Stooq", symbol)
        df = _fetch_stooq(ticker, start=start, period=period)
    return df


# ---------------------------------------------------------------------------
# Quote snapshot
# ---------------------------------------------------------------------------

def get_quote(ticker: str) -> dict:
    """
    Return the most-recent quote for a ticker as a flat dict.
    Keys: symbol, short_name, currency, price, prev_close, change_pct,
          volume, market_cap, pe_ratio, week52_high, week52_low, timestamp.
    """
    symbol = resolve_symbol(ticker)
    try:
        t = yf.Ticker(symbol)
        info = t.info or {}
        hist = t.history(period="2d", interval="1d", auto_adjust=True)

        price = _coerce_float(
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or (hist["Close"].iloc[-1] if not hist.empty else None)
        )
        prev_close = _coerce_float(
            info.get("previousClose")
            or info.get("regularMarketPreviousClose")
            or (hist["Close"].iloc[-2] if len(hist) >= 2 else None)
        )
        change_pct = (
            round((price - prev_close) / prev_close * 100, 2)
            if price and prev_close
            else float("nan")
        )

        return {
            "symbol": symbol,
            "short_name": info.get("shortName", ticker),
            "currency": info.get("currency", "EGP"),
            "price": round(price, 2) if price else None,
            "prev_close": round(prev_close, 2) if prev_close else None,
            "change_pct": change_pct,
            "volume": info.get("regularMarketVolume") or info.get("volume"),
            "market_cap": info.get("marketCap"),
            "pe_ratio": info.get("trailingPE"),
            "week52_high": info.get("fiftyTwoWeekHigh"),
            "week52_low": info.get("fiftyTwoWeekLow"),
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as exc:
        logger.error("get_quote failed for %s: %s", ticker, exc)
        return {"symbol": symbol, "error": str(exc)}


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------

def get_news(ticker: str, max_items: int = 10) -> list[dict]:
    """
    Fetch recent news via yfinance (English + Arabic headlines).
    Returns list of {title, publisher, link, published_at, type}.
    """
    symbol = resolve_symbol(ticker)
    try:
        raw = yf.Ticker(symbol).news or []
        items = []
        for item in raw[:max_items]:
            published_at = None
            if "providerPublishTime" in item:
                published_at = datetime.fromtimestamp(item["providerPublishTime"]).isoformat()
            items.append({
                "title": item.get("title", ""),
                "publisher": item.get("publisher", ""),
                "link": item.get("link", ""),
                "published_at": published_at,
                "type": item.get("type", "STORY"),
            })
        return items
    except Exception as exc:
        logger.error("get_news failed for %s: %s", ticker, exc)
        return []


# ---------------------------------------------------------------------------
# Raw fundamentals
# ---------------------------------------------------------------------------

def get_ticker_info(ticker: str) -> dict:
    """Return the full yfinance .info dict. Used by Shariah screener and DCF."""
    symbol = resolve_symbol(ticker)
    try:
        return yf.Ticker(symbol).info or {}
    except Exception as exc:
        logger.error("get_ticker_info failed for %s: %s", ticker, exc)
        return {}


def get_egx30_index(period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    """Fetch EGX 30 index OHLCV (^EGX30)."""
    return _fetch_yfinance(EGX30_SYMBOL, period=period, interval=interval)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_yfinance(
    symbol: str,
    period: str = "6mo",
    interval: str = "1d",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    try:
        t = yf.Ticker(symbol)
        df = (
            t.history(start=start, end=end, interval=interval, auto_adjust=True)
            if start
            else t.history(period=period, interval=interval, auto_adjust=True)
        )
        if not df.empty and hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_convert("Africa/Cairo").tz_localize(None)
        return df[["Open", "High", "Low", "Close", "Volume"]] if not df.empty else df
    except Exception as exc:
        logger.warning("_fetch_yfinance error for %s: %s", symbol, exc)
        return pd.DataFrame()


def _fetch_stooq(
    ticker: str,
    start: Optional[str] = None,
    period: str = "6mo",
) -> pd.DataFrame:
    """Fallback via Stooq (pandas-datareader). Stooq uses SHORT.EG format."""
    try:
        from pandas_datareader import data as pdr
    except ImportError:
        logger.error("pandas-datareader not installed — Stooq unavailable")
        return pd.DataFrame()
    try:
        stooq_sym = f"{ticker.upper()}.EG"
        if start is None:
            start = _period_to_start_date(period)
        df = pdr.DataReader(stooq_sym, "stooq", start=start, end=datetime.today().strftime("%Y-%m-%d"))
        df = df.sort_index()
        logger.info("Stooq fallback succeeded for %s → %s", ticker, stooq_sym)
        return df[["Open", "High", "Low", "Close", "Volume"]] if not df.empty else df
    except Exception as exc:
        logger.error("_fetch_stooq failed for %s: %s", ticker, exc)
        return pd.DataFrame()


def _period_to_start_date(period: str) -> str:
    today = datetime.today()
    p = period.lower().strip()
    if p == "max":
        return "1990-01-01"
    if p == "ytd":
        return f"{today.year}-01-01"
    if p.endswith("d"):
        return (today - timedelta(days=int(p[:-1]))).strftime("%Y-%m-%d")
    if p.endswith("mo"):
        return (today - timedelta(days=int(p[:-2]) * 30)).strftime("%Y-%m-%d")
    if p.endswith("y"):
        return (today - timedelta(days=int(p[:-1]) * 365)).strftime("%Y-%m-%d")
    return (today - timedelta(days=180)).strftime("%Y-%m-%d")


def _coerce_float(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

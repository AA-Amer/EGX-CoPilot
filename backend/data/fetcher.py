"""
backend/data/fetcher.py

yfinance wrapper for EGX (Egyptian Exchange) data.

Price data is 15-minute delayed — acceptable for end-of-day signal generation.
EGX tickers use the .CA suffix on yfinance (e.g. COMI.CA).
Stooq via pandas-datareader is the automatic fallback source.

Public API:
    resolve_symbol(ticker)          short name → yfinance symbol
    get_price_history(...)          OHLCV DataFrame for one ticker
    get_quote(ticker)               latest quote as a flat dict
    get_batch_quotes(tickers)       quotes for many tickers in one download
    get_news(ticker)                recent news items from yfinance
    get_ticker_info(ticker)         raw yfinance .info dict (fundamentals)
    get_egx30_index(period)         EGX 30 index OHLCV
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ticker registry
# ---------------------------------------------------------------------------
# Keys  : short EGX ticker as used on the exchange floor (e.g. "COMI")
# Values: yfinance symbol — either SHORT.CA or an ISIN-derived EGSxxxxxxx.CA
#
# ISIN-based symbols come directly from the SPEC; all others use SHORT.CA.
# Call resolve_symbol() to look up any ticker — it falls back to SHORT.CA
# so you never need to pre-register a ticker to fetch it.

EGX_TICKERS: dict[str, str] = {
    # ── ISIN-based symbols confirmed in SPEC ─────────────────────────────
    "ORAS": "EGS95001C011.CA",
    "AMOC": "EGS380P1C010.CA",
    "MICH": "EGS38211C016.CA",
    "SWDY": "EGS3G0Z1C014.CA",
    "MPCI": "EGS38351C010.CA",
    "ORWE": "EGS33041C012.CA",
    "SUGR": "EGS30201C015.CA",
    "ABUK": "EGS38191C010.CA",
    "OLFI": "EGS30AL1C012.CA",
    # ── EGX 30 blue-chips (standard SHORT.CA format) ──────────────────────
    "COMI": "COMI.CA",   # Commercial International Bank
    "ETEL": "ETEL.CA",   # Telecom Egypt
    "HRHO": "HRHO.CA",   # Hassan Allam Holding
    "EKHO": "EKHO.CA",   # El Sewedy Electric
    "SKPC": "SKPC.CA",   # Sidi Kerir Petrochemicals
    "ESRS": "ESRS.CA",   # Ezz Steel
    "EAST": "EAST.CA",   # Eastern Company
    "EFIC": "EFIC.CA",   # Egyptian Financial & Industrial
    "JUFO": "JUFO.CA",   # Juhayna Food Industries
    "TALM": "TALM.CA",   # Talaat Moustafa Group
    "SVCE": "SVCE.CA",   # Six of October Development
    "MNHD": "MNHD.CA",   # Madinet Nasr for Housing
    "PHDC": "PHDC.CA",   # Palm Hills Developments
    "OCDI": "OCDI.CA",   # Orascom Development Egypt
    "TMGH": "TMGH.CA",   # TMG Holding
    "EGTS": "EGTS.CA",   # Egyptian for Tourism Resorts
    "ABUK": "ABUK.CA",   # Abu Kir Fertilizers (also ISIN above — ISIN takes priority)
}

# EGX 30 index on yfinance
EGX30_SYMBOL = "^EGX30"


# ---------------------------------------------------------------------------
# Symbol resolution
# ---------------------------------------------------------------------------

def resolve_symbol(ticker: str) -> str:
    """
    Map a short EGX ticker to its yfinance symbol.

    Lookup order:
      1. EGX_TICKERS registry (catches ISIN-based overrides)
      2. If the input already contains a dot (e.g. "COMI.CA"), return as-is
      3. Append ".CA" as the universal EGX suffix

    Args:
        ticker: Short ticker string, case-insensitive (e.g. "comi", "COMI").

    Returns:
        yfinance-compatible symbol string (e.g. "COMI.CA").
    """
    ticker = ticker.upper().strip()
    if ticker in EGX_TICKERS:
        return EGX_TICKERS[ticker]
    if "." in ticker:
        return ticker
    return f"{ticker}.CA"


# ---------------------------------------------------------------------------
# Single-ticker price history
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

    Tries yfinance first; on empty result falls back to Stooq via
    pandas-datareader.  Both sources return auto-adjusted prices.

    Args:
        ticker:   Short EGX ticker (e.g. "COMI") or full yfinance symbol.
        period:   yfinance period string: "1d" "5d" "1mo" "3mo" "6mo"
                  "1y" "2y" "5y" "10y" "ytd" "max".
        interval: Bar size: "1m" "2m" "5m" "15m" "30m" "60m" "90m"
                  "1h" "1d" "5d" "1wk" "1mo" "3mo".
        start:    Optional start date "YYYY-MM-DD"; overrides period.
        end:      Optional end date "YYYY-MM-DD"; defaults to today.

    Returns:
        DataFrame indexed by Datetime with columns:
        [Open, High, Low, Close, Volume].
        Empty DataFrame if both sources fail.
    """
    symbol = resolve_symbol(ticker)
    df = _fetch_yfinance(symbol, period=period, interval=interval, start=start, end=end)

    if df.empty:
        logger.warning(
            "yfinance returned no data for %s — trying Stooq fallback", symbol
        )
        df = _fetch_stooq(ticker, start=start, period=period)

    return df


# ---------------------------------------------------------------------------
# Single-ticker quote (latest snapshot)
# ---------------------------------------------------------------------------

def get_quote(ticker: str) -> dict:
    """
    Return the most-recent quote for a ticker as a flat dict.

    Data comes from yfinance .info where available, supplemented by
    the last two rows of the daily history for price/change calculation.

    Returns:
        {
          symbol, short_name, currency,
          price, prev_close, change_pct,
          volume, market_cap, pe_ratio,
          week52_high, week52_low, timestamp
        }
        On error: {"symbol": ..., "error": "<message>"}
    """
    symbol = resolve_symbol(ticker)
    try:
        t = yf.Ticker(symbol)
        info = t.info or {}

        # Price — prefer currentPrice, fall back to regularMarketPrice, then last Close
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
# Batch quotes
# ---------------------------------------------------------------------------

def get_batch_quotes(tickers: list[str]) -> dict[str, dict]:
    """
    Fetch latest quotes for multiple tickers in one yfinance download call.

    Uses yf.download with group_by="ticker" which is much faster than
    calling get_quote() in a loop.  Falls back to serial get_quote() calls
    if the bulk download fails entirely.

    Args:
        tickers: List of short EGX tickers (e.g. ["COMI", "ETEL"]).

    Returns:
        Dict keyed by short ticker name, values match get_quote() schema
        (minus info-only fields like pe_ratio, market_cap).
    """
    symbols = {t.upper(): resolve_symbol(t) for t in tickers}
    symbol_list = list(symbols.values())
    result: dict[str, dict] = {}

    try:
        raw = yf.download(
            tickers=symbol_list,
            period="2d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        for short, sym in symbols.items():
            try:
                # When a single symbol is passed yf.download returns a flat DataFrame
                df = raw if len(symbol_list) == 1 else raw[sym]
                if df.empty:
                    result[short] = {"symbol": sym, "error": "no data"}
                    continue

                latest = df.iloc[-1]
                prev = df.iloc[-2] if len(df) >= 2 else latest
                price = float(latest["Close"])
                prev_close = float(prev["Close"])
                change_pct = (
                    round((price - prev_close) / prev_close * 100, 2)
                    if prev_close
                    else float("nan")
                )
                result[short] = {
                    "symbol": sym,
                    "price": round(price, 2),
                    "prev_close": round(prev_close, 2),
                    "change_pct": change_pct,
                    "volume": int(latest.get("Volume", 0)),
                    "timestamp": str(df.index[-1]),
                }
            except Exception as exc:
                logger.warning("batch quote processing failed for %s: %s", short, exc)
                result[short] = {"symbol": sym, "error": str(exc)}

    except Exception as exc:
        logger.error("yf.download failed (%s) — falling back to serial fetch", exc)
        for short in tickers:
            result[short.upper()] = get_quote(short)

    return result


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------

def get_news(ticker: str, max_items: int = 10) -> list[dict]:
    """
    Fetch recent news articles for a ticker via yfinance.

    yfinance returns English and sometimes Arabic headlines depending on
    what publishers have filed for the symbol.

    Args:
        ticker:    Short EGX ticker or full symbol.
        max_items: Maximum number of articles to return.

    Returns:
        List of dicts with keys:
        {title, publisher, link, published_at (ISO-8601), type}
    """
    symbol = resolve_symbol(ticker)
    try:
        t = yf.Ticker(symbol)
        raw_news = t.news or []
        items = []
        for item in raw_news[:max_items]:
            published_at = None
            if "providerPublishTime" in item:
                published_at = datetime.fromtimestamp(
                    item["providerPublishTime"]
                ).isoformat()
            items.append(
                {
                    "title": item.get("title", ""),
                    "publisher": item.get("publisher", ""),
                    "link": item.get("link", ""),
                    "published_at": published_at,
                    "type": item.get("type", "STORY"),
                }
            )
        return items
    except Exception as exc:
        logger.error("get_news failed for %s: %s", ticker, exc)
        return []


# ---------------------------------------------------------------------------
# Raw fundamentals (yfinance .info)
# ---------------------------------------------------------------------------

def get_ticker_info(ticker: str) -> dict:
    """
    Return the full yfinance .info dict for a ticker.
    Useful for fundamental screens (Shariah, DCF inputs, P/E, etc.).

    Returns empty dict on failure.
    """
    symbol = resolve_symbol(ticker)
    try:
        return yf.Ticker(symbol).info or {}
    except Exception as exc:
        logger.error("get_ticker_info failed for %s: %s", ticker, exc)
        return {}


# ---------------------------------------------------------------------------
# EGX 30 index
# ---------------------------------------------------------------------------

def get_egx30_index(period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    """Fetch EGX 30 index OHLCV history (symbol: ^EGX30)."""
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
    """Raw yfinance history fetch — returns empty DataFrame on any error."""
    try:
        t = yf.Ticker(symbol)
        if start:
            df = t.history(start=start, end=end, interval=interval, auto_adjust=True)
        else:
            df = t.history(period=period, interval=interval, auto_adjust=True)

        # Drop timezone info from index for consistent downstream handling
        if not df.empty and hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_convert("Africa/Cairo").tz_localize(None)

        if df.empty:
            logger.debug("yfinance returned empty DataFrame for %s", symbol)

        return df[["Open", "High", "Low", "Close", "Volume"]] if not df.empty else df

    except Exception as exc:
        logger.warning("_fetch_yfinance error for %s: %s", symbol, exc)
        return pd.DataFrame()


def _fetch_stooq(
    ticker: str,
    start: Optional[str] = None,
    period: str = "6mo",
) -> pd.DataFrame:
    """
    Fallback price fetch via Stooq (pandas-datareader).

    Stooq uses the format "SHORT.EG" for Egyptian Exchange tickers.
    Only daily ("1d") data is available from Stooq.

    Args:
        ticker: Short EGX ticker (NOT the .CA symbol).
        start:  Optional start date "YYYY-MM-DD".
        period: yfinance-style period string used to derive start if
                start is not provided.
    """
    try:
        from pandas_datareader import data as pdr  # optional dependency
    except ImportError:
        logger.error("pandas-datareader not installed — Stooq fallback unavailable")
        return pd.DataFrame()

    try:
        stooq_sym = f"{ticker.upper()}.EG"

        if start is None:
            start = _period_to_start_date(period)
        end = datetime.today().strftime("%Y-%m-%d")

        df = pdr.DataReader(stooq_sym, "stooq", start=start, end=end)
        df = df.sort_index()

        logger.info("Stooq fallback succeeded for %s → %s", ticker, stooq_sym)
        # Stooq returns columns: Open High Low Close Volume
        return df[["Open", "High", "Low", "Close", "Volume"]] if not df.empty else df

    except Exception as exc:
        logger.error("_fetch_stooq failed for %s: %s", ticker, exc)
        return pd.DataFrame()


def _period_to_start_date(period: str) -> str:
    """
    Convert a yfinance period string to an absolute start date string.

    Examples: "6mo" → 6 months ago, "1y" → 1 year ago, "max" → 1990-01-01.
    """
    today = datetime.today()
    period = period.lower().strip()

    if period == "max":
        return "1990-01-01"
    if period == "ytd":
        return f"{today.year}-01-01"

    # Handle e.g. "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y"
    if period.endswith("d"):
        days = int(period[:-1])
        return (today - timedelta(days=days)).strftime("%Y-%m-%d")
    if period.endswith("mo"):
        months = int(period[:-2])
        return (today - timedelta(days=months * 30)).strftime("%Y-%m-%d")
    if period.endswith("y"):
        years = int(period[:-1])
        return (today - timedelta(days=years * 365)).strftime("%Y-%m-%d")

    # Unknown format — default to 6 months
    logger.warning("Unrecognised period '%s', defaulting to 6 months", period)
    return (today - timedelta(days=180)).strftime("%Y-%m-%d")


def _coerce_float(value) -> Optional[float]:
    """Return float or None; swallows TypeError/ValueError silently."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

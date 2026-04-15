"""
backend/data/price_collector.py

The ONLY file in this project that calls yfinance.

Runs daily at 15:05 EET (scheduled by scheduler.py) to append today's
closing row for each ticker.  Historical data is pre-loaded from
Investing.com — this file never attempts to backfill.

Manual run:
    python -m backend.data.price_collector
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# ── Symbol map ────────────────────────────────────────────────────────────────
# EGX tickers require ISIN-based long-form codes on yfinance.
# This dict is the single authoritative mapping; _yf_symbol() uses it.

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


def _yf_symbol(ticker: str) -> str:
    """Map a short EGX ticker to its yfinance symbol."""
    return _SYMBOL_OVERRIDES.get(ticker.upper(), ticker.upper() + ".CA")


# ── Collector ─────────────────────────────────────────────────────────────────

def collect_today() -> dict:
    """
    Fetch the most recent daily OHLCV bar for every ticker in the config
    universe plus the EGX 30 index, then INSERT OR IGNORE into the prices table.

    A row is only written if its date is today or yesterday — this window
    handles late runs and timezone edge cases without backfilling old data.

    Returns:
        {
            "updated": N,                  # rows newly inserted
            "failed":  ["TICKER", ...],    # yfinance errors
            "skipped": ["TICKER", ...],    # date outside window / already exists
        }
    """
    from backend.data.config_loader import load_config
    from backend.data.db import get_connection

    cfg = load_config()
    universe: list[str] = cfg["tickers"]["universe"]

    today = date.today()
    yesterday = today - timedelta(days=1)
    valid_dates = {today.isoformat(), yesterday.isoformat()}

    updated: int = 0
    failed:  list[str] = []
    skipped: list[str] = []

    # Universe tickers + EGX30 index
    targets = [(t, _yf_symbol(t)) for t in universe] + [("EGX30", EGX30_SYMBOL)]

    for short, sym in targets:
        try:
            df = yf.download(
                sym,
                period="1d",
                interval="1d",
                progress=False,
                auto_adjust=True,
            )

            if df.empty:
                logger.warning("collect_today: empty response for %s (%s)", short, sym)
                failed.append(short)
                continue

            # Normalize columns — newer yfinance may return MultiIndex
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.columns = [str(c).capitalize() for c in df.columns]

            latest   = df.iloc[-1]
            row_date = df.index[-1].date().isoformat()

            if row_date not in valid_dates:
                logger.info(
                    "collect_today: %s — date %s outside window %s, skipping",
                    short, row_date, valid_dates,
                )
                skipped.append(short)
                continue

            close_val = _safe_float(latest.get("Close"))
            if close_val is None:
                logger.warning("collect_today: NaN close for %s — skipping", short)
                skipped.append(short)
                continue

            open_val   = _safe_float(latest.get("Open"))
            high_val   = _safe_float(latest.get("High"))
            low_val    = _safe_float(latest.get("Low"))
            volume_val = _safe_int(latest.get("Volume"))

            conn = get_connection()
            try:
                cur = conn.execute(
                    """
                    INSERT OR REPLACE INTO prices
                        (ticker, date, open, high, low, close, volume, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'yfinance')
                    """,
                    (short, row_date, open_val, high_val, low_val,
                     close_val, volume_val),
                )
                conn.commit()
            finally:
                conn.close()

            if cur.rowcount > 0:
                updated += 1
                logger.info(
                    "collect_today: inserted %s %s  close=%.2f",
                    short, row_date, close_val,
                )
            else:
                skipped.append(short)
                logger.debug(
                    "collect_today: %s %s already exists — skipped",
                    short, row_date,
                )

        except Exception as exc:
            logger.error(
                "collect_today: error fetching %s (%s): %s", short, sym, exc
            )
            failed.append(short)

    # ── Log the run ───────────────────────────────────────────────────────────
    try:
        from backend.data.db import get_connection as _gc
        conn = _gc()
        conn.execute(
            """
            INSERT INTO collection_log (run_at, tickers_ok, tickers_fail, notes)
            VALUES (?, ?, ?, ?)
            """,
            (
                datetime.now().isoformat(),
                updated,
                len(failed),
                json.dumps({"failed": failed, "skipped": skipped}),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.error("collect_today: failed to write collection_log: %s", exc)

    logger.info(
        "collect_today complete — updated=%d  failed=%d  skipped=%d",
        updated, len(failed), len(skipped),
    )
    return {"updated": updated, "failed": failed, "skipped": skipped}


def get_last_collection() -> tuple[Optional[str], int]:
    """
    Return (run_at_iso_string, tickers_ok) from the most recent run.
    Returns (None, 0) if no collection has been run yet.
    """
    from backend.data.db import get_connection

    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT run_at, tickers_ok
            FROM   collection_log
            ORDER  BY id DESC
            LIMIT  1
            """
        ).fetchone()
    except Exception:
        return None, 0
    finally:
        conn.close()

    if row is None:
        return None, 0
    return str(row["run_at"]), int(row["tickers_ok"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(val) -> Optional[float]:
    """Return float or None for NaN / None / unconvertible values."""
    try:
        v = float(val)
        return None if v != v else v   # v != v is True only for NaN
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> Optional[int]:
    try:
        v = float(val)
        return None if v != v else int(v)
    except (TypeError, ValueError):
        return None


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    from dotenv import load_dotenv
    load_dotenv()
    from backend.data.db import init_db
    init_db()
    result = collect_today()
    print(result)
    sys.exit(0 if not result["failed"] else 1)

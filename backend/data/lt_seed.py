"""
backend/data/lt_seed.py

One-time seed script — imports all historical LT wallet transactions and
inflation data from the Google Sheet into SQLite.

Run once from the project root:
    python -m backend.data.lt_seed

Safe to re-run: transactions are appended (no UNIQUE constraint on
lt_transactions), but running twice will double-count. Clear the table first
if you need to re-seed:
    DELETE FROM lt_transactions;
    DELETE FROM lt_positions;
    DELETE FROM inflation_data;
"""
from __future__ import annotations

import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Seed data ─────────────────────────────────────────────────────────────────
# Columns: date, category, ticker, qty, price, fees, div_tax,
#          total, fx_rate, usd_val, net_wallet, ext_capital

TRANSACTIONS = [
    ("2025-04-18", "Top-Up",      None,   1,    200.0,   2.0,  0,    202.0,    50.16,      4.03,    200.0,    -202.0),
    ("2025-04-23", "Buy",         "ABUK", 3,     51.85,  3.21, 0,    158.76,   50.9353,    3.12,   -158.76,     0),
    ("2025-10-28", "Dividend",    "ABUK", 1,      5.70,  0,    0,      5.70,   47.3385,    0.12,      5.415,    0),
    ("2026-01-06", "Dividend",    "ABUK", 1,      4.28,  0,    0,      4.28,   47.2147,    0.09,      4.066,    0),
    ("2026-02-01", "Buy",         "BSB",  25,     1.846, 2.97, 0,     49.12,   47.15835,   1.04,    -49.12,     0),
    ("2026-02-03", "Subscription",None,   1,   1000.0,   0,    0,   1000.0,    47.0568,   21.25,      0,     -1000.0),
    ("2026-02-08", "Top-Up",      None,   1,   1000.0,  10.0,  0,   1010.0,    47.0206,   21.48,   1000.0,  -1010.0),
    ("2026-02-08", "Top-Up",      None,   1,  19000.0,  19.0,  0,  19019.0,    47.0206,  404.48,  19000.0, -19019.0),
    ("2026-02-09", "Buy",         "AMOC", 500,    7.22,  7.51, 0,   3617.51,   46.8257,   77.25,  -3617.51,    0),
    ("2026-02-09", "Buy",         "MICH", 133,   29.80,  7.96, 0,   3971.36,   46.8257,   84.81,  -3971.36,    0),
    ("2026-02-09", "Buy",         "ORAS",   9,  450.0,   8.07, 0,   4058.07,   46.8257,   86.66,  -4058.07,    0),
    ("2026-02-09", "Buy",         "ORWE", 168,   23.48,  7.92, 0,   3952.56,   46.8257,   84.41,  -3952.56,    0),
    ("2026-02-09", "Buy",         "SUGR",  80,   48.60,  7.86, 0,   3895.86,   46.8257,   83.20,  -3895.86,    0),
    ("2026-02-10", "Sell",        "BSB",   25,    1.828, 2.05, 0,     43.65,   46.7584,    0.93,     43.65,     0),
    ("2026-02-11", "Sell",        "ABUK",   3,   69.30,  3.26, 0,    204.64,   46.7353,    4.38,    204.64,     0),
    ("2026-02-24", "Top-Up",      None,   1,  12000.0,  12.0,  0,  12012.0,    47.9706,  250.40,  12000.0, -12012.0),
    ("2026-02-25", "Buy",         "AMOC", 270,    7.43,  5.51, 0,   2011.61,   47.952,    41.95,  -2011.61,    0),
    ("2026-02-25", "Buy",         "MICH",  70,   30.06,  5.63, 0,   2109.83,   47.952,    44.00,  -2109.83,    0),
    ("2026-02-25", "Buy",         "MPCI",  15,  149.99,  5.80, 0,   2255.65,   47.952,    47.04,  -2255.65,    0),
    ("2026-02-25", "Buy",         "ORAS",   4,  485.0,   5.42, 0,   1945.42,   47.952,    40.57,  -1945.42,    0),
    ("2026-02-25", "Buy",         "ORWE",  95,   22.95,  5.73, 0,   2185.98,   47.952,    45.59,  -2185.98,    0),
    ("2026-02-25", "Buy",         "SWDY",  25,   79.89,  5.50, 0,   2002.75,   47.952,    41.77,  -2002.75,    0),
    ("2026-03-01", "Buy",         "SWDY",   3,   75.90,  3.28, 0,    230.98,   47.71315,   4.84,   -230.98,    0),
    ("2026-03-02", "Top-Up",      None,   1,  12000.0,  12.0,  0,  12012.0,    49.302,   243.64,  12000.0, -12012.0),
    ("2026-03-03", "Buy",         "MPCI",  82,  144.985,18.84, 0,  11907.61,   49.8495,  238.87, -11907.61,    0),
    ("2026-03-04", "Dividend",    "ABUK",   1,    7.13,  0,    0,      7.13,   50.1574,    0.14,      6.774,   0),
    ("2026-03-29", "Top-Up",      None,   1,  12000.0,  12.0,  0,  12012.0,    52.7,     227.93,  12000.0, -12012.0),
    ("2026-03-30", "Buy",         "OLFI", 365,   21.13, 12.64, 0,   7725.09,   52.7,     146.59,  -7725.09,    0),
    ("2026-03-30", "Buy",         "ORWE",  71,   22.17,  4.97, 0,   1579.04,   52.7,      29.96,  -1579.04,    0),
    ("2026-03-30", "Buy",         "ORAS",   5,  477.0,   5.99, 0,   2390.99,   52.7,      45.37,  -2390.99,    0),
    ("2026-04-14", "Top-Up",      None,   1,  20000.0,  20.0,  0,  20020.0,    52.44,    381.77,  20000.0, -20020.0),
    ("2026-04-15", "Buy",         "AMOC",1150,    8.28, 14.90, 0,   9536.90,   None,      None,   -9536.90,    0),
    ("2026-04-15", "Buy",         "OLFI", 130,   20.97,  6.41, 0,   2732.51,   None,      None,   -2732.51,    0),
    ("2026-04-15", "Buy",         "MICH", 100,   36.00,  7.50, 0,   3607.50,   None,      None,   -3607.50,    0),
    ("2026-04-15", "Buy",         "ORWE", 150,   22.97,  7.30, 0,   3452.80,   None,      None,   -3452.80,    0),
]

INFLATION = [
    ("Nov 2025", 0.003, 1.003000, 0.3000),
    ("Dec 2025", 0.002, 1.005006, 0.5006),
    ("Jan 2026", 0.012, 1.017066, 1.7066),
    ("Feb 2026", 0.028, 1.045544, 4.5544),
    ("Mar 2026", 0.032, 1.079001, 7.9001),
]


# ── Runner ────────────────────────────────────────────────────────────────────

def seed() -> None:
    from dotenv import load_dotenv
    load_dotenv()

    from backend.data.db import init_db
    init_db()

    from backend.data.lt_db import (
        insert_transaction,
        insert_inflation,
        recalculate_positions,
        get_positions,
        get_portfolio_summary,
    )

    # ── Transactions ──────────────────────────────────────────────────────────
    logger.info("Seeding %d transactions…", len(TRANSACTIONS))
    for row in TRANSACTIONS:
        (date_str, category, ticker, qty, price, fees,
         div_tax, total, fx_rate, usd_val, net_wallet, ext_capital) = row
        insert_transaction(
            date_str=date_str,
            category=category,
            ticker=ticker,
            quantity=qty,
            fulfillment_price=price,
            fees=fees,
            dividend_tax=div_tax,
            total_amount=total,
            fx_rate=fx_rate,
            usd_value=usd_val,
            net_wallet_impact=net_wallet,
            external_capital_impact=ext_capital,
        )
    logger.info("Transactions seeded.")

    # ── Inflation ─────────────────────────────────────────────────────────────
    logger.info("Seeding %d inflation rows…", len(INFLATION))
    for month_year, mom, idx, pct in INFLATION:
        insert_inflation(month_year, mom, idx, pct)
    logger.info("Inflation seeded.")

    # ── Rebuild positions ─────────────────────────────────────────────────────
    logger.info("Recalculating positions from transactions…")
    recalculate_positions()

    # ── Summary ───────────────────────────────────────────────────────────────
    summary = get_portfolio_summary()
    print("\n── Portfolio Summary ─────────────────────────────────────────")
    for k, v in summary.items():
        print(f"  {k:<25} {v:>12,.2f}")

    positions = get_positions()
    print("\n── Positions ─────────────────────────────────────────────────")
    print(positions.to_string(index=False))
    print()


if __name__ == "__main__":
    seed()
    sys.exit(0)

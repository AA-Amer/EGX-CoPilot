"""
backend/data/lt_db.py

All database operations for the Long-Term Wallet.
Reads/writes lt_transactions, lt_positions, lt_signals, and inflation_data.
"""
from __future__ import annotations

import logging
from collections import deque
from datetime import date, datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _quarter(d: date) -> str:
    return f"Q{(d.month - 1) // 3 + 1}"


# ── Transactions ──────────────────────────────────────────────────────────────

def insert_transaction(
    date_str: str,
    category: str,
    ticker: Optional[str],
    quantity: float,
    fulfillment_price: float,
    fees: float,
    dividend_tax: float,
    total_amount: float,
    fx_rate: Optional[float],
    usd_value: Optional[float],
    net_wallet_impact: float,
    external_capital_impact: float,
    notes: str = "",
) -> int:
    """
    Insert a row into lt_transactions.
    Auto-calculates: actual_price_per_share, year, quarter.
    Returns the new row id.
    """
    from backend.data.db import get_connection

    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    yr = d.year
    q  = _quarter(d)

    # all-in cost per share (for Buy: includes fees; for Sell: net received per share)
    actual_pps: Optional[float] = None
    if quantity and quantity > 0:
        actual_pps = total_amount / quantity

    conn = get_connection()
    try:
        cur = conn.execute(
            """
            INSERT INTO lt_transactions
                (date, category, ticker, quantity, fulfillment_price, fees,
                 dividend_tax, actual_price_per_share, total_amount,
                 year, quarter, fx_rate, usd_value,
                 net_wallet_impact, external_capital_impact, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (date_str, category, ticker, quantity, fulfillment_price, fees,
             dividend_tax, actual_pps, total_amount, yr, q,
             fx_rate, usd_value, net_wallet_impact, external_capital_impact, notes),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_transactions(
    ticker: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Return recent transactions, optionally filtered by ticker and/or category.
    Sorted by date DESC, id DESC (newest first).
    """
    from backend.data.db import get_connection

    clauses: list[str] = []
    params:  list      = []

    if ticker:
        clauses.append("ticker = ?")
        params.append(ticker.upper())
    if category:
        clauses.append("category = ?")
        params.append(category)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)

    conn = get_connection()
    try:
        df = pd.read_sql_query(
            f"""
            SELECT id, date, category, ticker, quantity, fulfillment_price,
                   fees, actual_price_per_share, total_amount,
                   fx_rate, usd_value, net_wallet_impact, external_capital_impact, notes
            FROM   lt_transactions
            {where}
            ORDER  BY date DESC, id DESC
            LIMIT  ?
            """,
            conn,
            params=params,
        )
    except Exception as exc:
        logger.error("get_transactions: %s", exc)
        return pd.DataFrame()
    finally:
        conn.close()
    return df


# ── Position recalculation (FIFO) ─────────────────────────────────────────────

def recalculate_positions() -> None:
    """
    Rebuild lt_positions from scratch using all lt_transactions rows.

    Algorithm (per ticker):
    - Buy  → push lot (shares, actual_price_per_share) onto FIFO queue
    - Sell → pop lots from queue front, accumulate realized P/L
    - Dividend → accumulate net_wallet_impact into dividends_net

    Upserts each ticker result into lt_positions.
    Tickers with 0 remaining shares get status='Closed'.
    """
    from backend.data.db import get_connection

    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT date, category, ticker, quantity,
                   actual_price_per_share, fulfillment_price, net_wallet_impact
            FROM   lt_transactions
            WHERE  ticker IS NOT NULL
              AND  category IN ('Buy', 'Sell', 'Dividend')
            ORDER  BY date ASC, id ASC
            """
        ).fetchall()
    finally:
        conn.close()

    # Group transactions by ticker
    by_ticker: dict[str, list] = {}
    for r in rows:
        t = r["ticker"].upper()
        by_ticker.setdefault(t, []).append(r)

    today_str = date.today().isoformat()

    conn = get_connection()
    try:
        # Wipe and rebuild
        conn.execute("DELETE FROM lt_positions")

        for ticker, txns in by_ticker.items():
            lots: deque[list] = deque()   # each entry: [shares, cost_per_share]
            realized_pl   = 0.0
            dividends_net = 0.0

            for tx in txns:
                cat = tx["category"]
                qty = float(tx["quantity"] or 0)
                pps = (
                    float(tx["actual_price_per_share"])
                    if tx["actual_price_per_share"] is not None
                    else float(tx["fulfillment_price"] or 0)
                )

                if cat == "Buy":
                    lots.append([qty, pps])

                elif cat == "Sell":
                    remaining = qty
                    while remaining > 1e-9 and lots:
                        lot_qty, lot_cost = lots[0]
                        if lot_qty <= remaining + 1e-9:
                            # consume entire lot
                            realized_pl += (pps - lot_cost) * lot_qty
                            remaining   -= lot_qty
                            lots.popleft()
                        else:
                            # partial lot
                            realized_pl += (pps - lot_cost) * remaining
                            lots[0][0]  -= remaining
                            remaining    = 0.0

                elif cat == "Dividend":
                    net = tx["net_wallet_impact"]
                    dividends_net += float(net) if net is not None else 0.0

            # Summarise remaining lots
            total_shares   = sum(q for q, _ in lots)
            total_cost_net = sum(q * c for q, c in lots)
            weighted_avg   = total_cost_net / total_shares if total_shares > 1e-9 else 0.0
            status         = "Open" if total_shares > 1e-9 else "Closed"

            conn.execute(
                """
                INSERT OR REPLACE INTO lt_positions
                    (ticker, total_shares, total_cost_net, weighted_avg_cost,
                     realized_pl, dividends_net, status, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ticker, round(total_shares, 6), round(total_cost_net, 4),
                 round(weighted_avg, 6), round(realized_pl, 4),
                 round(dividends_net, 4), status, today_str),
            )

        conn.commit()
        logger.info("recalculate_positions: rebuilt %d ticker rows", len(by_ticker))
    except Exception as exc:
        logger.error("recalculate_positions: %s", exc)
        conn.rollback()
    finally:
        conn.close()


# ── Positions read ────────────────────────────────────────────────────────────

def get_positions() -> pd.DataFrame:
    """
    Return lt_positions joined with the latest price from the prices table.

    Columns: ticker, total_shares, weighted_avg_cost, current_price,
             market_value, unrealized_pl, unrealized_pct,
             realized_pl, dividends_net, total_return, allocation_pct, status
    """
    from backend.data.db import get_connection

    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT
                p.ticker,
                p.total_shares,
                p.total_cost_net,
                p.weighted_avg_cost,
                p.realized_pl,
                p.dividends_net,
                p.status,
                pr.close AS current_price
            FROM lt_positions p
            LEFT JOIN (
                SELECT px.ticker, px.close
                FROM   prices px
                INNER JOIN (
                    SELECT ticker, MAX(date) AS max_date
                    FROM   prices
                    GROUP  BY ticker
                ) lp ON px.ticker = lp.ticker AND px.date = lp.max_date
            ) pr ON pr.ticker = p.ticker
            ORDER BY p.total_cost_net DESC
            """,
            conn,
        )
    except Exception as exc:
        logger.error("get_positions: %s", exc)
        return pd.DataFrame()
    finally:
        conn.close()

    if df.empty:
        return df

    # Derived columns
    df["market_value"] = df.apply(
        lambda r: r["total_shares"] * r["current_price"]
        if r["current_price"] is not None else None,
        axis=1,
    )
    df["unrealized_pl"] = df.apply(
        lambda r: r["market_value"] - r["total_cost_net"]
        if r["market_value"] is not None else None,
        axis=1,
    )
    df["unrealized_pct"] = df.apply(
        lambda r: (r["unrealized_pl"] / r["total_cost_net"] * 100)
        if (r["unrealized_pl"] is not None and r["total_cost_net"] > 0) else None,
        axis=1,
    )
    df["total_return"] = df.apply(
        lambda r: (r["unrealized_pl"] or 0) + r["realized_pl"] + r["dividends_net"],
        axis=1,
    )

    total_mv = df["market_value"].sum()
    df["allocation_pct"] = df["market_value"].apply(
        lambda v: round(v / total_mv * 100, 1) if (total_mv and v is not None) else None
    )

    # Round for display
    for col in ["weighted_avg_cost", "current_price", "market_value",
                "unrealized_pl", "unrealized_pct", "realized_pl",
                "dividends_net", "total_return", "allocation_pct"]:
        if col in df.columns:
            df[col] = df[col].round(2)

    return df[[
        "ticker", "total_shares", "weighted_avg_cost", "current_price",
        "market_value", "unrealized_pl", "unrealized_pct",
        "realized_pl", "dividends_net", "total_return",
        "allocation_pct", "status",
    ]]


def get_portfolio_summary() -> dict:
    """
    Return aggregate metrics for the LT portfolio.

    Keys: total_market_value, total_cost, unrealized_pl, unrealized_pct,
          realized_pl, dividends_total, total_return, total_return_pct
    """
    df = get_positions()
    if df.empty:
        return {
            "total_market_value": 0, "total_cost": 0,
            "unrealized_pl": 0,      "unrealized_pct": 0,
            "realized_pl": 0,        "dividends_total": 0,
            "total_return": 0,       "total_return_pct": 0,
        }

    from backend.data.db import get_connection
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT SUM(total_cost_net) AS tc, SUM(realized_pl) AS rpl, SUM(dividends_net) AS div FROM lt_positions"
        ).fetchone()
    finally:
        conn.close()

    total_cost    = float(row["tc"]  or 0)
    realized_pl   = float(row["rpl"] or 0)
    dividends     = float(row["div"] or 0)
    total_mv      = df["market_value"].dropna().sum()
    unrealized_pl = total_mv - total_cost
    unrealized_pct = (unrealized_pl / total_cost * 100) if total_cost > 0 else 0
    total_return   = unrealized_pl + realized_pl + dividends
    total_return_pct = (total_return / total_cost * 100) if total_cost > 0 else 0

    return {
        "total_market_value": round(total_mv, 2),
        "total_cost":         round(total_cost, 2),
        "unrealized_pl":      round(unrealized_pl, 2),
        "unrealized_pct":     round(unrealized_pct, 2),
        "realized_pl":        round(realized_pl, 2),
        "dividends_total":    round(dividends, 2),
        "total_return":       round(total_return, 2),
        "total_return_pct":   round(total_return_pct, 2),
    }


# ── Signals ───────────────────────────────────────────────────────────────────

# All valid columns in lt_signals (excluding id, run_date, ticker)
_SIGNAL_COLS = {
    "avg_cost", "price", "signal", "action", "score",
    "position_size_pct", "current_allocation_pct", "recommended_shares",
    "recommended_capital", "suggested_buy_price", "profit_pct", "sell_price",
    "fib_zone", "swing_high", "swing_low", "target_1m", "target_6m", "target_12m",
    "exp_return_1m", "exp_return_6m", "exp_return_12m", "forecast_confidence",
    "description",
}


def insert_signal(run_date, ticker, avg_cost, price, signal, action, score,
                  position_size_pct, current_allocation_pct, recommended_shares,
                  recommended_capital, suggested_buy_price, profit_pct, sell_price,
                  fib_zone, swing_high, swing_low, target_1m, target_6m, target_12m,
                  exp_return_1m, exp_return_6m, exp_return_12m,
                  forecast_confidence, description,
                  deploy_pct=0, deploy_label=None, deploy_note=None, deploy_tier=None,
                  enhanced_json=None):
    """INSERT OR REPLACE a signal row for (run_date, ticker)."""
    from backend.data.db import get_connection

    conn = get_connection()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO lt_signals (
                run_date, ticker, avg_cost, price, signal, action, score,
                position_size_pct, current_allocation_pct, recommended_shares,
                recommended_capital, suggested_buy_price, profit_pct, sell_price,
                fib_zone, swing_high, swing_low, target_1m, target_6m, target_12m,
                exp_return_1m, exp_return_6m, exp_return_12m,
                forecast_confidence, description,
                deploy_pct, deploy_label, deploy_note, deploy_tier,
                enhanced_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (run_date, ticker, avg_cost, price, signal, action, score,
              position_size_pct, current_allocation_pct, recommended_shares,
              recommended_capital, suggested_buy_price, profit_pct, sell_price,
              fib_zone, swing_high, swing_low, target_1m, target_6m, target_12m,
              exp_return_1m, exp_return_6m, exp_return_12m,
              forecast_confidence, description,
              deploy_pct, deploy_label, deploy_note, deploy_tier,
              enhanced_json))
        conn.commit()
    finally:
        conn.close()


def get_latest_signals() -> pd.DataFrame:
    """Return the most recent signal row per ticker (latest run_date)."""
    from backend.data.db import get_connection

    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT s.*
            FROM   lt_signals s
            INNER JOIN (
                SELECT ticker, MAX(run_date) AS max_date
                FROM   lt_signals
                GROUP  BY ticker
            ) latest ON s.ticker = latest.ticker AND s.run_date = latest.max_date
            ORDER  BY s.ticker
            """,
            conn,
        )
    except Exception as exc:
        logger.error("get_latest_signals: %s", exc)
        return pd.DataFrame()
    finally:
        conn.close()
    return df


# ── Inflation data ────────────────────────────────────────────────────────────

def insert_inflation(
    month_year: str,
    headline_mom: float,
    cumulative_index: float,
    cumulative_pct: float,
) -> None:
    """INSERT OR REPLACE a row into inflation_data."""
    from backend.data.db import get_connection

    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO inflation_data
                (month_year, headline_mom, cumulative_index, cumulative_pct)
            VALUES (?, ?, ?, ?)
            """,
            (month_year, headline_mom, cumulative_index, cumulative_pct),
        )
        conn.commit()
    finally:
        conn.close()


def get_latest_inflation_index() -> Optional[float]:
    """Return the most recent cumulative_index value, or None if no data."""
    from backend.data.db import get_connection

    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT cumulative_index FROM inflation_data ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    return float(row["cumulative_index"]) if row else None


def get_transaction_by_id(transaction_id: int) -> Optional[dict]:
    """Return a single transaction as a dict, or None if not found."""
    from backend.data.db import get_connection

    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM lt_transactions WHERE id = ?",
            conn,
            params=(transaction_id,),
        )
    finally:
        conn.close()

    if df.empty:
        return None
    return df.iloc[0].to_dict()


def update_transaction(
    transaction_id: int,
    date: str,
    category: str,
    ticker: Optional[str],
    quantity: float,
    fulfillment_price: float,
    fees: float,
    dividend_tax: float,
    total_amount: float,
    fx_rate: Optional[float],
    usd_value: Optional[float],
    net_wallet_impact: float,
    external_capital_impact: float,
    notes: str = "",
) -> None:
    """
    UPDATE an existing lt_transactions row by ID.
    Re-derives actual_price_per_share, year, and quarter from the new values,
    then calls recalculate_positions() to rebuild lt_positions.
    """
    from backend.data.db import get_connection

    actual_price = (total_amount / quantity) if quantity and quantity > 0 else 0.0
    dt           = pd.to_datetime(date)
    yr           = int(dt.year)
    q            = f"Q{((int(dt.month) - 1) // 3) + 1}"

    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE lt_transactions SET
                date=?, category=?, ticker=?, quantity=?, fulfillment_price=?,
                fees=?, dividend_tax=?, actual_price_per_share=?, total_amount=?,
                year=?, quarter=?, fx_rate=?, usd_value=?,
                net_wallet_impact=?, external_capital_impact=?, notes=?
            WHERE id=?
            """,
            (str(date), category, ticker, quantity, fulfillment_price,
             fees, dividend_tax, actual_price, total_amount,
             yr, q, fx_rate, usd_value,
             net_wallet_impact, external_capital_impact, notes,
             transaction_id),
        )
        conn.commit()
    finally:
        conn.close()

    recalculate_positions()


def get_all_inflation() -> pd.DataFrame:
    """Return all rows from inflation_data ordered oldest-first."""
    from backend.data.db import get_connection

    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT month_year, headline_mom, cumulative_index, cumulative_pct "
            "FROM inflation_data ORDER BY rowid ASC",
            conn,
        )
    except Exception as exc:
        logger.error("get_all_inflation: %s", exc)
        return pd.DataFrame()
    finally:
        conn.close()
    return df


# ── Wallet & KPI summaries ────────────────────────────────────────────────────

def get_wallet_summary() -> dict:
    """
    Compute wallet-level financial summary from lt_transactions.

    Returns:
        total_invested:  abs sum of Top-Up + Subscription net_wallet_impact
        wallet_balance:  running cash balance (sum of all net_wallet_impact)
        total_fees:      sum of all fees paid
        total_dividends: sum of Dividend net_wallet_impact
        realized_pl:     sum of realized_pl from lt_positions
    """
    from backend.data.db import get_connection

    conn = get_connection()
    try:
        tx_row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN category IN ('Top-Up', 'Subscription')
                         THEN ABS(net_wallet_impact) ELSE 0 END) AS total_invested,
                SUM(net_wallet_impact)                             AS wallet_balance,
                SUM(COALESCE(fees, 0))                             AS total_fees,
                SUM(CASE WHEN category = 'Dividend'
                         THEN net_wallet_impact ELSE 0 END)        AS total_dividends
            FROM lt_transactions
            """
        ).fetchone()
        pos_row = conn.execute(
            "SELECT SUM(realized_pl) AS rpl FROM lt_positions"
        ).fetchone()
    except Exception as exc:
        logger.error("get_wallet_summary: %s", exc)
        return {
            "total_invested": 0.0, "wallet_balance": 0.0,
            "total_fees": 0.0, "total_dividends": 0.0, "realized_pl": 0.0,
        }
    finally:
        conn.close()

    return {
        "total_invested":  round(float(tx_row["total_invested"]  or 0), 2),
        "wallet_balance":  round(float(tx_row["wallet_balance"]  or 0), 2),
        "total_fees":      round(float(tx_row["total_fees"]      or 0), 2),
        "total_dividends": round(float(tx_row["total_dividends"] or 0), 2),
        "realized_pl":     round(float(pos_row["rpl"]            or 0), 2),
    }


def get_kpi_summary() -> dict:
    """
    Full KPI summary combining wallet transactions + positions data.

    Returns dict with keys:
        thndr_capital, advisor_cost, total_fees, real_capital,
        wallet_balance, market_value,
        unrealized_pl, unrealized_pct, realized_pl, dividends_net,
        gross_profit, net_profit, total_return, total_return_pct,
        positions: list of {ticker, market_value, unrealized_pl, allocation_pct}
    """
    from backend.data.db import get_connection

    ws  = get_wallet_summary()
    ps  = get_portfolio_summary()
    pos = get_positions()

    # Load transactions as DataFrame for capital calculations
    conn = get_connection()
    try:
        tx = pd.read_sql_query(
            "SELECT date, category, quantity, fulfillment_price, total_amount, "
            "fees, external_capital_impact FROM lt_transactions",
            conn,
        )
    except Exception as exc:
        logger.error("get_kpi_summary tx load: %s", exc)
        tx = pd.DataFrame(columns=[
            "date", "category", "quantity", "fulfillment_price",
            "total_amount", "fees", "external_capital_impact",
        ])
    finally:
        conn.close()

    # Card 1: Thndr capital invested = Top-Up principal only (no fees)
    topups = tx[tx["category"] == "Top-Up"]
    thndr_capital = (topups["quantity"] * topups["fulfillment_price"]).sum()

    # Card 2: Advisor cost = subscription principal only (excl. bank transfer fees)
    subscriptions = tx[tx["category"] == "Subscription"]
    advisor_cost = (subscriptions["quantity"] * subscriptions["fulfillment_price"]).sum()

    # Card 3: Total fees = bank transfer fees on Top-Ups + Subscriptions only
    transfer_cats = tx[tx["category"].isin(["Top-Up", "Subscription"])]
    total_fees = transfer_cats["fees"].fillna(0).sum()

    # Card 4: Real invested capital = abs(sum of external_capital_impact)
    real_capital = abs(tx["external_capital_impact"].fillna(0).sum())

    gross_profit = ps["unrealized_pl"] + ps["realized_pl"] + ps["dividends_total"]
    net_profit   = gross_profit - total_fees

    ticker_data: list[dict] = []
    if not pos.empty:
        open_pos = pos[pos["status"] == "Open"]
        for _, r in open_pos.iterrows():
            ticker_data.append({
                "ticker":         r["ticker"],
                "market_value":   float(r["market_value"])   if r["market_value"]   is not None else 0.0,
                "unrealized_pl":  float(r["unrealized_pl"])  if r["unrealized_pl"]  is not None else 0.0,
                "allocation_pct": float(r["allocation_pct"]) if r["allocation_pct"] is not None else 0.0,
            })

    _mv = ps["total_market_value"]
    _wb = ws["wallet_balance"]

    # ── Position stats ──
    alloc_df = pd.DataFrame(ticker_data) if ticker_data else pd.DataFrame()

    total_open_positions = len(alloc_df)

    if not alloc_df.empty:
        largest_position_pct = alloc_df["allocation_pct"].max()
        top3_exposure_pct    = alloc_df.nlargest(3, "allocation_pct")["allocation_pct"].sum()

        weights   = alloc_df["allocation_pct"] / 100
        div_score = round(1 / (weights ** 2).sum(), 2) if (weights ** 2).sum() > 0 else 0

        if largest_position_pct > 30:
            concentration_risk = "High"
        elif largest_position_pct > 20:
            concentration_risk = "Moderate"
        else:
            concentration_risk = "Healthy"

        top_performer   = alloc_df.loc[alloc_df["unrealized_pl"].idxmax(), "ticker"]
        worst_performer = alloc_df.loc[alloc_df["unrealized_pl"].idxmin(), "ticker"]
    else:
        largest_position_pct = 0
        top3_exposure_pct    = 0
        div_score            = 0
        concentration_risk   = "N/A"
        top_performer        = "N/A"
        worst_performer      = "N/A"

    ticker_data = sorted(ticker_data, key=lambda x: x["allocation_pct"], reverse=True)

    total_portfolio_value = round(_mv + _wb, 2)

    # ── Money-Weighted Inflation Adjustment ──
    conn2 = get_connection()
    try:
        infl_df = pd.read_sql_query(
            "SELECT month_year, cumulative_index FROM inflation_data ORDER BY rowid ASC",
            conn2,
        )
    except Exception:
        infl_df = pd.DataFrame(columns=["month_year", "cumulative_index"])
    finally:
        conn2.close()

    current_cpi = float(infl_df.iloc[-1]["cumulative_index"]) if not infl_df.empty else 1.0
    cpi_lookup  = dict(zip(infl_df["month_year"], infl_df["cumulative_index"].astype(float)))

    def _get_cpi_for_date(date_str):
        """Return CPI index for the month of a given date (YYYY-MM-DD).
        Falls back to nearest available month, then 1.0."""
        try:
            dt  = pd.to_datetime(date_str)
            key = dt.strftime("%b %Y")
            if key in cpi_lookup:
                return cpi_lookup[key]
            dt_month  = dt.to_period("M")
            available = {}
            for k, v in cpi_lookup.items():
                try:
                    available[pd.to_datetime(k).to_period("M")] = v
                except Exception:
                    pass
            past = {p: v for p, v in available.items() if p <= dt_month}
            if past:
                return past[max(past.keys())]
            return 1.0
        except Exception:
            return 1.0

    external_tx = tx[tx["category"].isin(["Top-Up", "Subscription"])].copy()

    inflation_adjusted_capital = 0.0
    for _, row in external_tx.iterrows():
        tx_amount  = abs(float(row["external_capital_impact"] or 0))
        tx_cpi     = _get_cpi_for_date(str(row["date"]))
        adj_factor = current_cpi / tx_cpi if tx_cpi > 0 else 1.0
        inflation_adjusted_capital += tx_amount * adj_factor
    inflation_adjusted_capital = round(inflation_adjusted_capital, 2)

    lost_to_inflation = round(inflation_adjusted_capital - real_capital, 2)
    real_gain         = round(total_portfolio_value - inflation_adjusted_capital, 2)
    real_gain_pct     = round(
        (real_gain / inflation_adjusted_capital * 100) if inflation_adjusted_capital > 0 else 0, 2
    )
    nominal_return_pct = round(
        (net_profit / real_capital * 100) if real_capital > 0 else 0, 2
    )
    first_tx_cpi = (
        _get_cpi_for_date(str(external_tx["date"].min()))
        if not external_tx.empty else 1.0
    )
    total_inflation_pct = round((current_cpi / first_tx_cpi - 1) * 100, 2)
    beating_inflation   = real_gain > 0

    return {
        "thndr_capital":              round(float(thndr_capital), 2),
        "advisor_cost":               round(float(advisor_cost),  2),
        "total_fees":                 round(float(total_fees),    2),
        "real_capital":               round(float(real_capital),  2),
        "total_invested":             ws["total_invested"],
        "wallet_balance":             _wb,
        "market_value":               _mv,
        "total_market_value":         _mv,
        "total_portfolio_value":      total_portfolio_value,
        "unrealized_pl":              ps["unrealized_pl"],
        "unrealized_pct":             ps["unrealized_pct"],
        "realized_pl":                ps["realized_pl"],
        "dividends_net":              ps["dividends_total"],
        "gross_profit":               round(gross_profit, 2),
        "net_profit":                 round(net_profit, 2),
        "total_return":               ps["total_return"],
        "total_return_pct":           ps["total_return_pct"],
        "positions":                  ticker_data,
        "allocation":                 ticker_data,
        "total_open_positions":       total_open_positions,
        "largest_position_pct":       round(float(largest_position_pct), 2),
        "top3_exposure_pct":          round(float(top3_exposure_pct), 2),
        "div_score":                  div_score,
        "concentration_risk":         concentration_risk,
        "top_performer":              top_performer,
        "worst_performer":            worst_performer,
        "cpi_index":                  round(current_cpi, 6),
        "inflation_adjusted_capital": inflation_adjusted_capital,
        "lost_to_inflation":          lost_to_inflation,
        "real_gain":                  real_gain,
        "real_gain_pct":              real_gain_pct,
        "nominal_return_pct":         nominal_return_pct,
        "total_inflation_pct":        total_inflation_pct,
        "beating_inflation":          beating_inflation,
    }

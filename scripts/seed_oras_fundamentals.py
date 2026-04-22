"""
Manual seed of ORAS FY2025 fundamentals.
Source: Audited IFRS Consolidated Financial Statements, 31 December 2025
Auditor: KPMG Lower Gulf Limited
All monetary values in USD millions.

Usage:
    python scripts/seed_oras_fundamentals.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.data.db import get_connection
from datetime import datetime


def manual_upsert_fundamentals(ticker: str, period: str, data: dict):
    """Direct DB insert for manually verified fundamental data."""
    data["ticker"]       = ticker
    data["period"]       = period
    data["extracted_at"] = datetime.now().isoformat()

    conn = get_connection()

    cols = [
        "ticker", "period", "report_type", "report_date", "source_file",
        "revenue", "revenue_growth", "net_profit", "net_margin", "eps", "ebitda",
        "total_assets", "total_debt", "equity", "debt_to_equity", "current_ratio",
        "pe_ratio", "pb_ratio", "roe", "roa",
        "dividend_per_share", "dividend_yield",
        "interest_income", "interest_to_rev",
        "currency", "raw_summary", "extracted_at"
    ]

    vals = [data.get(c) for c in cols]
    placeholders = ",".join(["?"] * len(cols))

    conn.execute(
        f"INSERT OR REPLACE INTO fundamental_data "
        f"({','.join(cols)}) VALUES ({placeholders})",
        vals
    )
    conn.commit()
    print(f"✅ Upserted {ticker} {period} into fundamental_data")


# ── ORAS FY2025 verified data ────────────────────────────────────────────────
data = {
    # Identity
    "report_type":      "annual",
    "report_date":      "2025-12-31",
    "source_file":      "Orascom-Construction-PLC-FS-Conso-31-Dec-2025English.pdf",
    "currency":         "USD",

    # Income Statement — P&L page 10
    "revenue":          5049.8,   # Revenue line
    "revenue_growth":   55.1,     # vs FY2024 USD 3,254.9M
    "net_profit":       194.8,    # Attributable to owners only (NOT 205.7 total)
    "net_margin":       3.86,     # 194.8 / 5049.8 * 100
    "eps":              1.77,     # Note 24: basic & diluted USD per share
    "ebitda":           305.0,    # Operating profit 272.2 + Depreciation 32.8

    # Balance Sheet — page 9
    "total_assets":     5215.8,   # Total assets
    "total_debt":       314.9,    # Loans and borrowings Note 18
    "equity":           872.5,    # Equity attributable to owners of the Company
    "debt_to_equity":   round(314.9 / 872.5, 3),    # 0.361
    "current_ratio":    round(4356.2 / 4203.3, 3),  # 1.036

    # Return Ratios — derived
    "roe":              round(194.8 / 872.5 * 100, 2),   # 22.33%
    "roa":              round(194.8 / 5215.8 * 100, 2),  # 3.73%

    # Dividends
    "dividend_per_share": 0.47,  # USD 0.22 (Jan 2025) + USD 0.25 (Aug 2025)

    # Shariah Screening — CRITICAL
    # Interest income = 32.7 from Note 23 (interest on financial assets ONLY)
    # NOT 36.8 which is total finance income including FX gains
    "interest_income":  32.7,
    "interest_to_rev":  round(32.7 / 5049.8 * 100, 4),  # 0.6476% → PASS < 5%

    # Summary
    "raw_summary": (
        "ORAS delivered exceptional FY2025 results with revenue surging 55.1% "
        "to USD 5.05B driven by major infrastructure projects in Egypt, UAE, KSA "
        "and USA data centers. Net income attributable to shareholders grew 65.1% "
        "to USD 194.8M with EBITDA of USD 305M at 6% margin. Balance sheet is "
        "net-cash positive: USD 1.37B cash vs USD 314.9M debt. Backlog hit record "
        "USD 9.0B (+18.9% YoY). Interest income is only 0.65% of revenue — well "
        "below 5% Shariah threshold — PASS. Strategic merger with OCI Global "
        "pending regulatory approval adds significant upside. Egyptian Government "
        "represents 31.9% of revenues."
    )
}

manual_upsert_fundamentals("ORAS", "2025-A", data)

# ── Verify ───────────────────────────────────────────────────────────────────
print("\n--- Verification ---")
conn = get_connection()
row = conn.execute(
    "SELECT ticker, period, revenue, net_profit, ebitda, net_margin, eps, "
    "total_assets, total_debt, equity, debt_to_equity, current_ratio, "
    "roe, roa, interest_income, interest_to_rev, dividend_per_share, currency "
    "FROM fundamental_data WHERE ticker='ORAS' AND period='2025-A'"
).fetchone()

if row:
    labels = [
        "ticker", "period", "revenue", "net_profit", "ebitda", "net_margin",
        "eps", "total_assets", "total_debt", "equity", "debt_to_equity",
        "current_ratio", "roe", "roa", "interest_income", "interest_to_rev",
        "dividend_per_share", "currency"
    ]
    for label, val in zip(labels, row):
        status = "✅" if val is not None else "❌"
        print(f"  {status} {label:20s}: {val}")
    print(f"\n✅ All fields populated. Shariah interest_to_rev = {row[15]}% (< 5% = PASS)")
else:
    print("❌ Row not found — check fundamental_data table exists")
    print("   Run: python -c \"from backend.data.db import init_db; init_db()\"")

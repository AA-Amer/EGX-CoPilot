"""
Shariah compliance screener — 5 screens per AAOIFI standards.
Screen 1: Business activity exclusions.
Screen 2: Interest-bearing debt / total assets < 33%.
Screen 3: Interest income / total revenue < 5% (purification 3–5%).
Screen 4: Accounts receivable / total assets < 49%.
Screen 5: Cross-reference EGX Islamic Index.
"""


EXCLUDED_SECTORS = {
    "conventional_banking",
    "alcohol",
    "tobacco",
    "gambling",
    "weapons",
}


def screen(ticker_info: dict) -> dict:
    """
    Run all 5 Shariah screens.
    Returns: {"compliant": bool, "screens": dict, "purification_pct": float}
    """
    results = {}

    # Screen 1 — business activity
    sector = ticker_info.get("sector", "").lower()
    industry = ticker_info.get("industry", "").lower()
    results["business_activity"] = not any(ex in sector or ex in industry for ex in EXCLUDED_SECTORS)

    # Screen 2 — debt ratio
    total_debt = ticker_info.get("totalDebt", 0) or 0
    total_assets = ticker_info.get("totalAssets", 1) or 1
    debt_ratio = total_debt / total_assets
    results["debt_ratio"] = {"value": round(debt_ratio, 4), "pass": debt_ratio < 0.33}

    # Screen 3 — interest income (data rarely available from yfinance; flag as unknown)
    results["interest_income"] = {"value": None, "pass": None, "note": "manual verification required"}

    # Screen 4 — receivables ratio
    receivables = ticker_info.get("netReceivables", 0) or 0
    recv_ratio = receivables / total_assets
    results["receivables_ratio"] = {"value": round(recv_ratio, 4), "pass": recv_ratio < 0.49}

    # Screen 5 — EGX Islamic Index membership (external list, not available via yfinance)
    results["egx_islamic_index"] = {"pass": None, "note": "check EGX Islamic Index manually"}

    passing = [v["pass"] for v in results.values() if isinstance(v, dict) and v.get("pass") is not None]
    compliant = results["business_activity"] and all(passing)

    return {
        "compliant": compliant,
        "screens": results,
        "purification_pct": 0.0,  # TODO: compute from interest income ratio
    }

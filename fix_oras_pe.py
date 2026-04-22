"""
Fix ORAS P/E — still showing 13x instead of 9x (Industrials)
Root cause: lt_signals has cached enhanced_json with old P/E
Run from project root: python fix_oras_pe.py
"""
import sys, json
sys.path.insert(0, '.')

from backend.data.db import get_connection
conn = get_connection()

# ── Step 1: Verify sector in watchlist ──────────────────────────────
print("=== STEP 1: WATCHLIST SECTOR ===")
row = conn.execute(
    "SELECT ticker, sector FROM watchlist WHERE ticker='ORAS'"
).fetchone()
if row:
    print(f"  Current sector: {tuple(row)}")
    if row[1] != 'Industrials':
        conn.execute(
            "UPDATE watchlist SET sector='Industrials' WHERE ticker='ORAS'"
        )
        conn.commit()
        print("  FIXED: sector updated to Industrials")
    else:
        print("  OK: sector is already Industrials")
else:
    print("  ERROR: ORAS not found in watchlist")

# ── Step 2: Show what P/E is currently cached ────────────────────────
print("\n=== STEP 2: CACHED SIGNAL (before fix) ===")
row = conn.execute(
    "SELECT enhanced_json, RUN_DATE FROM lt_signals "
    "WHERE ticker='ORAS' ORDER BY RUN_DATE DESC LIMIT 1"
).fetchone()
if row and row[0]:
    data = json.loads(row[0])
    fv   = data.get('fair_value', {})
    print(f"  P/E used:    {fv.get('pe_used')}")
    print(f"  FV Mid:      {fv.get('fair_value_mid')}")
    print(f"  Sector used: {data.get('sector')}")
    print(f"  Updated at:  {row[1]}")
else:
    print("  No cached signal found")

# ── Step 3: Clear cached signal ──────────────────────────────────────
print("\n=== STEP 3: CLEARING CACHE ===")
conn.execute("DELETE FROM lt_signals WHERE ticker='ORAS'")
conn.commit()
print("  ORAS signal cache cleared")

# ── Step 4: Re-run signal ────────────────────────────────────────────
print("\n=== STEP 4: RE-RUNNING SIGNAL ===")
try:
    from backend.agents.longterm_agent import run_signals
    results = run_signals(tickers=['ORAS'])
    print(f"  Signal result: {results}")
except Exception as e:
    import traceback
    print(f"  ERROR: {e}")
    traceback.print_exc()

# ── Step 5: Verify new cached signal ────────────────────────────────
print("\n=== STEP 5: VERIFY NEW SIGNAL ===")
row = conn.execute(
    "SELECT enhanced_json, RUN_DATE FROM lt_signals "
    "WHERE ticker='ORAS' ORDER BY RUN_DATE DESC LIMIT 1"
).fetchone()
if row and row[0]:
    data = json.loads(row[0])
    fv   = data.get('fair_value', {})
    pe   = fv.get('pe_used')
    fv_mid = fv.get('fair_value_mid')
    sector = data.get('sector')
    print(f"  Sector used: {sector}")
    print(f"  P/E used:    {pe}")
    print(f"  FV Mid:      {fv_mid}")
    print(f"  Updated at:  {row[1]}")

    # Assertions
    assert sector == 'Industrials', \
        f"FAIL: sector should be Industrials, got {sector}"
    assert pe == 9 or pe == 9.0, \
        f"FAIL: P/E should be 9x for Industrials, got {pe}"
    assert fv_mid and fv_mid < 1000, \
        f"FAIL: FV should be ~824, got {fv_mid} (still using wrong P/E)"
    print("\n  ALL CHECKS PASSED")
    print(f"  ORAS Fair Value is now EGP {fv_mid} at 9x P/E ✅")
else:
    print("  ERROR: No signal saved after re-run")
    print("  Check longterm_agent.py for errors")

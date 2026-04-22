"""
Fix SWDY Q1 record — wrong period_type='FY' and missing fiscal_year
Run from project root: python fix_swdy_q1.py
"""
import sys
sys.path.insert(0, '.')

from backend.data.db import get_connection

conn = get_connection()

print("=== BEFORE FIX ===")
rows = conn.execute(
    "SELECT period, period_type, fiscal_year, quarter, eps "
    "FROM fundamental_data WHERE ticker='SWDY' ORDER BY period DESC"
).fetchall()
for r in rows:
    print(" ", tuple(r))

# Fix Q1 record: wrong period_type='FY' and missing fiscal_year
conn.execute("""
    UPDATE fundamental_data
    SET period_type = 'Q1',
        fiscal_year = 2025,
        quarter     = 1
    WHERE ticker  = 'SWDY'
    AND   period  = '2025-Q1'
""")
conn.commit()
print("\n=== AFTER FIX ===")
rows = conn.execute(
    "SELECT period, period_type, fiscal_year, quarter, eps "
    "FROM fundamental_data WHERE ticker='SWDY' ORDER BY period DESC"
).fetchall()
for r in rows:
    print(" ", tuple(r))

# Verify engine picks FY correctly
print("\n=== ENGINE CHECK ===")
try:
    from backend.data.fundamental_db import get_best_fundamentals
    result = get_best_fundamentals('SWDY')
    if result:
        print("Period used:  ", result.get('_period_used'))
        print("Period label: ", result.get('_period_label'))
        print("Quality:      ", result.get('_data_quality'))
        print("Annualized:   ", result.get('_annualized'))
        print("EPS:          ", result.get('eps'))
        print("Net Profit:   ", result.get('net_profit'))
        assert result.get('_period_used') == '2025-FY', \
            f"FAIL: expected 2025-FY, got {result.get('_period_used')}"
        assert result.get('_annualized') == False, \
            "FAIL: FY should not be annualized"
        assert result.get('_data_quality') == 'HIGH', \
            "FAIL: FY should be HIGH quality"
        print("\nALL CHECKS PASSED")
    else:
        print("ERROR: get_best_fundamentals returned None")
except ImportError:
    print("NOTE: get_best_fundamentals not yet implemented")
    print("Engine check skipped — DB fix applied correctly")

# Clear cached signal and re-run
print("\n=== REFRESHING SIGNAL ===")
conn.execute("DELETE FROM lt_signals WHERE ticker='SWDY'")
conn.commit()
print("Signal cache cleared")

try:
    from backend.agents.longterm_agent import run_signals
    results = run_signals(tickers=['SWDY'])
    print("Signal refreshed:", results)
except Exception as e:
    print(f"Signal refresh error: {e}")
    print("Run manually: python scripts/refresh_signals.py")

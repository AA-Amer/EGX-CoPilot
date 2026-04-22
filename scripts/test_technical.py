"""Quick test for technical.py — run with: python scripts/test_technical.py"""
from dotenv import load_dotenv
load_dotenv()

from backend.analysis.technical import get_signal_snapshot, get_swing_score, get_fibonacci_levels

TEST_TICKERS = ["AMOC", "ORAS", "MICH", "SWDY"]

for ticker in TEST_TICKERS:
    print(f"\n{'='*50}")
    print(f"  {ticker}")
    print(f"{'='*50}")

    snap = get_signal_snapshot(ticker)
    if not snap:
        print(f"  ERROR: No data for {ticker}")
        continue

    print(f"  Close:        EGP {snap['close']}")
    print(f"  RSI:          {snap['rsi']}  ({snap['rsi_zone']})")
    print(f"  EMA9/21:      {'BULLISH' if snap['ema9_above_21'] else 'BEARISH'}")
    print(f"  MACD hist:    {snap['macd_histogram']}")
    print(f"  Vol ratio:    {snap['vol_ratio']}x")
    print(f"  BB position:  {snap['bb_pct']}")
    print(f"  Fib zone:     {snap.get('current_zone')}")
    print(f"  Support:      EGP {snap['nearest_support']}")
    print(f"  Resistance:   EGP {snap['nearest_resistance']}")
    print(f"  Target 1M:    EGP {snap['target_1m']}")
    print(f"  ATR:          {snap['atr']}")
    print(f"  Stop (1.5x):  EGP {snap['stop_loss_15x']}")
    print(f"  Data rows:    {snap['data_rows']} ({snap['date_from']} → {snap['date_to']})")

    score = get_swing_score(ticker)
    print(f"\n  Swing Score:  {score['total_score']}/100  {'✅ TRADEABLE' if score['tradeable'] else '❌ BELOW THRESHOLD'}")
    for k, v in score["scores"].items():
        print(f"    {k:<10} {v}")

print(f"\n{'='*50}")
print("  Done.")
print(f"{'='*50}\n")

"""Test longterm_agent — run with: python -m scripts.test_longterm_agent"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from backend.agents.longterm_agent import run_signals

print("Running signal analysis for all open positions...")
print("This calls Groq LLM — may take 30-60 seconds.\n")

results = run_signals()

for ticker, data in results.items():
    print(f"\n{'='*50}")
    print(f"  {ticker}")
    print(f"{'='*50}")
    if "error" in data:
        print(f"  ERROR: {data['error']}")
        continue
    print(f"  Signal:      {data.get('signal')}")
    print(f"  Action:      {data.get('action')}")
    print(f"  Score:       {data.get('score')}/100")
    print(f"  Confidence:  {data.get('forecast_confidence')}")
    print(f"  Target 1M:   EGP {data.get('target_1m')}")
    print(f"  Target 6M:   EGP {data.get('target_6m')}")
    print(f"  Target 12M:  EGP {data.get('target_12m')}")
    print(f"  Description: {data.get('description')}")

print(f"\n{'='*50}")
print("  Done.")
print(f"{'='*50}\n")

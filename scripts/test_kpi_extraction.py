"""
Diagnostic script — run directly to see exactly what LLM receives and returns.
Usage: python scripts/test_kpi_extraction.py
"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from backend.analysis.pdf_processor import extract_text_from_pdf, build_extraction_text

# Most likely location:
PDF_PATH = r"C:\Users\Abdelrahman\Downloads\Orascom-Construction-PLC-FS-Conso-31-Dec-2025English.pdf"
# ↑ UPDATE THIS PATH

print("=" * 60)
print("STEP 1 — Extracting pages...")
pages = extract_text_from_pdf(PDF_PATH)
print(f"Total pages extracted: {len(pages)}")
for p in pages:
    print(f"  Page {p['page_num']:3d} | method: {p['method']:20s} | chars: {len(p['text'])}")

print("\n" + "=" * 60)
print("STEP 2 — Building text sample for LLM...")
sample = build_extraction_text(pages, max_chars=12000)
print(f"Sample length: {len(sample)} chars")
print("\n--- SAMPLE CONTENT (first 3000 chars) ---")
print(sample[:3000])
print("\n--- SAMPLE CONTENT (last 1000 chars) ---")
print(sample[-1000:])

print("\n" + "=" * 60)
print("STEP 3 — Checking for key financial terms in sample...")
keywords = [
    "total assets", "5,215", "5215",
    "net profit", "194.8",
    "total equity", "872",
    "loans and borrowings", "314",
    "current assets", "4,356",
    "revenue", "5,049",
    "earnings per share", "1.77"
]
for kw in keywords:
    found = kw.lower() in sample.lower()
    status = "✅" if found else "❌"
    print(f"  {status} '{kw}'")

print("\n--- PAGE 11 STATUS ---")
page_11 = next((p for p in pages if p["page_num"] == 11), None)
if page_11:
    print(f"Page 11 found: method={page_11['method']}, chars={len(page_11['text'])}")
    print(f"Content preview: {page_11['text'][:500]}")
else:
    print("❌ Page 11 NOT in extracted pages — this is the balance sheet page!")
    print("This is why total_assets, equity, total_debt are all null.")
    print("Fix: lower MIN_TEXT_LENGTH or force-include by page number.")

print("\n" + "=" * 60)
print("STEP 4 — Calling LLM and capturing raw response...")

prompt = f"""
You are a financial analyst. Extract KPIs from this financial report.
Company: ORAS, Period: FY2025

REPORT TEXT:
{sample}

Return ONLY a valid JSON object with these exact keys:
{{
  "revenue": <number or null>,
  "net_profit": <number or null>,
  "ebitda": <number or null>,
  "total_assets": <number or null>,
  "total_debt": <number or null>,
  "equity": <number or null>,
  "eps": <number or null>,
  "interest_income": <number or null>,
  "dividend_per_share": <number or null>
}}

All values in USD millions. Use comma-separated numbers as-is (5,049.8 = 5049.8).
"""

import os
import inspect
from backend.data.llm_client import ask_llm

print(f"ask_llm signature: {inspect.signature(ask_llm)}")

try:
    raw = ask_llm(
        system_message="You are a financial analyst. Return only valid JSON.",
        user_message=prompt
    )
except TypeError:
    try:
        raw = ask_llm(prompt)
    except TypeError:
        raw = ask_llm(task="signal_scoring", user_message=prompt)

print(f"\nRAW LLM RESPONSE ({len(raw)} chars):")
print(raw)

print("\n" + "=" * 60)
print("STEP 5 — Parsing response...")
try:
    clean = raw.strip()
    if "```" in clean:
        for part in clean.split("```"):
            part = part.strip().lstrip("json").strip()
            if part.startswith("{"):
                clean = part
                break
    start = clean.find("{")
    end = clean.rfind("}") + 1
    parsed = json.loads(clean[start:end])
    print("Parsed successfully:")
    for k, v in parsed.items():
        print(f"  {k}: {v}")
except Exception as e:
    print(f"Parse error: {e}")

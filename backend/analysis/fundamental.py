"""
fundamental.py — KPI extraction from PDF text and RAG context retrieval.
Uses Groq LLM for KPI extraction, sentence-transformers for semantic search.
"""
import logging
import re
import json
from backend.data.llm_client import ask_llm
from backend.data.fundamental_db import (
    upsert_fundamentals, get_latest_fundamentals, search_chunks
)
from backend.analysis.pdf_processor import embed_query

logger = logging.getLogger(__name__)


def convert_to_egp(value: float, currency: str, fx_rate: float = None) -> float:
    """
    Convert a financial value to EGP using live FX rate.
    Falls back to stored rate if not provided.

    Args:
        value: float — monetary value in original currency
        currency: str — 'USD', 'EGP', 'EUR'
        fx_rate: float — EGP per 1 USD (from prices DB or config)

    Returns:
        float — value in EGP, same unit (millions stays millions)
    """
    if not value or currency == "EGP":
        return value

    if fx_rate is None:
        try:
            from backend.data.db import get_connection
            conn = get_connection()
            row = conn.execute(
                "SELECT close FROM prices WHERE ticker='USDFX' "
                "ORDER BY date DESC LIMIT 1"
            ).fetchone()
            conn.close()
            fx_rate = float(row[0]) if row else 51.75
        except Exception:
            fx_rate = 51.75  # safe fallback

    if currency == "USD":
        return round(value * fx_rate, 2)
    elif currency == "EUR":
        return round(value * fx_rate * 1.08, 2)  # approximate EUR/USD
    return value


def extract_kpis_from_text(ticker: str, full_text: str, period: str,
                            source_file: str = "",
                            pages: list = None) -> dict:
    """
    Use LLM to extract structured KPIs from PDF text.
    Uses smart section detection if pages list is provided.
    Does NOT call upsert — caller is responsible for saving.
    Returns the extracted KPI dict (or a minimal error dict on failure).
    """
    from backend.analysis.pdf_processor import build_extraction_text

    # ── Build optimized text sample for LLM ───────────────────────────
    MAX_CHARS = 12000  # Hard limit — Groq llama3 handles ~16K tokens safely

    if pages and len(pages) > 0:
        text_sample = build_extraction_text(pages, max_chars=MAX_CHARS)
        logger.info(f"Using smart page selection: {len(text_sample)} chars "
                    f"from {len(pages)} pages")
    else:
        # Fallback when pages not provided
        total = len(full_text)
        text_sample = (
            full_text[:4000] +
            "\n\n[...middle section...]\n\n" +
            full_text[total // 2 - 2000: total // 2 + 2000] +
            "\n\n[...end section...]\n\n" +
            full_text[-4000:]
        )
        logger.warning(f"No pages provided — using text sampling fallback: "
                       f"{len(text_sample)} chars")

    # Safety truncation — never exceed 12K no matter what
    text_sample = text_sample[:MAX_CHARS]
    logger.info(f"Final text_sample length: {len(text_sample)} chars")

    # ORAS FY2025 expected values (USD millions) for regression testing:
    # revenue          = 5049.8
    # net_profit       = 194.8   (attributable to shareholders, NOT 205.7 total)
    # ebitda           = 305.0
    # net_margin       = 3.86
    # eps              = 1.77
    # total_assets     = 5215.8
    # total_debt       = 314.9
    # equity           = 872.5
    # debt_to_equity   = 0.361
    # current_ratio    = 1.036
    # roe              = 22.3
    # roa              = 3.73
    # interest_income  = 32.7    (NOT 36.8 which is total finance income incl. FX)
    # interest_to_rev  = 0.648   (32.7 / 5049.8 * 100)
    # dividend_per_share = 0.47

    prompt = f"""You are a financial analyst extracting data from an EGX (Egyptian Stock Exchange) \
financial report. The report may be in English or Arabic or both.

Company: {ticker}
Period: {period}

IMPORTANT INSTRUCTIONS:
- Look carefully through ALL the text including tables
- Note the unit and currency used (USD, EGP, EUR) and report all values
  in that SAME native currency — do NOT convert between currencies
- If the report is in USD millions, store all values as USD millions
- If the report is in EGP millions, store all values as EGP millions
- Record the currency and unit you found in the "currency" and "unit" fields
- The document header says "$ millions" — ALL numbers are already in MILLIONS
- A value written as "5,049.8" means FIVE THOUSAND AND FORTY NINE POINT EIGHT millions
- The comma is a THOUSANDS SEPARATOR, not a decimal point
- Store the number EXACTLY as printed: 5,049.8 → store as 5049.8 (NOT 5.0498)
- Do NOT divide or convert — "$ millions" is already the unit
- Store unit as "millions" in the unit field
- Arabic numbers and text are valid — read them carefully
- If a value appears multiple times, use the most recent/consolidated figure
- For quarterly reports: annualize where noted
- Never guess — only extract values clearly stated in the text
- Return null for any value not explicitly found
- For total_assets: look for "Total assets" on the Balance Sheet /
  "Consolidated Statement of Financial Position"
- For total_debt: look for "Loans and borrowings" TOTAL (sum long-term + short-term
  + bank facilities). In Note 18 or on the balance sheet.
- For equity: look for "Equity attributable to owners of the Company"
  NOT "Total equity" (which includes non-controlling interest)
- For current_ratio: divide "Total current assets" by "Total current liabilities"
- For interest_income: use ONLY "Interest income" line, NOT total "Finance income"
  which includes FX gains. Look for Note 23 or cash flow adjustments line.
- For dividend_per_share: look for "dividends of USD X.XX per share" in the
  Directors Report or Dividends section. Sum all dividends per share paid
  during the year (interim + final). Do NOT use total dividend amount in millions.

REPORT TEXT:
{text_sample}

Return ONLY a valid JSON object. No markdown, no explanation, no preamble.
All monetary values should be in the document's native currency and unit.
Do NOT convert between currencies. Use whatever currency the document uses.

{{
  "report_type": "annual" or "quarterly",
  "report_date": "YYYY-MM-DD or null",
  "currency": "<USD or EGP or EUR — whatever the document uses>",
  "unit": "millions" or "thousands" or "billions",
  "revenue": <float in native currency millions or null>,
  "revenue_growth": <float YoY % or null>,
  "net_profit": <float — use NET PROFIT ATTRIBUTABLE TO OWNERS/SHAREHOLDERS only,
                 NOT total group net profit which includes minority interest.
                 Look for the line: "Profit attributable to: Owners of the Company"
                 or "Net profit attributable to shareholders". In native currency millions.>,
  "net_margin": <float net_profit/revenue * 100 or null>,
  "eps": <float per share in native currency or null>,
  "ebitda": <float in native currency millions or null>,
  "total_assets": <float in native currency millions or null>,
  "total_debt": <float total borrowings/loans in native currency millions or null>,
  "equity": <float equity attributable to owners (excl. non-controlling interest) in native currency millions or null>,
  "debt_to_equity": <float total_debt/equity ratio or null>,
  "current_ratio": <float current assets/current liabilities or null>,
  "pe_ratio": <float price/EPS or null>,
  "pb_ratio": <float price/book value per share or null>,
  "roe": <float net_profit/equity * 100 or null>,
  "roa": <float net_profit/total_assets * 100 or null>,
  "dividend_per_share": <float per share in native currency — sum interim + final dividends paid during the year or null>,
  "dividend_yield": <float dividend/price * 100 or null>,
  "interest_income": <float — ONLY pure "Interest income" line, NOT total finance income. In native currency millions or null>,
  "interest_to_rev": <float interest_income/revenue * 100 or null — KEY for Shariah screening>,
  "raw_summary": "<4-5 sentence summary: revenue trend, profitability, debt level, key highlights, outlook if mentioned>"
}}"""

    try:
        raw = ask_llm(
            system_prompt=(
                "You are a financial analyst specialising in Egyptian listed companies. "
                "Extract structured financial KPIs from the provided report text. "
                "Respond with valid JSON only — no markdown, no prose, no explanation."
            ),
            user_message=prompt,
            task="signal_scoring"
        )
        clean = raw.strip()

        # Strip markdown fences
        if "```" in clean:
            parts = clean.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{"):
                    clean = part
                    break

        start = clean.find("{")
        end   = clean.rfind("}") + 1
        if start != -1 and end > 0:
            clean = clean[start:end]
        clean = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', clean)

        data = json.loads(clean)
        data["source_file"] = source_file

        # Auto-calculate derived ratios if components available but ratio missing
        if data.get("net_profit") and data.get("revenue") and not data.get("net_margin"):
            data["net_margin"] = round(data["net_profit"] / data["revenue"] * 100, 2)
        if data.get("net_profit") and data.get("equity") and not data.get("roe"):
            data["roe"] = round(data["net_profit"] / data["equity"] * 100, 2)
        if data.get("net_profit") and data.get("total_assets") and not data.get("roa"):
            data["roa"] = round(data["net_profit"] / data["total_assets"] * 100, 2)
        if data.get("total_debt") and data.get("equity") and not data.get("debt_to_equity"):
            data["debt_to_equity"] = round(data["total_debt"] / data["equity"], 3)
        if data.get("interest_income") and data.get("revenue") and not data.get("interest_to_rev"):
            data["interest_to_rev"] = round(data["interest_income"] / data["revenue"] * 100, 4)

        logger.info("KPI extraction succeeded for %s / %s", ticker, period)
        return data

    except Exception as e:
        logger.error("KPI extraction failed for %s: %s", ticker, e)
        return {
            "source_file": source_file,
            "raw_summary": f"KPI extraction failed: {e}"
        }


def get_fundamental_context(ticker: str, query: str = None) -> str:
    """
    Build a formatted fundamental context string for injection into signal prompts.
    Combines structured KPIs from fundamental_data + top-k RAG chunks from fundamental_chunks.
    Returns empty string if no data available.
    """
    lines = []

    # 1. Structured KPIs
    kpis = get_latest_fundamentals(ticker)
    if kpis:
        period   = kpis.get("period", "N/A")
        rdate    = kpis.get("report_date", "N/A")
        currency = kpis.get("currency", "EGP")

        def _fmt(val, decimals=2):
            if val is None:
                return "N/A"
            try:
                return f"{float(val):.{decimals}f}"
            except (TypeError, ValueError):
                return "N/A"

        lines.append(f"Period: {period} | Report date: {rdate} | Currency: {currency}")
        lines.append(
            f"Revenue: {_fmt(kpis.get('revenue'))}M  |  "
            f"Revenue growth: {_fmt(kpis.get('revenue_growth'))}%  |  "
            f"Net profit: {_fmt(kpis.get('net_profit'))}M  |  "
            f"Net margin: {_fmt(kpis.get('net_margin'))}%"
        )
        lines.append(
            f"EPS: {_fmt(kpis.get('eps'))} {currency}  |  "
            f"EBITDA: {_fmt(kpis.get('ebitda'))}M  |  "
            f"ROE: {_fmt(kpis.get('roe'))}%  |  "
            f"ROA: {_fmt(kpis.get('roa'))}%"
        )
        lines.append(
            f"Total assets: {_fmt(kpis.get('total_assets'))}M  |  "
            f"Total debt: {_fmt(kpis.get('total_debt'))}M  |  "
            f"Equity: {_fmt(kpis.get('equity'))}M  |  "
            f"D/E ratio: {_fmt(kpis.get('debt_to_equity'))}"
        )
        lines.append(
            f"Current ratio: {_fmt(kpis.get('current_ratio'))}  |  "
            f"P/E: {_fmt(kpis.get('pe_ratio'))}  |  "
            f"P/B: {_fmt(kpis.get('pb_ratio'))}  |  "
            f"Dividend/share: {_fmt(kpis.get('dividend_per_share'))} {currency}"
        )
        if kpis.get("raw_summary"):
            lines.append(f"Summary: {kpis['raw_summary']}")

    # 2. Semantic RAG chunks
    if query:
        try:
            q_emb = embed_query(query)
            chunks = search_chunks(ticker, q_emb, top_k=3)
            if chunks:
                lines.append("")
                lines.append("Relevant excerpts from financial reports:")
                for ch in chunks:
                    sec  = ch.get("section", "general")
                    page = ch.get("page_num", "?")
                    text = ch.get("chunk_text", "").strip()[:500]
                    lines.append(f"[{sec} | page {page}] {text}")
        except Exception as e:
            logger.warning("RAG chunk search failed for %s: %s", ticker, e)

    return "\n".join(lines)

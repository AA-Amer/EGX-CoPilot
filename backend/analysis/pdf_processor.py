"""
pdf_processor.py — PDF text extraction, chunking, and embedding generation.
Uses pdfplumber for text-based pages and pytesseract OCR for image-based pages.
Model: all-MiniLM-L6-v2 (80MB, runs fully offline, no API cost)
"""
import os
import re
import logging
import numpy as np
import pdfplumber
from pathlib import Path
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

# ── Tesseract path (Windows) ──────────────────────────────────────────────────
try:
    import pytesseract
    TESSERACT_PATH = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    if os.path.exists(TESSERACT_PATH):
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH
    TESSERACT_AVAILABLE = True
except ImportError:
    TESSERACT_AVAILABLE = False
    logger.warning("pytesseract not installed — OCR fallback disabled")

MIN_TEXT_LENGTH = 50   # pages with fewer chars are treated as image-based
MIN_TABLE_CELLS = 5    # minimum cells to consider a table valid

# Load embedding model once at module level (cached after first load)
_EMBED_MODEL = None


def _get_embed_model():
    global _EMBED_MODEL
    if _EMBED_MODEL is None:
        logger.info("Loading embedding model all-MiniLM-L6-v2 (first time may take 30s)...")
        _EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    return _EMBED_MODEL


CHUNK_SIZE    = 600   # approximate tokens (characters / 4)
CHUNK_OVERLAP = 100


def _tables_to_text(tables: list) -> str:
    """Convert pdfplumber table data into readable pipe-separated text."""
    lines = []
    for table in tables:
        for row in table:
            if not row:
                continue
            cells = [str(c).strip() if c is not None else "" for c in row]
            line = " | ".join(c for c in cells if c)
            if line:
                lines.append(line)
        lines.append("")  # blank line between tables
    return "\n".join(lines)


def _count_table_cells(tables: list) -> int:
    """Count total non-empty cells across all tables on a page."""
    count = 0
    for table in tables:
        for row in table:
            if row:
                count += sum(1 for c in row if c and str(c).strip())
    return count


def _ocr_page(pdf_path: str, page_num: int) -> str:
    """
    Rasterize a single PDF page and run OCR on it.
    Returns extracted text string or empty string on failure.
    """
    if not TESSERACT_AVAILABLE:
        return ""
    try:
        from pdf2image import convert_from_path
        images = convert_from_path(
            pdf_path,
            dpi=300,
            first_page=page_num + 1,   # pdf2image is 1-indexed
            last_page=page_num + 1
        )
        if not images:
            return ""
        img = images[0].convert("L")  # grayscale improves OCR accuracy
        return pytesseract.image_to_string(img, lang="eng").strip()
    except Exception as e:
        logger.error(f"OCR failed on page {page_num}: {e}")
        return ""


def extract_text_from_pdf(pdf_path: str) -> list:
    """
    Extract text from all pages using three methods in priority order:
      1. pdfplumber extract_text() — for narrative/text pages
      2. pdfplumber extract_tables() — for financial tables (BS, P&L, CF)
      3. pytesseract OCR — fallback for true image-only pages

    Returns list of dicts: [{page_num, text, method}, ...]
    """
    pages = []
    stats = {"pdfplumber_text": 0, "pdfplumber_table": 0, "ocr": 0, "skipped": 0}

    try:
        with pdfplumber.open(pdf_path) as pdf:
            total = len(pdf.pages)
            logger.info(f"Processing {total} pages from {Path(pdf_path).name}")

            for i, page in enumerate(pdf.pages):
                page_num = i + 1

                # Method 1: direct text extraction
                raw_text = (page.extract_text() or "").strip()

                # Method 2: table extraction
                try:
                    tables = page.extract_tables() or []
                except Exception:
                    tables = []

                table_text  = _tables_to_text(tables)
                table_cells = _count_table_cells(tables)

                # Combine text + table data
                combined = raw_text
                if table_cells >= MIN_TABLE_CELLS:
                    combined = raw_text + "\n\n[TABLE DATA]\n" + table_text
                combined = combined.strip()

                # Force-try tables on ALL pages regardless of text length.
                # Fixes balance sheet pages that have very little extractable
                # text but rich table data (e.g. ORAS page 11).
                if not tables:
                    try:
                        tables = page.extract_tables() or []
                    except Exception:
                        tables = []
                    table_text  = _tables_to_text(tables)
                    table_cells = _count_table_cells(tables)
                    if table_cells >= MIN_TABLE_CELLS:
                        combined = raw_text + "\n\n[TABLE DATA]\n" + table_text
                        combined = combined.strip()

                if len(combined) >= MIN_TEXT_LENGTH:
                    if table_cells >= MIN_TABLE_CELLS:
                        method = "pdfplumber+tables"
                        stats["pdfplumber_table"] += 1
                    else:
                        method = "pdfplumber"
                        stats["pdfplumber_text"] += 1
                    pages.append({"page_num": page_num, "text": combined, "method": method})

                elif table_cells >= 1:
                    # Partial table data — better than nothing
                    pages.append({
                        "page_num": page_num,
                        "text": combined,
                        "method": "pdfplumber+tables(partial)"
                    })
                    stats["pdfplumber_table"] += 1
                    logger.info(f"Page {page_num}: partial table data included "
                                f"({len(combined)} chars, {table_cells} cells)")

                elif TESSERACT_AVAILABLE:
                    logger.info(f"Page {page_num}: no text/tables, trying OCR...")
                    ocr_text = _ocr_page(pdf_path, i)
                    if len(ocr_text) >= MIN_TEXT_LENGTH:
                        pages.append({"page_num": page_num, "text": ocr_text, "method": "ocr"})
                        stats["ocr"] += 1
                    else:
                        stats["skipped"] += 1

                else:
                    stats["skipped"] += 1
                    logger.warning(f"Page {page_num}: no content extracted, skipping")

    except Exception as e:
        logger.error(f"PDF extraction failed: {e}")
        raise

    logger.info(
        f"Extraction complete — "
        f"text:{stats['pdfplumber_text']} "
        f"tables:{stats['pdfplumber_table']} "
        f"ocr:{stats['ocr']} "
        f"skipped:{stats['skipped']}"
    )
    return pages


def _detect_section(text: str) -> str:
    """Detect which financial section a chunk belongs to."""
    t = text.lower()
    if any(k in t for k in ["income statement", "profit", "revenue", "sales",
                              "قائمة الدخل", "الإيرادات"]):
        return "income_statement"
    elif any(k in t for k in ["balance sheet", "assets", "liabilities", "equity",
                                "الميزانية", "الأصول"]):
        return "balance_sheet"
    elif any(k in t for k in ["cash flow", "operating activities",
                                "التدفقات النقدية"]):
        return "cash_flow"
    elif any(k in t for k in ["note", "significant accounting",
                                "إيضاح", "السياسات المحاسبية"]):
        return "notes"
    elif any(k in t for k in ["chairman", "board", "management", "director",
                                "مجلس الإدارة", "الرئيس"]):
        return "management"
    else:
        return "general"


def chunk_text(pages: list) -> list:
    """
    Split extracted pages into overlapping chunks for embedding.
    Returns list of dicts: {chunk_index, chunk_text, page_num, section}
    """
    chunks = []
    chunk_index   = 0
    chars_per_chunk = CHUNK_SIZE * 4
    overlap_chars   = CHUNK_OVERLAP * 4

    for page_data in pages:
        text     = page_data["text"]
        page_num = page_data["page_num"]

        paragraphs    = [p.strip() for p in re.split(r'\n{2,}', text) if p.strip()]
        current_chunk = ""

        for para in paragraphs:
            if len(current_chunk) + len(para) <= chars_per_chunk:
                current_chunk += "\n" + para
            else:
                if current_chunk.strip():
                    chunks.append({
                        "chunk_index": chunk_index,
                        "chunk_text":  current_chunk.strip(),
                        "page_num":    page_num,
                        "section":     _detect_section(current_chunk)
                    })
                    chunk_index += 1
                current_chunk = current_chunk[-overlap_chars:] + "\n" + para

        if current_chunk.strip():
            chunks.append({
                "chunk_index": chunk_index,
                "chunk_text":  current_chunk.strip(),
                "page_num":    page_num,
                "section":     _detect_section(current_chunk)
            })
            chunk_index += 1

    return chunks


def embed_chunks(chunks: list) -> list:
    """
    Generate embeddings for each chunk using sentence-transformers.
    Adds 'embedding' key (np.array) to each chunk dict.
    Processes in batches of 32 for efficiency.
    """
    model  = _get_embed_model()
    texts  = [ch["chunk_text"] for ch in chunks]

    all_embeddings = []
    batch_size = 32
    for i in range(0, len(texts), batch_size):
        batch      = texts[i:i + batch_size]
        embeddings = model.encode(batch, show_progress_bar=False)
        all_embeddings.extend(embeddings)

    for i, ch in enumerate(chunks):
        ch["embedding"] = all_embeddings[i]

    return chunks


def embed_query(query: str) -> np.ndarray:
    """Generate embedding for a search query string."""
    model = _get_embed_model()
    return model.encode([query])[0]


def extract_financial_sections(pages: list) -> dict:
    """
    Identify and extract the most financially relevant pages from a PDF.
    Returns dict with sections: income_statement, balance_sheet,
    cash_flow, other, all_pages.
    """
    INCOME_KEYWORDS = [
        "income statement", "profit and loss", "statement of profit",
        "consolidated income", "revenue", "net profit", "net income",
        "قائمة الدخل", "الأرباح والخسائر", "إيرادات", "صافي الربح"
    ]
    BALANCE_KEYWORDS = [
        "balance sheet", "statement of financial position",
        "total assets", "total liabilities", "shareholders equity",
        "الميزانية", "المركز المالي", "إجمالي الأصول"
    ]
    CASHFLOW_KEYWORDS = [
        "cash flow", "cash and cash equivalents", "operating activities",
        "التدفقات النقدية", "الأنشطة التشغيلية"
    ]

    sections = {
        "income_statement": [],
        "balance_sheet":    [],
        "cash_flow":        [],
        "other":            [],
        "all_pages":        pages
    }

    for page in pages:
        text_lower = page["text"].lower()
        if any(kw in text_lower for kw in INCOME_KEYWORDS):
            sections["income_statement"].append(page)
        elif any(kw in text_lower for kw in BALANCE_KEYWORDS):
            sections["balance_sheet"].append(page)
        elif any(kw in text_lower for kw in CASHFLOW_KEYWORDS):
            sections["cash_flow"].append(page)
        else:
            sections["other"].append(page)

    return sections


def build_extraction_text(pages: list, max_chars: int = 12000) -> str:
    """
    Build optimized text for LLM KPI extraction.

    Strategy:
    1. Force-include pages that contain actual financial statement numbers
       (identified by high-value anchor strings like 'Total assets')
    2. Fill remaining space with other scored pages
    3. Hard cap at max_chars
    """
    # Tier 1: anchors that ONLY appear on core financial statement pages
    ANCHOR_STRINGS = [
        # Balance sheet
        "Total assets",
        "Total equity",
        "Equity attributable to owners",
        "Loans and borrowings",
        "Total current assets",
        "Total non-current assets",
        "Total liabilities",
        # P&L
        "Revenue",
        "Gross profit",
        "Operating profit",
        "Net profit",
        "Earnings per share",
        "Basic and diluted earnings",
        # Cash flow
        "Cash flow generated from operating",
        "Cash and cash equivalents at 31 December",
        # Segment
        "Total assets as at 31 December",
        "Profit before tax for the year",
    ]

    # Tier 2: general keywords (notes also contain these)
    GENERAL_KEYWORDS = [
        "total assets", "net profit", "revenue", "ebitda",
        "interest income", "dividend", "earnings per share",
        "[table data]",
    ]

    tier1_pages = []
    tier2_pages = []

    for page in pages:
        text = page["text"]
        text_lower = text.lower()
        table_bonus = 2 if "table" in page.get("method", "") else 0

        tier1_score = sum(1 for anchor in ANCHOR_STRINGS if anchor in text)
        tier2_score = sum(1 for kw in GENERAL_KEYWORDS if kw in text_lower)

        if tier1_score >= 2:
            tier1_pages.append((tier1_score + table_bonus, page))
        elif tier2_score >= 1:
            tier2_pages.append((tier2_score + table_bonus, page))

    tier1_pages.sort(key=lambda x: x[0], reverse=True)
    tier2_pages.sort(key=lambda x: x[0], reverse=True)

    selected_text = ""

    for score, page in tier1_pages:
        candidate = (
            f"\n\n=== PAGE {page['page_num']} "
            f"[FINANCIAL STATEMENT | score:{score}] ===\n"
            f"{page['text']}"
        )
        if len(selected_text) + len(candidate) <= max_chars:
            selected_text += candidate

    for score, page in tier2_pages:
        if len(selected_text) >= max_chars:
            break
        candidate = (
            f"\n\n=== PAGE {page['page_num']} "
            f"[NOTE | score:{score}] ===\n"
            f"{page['text']}"
        )
        remaining = max_chars - len(selected_text)
        if remaining > 300:
            selected_text += candidate[:remaining]

    if not selected_text:
        # Absolute fallback — sample beginning + middle + end
        all_text = "\n\n".join(p["text"] for p in pages)
        third = len(all_text) // 3
        selected_text = (
            all_text[:4000] + "\n...\n" +
            all_text[third:third + 4000] + "\n...\n" +
            all_text[-4000:]
        )

    return selected_text[:max_chars]


def process_pdf(pdf_path: str) -> dict:
    """Full pipeline: extract (text+tables+OCR) → chunk → embed"""
    pages = extract_text_from_pdf(pdf_path)

    if not pages:
        return {"pages": [], "chunks": [], "total_pages": 0,
                "total_chunks": 0, "ocr_pages": 0, "table_pages": 0}

    ocr_count   = sum(1 for p in pages if p.get("method") == "ocr")
    table_count = sum(1 for p in pages if "tables" in p.get("method", ""))

    chunks = chunk_text(pages)
    logger.info(f"  Generated {len(chunks)} chunks from {len(pages)} pages "
                f"({table_count} table, {ocr_count} OCR)")

    chunks = embed_chunks(chunks)
    logger.info(f"  Embedded {len(chunks)} chunks")

    return {
        "pages":        pages,
        "chunks":       chunks,
        "total_pages":  len(pages),
        "total_chunks": len(chunks),
        "ocr_pages":    ocr_count,
        "table_pages":  table_count,
    }

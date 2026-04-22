"""
fundamental_db.py — DB operations for RAG fundamentals system.
Tables: fundamental_data (structured KPIs) + fundamental_chunks (RAG embeddings)
"""
import numpy as np
import pandas as pd
from datetime import datetime
from backend.data.db import get_connection


def init_fundamental_tables():
    """Create fundamental_data, fundamental_chunks, and fundamental_reports tables if not exist."""
    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fundamental_data (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker            TEXT NOT NULL,
                report_type       TEXT,
                period            TEXT,
                report_date       DATE,
                source_file       TEXT,
                revenue           REAL,
                revenue_growth    REAL,
                net_profit        REAL,
                net_margin        REAL,
                eps               REAL,
                ebitda            REAL,
                total_assets      REAL,
                total_debt        REAL,
                equity            REAL,
                debt_to_equity    REAL,
                current_ratio     REAL,
                pe_ratio          REAL,
                pb_ratio          REAL,
                roe               REAL,
                roa               REAL,
                dividend_per_share REAL,
                dividend_yield    REAL,
                interest_income   REAL,
                interest_to_rev   REAL,
                currency          TEXT DEFAULT 'EGP',
                raw_summary       TEXT,
                extracted_at      DATETIME,
                UNIQUE(ticker, period)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fundamental_chunks (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker       TEXT NOT NULL,
                period       TEXT,
                source_file  TEXT,
                chunk_index  INTEGER,
                chunk_text   TEXT,
                embedding    BLOB,
                page_num     INTEGER,
                section      TEXT,
                created_at   DATETIME
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fundamental_reports (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker       TEXT NOT NULL,
                period       TEXT NOT NULL,
                report_type  TEXT,
                source_file  TEXT,
                upload_date  DATE,
                pages        INTEGER,
                chunks       INTEGER,
                status       TEXT DEFAULT 'processed',
                notes        TEXT,
                UNIQUE(ticker, period)
            )
        """)

        conn.commit()
    finally:
        conn.close()

    # Migrate: add new columns if they don't exist yet
    conn = get_connection()
    try:
        for col, typedef in [
            ("period_type", "TEXT DEFAULT 'A'"),
            ("quarter",     "INTEGER"),
            ("fiscal_year", "INTEGER"),
            ("updated_at",  "DATETIME"),
        ]:
            try:
                conn.execute(f"ALTER TABLE fundamental_data ADD COLUMN {col} {typedef}")
                conn.commit()
            except Exception:
                pass  # column already exists
    finally:
        conn.close()


def manual_upsert_fundamentals(
    ticker:      str,
    period:      str,
    period_type: str   = "A",
    quarter:     "int | None" = None,
    currency:    str   = "EGP",
    fiscal_year: "int | None" = None,
    **kwargs,
) -> bool:
    """
    Insert or replace a manually-entered fundamental record.
    Accepts all fundamental_data financial columns as **kwargs.
    Returns True on success, False on failure.
    """
    import logging
    _log = logging.getLogger(__name__)

    financial_cols = [
        "report_date", "report_type",
        "revenue", "revenue_growth", "net_profit", "net_margin", "eps", "ebitda",
        "total_assets", "total_debt", "equity", "debt_to_equity", "current_ratio",
        "pe_ratio", "pb_ratio", "roe", "roa",
        "dividend_per_share", "dividend_yield",
        "interest_income", "interest_to_rev",
        "raw_summary", "source_file",
    ]

    cols = ["ticker", "period", "period_type", "quarter", "fiscal_year",
            "currency", "extracted_at", "updated_at"]
    vals = [ticker, period, period_type, quarter, fiscal_year,
            currency, datetime.now().isoformat(), datetime.now().isoformat()]

    for col in financial_cols:
        if col in kwargs:
            cols.append(col)
            vals.append(kwargs[col])

    placeholders = ",".join(["?"] * len(cols))
    update_pairs = ", ".join(
        f"{c}=excluded.{c}" for c in cols if c not in ("ticker", "period")
    )

    conn = get_connection()
    try:
        conn.execute(
            f"INSERT INTO fundamental_data ({','.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT(ticker, period) DO UPDATE SET {update_pairs}",
            vals
        )
        conn.commit()
        _log.info("manual_upsert_fundamentals: saved %s %s", ticker, period)
        return True
    except Exception as e:
        _log.error("manual_upsert_fundamentals FAILED: %s", e)
        return False
    finally:
        conn.close()


def upsert_fundamentals(ticker: str, period: str, data: dict):
    """Insert or replace structured KPI data for a ticker/period."""
    import logging
    _log = logging.getLogger(__name__)

    conn = get_connection()
    data["ticker"]       = ticker
    data["period"]       = period
    data["extracted_at"] = datetime.now().isoformat()

    cols = [
        "ticker", "period", "report_type", "report_date", "source_file",
        "revenue", "revenue_growth", "net_profit", "net_margin", "eps", "ebitda",
        "total_assets", "total_debt", "equity", "debt_to_equity", "current_ratio",
        "pe_ratio", "pb_ratio", "roe", "roa",
        "dividend_per_share", "dividend_yield",
        "interest_income", "interest_to_rev",
        "currency", "raw_summary", "extracted_at"
    ]

    # Only use schema columns — ignore extra LLM fields; normalise numpy types
    vals = []
    for c in cols:
        v = data.get(c)
        if hasattr(v, 'item'):   # numpy scalar → Python native
            v = v.item()
        vals.append(v)

    try:
        placeholders = ",".join(["?"] * len(cols))
        conn.execute(
            f"INSERT OR REPLACE INTO fundamental_data "
            f"({','.join(cols)}) VALUES ({placeholders})",
            vals
        )
        conn.commit()
        _log.info("upsert_fundamentals: saved %s %s", ticker, period)
    except Exception as e:
        _log.error("upsert_fundamentals FAILED: %s", e)
        _log.error("Data was: %s", data)
        raise
    finally:
        conn.close()


def save_chunks(ticker: str, period: str, source_file: str, chunks: list):
    """
    Save text chunks + embeddings to fundamental_chunks table.
    chunks: list of dicts with keys: chunk_text, embedding (np.array),
            page_num, section, chunk_index
    """
    conn = get_connection()
    try:
        conn.execute(
            "DELETE FROM fundamental_chunks WHERE ticker=? AND period=?",
            (ticker, period)
        )
        now = datetime.now().isoformat()
        for ch in chunks:
            emb_bytes = ch["embedding"].astype(np.float32).tobytes()
            conn.execute("""
                INSERT INTO fundamental_chunks
                    (ticker, period, source_file, chunk_index, chunk_text,
                     embedding, page_num, section, created_at)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                ticker, period, source_file,
                ch["chunk_index"], ch["chunk_text"],
                emb_bytes, ch.get("page_num", 0),
                ch.get("section", ""), now
            ))
        conn.commit()
    finally:
        conn.close()


def get_latest_fundamentals(ticker: str) -> dict:
    """Return most recent fundamental KPIs for a ticker as dict.
    Prefers Annual over Quarterly within the same fiscal year.
    Quarterly rows are annualized before return."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """SELECT * FROM fundamental_data WHERE ticker=?
               ORDER BY
                   COALESCE(fiscal_year, CAST(substr(period, 1, 4) AS INTEGER)) DESC,
                   CASE WHEN COALESCE(period_type, 'A') = 'A' THEN 0 ELSE 1 END ASC,
                   COALESCE(quarter, 0) DESC,
                   report_date DESC
               LIMIT 1""",
            conn, params=(ticker,)
        )
    finally:
        conn.close()

    if df.empty:
        return {}

    row = df.iloc[0].to_dict()

    _LEGACY_FACTORS = {"Q1": 4.0, "H1": 2.0, "9M": 1.333, "Q": 4.0}
    pt = row.get("period_type") or "FY"
    legacy_factor = _LEGACY_FACTORS.get(pt)
    if legacy_factor:
        q = row.get("quarter") or 1
        factor = legacy_factor
        for field, decimals in [("revenue", 2), ("net_profit", 2), ("ebitda", 2), ("eps", 4)]:
            if row.get(field) is not None:
                row[field] = round(float(row[field]) * factor, decimals)
        row["annualized"] = True
        row["note"] = f"Annualized from {pt} × {factor:.2f}"
    else:
        row["annualized"] = False

    return row


def get_best_fundamentals(ticker: str) -> "dict | None":
    """
    Returns the best available fundamental data for a ticker,
    with income statement figures annualized if quarterly.

    Priority: Annual > Q3 > Q2 > Q1
    Balance sheet figures (total_assets, total_debt, equity, cash) are never annualized.

    Returned dict includes extra metadata keys:
      _period_used           : str   e.g. "2025-A" or "2025-Q2"
      _period_label          : str   e.g. "FY2025 Annual" or "FY2025 Q2 (annualized ×2.00)"
      _annualized            : bool
      _annualization_factor  : float
      _data_quality          : str   "HIGH" / "MEDIUM" / "LOW"
    """
    import logging
    log = logging.getLogger(__name__)

    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT *
            FROM fundamental_data
            WHERE ticker = ?
            ORDER BY
                COALESCE(fiscal_year, 0) DESC,
                CASE period_type
                    WHEN 'FY' THEN 0
                    WHEN '9M' THEN 1
                    WHEN 'H1' THEN 2
                    WHEN 'Q1' THEN 3
                    ELSE 4
                END ASC
        """, (ticker,)).fetchall()
        col_names = [d[1] for d in conn.execute(
            "PRAGMA table_info(fundamental_data)"
        ).fetchall()]
    finally:
        conn.close()

    if not rows:
        return None

    best_row = dict(zip(col_names, rows[0]))

    period_type = best_row.get("period_type") or "A"
    quarter_num = best_row.get("quarter")
    fiscal_year = best_row.get("fiscal_year") or (best_row.get("period", "")[:4])

    INCOME_FIELDS = ["revenue", "net_profit", "ebitda", "gross_profit", "interest_expense"]

    PERIOD_CONFIG = {
        "Q1": {"months": 3,  "factor": 4.00,  "quality": "LOW",    "label": "Q1"},
        "H1": {"months": 6,  "factor": 2.00,  "quality": "MEDIUM", "label": "H1"},
        "9M": {"months": 9,  "factor": 1.333, "quality": "HIGH",   "label": "9M"},
        "FY": {"months": 12, "factor": 1.00,  "quality": "HIGH",   "label": "Annual"},
        # legacy values kept for backward compat
        "A":  {"months": 12, "factor": 1.00,  "quality": "HIGH",   "label": "Annual"},
    }
    cfg    = PERIOD_CONFIG.get(period_type or "FY",
                               {"months": 3, "factor": 4.0, "quality": "LOW", "label": period_type or "?"})
    factor = cfg["factor"]
    quality = cfg["quality"]

    if factor == 1.0:
        annualized   = False
        period_label = f"FY{fiscal_year} {cfg['label']}"
    else:
        annualized = True

        for field in INCOME_FIELDS:
            val = best_row.get(field)
            if val is not None:
                try:
                    best_row[field] = round(float(val) * factor, 2)
                except (ValueError, TypeError):
                    pass

        if best_row.get("eps"):
            try:
                best_row["eps"] = round(float(best_row["eps"]) * factor, 4)
            except (ValueError, TypeError):
                pass

        # Recalculate net_margin from annualized figures
        try:
            if best_row.get("revenue") and best_row.get("net_profit"):
                best_row["net_margin"] = round(
                    best_row["net_profit"] / best_row["revenue"] * 100, 2
                )
        except Exception:
            pass

        period_label = f"FY{fiscal_year} {cfg['label']} (annualized ×{factor:.2f})"

    best_row["_period_used"]          = best_row.get("period", "")
    best_row["_period_label"]         = period_label
    best_row["_annualized"]           = annualized
    best_row["_annualization_factor"] = factor
    best_row["_data_quality"]         = quality

    log.info(
        "%s: best fundamentals = %s | quality=%s | annualized=%s | factor=%s",
        ticker, best_row["_period_used"], quality, annualized, factor
    )
    return best_row


def get_all_fundamentals() -> pd.DataFrame:
    """Return all fundamental data rows ordered by ticker + period."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM fundamental_data ORDER BY ticker, period DESC",
            conn
        )
    finally:
        conn.close()
    return df


def get_report_list() -> pd.DataFrame:
    """Return all uploaded reports."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM fundamental_reports ORDER BY upload_date DESC",
            conn
        )
    finally:
        conn.close()
    return df


def search_chunks(ticker: str, query_embedding: np.ndarray, top_k: int = 5) -> list:
    """
    Semantic search: find top_k most relevant chunks for a ticker
    using cosine similarity between query_embedding and stored embeddings.
    Returns list of dicts: {chunk_text, section, page_num, score}
    """
    from sklearn.metrics.pairwise import cosine_similarity

    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT chunk_text, embedding, section, page_num FROM fundamental_chunks WHERE ticker=?",
            (ticker,)
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    results = []
    q = query_embedding.reshape(1, -1).astype(np.float32)

    for text, emb_bytes, section, page_num in rows:
        try:
            emb   = np.frombuffer(emb_bytes, dtype=np.float32).reshape(1, -1)
            score = float(cosine_similarity(q, emb)[0][0])
            results.append({
                "chunk_text": text,
                "section":    section,
                "page_num":   page_num,
                "score":      score
            })
        except Exception:
            continue

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]


def get_fundamentals_egp(ticker: str, period: str = None) -> dict:
    """
    Return fundamental data for a ticker with all monetary fields
    converted to EGP using the latest live FX rate.
    Use this in the signal engine and UI display — never use raw values
    directly if currency might be USD.
    """
    from backend.analysis.fundamental import convert_to_egp

    data = get_latest_fundamentals(ticker)
    if not data:
        return {}

    currency = data.get("currency", "EGP") or "EGP"
    if currency == "EGP":
        return data  # already in EGP, no conversion needed

    # Get live FX rate once
    try:
        from backend.data.db import get_connection
        conn = get_connection()
        row = conn.execute(
            "SELECT close FROM prices WHERE ticker='USDFX' "
            "ORDER BY date DESC LIMIT 1"
        ).fetchone()
        conn.close()
        fx = float(row[0]) if row else 51.75
    except Exception:
        fx = 51.75

    monetary_fields = [
        "revenue", "net_profit", "ebitda", "total_assets",
        "total_debt", "equity", "interest_income",
        "dividend_per_share"
    ]

    converted = dict(data)
    for field in monetary_fields:
        if converted.get(field) is not None:
            converted[field] = convert_to_egp(converted[field], currency, fx)

    converted["currency_display"] = f"EGP (converted from {currency} @ {fx:.2f})"
    return converted


def upsert_report_meta(ticker, period, report_type, source_file,
                       upload_date, pages, chunks, status="processed", notes=""):
    """Save or update report metadata."""
    conn = get_connection()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO fundamental_reports
                (ticker, period, report_type, source_file, upload_date, pages, chunks, status, notes)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (ticker, period, report_type, source_file, upload_date, pages, chunks, status, notes))
        conn.commit()
    finally:
        conn.close()

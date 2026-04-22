"""
longterm_agent.py — Long-Term Investment Signal Generator
Reads positions from lt_positions, runs technical analysis via technical.py,
calls Groq LLM for signal interpretation, stores results in lt_signals table.
"""

import json
import logging
import re
from datetime import date
from backend.data.lt_db import get_positions, insert_signal
from backend.data.llm_client import ask_llm
from backend.analysis.technical import (
    get_signal_snapshot, get_fair_value, get_rsi_divergence,
    get_volume_analysis_enhanced, get_multi_timeframe_signal,
)
from backend.analysis.fundamental import get_fundamental_context
from backend.data.fundamental_db import get_latest_fundamentals, get_best_fundamentals

logger = logging.getLogger(__name__)


def _extract_json(raw: str) -> str:
    """Strip markdown code fences if the LLM wraps its JSON in them."""
    raw = raw.strip()
    match = re.search(r"\{[\s\S]*\}", raw)
    if match:
        return match.group(0)
    return raw


def _build_prompt(ticker: str, snap: dict, position: dict) -> str:
    """
    Build the LLM user message for a single ticker.
    Combines technical snapshot + position data + fundamentals into a structured analysis request.
    """
    avg_cost      = position.get("weighted_avg_cost", 0)
    total_shares  = position.get("total_shares", 0)
    total_cost    = position.get("total_cost_net", 0)
    current_price = snap.get("close", 0)
    unrealized_pct = ((current_price - avg_cost) / avg_cost * 100) if avg_cost > 0 else 0

    # Fundamental context (empty string if no report uploaded yet)
    try:
        fund_ctx = get_fundamental_context(ticker, query="revenue profit earnings growth")
    except Exception:
        fund_ctx = ""

    fund_section = ""
    if fund_ctx.strip():
        fund_section = f"""
═══ FUNDAMENTAL DATA (from uploaded financial reports) ═══
{fund_ctx}
"""

    return f"""
═══ POSITION DATA ═══
Ticker:           {ticker}
Shares held:      {total_shares}
Average cost:     EGP {avg_cost:.2f}
Current price:    EGP {current_price:.2f}
Unrealized P/L:   {unrealized_pct:+.2f}%
Total cost basis: EGP {total_cost:.2f}

═══ TECHNICAL SNAPSHOT ═══
RSI (14):         {snap.get('rsi')} → {snap.get('rsi_zone')}
EMA9:             {snap.get('ema9')}
EMA21:            {snap.get('ema21')}
EMA50:            {snap.get('ema50')}
EMA200:           {snap.get('ema200')}
EMA9 > EMA21:     {snap.get('ema9_above_21')}
Price > EMA50:    {snap.get('price_above_50')}
Price > EMA200:   {snap.get('price_above_200')}
EMA slope (5d):   {snap.get('ema_slope_5d')}%
MACD histogram:   {snap.get('macd_histogram')} ({'bullish' if snap.get('macd_bullish') else 'bearish'})
ADX:              {snap.get('adx')} (trend strength)
BB position:      {snap.get('bb_pct')} (0=lower band, 1=upper band)
BB width:         {snap.get('bb_width')}
Volume ratio:     {snap.get('vol_ratio')}x avg
Vol confirmed:    {snap.get('vol_confirmation')}
ATR:              {snap.get('atr')}
Fibonacci zone:   {snap.get('current_zone')}
Swing high:       {snap.get('swing_high')}
Swing low:        {snap.get('swing_low')}
Fib 38.2%:        {snap.get('fib_382')}
Fib 50.0%:        {snap.get('fib_50')}
Fib 61.8%:        {snap.get('fib_618')}
Nearest support:  {snap.get('nearest_support')}
Nearest resist:   {snap.get('nearest_resistance')}
Data rows:        {snap.get('data_rows')} days ({snap.get('date_from')} to {snap.get('date_to')})
{fund_section}
═══ RULES ═══
- This is a LONG-TERM wallet (6+ month horizon, NOT swing trading)
- Max single stock allocation: 20% of wallet
- Annual return target: 25%
- Shariah-compliant (no interest, no speculation)
- EGX is a frontier market — factor in lower liquidity vs developed markets

═══ REQUIRED OUTPUT ═══
Return ONLY a valid JSON object with NO markdown, NO explanation, NO preamble.
Exact structure required:

{{
  "signal": "BUY" | "ACCUMULATE" | "HOLD" | "TRIM_PEAK" | "SELL",
  "action": "BUY_MORE" | "HOLD" | "TAKE_PROFIT" | "REDUCE",
  "score": <integer 0-100>,
  "position_size_pct": <float, recommended % of wallet, 0 if no action>,
  "suggested_buy_price": <float — ALWAYS provide a price level to watch for entry,
even on HOLD. Use nearest support or Fib 61.8% as the watch level.
Never return null.>,
  "sell_price": <float or null>,
  "fib_zone": "<current fib zone string>",
  "target_1m": <float>,
  "target_6m": <float>,
  "target_12m": <float>,
  "exp_return_1m": <float, % expected return in 1 month>,
  "exp_return_6m": <float, % expected return in 6 months>,
  "exp_return_12m": <float, % expected return in 12 months>,
  "forecast_confidence": "LOW" | "MEDIUM" | "HIGH",
  "fundamental_quality": "STRONG" | "ADEQUATE" | "WEAK" | "NO_DATA",
  "description": "<2-3 sentence rationale combining technicals + fundamentals + position context>"
}}

Signal definitions:
- BUY: Strong setup, add significant position
- ACCUMULATE: Good setup, add small position gradually
- HOLD: No clear edge, maintain current position
- TRIM_PEAK: Price extended, take partial profits
- SELL: Exit position (stop-loss hit or fundamental change)
""".strip()


def get_capital_allocation(
    score:           int,
    signal:          str,
    current_price:   float = None,
    entry_zone_low:  float = None,
    entry_zone_high: float = None,
    fair_value_mid:  float = None,
    rsi_daily:       float = None,
    rsi_weekly:      float = None,
) -> tuple:
    """
    Final unified recommendation engine.
    Integrates: technical score + entry zone distance
              + valuation + RSI overbought status

    Priority of overrides (strongest first):
      1. RSI extreme overbought (>80) → always reduce
      2. Severely overvalued (>25%)   → always reduce
      3. Moderately overvalued (>15%) → cap at half
      4. Distance from entry zone     → standard logic
      5. Score                        → base logic

    Returns 2-tuple: (deploy_label, deploy_note)
    """

    # ── Distance from entry zone ─────────────────────────────────
    distance_pct = 0.0
    inside_zone  = False

    if current_price and entry_zone_low and entry_zone_high:
        if entry_zone_low <= current_price <= entry_zone_high:
            inside_zone  = True
            distance_pct = 0.0
        elif current_price > entry_zone_high:
            distance_pct = (current_price - entry_zone_high) / entry_zone_high * 100
        else:
            inside_zone  = True
            distance_pct = 0.0

    zone_str = ""
    if entry_zone_low and entry_zone_high:
        zone_str = f"EGP {entry_zone_low:.2f}–{entry_zone_high:.2f}"

    # ── Valuation gap ────────────────────────────────────────────
    overvalued_pct  = 0.0
    undervalued_pct = 0.0
    val_note        = ""

    if fair_value_mid and fair_value_mid > 0 and current_price:
        gap = (current_price - fair_value_mid) / fair_value_mid * 100
        if gap > 0:
            overvalued_pct = gap
            val_note = (f"trading {gap:.1f}% above fair value "
                        f"EGP {fair_value_mid:.2f}")
        else:
            undervalued_pct = abs(gap)

    # ── RSI overbought check ─────────────────────────────────────
    rsi_overbought = False
    rsi_note       = ""
    rsi_val        = rsi_daily or 50

    if rsi_val >= 80:
        rsi_overbought = True
        rsi_note = f"RSI {rsi_val:.0f} — extreme overbought"
    elif rsi_val >= 75:
        rsi_note = f"RSI {rsi_val:.0f} — overbought, correction risk"

    # ── Base label from distance + score ─────────────────────────
    if distance_pct <= 3.0:
        if score >= 75:
            base_label = "FULL DEPLOY"
            base_msg   = f"Price at entry zone {zone_str} — strong setup."
        elif score >= 60:
            base_label = "HALF NOW"
            base_msg   = f"Good setup at entry zone {zone_str}."
        elif score >= 45:
            base_label = "WAIT"
            base_msg   = "No high-probability entry — preserve cash."
        else:
            base_label = "AVOID"
            base_msg   = "Weak setup — do not enter."

    elif distance_pct <= 8.0:
        if score >= 75:
            base_label = "HALF NOW"
            base_msg   = (f"Strong setup but price {distance_pct:.1f}%"
                          f" above {zone_str}.")
        elif score >= 60:
            base_label = "QUARTER NOW"
            base_msg   = (f"Decent setup, {distance_pct:.1f}% extended"
                          f" above {zone_str}.")
        else:
            base_label = "WAIT"
            base_msg   = (f"Score {score} and price {distance_pct:.1f}%"
                          f" above entry — wait.")

    elif distance_pct <= 15.0:
        if score >= 75:
            base_label = "WAIT FOR DIP"
            base_msg   = (f"Bullish but {distance_pct:.1f}% above "
                          f"{zone_str} — set limit, do not chase.")
        else:
            base_label = "WAIT"
            base_msg   = (f"Price {distance_pct:.1f}% above entry,"
                          f" score {score} — wait.")
    else:
        base_label = "WATCH ONLY"
        base_msg   = (f"Too extended — {distance_pct:.1f}% above "
                      f"{zone_str}. Monitor for pullback.")

    # ── Override 1: Extreme RSI ──────────────────────────────────
    if rsi_overbought and base_label == "FULL DEPLOY":
        base_label = "HALF NOW"
        base_msg   = (f"{base_msg} ⚠️ {rsi_note} — "
                      f"reduce size, wait for RSI to cool below 70.")
    elif rsi_val >= 75 and base_label == "FULL DEPLOY":
        base_label = "HALF NOW"
        base_msg   = f"{base_msg} ⚠️ {rsi_note}."

    # ── Override 2: Severely overvalued (>25%) ───────────────────
    if overvalued_pct > 25:
        if base_label in ("FULL DEPLOY", "HALF NOW"):
            base_label = "QUARTER NOW"
            base_msg   = (f"Technical setup is bullish but ⚠️ {val_note}"
                          f" — limit position size until price corrects"
                          f" or earnings grow into valuation.")
        elif base_label == "WAIT FOR DIP":
            base_msg   = f"{base_msg} ⚠️ Also {val_note}."

    # ── Override 3: Moderately overvalued (15–25%) ───────────────
    elif overvalued_pct > 15:
        if base_label == "FULL DEPLOY":
            base_label = "HALF NOW"
            base_msg   = f"{base_msg} ⚠️ {val_note} — deploy 50% only."

    # ── Undervalued bonus ────────────────────────────────────────
    if (undervalued_pct > 20
            and score >= 55
            and distance_pct <= 15
            and rsi_val < 75
            and base_label in ("HALF NOW", "WAIT FOR DIP", "WAIT")):
        base_label = "ACCUMULATE ON DIPS"
        base_msg   = (
            f"Stock is {undervalued_pct:.1f}% below fair value "
            f"EGP {fair_value_mid:.2f} — strong fundamental case. "
            f"Technical setup needs improvement (score {score}/100). "
            f"Accumulate in small tranches on pullbacks toward "
            f"entry zone {zone_str}. "
            f"RSI {rsi_val:.0f} — wait for cooldown below 65 "
            f"before each tranche."
        )

    return (base_label, base_msg)


_NON_RETRYABLE = ["tokens", "rate_limit", "invalid_request", "credit", "overloaded"]
_MAX_LLM_RETRIES = 2


def _call_llm_with_retry(ticker: str, system_prompt: str, user_message: str) -> str:
    """
    Call ask_llm with up to _MAX_LLM_RETRIES retries.
    Aborts immediately on non-retryable errors (token limit, quota, etc.).
    Returns the raw response string, or a human-readable error message on failure.
    """
    import time
    last_err = None
    for attempt in range(_MAX_LLM_RETRIES + 1):
        try:
            return ask_llm(
                system_prompt=system_prompt,
                user_message=user_message,
                task="signal_scoring",
            )
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            if any(kw in err_str for kw in _NON_RETRYABLE):
                logger.warning("%s: LLM non-retryable error — %s", ticker, e)
                return f"⚠️ AI analysis unavailable: {str(e)[:120]}"
            if attempt < _MAX_LLM_RETRIES:
                logger.warning("%s: LLM attempt %d failed, retrying in 3s — %s",
                               ticker, attempt + 1, e)
                time.sleep(3)
            else:
                logger.error("%s: LLM failed after %d attempts — %s",
                             ticker, _MAX_LLM_RETRIES + 1, e)
    return f"⚠️ AI analysis temporarily unavailable: {str(last_err)[:120]}"


def _generate_description(ticker: str, snap: dict, position: dict,
                           signal: str, score: int) -> str:
    """
    Generate a short 2–3 sentence AI description for an existing signal.
    Used by run_llm_descriptions() in the second LLM pass.
    """
    avg_cost = float(position.get("weighted_avg_cost") or 0)
    price    = float(snap.get("close") or 0)
    pl_pct   = round((price - avg_cost) / avg_cost * 100, 1) if avg_cost > 0 else 0
    rsi      = snap.get("rsi") or "N/A"
    fib      = snap.get("current_zone") or "unknown"

    prompt = (
        f"Ticker: {ticker} | Signal: {signal} | Score: {score}/100\n"
        f"Avg cost: EGP {avg_cost:.2f} | Current price: EGP {price:.2f} | P/L: {pl_pct:+.1f}%\n"
        f"RSI: {rsi} | Fib zone: {fib}\n\n"
        f"Write 2-3 concise sentences explaining the signal and key risk/opportunity for "
        f"a Shariah-compliant EGX long-term investor. Plain text only, no JSON."
    )
    return _call_llm_with_retry(
        ticker,
        system_prompt="You are a concise investment analyst. Plain text only.",
        user_message=prompt,
    )


def run_signals(tickers: list = None, skip_llm: bool = False) -> dict:
    """
    Run signal analysis for all open LT positions (or a specified ticker list).
    Calls LLM for each ticker, stores results in lt_signals table.
    Returns dict of {ticker: signal_data}.
    """
    positions_df = get_positions()
    if positions_df.empty:
        logger.warning("run_signals: no open positions found.")
        return {}

    open_positions = positions_df[positions_df["status"] == "Open"]

    if tickers:
        open_positions = open_positions[open_positions["ticker"].isin(tickers)]

    if open_positions.empty:
        return {}

    today = str(date.today())
    results = {}

    for _, pos_row in open_positions.iterrows():
        ticker = pos_row["ticker"]
        logger.info("Generating signal for %s...", ticker)

        try:
            # 1. Technical snapshot
            snap = get_signal_snapshot(ticker)
            if not snap:
                logger.warning("%s: no technical data — skipping.", ticker)
                results[ticker] = {"error": "no_data"}
                continue

            # 2. Enhanced analysis (non-critical — failures are skipped)
            enhanced      = {}
            enhanced_json = None
            try:
                fund_data = get_best_fundamentals(ticker) or {}
                eps_raw   = fund_data.get("eps")
                currency  = fund_data.get("currency") or "EGP"

                # Sector: fundamentals DB first, then watchlist fallback
                sector = fund_data.get("sector")
                if not sector:
                    from backend.data.db import get_connection as _gc
                    _wconn = _gc()
                    _wrow  = _wconn.execute(
                        "SELECT sector FROM watchlist WHERE ticker=?", (ticker,)
                    ).fetchone()
                    _wconn.close()
                    sector = (_wrow[0] if _wrow and _wrow[0] else None) or "Other"

                logger.info("%s: sector=%s eps_raw=%s currency=%s", ticker, sector, eps_raw, currency)

                # EPS → EGP conversion
                eps_egp = None
                try:
                    if eps_raw and float(eps_raw) > 0:
                        if currency == "USD":
                            from backend.data.db import get_connection as _gc2
                            _fc = _gc2()
                            _fr = _fc.execute(
                                "SELECT close FROM prices WHERE ticker='USDFX' "
                                "ORDER BY date DESC LIMIT 1"
                            ).fetchone()
                            _fc.close()
                            fx      = float(_fr[0]) if _fr else 51.75
                            eps_egp = round(float(eps_raw) * fx, 2)
                        else:
                            eps_egp = float(eps_raw)
                except Exception:
                    eps_egp = None

                enhanced = {
                    "fair_value":      get_fair_value(
                        ticker,
                        eps_egp      = eps_egp,
                        sector       = sector,
                        data_quality = fund_data.get("_data_quality", "HIGH"),
                        annualized   = fund_data.get("_annualized", False),
                        period_label = fund_data.get("_period_label", ""),
                    ),
                    "divergence":      get_rsi_divergence(ticker),
                    "volume":          get_volume_analysis_enhanced(ticker),
                    "mtf":             get_multi_timeframe_signal(ticker),
                    "eps_egp":         eps_egp,
                    "sector":          sector,
                    "entry_zone_low":  snap.get("entry_zone_low"),
                    "entry_zone_high": snap.get("entry_zone_high"),
                    "entry_zone_mid":  snap.get("entry_zone_mid"),
                }
                enhanced_json = json.dumps(enhanced)

            except Exception as _enh_err:
                import traceback as _tb
                logger.error(
                    "%s: enhanced analysis failed — %s\n%s",
                    ticker, _enh_err, _tb.format_exc()
                )

            # 3. Build prompt + call LLM (or derive rule-based signal when skip_llm=True)
            position = pos_row.to_dict()

            if skip_llm:
                # ── Rule-based signal from swing score (fast, no LLM) ──────────
                from backend.analysis.technical import get_swing_score
                _sw = get_swing_score(ticker)
                _sc = int(_sw.get("total_score", 0))
                if _sc >= 70:
                    _sig, _act = "BUY",        "Open new position"
                elif _sc >= 55:
                    _sig, _act = "ACCUMULATE", "Add to existing position"
                elif _sc >= 35:
                    _sig, _act = "HOLD",       "Hold current position"
                else:
                    _sig, _act = "WAIT",       "Wait for better entry"

                _close = snap.get("close") or 0
                signal_data = {
                    "signal":              _sig,
                    "action":              _act,
                    "score":               _sc,
                    "position_size_pct":   0,
                    "suggested_buy_price": snap.get("nearest_support"),
                    "sell_price":          None,
                    "fib_zone":            snap.get("current_zone", "unknown"),
                    "target_1m":           snap.get("target_1m") or round(_close * 1.05, 2),
                    "target_6m":           snap.get("target_6m") or round(_close * 1.15, 2),
                    "target_12m":          snap.get("target_12m") or round(_close * 1.25, 2),
                    "exp_return_1m":       5.0,
                    "exp_return_6m":       15.0,
                    "exp_return_12m":      25.0,
                    "forecast_confidence": "LOW",
                    "description":         "⏳ AI analysis pending — click Run AI Descriptions to generate.",
                    "recommended_shares":  0,
                    "recommended_capital": 0,
                }
            else:
                # ── Full LLM signal ───────────────────────────────────────────
                user_msg     = _build_prompt(ticker, snap, position)
                raw_response = _call_llm_with_retry(
                    ticker,
                    system_prompt=(
                        "You are a professional portfolio manager specialising in EGX "
                        "Shariah-compliant long-term investing. "
                        "Respond with valid JSON only — no markdown, no prose."
                    ),
                    user_message=user_msg,
                )

                # 4. Robust JSON extraction and parsing
                clean = raw_response.strip()

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
                if start == -1 or end == 0:
                    raise ValueError(f"No JSON found in LLM response: {clean[:200]}")
                clean = clean[start:end]

                clean = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', clean)

                def _fix_json_strings(text):
                    result = []
                    in_string = False
                    escape_next = False
                    for char in text:
                        if escape_next:
                            result.append(char)
                            escape_next = False
                        elif char == '\\':
                            result.append(char)
                            escape_next = True
                        elif char == '"':
                            in_string = not in_string
                            result.append(char)
                        elif char == '\n' and in_string:
                            result.append('\\n')
                        elif char == '\r' and in_string:
                            result.append('\\r')
                        elif char == '\t' and in_string:
                            result.append('\\t')
                        else:
                            result.append(char)
                    return ''.join(result)

                clean = _fix_json_strings(clean)

                try:
                    signal_data = json.loads(clean)
                except json.JSONDecodeError:
                    logger.warning("  %s: JSON parse failed, using regex extraction", ticker)

                    def _extract(pattern, text, default=None):
                        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                        return m.group(1).strip() if m else default

                    signal_data = {
                        "signal":              _extract(r'"signal"\s*:\s*"([^"]+)"', raw_response, "HOLD"),
                        "action":              _extract(r'"action"\s*:\s*"([^"]+)"', raw_response, "HOLD"),
                        "score":               int(_extract(r'"score"\s*:\s*(\d+)', raw_response, "50")),
                        "position_size_pct":   float(_extract(r'"position_size_pct"\s*:\s*([\d.]+)', raw_response, "0")),
                        "suggested_buy_price": None,
                        "sell_price":          None,
                        "fib_zone":            _extract(r'"fib_zone"\s*:\s*"([^"]+)"', raw_response, "unknown"),
                        "target_1m":           float(_extract(r'"target_1m"\s*:\s*([\d.]+)', raw_response, "0")),
                        "target_6m":           float(_extract(r'"target_6m"\s*:\s*([\d.]+)', raw_response, "0")),
                        "target_12m":          float(_extract(r'"target_12m"\s*:\s*([\d.]+)', raw_response, "0")),
                        "exp_return_1m":       float(_extract(r'"exp_return_1m"\s*:\s*([-\d.]+)', raw_response, "0")),
                        "exp_return_6m":       float(_extract(r'"exp_return_6m"\s*:\s*([-\d.]+)', raw_response, "0")),
                        "exp_return_12m":      float(_extract(r'"exp_return_12m"\s*:\s*([-\d.]+)', raw_response, "0")),
                        "forecast_confidence": _extract(r'"forecast_confidence"\s*:\s*"([^"]+)"', raw_response, "LOW"),
                        "description":         _extract(r'"description"\s*:\s*"([^"]*)"', raw_response, "Analysis unavailable."),
                    }

            # 5. Enrich with position context
            avg_cost = float(pos_row.get("weighted_avg_cost") or 0)
            price    = snap.get("close") or 0
            signal_data["ticker"]   = ticker
            signal_data["run_date"] = today
            signal_data["avg_cost"] = round(avg_cost, 2)
            signal_data["price"]    = price
            signal_data["swing_high"] = snap.get("swing_high")
            signal_data["swing_low"]  = snap.get("swing_low")
            signal_data["current_allocation_pct"] = round(
                float(pos_row.get("allocation_pct", 0)), 2
            ) if "allocation_pct" in pos_row.index else 0.0

            profit_pct = round((price - avg_cost) / avg_cost * 100, 2) if avg_cost > 0 else 0

            # ── Auto-calculate suggested_buy_price first (needed for allocation) ──
            sbp = signal_data.get("suggested_buy_price")

            def _is_missing(v):
                if v is None:
                    return True
                try:
                    f = float(v)
                    return f == 0 or (f != f)
                except (TypeError, ValueError):
                    return True

            if _is_missing(sbp):
                _support = snap.get("nearest_support")
                _fib_618 = snap.get("fib_618")
                _fib_50  = snap.get("fib_50")
                _close   = snap.get("close") or 0
                _ema21   = snap.get("ema21")

                if _support and _support < _close:
                    sbp = round(_support, 2)
                elif _fib_618 and _fib_618 < _close:
                    sbp = round(_fib_618, 2)
                elif _fib_50 and _fib_50 < _close:
                    sbp = round(_fib_50, 2)
                elif _ema21 and _ema21 < _close:
                    sbp = round(_ema21 * 0.97, 2)
                else:
                    sbp = round(_close * 0.95, 2)

            signal_data["suggested_buy_price"] = sbp

            # ── Apply deployment rules (signal + score + distance from entry) ──
            sig   = signal_data.get("signal", "HOLD")
            sc    = int(signal_data.get("score", 0))
            alloc = float(pos_row.get("allocation_pct", 0)) if "allocation_pct" in pos_row else 0.0

            if sig in ("TRIM_PEAK", "SELL", "REDUCE"):
                deploy_label = "Reduce"
                deploy_tier  = "reduce"
                deploy_pct   = 0.00
                deploy_note  = "Take profits or reduce — do not add capital."
            elif sig == "HOLD" and sc >= 55 and alloc < 15.0:
                deploy_label = "Starter only"
                deploy_tier  = "starter"
                deploy_pct   = 0.30
                deploy_note  = "HOLD but underweight — max 30% of capital, min EGP 2,000/ticker."
            else:
                deploy_label, deploy_note = get_capital_allocation(
                    score           = sc,
                    signal          = sig,
                    current_price   = snap.get("close"),
                    entry_zone_low  = snap.get("entry_zone_low"),
                    entry_zone_high = snap.get("entry_zone_high"),
                    fair_value_mid  = enhanced.get("fair_value", {}).get("fair_value_mid"),
                    rsi_daily       = snap.get("rsi"),
                    rsi_weekly      = enhanced.get("mtf", {}).get("rsi_weekly"),
                )
                _tier_map = {
                    "FULL DEPLOY": ("full",    1.00),
                    "HALF NOW":    ("half",    0.60),
                    "QUARTER NOW": ("quarter", 0.25),
                    "WAIT FOR DIP":("wait",    0.00),
                    "WATCH ONLY":  ("watch",   0.00),
                    "WAIT":        ("wait",    0.00),
                    "AVOID":       ("avoid",   0.00),
                }
                deploy_tier, deploy_pct = _tier_map.get(deploy_label, ("wait", 0.00))

            signal_data["deploy_pct"]      = deploy_pct
            signal_data["deploy_label"]    = deploy_label
            signal_data["deploy_tier"]     = deploy_tier
            signal_data["deploy_note"]     = deploy_note
            signal_data["entry_zone_low"]  = snap.get("entry_zone_low")
            signal_data["entry_zone_high"] = snap.get("entry_zone_high")
            signal_data["entry_zone_mid"]  = snap.get("entry_zone_mid")

            # 6. Persist to lt_signals
            insert_signal(
                run_date=today,
                ticker=ticker,
                avg_cost=signal_data["avg_cost"],
                price=signal_data["price"],
                signal=signal_data.get("signal"),
                action=signal_data.get("action"),
                score=signal_data.get("score"),
                position_size_pct=signal_data.get("position_size_pct"),
                current_allocation_pct=signal_data.get("current_allocation_pct"),
                recommended_shares=signal_data.get("recommended_shares", 0),
                recommended_capital=signal_data.get("recommended_capital", 0),
                suggested_buy_price=signal_data.get("suggested_buy_price"),
                profit_pct=profit_pct,
                sell_price=signal_data.get("sell_price"),
                fib_zone=signal_data.get("fib_zone"),
                swing_high=signal_data.get("swing_high"),
                swing_low=signal_data.get("swing_low"),
                target_1m=signal_data.get("target_1m"),
                target_6m=signal_data.get("target_6m"),
                target_12m=signal_data.get("target_12m"),
                exp_return_1m=signal_data.get("exp_return_1m"),
                exp_return_6m=signal_data.get("exp_return_6m"),
                exp_return_12m=signal_data.get("exp_return_12m"),
                forecast_confidence=signal_data.get("forecast_confidence"),
                description=signal_data.get("description"),
                deploy_pct=deploy_pct,
                deploy_label=deploy_label,
                deploy_note=deploy_note,
                deploy_tier=deploy_tier,
                enhanced_json=enhanced_json,
            )

            results[ticker] = signal_data
            logger.info("  %s: %s (score=%s)", ticker, signal_data.get("signal"), signal_data.get("score"))

        except json.JSONDecodeError as e:
            logger.error("  %s: LLM returned invalid JSON — %s", ticker, e)
            results[ticker] = {"error": "invalid_json"}
        except Exception as e:
            import traceback
            logger.error("  %s: Error — %s\n%s", ticker, e, traceback.format_exc())
            results[ticker] = {"error": str(e)}

    return results


def run_llm_descriptions(tickers: list = None) -> dict:
    """
    Second-pass: generate LLM descriptions for tickers that already have a
    signal score in lt_signals but whose description is still "pending".
    Returns dict: {ticker: "ok" | "skipped — ..." | "error — ..."}
    """
    from backend.data.db import get_connection
    from backend.analysis.technical import get_signal_snapshot

    conn    = get_connection()
    results = {}

    if tickers is None:
        rows   = conn.execute("SELECT DISTINCT ticker FROM lt_signals").fetchall()
        tickers = [r[0] for r in rows]

    for ticker in tickers:
        try:
            row = conn.execute(
                "SELECT score, signal, description, avg_cost, price "
                "FROM lt_signals WHERE ticker=? "
                "ORDER BY run_date DESC LIMIT 1",
                (ticker,),
            ).fetchone()

            if not row:
                results[ticker] = "skipped — no signal row"
                continue

            score, signal, existing_desc, avg_cost, price = row

            # Skip if already has real AI content
            if (existing_desc
                    and len(existing_desc) > 50
                    and "pending" not in existing_desc.lower()
                    and "⏳" not in existing_desc
                    and "unavailable" not in existing_desc.lower()):
                results[ticker] = "skipped — already has description"
                continue

            snap = get_signal_snapshot(ticker) or {}
            position = {"weighted_avg_cost": avg_cost}
            description = _generate_description(
                ticker, snap, position, signal or "HOLD", score or 0
            )

            conn.execute(
                "UPDATE lt_signals SET description=? "
                "WHERE ticker=? AND run_date=("
                "  SELECT MAX(run_date) FROM lt_signals WHERE ticker=?"
                ")",
                (description, ticker, ticker),
            )
            conn.commit()
            results[ticker] = "ok"
            logger.info("run_llm_descriptions: %s — description updated", ticker)

        except Exception as e:
            import traceback as _tb
            logger.error("run_llm_descriptions failed for %s: %s\n%s",
                         ticker, e, _tb.format_exc())
            results[ticker] = f"error — {e}"

    conn.close()
    return results

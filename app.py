"""
EGX Copilot — Streamlit entry point.
Run: streamlit run app.py
"""
import copy
import json
import logging
from pathlib import Path

# ── Startup — executes once per process, not on every Streamlit rerun ─────────
# load_dotenv() must come first so DB_PATH and API keys are in os.environ
# before any backend module reads them.
from dotenv import load_dotenv
load_dotenv()

from backend.data.db import init_db
from backend.data.scheduler import start_scheduler

init_db()
start_scheduler()

# ── Streamlit ─────────────────────────────────────────────────────────────────
import streamlit as st

ROOT = Path(__file__).parent
logging.basicConfig(level=logging.INFO)

st.set_page_config(
    page_title="EGX Copilot",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"About": "EGX Copilot — AI investment assistant for the Egyptian Stock Exchange."},
)

# ── Config ────────────────────────────────────────────────────────────────────
from backend.data.config_loader import load_config

cfg = load_config()
wallets = cfg["wallets"]
total  = wallets["total_capital_egp"]
lt_pct = wallets["long_term_pct"]
sw_pct = wallets["swing_pct"]
ca_pct = wallets["cash_pct"]

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📈 EGX Copilot")
    st.caption("Egyptian Stock Exchange · Shariah-compliant")
    st.divider()

    # ── Wallet summary ────────────────────────────────────────────────────────
    st.subheader("Wallet Summary")
    c1, c2 = st.columns(2)
    c1.metric("Long-term", f"EGP {int(total * lt_pct / 100):,}", f"{lt_pct}%")
    c2.metric("Swing",     f"EGP {int(total * sw_pct / 100):,}", f"{sw_pct}%")
    c1.metric("Cash",      f"EGP {int(total * ca_pct / 100):,}", f"{ca_pct}%")
    c2.metric("Total",     f"EGP {total:,}")

    st.divider()

    # ── Watchlist ─────────────────────────────────────────────────────────────
    st.subheader("Watchlist")

    @st.cache_data(ttl=300)
    def _watchlist_data(universe: tuple, lt: tuple, sw: tuple, bt: tuple):
        """Fetch last 2 prices per ticker and compute daily change. Cached 5 min."""
        from backend.data.db import get_prices
        lt_set = set(lt)
        sw_set = set(sw)
        bt_set = set(bt)
        rows = []
        for ticker in universe:
            df = get_prices(ticker, limit=2)
            if len(df) >= 2:
                today_close     = float(df["Close"].iloc[-1])
                yesterday_close = float(df["Close"].iloc[-2])
                change_pct = ((today_close - yesterday_close) / yesterday_close) * 100
            elif len(df) == 1:
                today_close = float(df["Close"].iloc[-1])
                change_pct  = None
            else:
                today_close = None
                change_pct  = None

            if ticker in lt_set:
                wallet = "LT"
            elif ticker in sw_set:
                wallet = "SW"
            elif ticker in bt_set:
                wallet = "Both"
            else:
                wallet = ""

            rows.append({
                "ticker":     ticker,
                "close":      today_close,
                "change_pct": change_pct,
                "wallet":     wallet,
            })
        return rows

    _wl = _watchlist_data(
        tuple(cfg["tickers"]["universe"]),
        tuple(cfg["tickers"]["long_term"]),
        tuple(cfg["tickers"]["swing"]),
        tuple(cfg["tickers"]["both"]),
    )

    # Column headers
    h1, h2, h3, h4 = st.columns([2, 1.5, 1.5, 1.5])
    h1.caption("**Ticker**")
    h2.caption("**Price**")
    h3.caption("**Change**")
    h4.caption("**Wallet**")

    _up = _down = 0
    for row in _wl:
        st.divider()
        c1, c2, c3, c4 = st.columns([2, 1.5, 1.5, 1.5])

        c1.markdown(f"**{row['ticker']}**")

        if row["close"] is not None:
            c2.caption(f"EGP {row['close']:.2f}")
        else:
            c2.caption("--")

        if row["change_pct"] is not None:
            if row["change_pct"] >= 0:
                _up += 1
                c3.markdown(
                    f'<span style="color:#00C896">▲ +{row["change_pct"]:.1f}%</span>',
                    unsafe_allow_html=True,
                )
            else:
                _down += 1
                c3.markdown(
                    f'<span style="color:#FF4757">▼ {row["change_pct"]:.1f}%</span>',
                    unsafe_allow_html=True,
                )
        else:
            c3.caption("--")

        _badge_html = {
            "LT":   '<span style="background:#0F6E56;color:#9FE1CB;padding:1px 6px;border-radius:4px;font-size:11px">LT</span>',
            "SW":   '<span style="background:#0C447C;color:#B5D4F4;padding:1px 6px;border-radius:4px;font-size:11px">SW</span>',
            "Both": '<span style="background:#633806;color:#FAC775;padding:1px 6px;border-radius:4px;font-size:11px">Both</span>',
        }
        if row["wallet"] in _badge_html:
            c4.markdown(_badge_html[row["wallet"]], unsafe_allow_html=True)
        else:
            c4.caption("")

    st.divider()
    st.caption(
        f"📊 {len(_wl)} tickers · "
        f"**{_up} up** · "
        f"**{_down} down** today"
    )
    st.divider()

    # ── Data status ───────────────────────────────────────────────────────────
    st.subheader("Data Status")

    from backend.data.price_collector import get_last_collection
    run_at, tickers_ok = get_last_collection()

    if run_at:
        st.caption(f"Last collection: **{run_at[:16]}** · {tickers_ok} tickers OK")
    else:
        st.caption("No collection runs yet.")

    # Show collect-result banner from previous rerun (avoids flash before rerun)
    if st.session_state.get("_collect_result"):
        r = st.session_state.pop("_collect_result")
        if r["failed"]:
            st.warning(
                f"Updated {r['updated']}  ·  "
                f"Failed: {', '.join(r['failed'])}"
            )
        else:
            st.success(f"✅ Updated {r['updated']} tickers")

    if st.button("⬇ Collect Now", use_container_width=True):
        with st.spinner("Fetching from yfinance…"):
            from backend.data.price_collector import collect_today
            result = collect_today()
        st.session_state._collect_result = result
        st.rerun()

    # DB summary table
    from backend.data.db import get_db_summary
    summary = get_db_summary()

    if summary:
        import pandas as pd
        _rows = [
            {
                "Ticker": t,
                "Rows":   v["rows"],
                "Latest": v["to_date"],
                "Close":  v["latest_close"],
            }
            for t, v in summary.items()
        ]
        st.dataframe(
            pd.DataFrame(_rows).set_index("Ticker"),
            use_container_width=True,
        )
    else:
        st.caption("No price data in DB yet.")

# ── Main tabs ─────────────────────────────────────────────────────────────────
tab_chat, tab_scan, tab_port, tab_cfg = st.tabs(
    ["💬 Chat", "📊 Swing Scanner", "🗂 Portfolio", "⚙️ Settings"]
)

# ─────────────────────────────────────────────────────────────────────────────
# Chat
# ─────────────────────────────────────────────────────────────────────────────
_SYSTEM_PROMPT = (
    "You are EGX Copilot, an AI investment assistant specialised in the Egyptian "
    "Stock Exchange (EGX). You help with Shariah-compliant investing across two "
    "portfolios: a long-term buy-and-hold wallet (60% allocation, 25% annual target) "
    "and a swing trading wallet (30% allocation, 10% monthly target, T+2 settlement). "
    "Current ticker universe: {universe}. "
    "All monetary values are in Egyptian Pounds (EGP). "
    "Be concise and precise. Always remind the user to verify live prices on their "
    "broker before placing any order — DB prices may be from the previous session."
).format(universe=", ".join(cfg["tickers"]["universe"]))

with tab_chat:
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    if prompt := st.chat_input("Ask about EGX stocks, signals, or your portfolio…"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

        with st.chat_message("assistant"):
            try:
                from backend.data.llm_client import stream_llm
                response = st.write_stream(
                    stream_llm(_SYSTEM_PROMPT, prompt, task="simple_chat")
                )
            except Exception as exc:
                response = f"⚠️ Could not reach LLM: {exc}"
                st.warning(response)

        st.session_state.messages.append({"role": "assistant", "content": response})

# ─────────────────────────────────────────────────────────────────────────────
# Swing Scanner
# ─────────────────────────────────────────────────────────────────────────────
with tab_scan:
    st.subheader("Swing Scanner")
    swing_tickers = cfg["tickers"]["swing"] + cfg["tickers"]["both"]
    st.info(
        "Signal scanner — coming soon. "
        "Will show top 3 scored setups from your ticker universe.\n\n"
        f"**Monitored:** {', '.join(swing_tickers)}\n\n"
        f"**Min score:** {cfg['swing']['signal_min_score']}/100 "
        "across RSI · EMA 9/21 · MACD · Volume · Support · Bollinger Bands\n\n"
        f"**Step target:** {cfg['swing']['step_target_pct']}%  |  "
        f"**Stop loss:** {cfg['swing']['stop_loss_pct']}%  |  "
        f"**Min R:R:** 1:{cfg['swing']['min_rr_ratio']}"
    )

# ─────────────────────────────────────────────────────────────────────────────
# Portfolio
# ─────────────────────────────────────────────────────────────────────────────
with tab_port:
    st.subheader("Portfolio")
    col_lt, col_sw = st.columns(2)

    with col_lt:
        st.subheader("🏦 Long-term Wallet")
        st.metric("Allocated", f"EGP {int(total * lt_pct / 100):,}")
        st.caption(
            f"Target: {cfg['long_term']['annual_target_pct']}% annual  ·  "
            f"Min hold: {cfg['long_term']['min_holding_days']} days  ·  "
            f"Max single stock: {cfg['long_term']['max_single_stock_pct']}%"
        )
        st.info("No open positions tracked yet.")

    with col_sw:
        st.subheader("⚡ Swing Wallet")
        st.metric("Allocated", f"EGP {int(total * sw_pct / 100):,}")
        st.caption(
            f"Target: {cfg['swing']['monthly_target_pct']}% monthly  ·  "
            f"Max positions: {cfg['swing']['max_positions']}  ·  "
            f"T+2 enforced"
        )
        st.info("No open positions tracked yet.")

# ─────────────────────────────────────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────────────────────────────────────
with tab_cfg:
    st.subheader("Settings")
    st.caption("Changes are saved to `config.json` immediately on clicking Save.")

    if st.session_state.get("_settings_saved"):
        st.success("Settings saved successfully.")
        st.session_state._settings_saved = False

    # ── Wallet allocation ─────────────────────────────────────────────────
    st.subheader("Wallet Allocation")
    new_total = st.number_input(
        "Total Capital (EGP)",
        min_value=10_000, max_value=10_000_000,
        value=total, step=10_000,
    )
    new_lt = st.slider("Long-term %", 0, 100, lt_pct)
    swing_max = 100 - new_lt
    new_sw = st.slider("Swing %", 0, swing_max, min(sw_pct, swing_max))
    new_ca = 100 - new_lt - new_sw
    st.info(
        f"**Cash automatically set to {new_ca}%** "
        f"= EGP {int(new_total * new_ca / 100):,}"
    )

    # ── Swing parameters ──────────────────────────────────────────────────
    st.divider()
    st.subheader("Swing Trading")
    new_min_score  = st.slider("Min signal score (/ 100)", 50, 100, cfg["swing"]["signal_min_score"])
    new_min_trade  = st.number_input(
        "Min trade size (EGP)",
        min_value=500, max_value=50_000,
        value=cfg["swing"]["min_trade_egp"], step=500,
    )
    new_monthly_tgt = st.slider("Monthly target %", 1, 30, cfg["swing"]["monthly_target_pct"])
    new_step_tgt    = st.slider("Step target %", 1.0, 10.0, float(cfg["swing"]["step_target_pct"]), step=0.5)
    new_stop        = st.slider("Stop loss %",   0.5, 10.0, float(cfg["swing"]["stop_loss_pct"]),   step=0.5)

    # ── Feature toggles ───────────────────────────────────────────────────
    st.divider()
    st.subheader("Feature Toggles")
    _toggle_labels = {
        "shariah_filter":           "Shariah filter — screen all signals before display",
        "t2_enforcement":           "T+2 enforcement — block re-entry on unsettled cash",
        "morning_scan_notify":      "Morning scan notification at 09:45 EET",
        "stop_loss_hard_block":     "Hard-block trade entry if stop-loss gate fails",
        "sector_correlation_check": "Sector correlation check (max 2 from same sector)",
        "cbe_event_blackout":       "CBE event blackout — freeze signals on rate-decision days",
    }
    new_toggles: dict[str, bool] = {}
    for key, label in _toggle_labels.items():
        new_toggles[key] = st.toggle(label, value=cfg["toggles"].get(key, False))

    # ── Save ──────────────────────────────────────────────────────────────
    st.divider()
    if st.button("💾 Save Settings", type="primary"):
        new_cfg = copy.deepcopy(cfg)
        new_cfg["wallets"]["total_capital_egp"] = new_total
        new_cfg["wallets"]["long_term_pct"]     = new_lt
        new_cfg["wallets"]["swing_pct"]         = new_sw
        new_cfg["wallets"]["cash_pct"]          = new_ca
        new_cfg["swing"]["signal_min_score"]    = new_min_score
        new_cfg["swing"]["min_trade_egp"]       = new_min_trade
        new_cfg["swing"]["monthly_target_pct"]  = new_monthly_tgt
        new_cfg["swing"]["step_target_pct"]     = new_step_tgt
        new_cfg["swing"]["stop_loss_pct"]       = new_stop
        new_cfg["toggles"]                      = new_toggles

        with open(ROOT / "config.json", "w", encoding="utf-8") as f:
            json.dump(new_cfg, f, indent=2)

        load_config.cache_clear()
        st.session_state._settings_saved = True
        st.rerun()

    # ── Manual price entry ────────────────────────────────────────────────
    st.divider()
    st.subheader("Manual price entry")

    from datetime import datetime
    from backend.data.db import get_connection, get_manual_entries

    # Success banner from previous Add interaction
    if st.session_state.get("_mp_success"):
        st.success(st.session_state.pop("_mp_success"))

    _mc1, _mc2, _mc3, _mc4 = st.columns([2, 2, 2, 1])
    with _mc1:
        _mp_ticker = st.selectbox("Ticker", options=cfg["tickers"]["universe"])
    with _mc2:
        _mp_date = st.date_input("Date", value=datetime.today())
    with _mc3:
        _mp_close = st.number_input("Close price (EGP)", min_value=0.01, step=0.01, format="%.2f")
    with _mc4:
        st.write("")   # vertical spacer to align button with inputs
        _mp_add = st.button("Add")

    if _mp_add:
        try:
            _conn = get_connection()
            try:
                _conn.execute(
                    """INSERT OR REPLACE INTO manual_prices
                           (ticker, date, close, entered_by)
                       VALUES (?, ?, ?, 'manual')""",
                    (_mp_ticker, str(_mp_date), _mp_close),
                )
                _conn.execute(
                    """INSERT OR REPLACE INTO prices
                           (ticker, date, open, high, low, close, volume, source)
                       VALUES (?, ?, ?, ?, ?, ?, 0, 'manual')""",
                    (_mp_ticker, str(_mp_date),
                     _mp_close, _mp_close, _mp_close, _mp_close),
                )
                _conn.commit()
            finally:
                _conn.close()
            st.session_state._mp_success = (
                f"Added {_mp_ticker} close {_mp_close:.2f} for {_mp_date}"
            )
            st.rerun()
        except Exception as exc:
            st.error(f"DB error: {exc}")

    # Recent manual entries
    _manual_df = get_manual_entries(limit=20)
    if not _manual_df.empty:
        st.dataframe(
            _manual_df[["ticker", "date", "close", "entered_by"]],
            use_container_width=True,
        )
        for _, _mrow in _manual_df.iterrows():
            _rd1, _rd2 = st.columns([6, 1])
            _rd1.caption(
                f"{_mrow['ticker']} · {_mrow['date']} · EGP {_mrow['close']:.2f}"
            )
            if _rd2.button(f"Delete {_mrow['rowid']}"):
                try:
                    _conn = get_connection()
                    try:
                        _conn.execute(
                            "DELETE FROM manual_prices WHERE rowid=?",
                            (int(_mrow["rowid"]),),
                        )
                        _conn.execute(
                            """DELETE FROM prices
                               WHERE ticker=? AND date=? AND source='manual'""",
                            (_mrow["ticker"], _mrow["date"]),
                        )
                        _conn.commit()
                    finally:
                        _conn.close()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Delete error: {exc}")

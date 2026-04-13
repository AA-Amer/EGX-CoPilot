"""
EGX Copilot — Streamlit entry point.
Run: streamlit run app.py
"""
import copy
import json
import logging
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).parent
logging.basicConfig(level=logging.INFO)

# ── Page config ───────────────────────────────────────────────────────────────
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
total = wallets["total_capital_egp"]
lt_pct = wallets["long_term_pct"]
sw_pct = wallets["swing_pct"]
ca_pct = wallets["cash_pct"]

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📈 EGX Copilot")
    st.caption("Egyptian Stock Exchange · Shariah-compliant")
    st.divider()

    # Wallet metrics
    st.subheader("Wallet Summary")
    c1, c2 = st.columns(2)
    c1.metric("Long-term", f"EGP {int(total * lt_pct / 100):,}", f"{lt_pct}%")
    c2.metric("Swing", f"EGP {int(total * sw_pct / 100):,}", f"{sw_pct}%")
    c1.metric("Cash", f"EGP {int(total * ca_pct / 100):,}", f"{ca_pct}%")
    c2.metric("Total", f"EGP {total:,}")

    st.divider()

    # Watchlist
    st.subheader("Watchlist")
    lt_set = set(cfg["tickers"]["long_term"])
    sw_set = set(cfg["tickers"]["swing"])
    bt_set = set(cfg["tickers"]["both"])

    for ticker in cfg["tickers"]["universe"]:
        if ticker in lt_set:
            badge = "🟢"
            role = "LT"
        elif ticker in sw_set:
            badge = "🔵"
            role = "SW"
        elif ticker in bt_set:
            badge = "🟡"
            role = "LT+SW"
        else:
            badge = "⚪"
            role = ""
        st.text(f"{badge} {ticker:<6}  {role}")

    st.divider()
    st.caption("🟢 Long-term  🔵 Swing  🟡 Both")

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
    "broker before placing any order, since data is 15 minutes delayed."
).format(universe=", ".join(cfg["tickers"]["universe"]))

with tab_chat:
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Render conversation history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    # User input
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

    # Show save-confirmation banner across reruns
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
    # Clamp swing default so it fits within remaining room
    swing_max = 100 - new_lt
    new_sw = st.slider(
        "Swing %", 0, swing_max,
        min(sw_pct, swing_max),
    )
    new_ca = 100 - new_lt - new_sw
    st.info(
        f"**Cash automatically set to {new_ca}%** "
        f"= EGP {int(new_total * new_ca / 100):,}"
    )

    # ── Swing parameters ──────────────────────────────────────────────────
    st.divider()
    st.subheader("Swing Trading")
    new_min_score = st.slider(
        "Min signal score (/ 100)", 50, 100,
        cfg["swing"]["signal_min_score"],
    )
    new_min_trade = st.number_input(
        "Min trade size (EGP)",
        min_value=500, max_value=50_000,
        value=cfg["swing"]["min_trade_egp"], step=500,
    )
    new_monthly_tgt = st.slider(
        "Monthly target %", 1, 30,
        cfg["swing"]["monthly_target_pct"],
    )
    new_step_tgt = st.slider(
        "Step target %", 1.0, 10.0,
        float(cfg["swing"]["step_target_pct"]), step=0.5,
    )
    new_stop = st.slider(
        "Stop loss %", 0.5, 10.0,
        float(cfg["swing"]["stop_loss_pct"]), step=0.5,
    )

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
        new_cfg["wallets"]["long_term_pct"] = new_lt
        new_cfg["wallets"]["swing_pct"] = new_sw
        new_cfg["wallets"]["cash_pct"] = new_ca
        new_cfg["swing"]["signal_min_score"] = new_min_score
        new_cfg["swing"]["min_trade_egp"] = new_min_trade
        new_cfg["swing"]["monthly_target_pct"] = new_monthly_tgt
        new_cfg["swing"]["step_target_pct"] = new_step_tgt
        new_cfg["swing"]["stop_loss_pct"] = new_stop
        new_cfg["toggles"] = new_toggles

        with open(ROOT / "config.json", "w", encoding="utf-8") as f:
            json.dump(new_cfg, f, indent=2)

        load_config.cache_clear()
        st.session_state._settings_saved = True
        st.rerun()

"""
EGX Copilot — Streamlit entry point.
Run: streamlit run app.py
"""
import copy
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

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
import time as _time

ROOT = Path(__file__).parent
logging.basicConfig(level=logging.INFO)

st.set_page_config(
    page_title="EGX Copilot",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
html, body, [class*="css"] { font-size: 13px !important; }

:root {
    --primary:    #5B8FA8;
    --primary-dk: #3D6E87;
    --teal:       #60A8B0;
    --bg:         #EDEDED;
    --bg2:        #D3D3D3;
    --mid:        #B8B8B6;
    --card:       #FFFFFF;
    --text:       #2C3E50;
    --muted:      #6B7B8D;
    --border:     #D3D3D3;
    --profit:     #00A86B;
    --loss:       #EF4444;
    --hold:       #F59E0B;
}

.stApp { background-color: var(--bg); color: var(--text); }

[data-testid="stSidebar"] { background-color: #2C3E50 !important; }
[data-testid="stSidebar"] * { color: #EDEDED !important; }
[data-testid="stSidebar"] .stMetric label { color: #B8B8B6 !important; font-size: 11px !important; }

/* ── Equal-height metric card rows ── */
[data-testid="column"] { display: flex; flex-direction: column; }
[data-testid="stMetric"] {
    flex: 1;
    background: var(--card);
    border: 1px solid var(--border);
    border-top: 3px solid var(--primary);
    border-radius: 6px;
    padding: 10px 14px !important;
    min-height: 80px;
    display: flex;
    flex-direction: column;
    justify-content: center;
}
[data-testid="stMetric"] label {
    font-size: 11px !important;
    color: var(--muted) !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-size: 20px !important;
    font-weight: 700;
    color: var(--text);
}
[data-testid="stMetricDelta"][data-direction="up"]   { color: var(--profit) !important; }
[data-testid="stMetricDelta"][data-direction="down"] { color: var(--loss)   !important; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 2px;
    background: var(--bg2);
    padding: 4px;
    border-radius: 8px;
}
.stTabs [data-baseweb="tab"] {
    font-size: 12px !important;
    padding: 6px 14px !important;
    border-radius: 6px;
    color: var(--muted);
    background: transparent;
    border: none;
}
.stTabs [aria-selected="true"] {
    background: var(--primary) !important;
    color: #FFFFFF !important;
    font-weight: 600;
}

/* ── Buttons ── */
.stButton > button {
    background-color: var(--primary) !important;
    color: #FFFFFF !important;
    border: none;
    border-radius: 6px;
    font-size: 12px !important;
    padding: 6px 16px !important;
    transition: background 0.15s;
}
.stButton > button:hover { background-color: var(--primary-dk) !important; }

/* ── Dataframes ── */
[data-testid="stDataFrame"] { font-size: 12px !important; }
[data-testid="stDataFrame"] th {
    background: var(--bg2) !important;
    color: var(--text) !important;
    font-size: 11px !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}

/* ── Expanders ── */
.streamlit-expanderHeader {
    font-size: 13px !important;
    font-weight: 600;
    background: var(--bg2) !important;
    border-radius: 6px !important;
}

/* ── Inputs ── */
.stTextInput input, .stNumberInput input {
    font-size: 12px !important;
    padding: 5px 10px !important;
    border: 1px solid var(--border) !important;
    border-radius: 4px !important;
}
.stSelectbox [data-baseweb="select"] { font-size: 12px !important; }

/* ── Headers ── */
h1 { font-size: 18px !important; color: var(--text)    !important; font-weight: 700; }
h2 { font-size: 15px !important; color: var(--primary) !important; font-weight: 600; }
h3 { font-size: 13px !important; color: var(--primary) !important; font-weight: 600; }

/* ── Signal chips ── */
.sig-buy  { background:#00A86B22; color:#00A86B; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:700; border:1px solid #00A86B44; }
.sig-hold { background:#F59E0B22; color:#C97D00; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:700; border:1px solid #F59E0B44; }
.sig-trim { background:#EF444422; color:#EF4444; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:700; border:1px solid #EF444444; }

/* ── Sidebar metric cards ── */
[data-testid="stSidebar"] [data-testid="stMetric"] {
    background: #3A4F65 !important;
    border: 1px solid #4A5A6A !important;
    border-top: 3px solid #5B8FA8 !important;
    border-radius: 6px;
    padding: 8px 10px !important;
    min-height: 60px;
}
[data-testid="stSidebar"] [data-testid="stMetricValue"] {
    font-size: 16px !important;
    font-weight: 700;
    color: #EDEDED !important;
}
[data-testid="stSidebar"] [data-testid="stMetricLabel"] {
    font-size: 10px !important;
    color: #B8B8B6 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

/* ── Sidebar divider ── */
[data-testid="stSidebar"] hr { border-color: #4A5A6A !important; }

/* ── Alerts / captions ── */
.stAlert { font-size: 12px !important; border-radius: 6px !important; }
.stCaption { color: var(--muted) !important; font-size: 11px !important; }
</style>
""", unsafe_allow_html=True)

# ── Auto-refresh polling ──────────────────────────────────────────────────────
# Checks every 60 s (via st_autorefresh) whether the scheduler has written a
# new price collection. If it has, shows a toast and reruns to refresh all data.
try:
    from streamlit_autorefresh import st_autorefresh as _st_autorefresh
    _st_autorefresh(interval=60_000, key="price_poll")
except ImportError:
    pass  # package not installed — DB-timestamp polling still works on manual navigation

from backend.data.db import get_last_collection_time as _get_last_ts

if "last_price_ts" not in st.session_state:
    st.session_state.last_price_ts = _get_last_ts()

_current_ts = _get_last_ts()
if _current_ts and _current_ts != st.session_state.last_price_ts:
    st.session_state.last_price_ts = _current_ts
    st.toast("📊 Prices updated by scheduler — refreshing...", icon="🔄")
    _time.sleep(0.5)
    st.rerun()

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
    from backend.data.lt_db import (
        get_kpi_summary       as _get_kpi,
        get_portfolio_summary as _get_ps,
    )

    st.sidebar.markdown("### 💼 Wallet Summary")

    _kpi = _get_kpi()
    _ps  = _get_ps()

    _lt_invested     = _kpi.get("total_invested", 0)
    _lt_market_value = _kpi.get("market_value", 0)
    _lt_cash         = _kpi.get("wallet_balance", 0)

    _sw_invested     = 0.0
    _sw_market_value = 0.0
    _sw_cash         = 0.0

    st.sidebar.markdown(
        '<p style="color:#60A8B0;font-size:10px;font-weight:700;'
        'letter-spacing:0.08em;margin:8px 0 4px 0;text-transform:uppercase;">'
        '📈 Long-Term Wallet</p>',
        unsafe_allow_html=True,
    )
    _la, _lb, _lc = st.sidebar.columns(3)
    _la.metric("Invested",  f"EGP {_lt_invested:,.0f}")
    _lb.metric("Cash",      f"EGP {_lt_cash:,.0f}")
    _lc.metric("Mkt Value", f"EGP {_lt_market_value:,.0f}")

    st.sidebar.markdown(
        '<hr style="border:none;border-top:1px solid rgba(255,255,255,0.1);margin:8px 0;"/>',
        unsafe_allow_html=True,
    )

    st.sidebar.markdown(
        '<p style="color:#B8B8B6;font-size:10px;font-weight:700;'
        'letter-spacing:0.08em;margin:4px 0;text-transform:uppercase;">'
        '⚡ Swing Wallet</p>',
        unsafe_allow_html=True,
    )
    _sa, _sb, _sc = st.sidebar.columns(3)
    _sa.metric("Invested",  f"EGP {_sw_invested:,.0f}")
    _sb.metric("Cash",      f"EGP {_sw_cash:,.0f}")
    _sc.metric("Mkt Value", f"EGP {_sw_market_value:,.0f}")

    st.sidebar.markdown(
        '<hr style="border:none;border-top:1px solid rgba(255,255,255,0.1);margin:8px 0;"/>',
        unsafe_allow_html=True,
    )

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

    _lt_tickers = set(cfg["tickers"]["long_term"])
    _sw_tickers = set(cfg["tickers"]["swing"])

    _up = _down = 0
    _tbl_rows = ""
    for _wr in _wl:
        _pr = f"EGP {_wr['close']:.2f}" if _wr["close"] is not None else "—"
        _chg_pct = _wr["change_pct"]
        if _chg_pct is not None:
            if _chg_pct > 0:
                _up += 1
                _chg = (
                    f'<span style="background:#00A86B22;color:#00A86B;padding:2px 6px;'
                    f'border-radius:4px;border:1px solid #00A86B55;font-weight:700;'
                    f'font-size:11px;">▲ +{_chg_pct:.2f}%</span>'
                )
            elif _chg_pct < 0:
                _down += 1
                _chg = (
                    f'<span style="background:#EF444422;color:#EF4444;padding:2px 6px;'
                    f'border-radius:4px;border:1px solid #EF444455;font-weight:700;'
                    f'font-size:11px;">▼ {_chg_pct:.2f}%</span>'
                )
            else:
                _chg = (
                    f'<span style="background:#B8B8B622;color:#B8B8B6;padding:2px 6px;'
                    f'border-radius:4px;border:1px solid #B8B8B655;font-weight:700;'
                    f'font-size:11px;">→ 0.00%</span>'
                )
        else:
            _chg = '<span style="color:#B8B8B6;font-size:11px;">—</span>'
        _tk = _wr["ticker"]
        if _tk in _lt_tickers and _tk in _sw_tickers:
            _bdg = '<span style="background:#9B8FA844;color:#D4C8E6;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:600;">BOTH</span>'
        elif _tk in _lt_tickers:
            _bdg = '<span style="background:#5B8FA844;color:#A8D4E6;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:600;">LT</span>'
        elif _tk in _sw_tickers:
            _bdg = '<span style="background:#60A8B044;color:#A8D4E6;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:600;">SW</span>'
        else:
            _bdg = ""
        _tbl_rows += (
            f"<tr>"
            f"<td style='padding:4px 3px;font-weight:700'>{_wr['ticker']}</td>"
            f"<td style='padding:4px 3px;font-size:11px'>{_pr}</td>"
            f"<td style='padding:4px 3px'>{_chg}</td>"
            f"<td style='padding:4px 3px'>{_bdg}</td>"
            f"</tr>"
        )

    st.markdown(
        f"""<table style="width:100%;border-collapse:collapse;font-size:12px;color:#EDEDED">
  <thead>
    <tr style="color:#B8B8B6;font-size:10px;text-transform:uppercase;border-bottom:1px solid #4A5A6A">
      <th style="padding:4px 3px;text-align:left;font-weight:600">Ticker</th>
      <th style="padding:4px 3px;text-align:left;font-weight:600">Price</th>
      <th style="padding:4px 3px;text-align:left;font-weight:600">Chg</th>
      <th style="padding:4px 3px;text-align:left;font-weight:600">W</th>
    </tr>
  </thead>
  <tbody>{_tbl_rows}</tbody>
</table>""",
        unsafe_allow_html=True,
    )
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
        st.caption(f"🟢 Last price update: {run_at[:16]} · {tickers_ok} tickers OK")
    else:
        st.caption("🔴 No price collection run yet")

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
tab_chat, tab_lt, tab_scan, tab_port, tab_cfg = st.tabs(
    ["💬 Chat", "🏦 Long-Term Wallet", "📊 Swing Scanner", "🗂 Portfolio", "⚙️ Settings"]
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
# Long-Term Wallet
# ─────────────────────────────────────────────────────────────────────────────
with tab_lt:
    import pandas as pd
    from datetime import datetime as _dt
    from backend.data.lt_db import (
        get_positions           as _lt_positions,
        get_portfolio_summary   as _lt_summary,
        get_transactions        as _lt_transactions,
        get_latest_signals      as _lt_signals,
        insert_transaction      as _lt_insert_tx,
        recalculate_positions   as _lt_recalc,
        get_all_inflation       as _lt_all_inflation,
        get_latest_inflation_index as _lt_latest_cpi,
        insert_inflation        as _lt_insert_inflation,
        get_transaction_by_id   as _lt_get_tx_by_id,
        update_transaction      as _lt_update_tx,
    )

    # ── Shared banner (persists across subtab switches) ───────────────────────
    if st.session_state.get("_lt_success"):
        st.success(st.session_state.pop("_lt_success"))
    if st.session_state.get("_lt_error"):
        st.error(st.session_state.pop("_lt_error"))

    # ── Subtabs ───────────────────────────────────────────────────────────────
    _lt_sub_tx, _lt_sub_pos, _lt_sub_kpi, _lt_sub_inf, _lt_sub_fun, _lt_sub_rec = st.tabs(
        ["📋 Transactions", "📊 Positions Summary",
         "📈 KPIs Dashboard", "🌡️ Inflation", "📁 Fundamentals", "🤖 Recommendations"]
    )

    # ─────────────────────────────────────────────────────────────────────────
    # SUBTAB 1 — Transactions
    # ─────────────────────────────────────────────────────────────────────────
    with _lt_sub_tx:
        try:
            # ── Filters ──────────────────────────────────────────────────────
            _f1, _f2 = st.columns(2)
            with _f1:
                _tx_cat_opts = ["All", "Buy", "Sell", "Dividend", "Top-Up", "Subscription"]
                _tx_cat_sel  = st.selectbox("Category", _tx_cat_opts, key="ltx_cat")
            with _f2:
                _tx_tick_opts = ["All"] + cfg["tickers"]["universe"] + ["ABUK", "BSB"]
                _tx_tick_sel  = st.selectbox("Ticker", _tx_tick_opts, key="ltx_tick")

            _txn_cat  = None if _tx_cat_sel  == "All" else _tx_cat_sel
            _txn_tick = None if _tx_tick_sel == "All" else _tx_tick_sel

            _txns_df = _lt_transactions(ticker=_txn_tick, category=_txn_cat, limit=200)
            if not _txns_df.empty:
                _tx_disp = _txns_df.copy()
                if "fulfillment_price" in _tx_disp.columns:
                    _tx_disp["fulfillment_price"] = _tx_disp["fulfillment_price"].map(
                        lambda x: f"EGP {x:.2f}" if pd.notna(x) else "")
                if "fees" in _tx_disp.columns:
                    _tx_disp["fees"] = _tx_disp["fees"].map(
                        lambda x: f"EGP {x:.2f}" if pd.notna(x) else "")
                if "total_amount" in _tx_disp.columns:
                    _tx_disp["total_amount"] = _tx_disp["total_amount"].map(
                        lambda x: f"EGP {x:,.2f}" if pd.notna(x) else "")
                if "net_wallet_impact" in _tx_disp.columns:
                    _tx_disp["net_wallet_impact"] = _tx_disp["net_wallet_impact"].map(
                        lambda x: f"EGP {x:,.2f}" if pd.notna(x) else "")
                if "fx_rate" in _tx_disp.columns:
                    _tx_disp["fx_rate"] = _tx_disp["fx_rate"].map(
                        lambda x: f"{x:.4f}" if pd.notna(x) else "")
                if "quantity" in _tx_disp.columns:
                    _tx_disp["quantity"] = _tx_disp["quantity"].map(
                        lambda x: f"{x:.0f}" if pd.notna(x) else "")
                _show_tx_cols = [c for c in (
                    "date", "category", "ticker", "quantity", "fulfillment_price",
                    "fees", "total_amount", "net_wallet_impact", "fx_rate",
                ) if c in _tx_disp.columns]
                st.dataframe(_tx_disp[_show_tx_cols], use_container_width=True)
                st.caption(f"{len(_txns_df)} transaction(s) shown")
            else:
                st.caption("No transactions match the selected filters.")

            # ── Edit / Add Transaction expanders ─────────────────────────────
            st.divider()
            with st.expander("✏️ Edit Existing Transaction"):
                _all_tx = _lt_transactions(limit=500)
                if _all_tx.empty:
                    st.info("No transactions to edit yet.")
                else:
                    _all_tx["_label"] = _all_tx.apply(
                        lambda r: (
                            f"#{int(r['id'])} | {r['date']} | {r['category']} "
                            f"| {r.get('ticker') or '—'} "
                            f"| EGP {float(r['total_amount'] or 0):,.2f}"
                        ),
                        axis=1,
                    )
                    _edit_label = st.selectbox(
                        "Select transaction to edit",
                        _all_tx["_label"].tolist(),
                        key="edit_tx_select",
                    )
                    _edit_id = int(_edit_label.split("|")[0].replace("#", "").strip())
                    _etx = _lt_get_tx_by_id(_edit_id)

                    if _etx:
                        _ec1, _ec2, _ec3 = st.columns(3)
                        _edit_cats = ["Buy", "Sell", "Dividend", "Top-Up", "Subscription"]
                        _edit_tickers = ["", "AMOC", "MICH", "MPCI", "OLFI",
                                         "ORAS", "ORWE", "SUGR", "SWDY", "ABUK", "BSB"]
                        _eid = _edit_id  # alias for key suffix
                        with _ec1:
                            _e_date = st.date_input(
                                "Date",
                                value=pd.to_datetime(_etx["date"]),
                                key=f"e_date_{_eid}",
                            )
                            _e_cat = st.selectbox(
                                "Category", _edit_cats,
                                index=_edit_cats.index(_etx["category"])
                                      if _etx["category"] in _edit_cats else 0,
                                key=f"e_cat_{_eid}",
                            )
                        with _ec2:
                            _e_ticker = st.selectbox(
                                "Ticker", _edit_tickers,
                                index=_edit_tickers.index(_etx["ticker"])
                                      if _etx["ticker"] in _edit_tickers else 0,
                                key=f"e_ticker_{_eid}",
                            )
                            _e_qty = st.number_input(
                                "Quantity", value=float(_etx["quantity"] or 1),
                                step=1.0, key=f"e_qty_{_eid}",
                            )
                        with _ec3:
                            _e_price = st.number_input(
                                "Price/share (EGP)",
                                value=float(_etx["fulfillment_price"] or 0),
                                step=0.01, format="%.4f", key=f"e_price_{_eid}",
                            )
                            _e_fees = st.number_input(
                                "Fees (EGP)", value=float(_etx["fees"] or 0),
                                step=0.01, format="%.2f", key=f"e_fees_{_eid}",
                            )
                            _e_fx = st.number_input(
                                "FX rate (EGP per USD)",
                                value=float(_etx["fx_rate"] or 0),
                                step=0.0001, format="%.4f", key=f"e_fx_{_eid}",
                            )

                        # Auto-calc (mirrors Add form logic)
                        _e_total = float(_e_qty) * float(_e_price) + float(_e_fees)
                        if _e_cat == "Buy":
                            _e_net, _e_ext = -_e_total, 0.0
                        elif _e_cat == "Sell":
                            _e_net, _e_ext = +_e_total, 0.0
                        elif _e_cat == "Dividend":
                            _e_net, _e_ext = float(_e_qty) * float(_e_price), 0.0
                        elif _e_cat == "Top-Up":
                            _e_net = float(_e_qty) * float(_e_price)
                            _e_ext = -_e_total
                        else:  # Subscription
                            _e_net, _e_ext = 0.0, -_e_total
                        _e_usd = round(_e_total / float(_e_fx), 2) if float(_e_fx) > 0 else 0.0

                        _em1, _em2, _em3 = st.columns(3)
                        _em1.metric("Total (EGP)",           f"EGP {_e_total:,.2f}")
                        _em2.metric("Net Wallet Impact",     f"EGP {_e_net:+,.2f}")
                        _em3.metric("Ext. Capital Impact",   f"EGP {_e_ext:+,.2f}")

                        _e_notes = st.text_input(
                            "Notes", value=str(_etx.get("notes") or ""), key=f"e_notes_{_eid}"
                        )

                        if st.button("💾 Save Changes", type="primary", key="btn_edit_save"):
                            try:
                                _lt_update_tx(
                                    transaction_id=_edit_id,
                                    date=str(_e_date),
                                    category=_e_cat,
                                    ticker=_e_ticker if _e_ticker else None,
                                    quantity=float(_e_qty),
                                    fulfillment_price=float(_e_price),
                                    fees=float(_e_fees),
                                    dividend_tax=0.0,
                                    total_amount=_e_total,
                                    fx_rate=float(_e_fx) if float(_e_fx) > 0 else None,
                                    usd_value=_e_usd if float(_e_fx) > 0 else None,
                                    net_wallet_impact=_e_net,
                                    external_capital_impact=_e_ext,
                                    notes=_e_notes,
                                )
                                st.session_state._lt_success = (
                                    f"Transaction #{_edit_id} updated and positions recalculated."
                                )
                                st.rerun()
                            except Exception as _exc:
                                st.error(f"Update error: {_exc}")

            with st.expander("➕ Add transaction"):
                _extra_tickers = ["ABUK", "BSB"]
                _all_lt_tickers = (
                    [""] + cfg["tickers"]["universe"]
                    + [t for t in _extra_tickers if t not in cfg["tickers"]["universe"]]
                )

                # User inputs — 3 columns
                _tf1, _tf2, _tf3 = st.columns(3)
                with _tf1:
                    _t_date = st.date_input("Date", value=_dt.today(), key="lt_t_date")
                    _t_cat  = st.selectbox(
                        "Category",
                        ["Buy", "Sell", "Dividend", "Top-Up", "Subscription"],
                        key="lt_t_cat",
                    )
                with _tf2:
                    _t_ticker = st.selectbox(
                        "Ticker (leave blank for Top-Up / Subscription)",
                        _all_lt_tickers,
                        key="lt_t_ticker",
                    )
                    _t_qty = st.number_input(
                        "Quantity / Shares", min_value=0.0, step=1.0, key="lt_t_qty"
                    )
                with _tf3:
                    _t_price = st.number_input(
                        "Price per share (EGP)", min_value=0.0, step=0.01,
                        format="%.4f", key="lt_t_price",
                    )
                    _t_fees = st.number_input(
                        "Fees (EGP)", min_value=0.0, step=0.01,
                        format="%.2f", key="lt_t_fees",
                    )

                _tf_fx, _tf_sp1, _tf_sp2 = st.columns(3)
                with _tf_fx:
                    _t_fx = st.number_input(
                        "FX rate (EGP per USD)",
                        min_value=0.0, step=0.01, format="%.4f", key="lt_t_fx",
                    )

                # Auto-calculated values
                _calc_total = float(_t_qty) * float(_t_price) + float(_t_fees)
                if _t_cat == "Buy":
                    _calc_net, _calc_ext = -_calc_total, 0.0
                elif _t_cat == "Sell":
                    _calc_net, _calc_ext = +_calc_total, 0.0
                elif _t_cat == "Dividend":
                    _calc_net, _calc_ext = float(_t_qty) * float(_t_price), 0.0
                elif _t_cat == "Top-Up":
                    _calc_net = float(_t_qty) * float(_t_price)
                    _calc_ext = -_calc_total
                else:  # Subscription
                    _calc_net, _calc_ext = 0.0, -_calc_total

                _mc1, _mc2, _mc3 = st.columns(3)
                _mc1.metric("Total Amount (EGP)",           f"{_calc_total:,.2f}")
                _mc2.metric("Net Wallet Impact (EGP)",       f"{_calc_net:+,.2f}")
                _mc3.metric("External Capital Impact (EGP)", f"{_calc_ext:+,.2f}")

                if float(_t_fx) > 0:
                    st.caption(
                        f"USD value: **{_calc_total / float(_t_fx):,.2f} USD** "
                        f"at {_t_fx:.4f} EGP/USD"
                    )

                _t_notes = st.text_input("Notes (optional)", key="lt_t_notes")

                if st.button("Save transaction", type="primary", key="lt_t_submit"):
                    _valid = True
                    if _t_cat in ("Buy", "Sell", "Dividend") and not _t_ticker:
                        st.error("Please select a ticker for this transaction category.")
                        _valid = False
                    elif float(_t_price) == 0 and _t_cat != "Subscription":
                        st.error("Price must be greater than 0.")
                        _valid = False

                    if _valid:
                        try:
                            _fx_val  = float(_t_fx) if float(_t_fx) > 0 else None
                            _usd_val = (_calc_total / _fx_val) if _fx_val else None
                            _lt_insert_tx(
                                date_str=str(_t_date),
                                category=_t_cat,
                                ticker=_t_ticker if _t_ticker else None,
                                quantity=float(_t_qty),
                                fulfillment_price=float(_t_price),
                                fees=float(_t_fees),
                                dividend_tax=0,
                                total_amount=_calc_total,
                                fx_rate=_fx_val,
                                usd_value=_usd_val,
                                net_wallet_impact=_calc_net,
                                external_capital_impact=_calc_ext,
                                notes=_t_notes,
                            )
                            _lt_recalc()
                            st.session_state._lt_success = (
                                "Transaction added and positions recalculated."
                            )
                            st.rerun()
                        except Exception as _exc:
                            st.error(f"Error: {_exc}")

        except Exception as _e:
            st.error(f"Transactions tab error: {_e}")

    # ─────────────────────────────────────────────────────────────────────────
    # SUBTAB 2 — Positions Summary
    # ─────────────────────────────────────────────────────────────────────────
    with _lt_sub_pos:
        try:
            _summary = _lt_summary()
            _pos_df  = _lt_positions()

            if _summary["total_cost"] == 0 and _pos_df.empty:
                st.info("No data yet — seed the database first.")
            else:
                # ── Summary cards (3 cards) ───────────────────────────────────
                _p1, _p2, _p3 = st.columns(3)
                _p1.metric(
                    "Unrealized P/L",
                    f"EGP {_summary['unrealized_pl']:,.2f}",
                    f"{_summary['unrealized_pct']:+.2f}%",
                )
                _p2.metric(
                    "Realized P/L",
                    f"EGP {_summary['realized_pl']:,.2f}",
                )
                _p3.metric(
                    "Dividends",
                    f"EGP {_summary['dividends_total']:,.2f}",
                )

                st.divider()

                # ── Open positions ────────────────────────────────────────────
                st.subheader("Open Positions")
                _open_df = _pos_df[_pos_df["status"] == "Open"] if not _pos_df.empty else _pos_df

                if not _open_df.empty:
                    def _color_pnl_str(val):
                        """Color pre-formatted string P/L cells: green if positive, red if negative."""
                        if isinstance(val, str):
                            _stripped = val.replace("EGP", "").replace("£", "").replace(",", "").replace("%", "").strip()
                            try:
                                return f"color: {'#00A86B' if float(_stripped) >= 0 else '#EF4444'}"
                            except ValueError:
                                return ""
                        return ""

                    _pos_disp = _open_df.copy()
                    if "total_shares" in _pos_disp.columns:
                        _pos_disp["Shares"] = _pos_disp["total_shares"].map(
                            lambda x: f"{x:.0f}" if pd.notna(x) else "")
                    if "weighted_avg_cost" in _pos_disp.columns:
                        _pos_disp["Avg Cost"] = _pos_disp["weighted_avg_cost"].map(
                            lambda x: f"EGP {x:.2f}" if pd.notna(x) else "")
                    if "current_price" in _pos_disp.columns:
                        _pos_disp["Current Price"] = _pos_disp["current_price"].map(
                            lambda x: f"EGP {x:.2f}" if pd.notna(x) else "")
                    if "market_value" in _pos_disp.columns:
                        _pos_disp["Market Value"] = _pos_disp["market_value"].map(
                            lambda x: f"EGP {x:,.2f}" if pd.notna(x) else "")
                    if "unrealized_pl" in _pos_disp.columns:
                        _pos_disp["Unrealized P/L"] = _pos_disp["unrealized_pl"].map(
                            lambda x: f"EGP {x:,.2f}" if pd.notna(x) else "")
                    if "unrealized_pct" in _pos_disp.columns:
                        _pos_disp["Unrealized %"] = _pos_disp["unrealized_pct"].map(
                            lambda x: f"{x:.2f}%" if pd.notna(x) else "")
                    if "allocation_pct" in _pos_disp.columns:
                        _pos_disp["Allocation %"] = _pos_disp["allocation_pct"].map(
                            lambda x: f"{x:.2f}%" if pd.notna(x) else "")

                    _pos_show = [c for c in
                                 ("ticker", "Shares", "Avg Cost", "Current Price",
                                  "Market Value", "Unrealized P/L", "Unrealized %",
                                  "Allocation %")
                                 if c in _pos_disp.columns]
                    st.dataframe(
                        _pos_disp[_pos_show].rename(columns={"ticker": "Ticker"})
                        .style.map(_color_pnl_str,
                                   subset=[c for c in ("Unrealized P/L", "Unrealized %")
                                           if c in _pos_show]),
                        use_container_width=True,
                    )
                else:
                    st.caption("No open positions.")

                # ── Closed positions ──────────────────────────────────────────
                _closed_df = (
                    _pos_df[_pos_df["status"] == "Closed"]
                    if not _pos_df.empty else pd.DataFrame()
                )
                if not _closed_df.empty:
                    st.divider()
                    st.subheader("Closed Positions")
                    _cl_cols = [c for c in
                                ("ticker", "realized_pl", "dividends_net", "total_return")
                                if c in _closed_df.columns]
                    st.dataframe(
                        _closed_df[_cl_cols].rename(columns={
                            "ticker":        "Ticker",
                            "realized_pl":   "Realized P/L",
                            "dividends_net": "Dividends",
                            "total_return":  "Total Return",
                        }),
                        use_container_width=True,
                    )

        except Exception as _e:
            st.error(f"Positions tab error: {_e}")

    # ─────────────────────────────────────────────────────────────────────────
    # SUBTAB 3 — KPIs Dashboard
    # ─────────────────────────────────────────────────────────────────────────
    with _lt_sub_kpi:
        try:
            from backend.data.lt_db import get_kpi_summary as _get_kpi
            import plotly.express as _px
            import plotly.graph_objects as _go

            _kpi = _get_kpi()
            _cpi = _lt_latest_cpi()

            if _kpi["total_invested"] == 0 and _kpi["market_value"] == 0:
                st.info("No data yet — seed the database first.")
            else:
                def _section_header(title):
                    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:12px;margin:16px 0 10px 0;">
        <span style="font-size:11px;font-weight:700;color:#5B8FA8;
        text-transform:uppercase;letter-spacing:0.1em;white-space:nowrap;">{title}</span>
        <div style="flex:1;height:1px;background:#D3D3D3;"></div>
    </div>
    """, unsafe_allow_html=True)

                # ── Wallet Summary ────────────────────────────────────────────
                _section_header("Wallet Summary")

                _mkt_val   = _kpi["market_value"]
                _cash      = _kpi["wallet_balance"]
                _total_val = _mkt_val + _cash
                _pl        = _kpi["unrealized_pl"]
                _pl_pct    = (_pl / _mkt_val * 100) if _mkt_val > 0 else 0
                _pl_sign   = "+" if _pl >= 0 else ""

                w1, w2, w3, w4 = st.columns(4)
                w1.metric("Market Value",       f"EGP {_mkt_val:,.2f}")
                w2.metric("Available Cash",     f"EGP {_cash:,.2f}")
                w3.metric("Total Wallet Value", f"EGP {_total_val:,.2f}")
                _pl_color = "#00A86B" if _pl >= 0 else "#EF4444"
                w4.markdown(f"""
<div style="
    background:#FFFFFF;
    border:1px solid #D3D3D3;
    border-top:3px solid #5B8FA8;
    border-radius:6px;
    padding:10px 14px;
    min-height:80px;
    display:flex;
    flex-direction:column;
    justify-content:center;
">
    <p style="
        font-size:11px;
        color:#6B7B8D;
        text-transform:uppercase;
        letter-spacing:0.04em;
        margin:0 0 4px 0;
    ">Unrealized P/L</p>
    <p style="
        font-size:18px;
        font-weight:700;
        color:{_pl_color};
        margin:0;
        white-space:nowrap;
    ">{_pl_sign}EGP {abs(_pl):,.2f} &nbsp;<span style="font-size:14px;">({_pl_sign}{_pl_pct:.2f}%)</span></p>
</div>
""", unsafe_allow_html=True)

                st.markdown("<div style='margin:8px 0;'></div>", unsafe_allow_html=True)

                # ── Capital ───────────────────────────────────────────────────
                _section_header("Capital")
                k1, k2, k3, k4 = st.columns(4)
                k1.metric(
                    "Thndr Capital Invested",
                    f"EGP {_kpi['thndr_capital']:,.2f}",
                    help="Sum of Top-Up principal only (excl. transfer fees)",
                )
                k2.metric(
                    "Advisor Cost",
                    f"EGP {_kpi['advisor_cost']:,.2f}",
                    help="Total subscription payments to advisor",
                )
                k3.metric(
                    "Total Fees",
                    f"EGP {_kpi['total_fees']:,.2f}",
                    help="Sum of all transaction fees (Top-Ups + trades)",
                )
                k4.metric(
                    "Real Invested Capital",
                    f"EGP {_kpi['real_capital']:,.2f}",
                    help="Everything paid from pocket = abs(sum of external capital impact)",
                )

                # ── Returns ───────────────────────────────────────────────────
                st.markdown("<div style='margin:8px 0;'></div>", unsafe_allow_html=True)
                _section_header("Returns")

                _open_pl      = _kpi["unrealized_pl"]
                _open_pl_pct  = (_open_pl / _kpi["total_market_value"] * 100) if _kpi["total_market_value"] > 0 else 0

                _gross_profit     = _kpi["total_portfolio_value"] - _kpi["thndr_capital"]
                _gross_profit_pct = (_gross_profit / _kpi["thndr_capital"] * 100) if _kpi["thndr_capital"] > 0 else 0

                _net_profit     = _kpi["total_portfolio_value"] - _kpi["real_capital"]
                _net_profit_pct = (_net_profit / _kpi["real_capital"] * 100) if _kpi["real_capital"] > 0 else 0

                def _pl_sign(v):
                    return "+" if v >= 0 else ""

                def _pl_color(v):
                    return "#00A86B" if v >= 0 else "#EF4444"

                r1, r2, r3 = st.columns(3)

                r1.markdown(f"""
<div style="background:#FFFFFF;border:1px solid #D3D3D3;border-top:3px solid #5B8FA8;
border-radius:6px;padding:10px 14px;min-height:80px;display:flex;
flex-direction:column;justify-content:center;">
    <p style="font-size:11px;color:#6B7B8D;text-transform:uppercase;
    letter-spacing:0.04em;margin:0 0 4px 0;">Open Positions P/L</p>
    <p style="font-size:18px;font-weight:700;color:{_pl_color(_open_pl)};
    margin:0;white-space:nowrap;">
    {_pl_sign(_open_pl)}EGP {abs(_open_pl):,.2f}
    &nbsp;<span style="font-size:14px;">({_pl_sign(_open_pl)}{_open_pl_pct:.2f}%)</span></p>
</div>
""", unsafe_allow_html=True)

                r2.markdown(f"""
<div style="background:#FFFFFF;border:1px solid #D3D3D3;border-top:3px solid #5B8FA8;
border-radius:6px;padding:10px 14px;min-height:80px;display:flex;
flex-direction:column;justify-content:center;">
    <p style="font-size:11px;color:#6B7B8D;text-transform:uppercase;
    letter-spacing:0.04em;margin:0 0 4px 0;">Gross Profit</p>
    <p style="font-size:18px;font-weight:700;color:{_pl_color(_gross_profit)};
    margin:0;white-space:nowrap;">
    {_pl_sign(_gross_profit)}EGP {abs(_gross_profit):,.2f}
    &nbsp;<span style="font-size:14px;">({_pl_sign(_gross_profit)}{_gross_profit_pct:.2f}%)</span></p>
</div>
""", unsafe_allow_html=True)

                r3.markdown(f"""
<div style="background:#FFFFFF;border:1px solid #D3D3D3;border-top:3px solid #5B8FA8;
border-radius:6px;padding:10px 14px;min-height:80px;display:flex;
flex-direction:column;justify-content:center;">
    <p style="font-size:11px;color:#6B7B8D;text-transform:uppercase;
    letter-spacing:0.04em;margin:0 0 4px 0;">Net Profit</p>
    <p style="font-size:18px;font-weight:700;color:{_pl_color(_net_profit)};
    margin:0;white-space:nowrap;">
    {_pl_sign(_net_profit)}EGP {abs(_net_profit):,.2f}
    &nbsp;<span style="font-size:14px;">({_pl_sign(_net_profit)}{_net_profit_pct:.2f}%)</span></p>
</div>
""", unsafe_allow_html=True)

                # ── Positions ─────────────────────────────────────────────────
                st.markdown("<div style='margin:8px 0;'></div>", unsafe_allow_html=True)
                _section_header("Positions")

                _conc_risk  = _kpi["concentration_risk"]
                _conc_color = {"High": "#EF4444", "Moderate": "#F59E0B", "Healthy": "#00A86B"}.get(_conc_risk, "#6B7B8D")

                def _stat_card(label, value, color="#2C3E50", border_color="#5B8FA8"):
                    return f"""<div style="background:#FFFFFF;border:1px solid #D3D3D3;border-top:3px solid {border_color};border-radius:6px;padding:14px 16px;height:90px;display:flex;flex-direction:column;justify-content:center;box-sizing:border-box;"><p style="font-size:10px;color:#6B7B8D;text-transform:uppercase;letter-spacing:0.05em;margin:0 0 6px 0;font-weight:600;">{label}</p><p style="font-size:20px;font-weight:700;color:{color};margin:0;">{value}</p></div>"""

                # ── Row 1: 4 cards ──
                r1c1, r1c2, r1c3, r1c4 = st.columns(4)
                r1c1.markdown(_stat_card("Open Positions",    str(_kpi["total_open_positions"])), unsafe_allow_html=True)
                r1c2.markdown(_stat_card("Largest Position",  f"{_kpi['largest_position_pct']:.2f}%"), unsafe_allow_html=True)
                r1c3.markdown(_stat_card("Concentration Risk", _conc_risk, color=_conc_color, border_color=_conc_color), unsafe_allow_html=True)
                r1c4.markdown(_stat_card("Top 3 Exposure",    f"{_kpi['top3_exposure_pct']:.2f}%"), unsafe_allow_html=True)

                # ── Row 2: 4 cards (slot 2 blank) ──
                r2c1, r2c2, r2c3, r2c4 = st.columns(4)
                r2c1.markdown(_stat_card("Diversification",   f"{_kpi['div_score']:.2f}"), unsafe_allow_html=True)
                _largest_ticker = _kpi["allocation"][0]["ticker"] if _kpi.get("allocation") else "N/A"
                r2c2.markdown(_stat_card("Largest Position", _largest_ticker, color="#5B8FA8", border_color="#5B8FA8"), unsafe_allow_html=True)
                r2c3.markdown(_stat_card("Top Performer",  _kpi["top_performer"],  color="#00A86B", border_color="#00A86B"), unsafe_allow_html=True)
                r2c4.markdown(_stat_card("Worst Performer", _kpi["worst_performer"], color="#EF4444", border_color="#EF4444"), unsafe_allow_html=True)

                st.markdown("<div style='margin:8px 0;'></div>", unsafe_allow_html=True)

                # ── Charts ────────────────────────────────────────────────────
                _pos_data = _kpi.get("positions", [])
                if _pos_data:
                    st.divider()
                    _ch_left, _ch_right = st.columns(2)

                    # Pie: allocation by ticker
                    with _ch_left:
                        _section_header("Allocation by Ticker")
                        _pie_labels  = [p["ticker"] for p in _pos_data]
                        _pie_values  = [p["market_value"] for p in _pos_data]
                        _pie_fig = _px.pie(
                            names=_pie_labels,
                            values=_pie_values,
                            hole=0.45,
                            color_discrete_sequence=[
                                "#5B8FA8", "#3D6E87", "#60A8B0", "#2C3E50",
                                "#8BA7BA", "#B8D0DC", "#7FA0B2",
                            ],
                        )
                        _pie_fig.update_traces(textposition="inside", textinfo="percent+label")
                        _pie_fig.update_layout(
                            height=300,
                            margin=dict(t=10, b=10, l=10, r=10),
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            font_color="#2C3E50",
                            showlegend=False,
                        )
                        st.plotly_chart(_pie_fig, use_container_width=True)

                    # Vertical bar: unrealized P/L by ticker
                    with _ch_right:
                        _section_header("Unrealized P/L by Ticker")
                        _alloc_df = pd.DataFrame(_pos_data)
                        _alloc_df_sorted = _alloc_df.sort_values("unrealized_pl", ascending=False)

                        fig_bar = _go.Figure(_go.Bar(
                            x=_alloc_df_sorted["ticker"],
                            y=_alloc_df_sorted["unrealized_pl"],
                            orientation="v",
                            marker_color=[
                                "#00A86B" if v >= 0 else "#EF4444"
                                for v in _alloc_df_sorted["unrealized_pl"]
                            ],
                            text=[
                                f"+EGP {v:,.0f}" if v >= 0 else f"EGP {v:,.0f}"
                                for v in _alloc_df_sorted["unrealized_pl"]
                            ],
                            textposition="outside",
                            hovertemplate="<b>%{x}</b><br>P/L: EGP %{y:,.2f}<extra></extra>",
                        ))
                        fig_bar.update_layout(
                            xaxis_title="",
                            yaxis_title="Unrealized P/L (EGP)",
                            margin=dict(t=30, b=20, l=20, r=20),
                            height=320,
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            yaxis=dict(
                                gridcolor="#E2E8F0",
                                zeroline=True,
                                zerolinecolor="#5B8FA8",
                                zerolinewidth=1.5,
                            ),
                            font=dict(size=11),
                            bargap=0.35,
                        )
                        st.plotly_chart(fig_bar, use_container_width=True)

            # ── Fundamentals Coverage ─────────────────────────────────────
            _section_header("Fundamentals Coverage")
            try:
                from backend.data.db import get_connection as _gc
                _cov_conn = _gc()
                _fund_rows = _cov_conn.execute("""
                    SELECT w.ticker, w.sector,
                           fd.period, fd.period_type, fd.quarter,
                           fd.fiscal_year, fd.eps, fd.net_profit
                    FROM watchlist w
                    LEFT JOIN (
                        SELECT ticker, period, period_type, quarter,
                               fiscal_year, eps, net_profit,
                               ROW_NUMBER() OVER (
                                   PARTITION BY ticker
                                   ORDER BY
                                       COALESCE(fiscal_year, 0) DESC,
                                       CASE period_type
                                           WHEN 'FY' THEN 0
                                           WHEN '9M' THEN 1
                                           WHEN 'H1' THEN 2
                                           WHEN 'Q1' THEN 3
                                           ELSE 4
                                       END ASC
                               ) AS rn
                        FROM fundamental_data
                    ) fd ON w.ticker = fd.ticker AND fd.rn = 1
                    WHERE w.active = 1
                    ORDER BY w.ticker
                """).fetchall()
                _cov_conn.close()

                _cov_c1, _cov_c2 = st.columns(2)
                for _ci, _fr in enumerate(_fund_rows):
                    _fticker, _fsector, _fperiod, _fpt, _fq, _ffy, _feps, _fnp = _fr
                    _col = _cov_c1 if _ci % 2 == 0 else _cov_c2
                    with _col:
                        if _fperiod:
                            _BADGE_MAP = {
                                "FY": "🟢 Annual",
                                "9M": "🟡 9M",
                                "H1": "🟡 H1",
                                "Q1": "🔴 Q1",
                            }
                            _fbadge   = _BADGE_MAP.get(_fpt, "⚪ Unknown")
                            _feps_str = f"{float(_feps):.4f}" if _feps else "—"
                            _ffy_disp = str(_ffy) if _ffy else "?"
                            st.caption(
                                f"✅ **{_fticker}** ({_fsector or '—'}) — "
                                f"{_fbadge} FY{_ffy_disp} | EPS: {_feps_str}"
                            )
                        else:
                            st.caption(
                                f"❌ **{_fticker}** ({_fsector or '—'}) — "
                                f"No fundamentals — fair value unavailable"
                            )
            except Exception as _cov_e:
                st.caption(f"Coverage unavailable: {_cov_e}")

        except Exception as _e:
            st.error(f"KPIs tab error: {_e}")

    # ─────────────────────────────────────────────────────────────────────────
    # SUBTAB 4 — Inflation
    # ─────────────────────────────────────────────────────────────────────────
    with _lt_sub_inf:
        try:
            from backend.data.lt_db import get_kpi_summary as _get_kpi_inf
            _kpi     = _get_kpi_inf()
            _cpi_now = _lt_latest_cpi()
            _inf_df  = _lt_all_inflation()

            # ── Inflation cards (2 rows × 3) ─────────────────────────────────
            _beating     = _kpi["beating_inflation"]
            _beat_color  = "#00A86B" if _beating else "#EF4444"
            _beat_border = "#00A86B" if _beating else "#EF4444"
            _beat_label  = "YES — Beating Inflation" if _beating else "NO — Losing to Inflation"
            _beat_icon   = "✓" if _beating else "✗"

            def _infl_card(label, value, color="#2C3E50", border="#5B8FA8"):
                return f"""<div style="background:#FFFFFF;border:1px solid #D3D3D3;
    border-top:3px solid {border};border-radius:6px;padding:14px 16px;
    height:90px;display:flex;flex-direction:column;justify-content:center;
    box-sizing:border-box;">
    <p style="font-size:10px;color:#6B7B8D;text-transform:uppercase;
    letter-spacing:0.05em;margin:0 0 6px 0;font-weight:600;">{label}</p>
    <p style="font-size:18px;font-weight:700;color:{color};margin:0;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{value}</p>
    </div>"""

            # ── Row 1 ──
            r1a, r1b, r1c = st.columns(3)
            r1a.markdown(_infl_card(
                "Latest CPI Index",
                f"{_kpi['cpi_index']:.6f}",
            ), unsafe_allow_html=True)
            r1b.markdown(_infl_card(
                "Cumulative Inflation",
                f"{_kpi['total_inflation_pct']:+.2f}%",
                color="#EF4444",
                border="#EF4444",
            ), unsafe_allow_html=True)
            r1c.markdown(_infl_card(
                "Real Invested Capital",
                f"EGP {_kpi['inflation_adjusted_capital']:,.2f}",
            ), unsafe_allow_html=True)

            st.markdown("<div style='margin:10px 0;'></div>", unsafe_allow_html=True)

            # ── Row 2 ──
            r2a, r2b, r2c = st.columns(3)
            r2a.markdown(_infl_card(
                "Lost to Inflation",
                f"EGP {_kpi['lost_to_inflation']:,.2f}",
                color="#EF4444",
                border="#EF4444",
            ), unsafe_allow_html=True)
            r2b.markdown(_infl_card(
                "Real Gain (Inflation-Adj.)",
                f"EGP {_kpi['real_gain']:,.2f}  ({_kpi['real_gain_pct']:+.2f}%)",
                color=_beat_color,
                border=_beat_border,
            ), unsafe_allow_html=True)
            r2c.markdown(_infl_card(
                "Beating Inflation?",
                f"{_beat_icon}  {_beat_label}",
                color=_beat_color,
                border=_beat_border,
            ), unsafe_allow_html=True)

            st.markdown("<div style='margin:16px 0;'></div>", unsafe_allow_html=True)

            # ── Inflation table ───────────────────────────────────────────────
            if not _inf_df.empty:
                st.dataframe(
                    _inf_df.rename(columns={
                        "month_year":       "Month / Year",
                        "headline_mom":     "Headline m/m",
                        "cumulative_index": "Cumulative Index",
                        "cumulative_pct":   "Cumulative %",
                    }).style.format({
                        "Headline m/m":     lambda x: f"{x * 100:.2f}%",
                        "Cumulative Index": "{:.6f}",
                        "Cumulative %":     lambda x: f"{x:.4f}%",
                    }),
                    use_container_width=True,
                )
            else:
                st.caption("No inflation data yet.")

            # ── Add / update row ──────────────────────────────────────────────
            with st.expander("➕ Add / Update Inflation Data"):
                _inf_month = st.text_input(
                    "Month Year (e.g. Apr 2026)", key="lt_inf_month"
                )
                _inf_mom_pct = st.number_input(
                    "Headline m/m % (e.g. 1.2 for 1.2%)",
                    min_value=-50.0, max_value=50.0,
                    step=0.01, format="%.4f", key="lt_inf_mom",
                )

                # Auto-calculate new index and cumulative pct
                _prev_idx = _cpi_now if _cpi_now else 1.0
                _new_idx  = _prev_idx * (1 + _inf_mom_pct / 100)
                _new_pct  = (_new_idx - 1) * 100

                _ai1, _ai2, _ai3 = st.columns(3)
                _ai1.metric("Input m/m %",           f"{_inf_mom_pct:.2f}%")
                _ai2.metric("New Cumulative Index",  f"{_new_idx:.6f}")
                _ai3.metric("New Cumulative %",      f"{_new_pct:.4f}%")

                if st.button("Save inflation row", type="primary", key="lt_inf_save"):
                    if not _inf_month.strip():
                        st.error("Month Year cannot be empty.")
                    else:
                        try:
                            _lt_insert_inflation(
                                _inf_month.strip(),
                                _inf_mom_pct / 100,
                                _new_idx,
                                _new_pct,
                            )
                            st.session_state._lt_success = (
                                f"Inflation data saved for {_inf_month.strip()}."
                            )
                            st.rerun()
                        except Exception as _exc:
                            st.error(f"Error: {_exc}")

        except Exception as _e:
            st.error(f"Inflation tab error: {_e}")

    # ─────────────────────────────────────────────────────────────────────────
    # SUBTAB 5 — Fundamentals (RAG PDF upload + KPI extraction)
    # ─────────────────────────────────────────────────────────────────────────
    with _lt_sub_fun:
        try:
            from backend.analysis.pdf_processor import process_pdf
            from backend.analysis.fundamental import extract_kpis_from_text
            from backend.data.fundamental_db import (
                get_all_fundamentals as _get_all_fund,
                get_report_list as _get_report_list,
                upsert_report_meta as _upsert_report_meta,
                save_chunks as _save_chunks,
                get_latest_fundamentals as _get_latest_fund,
                upsert_fundamentals as _upsert_fundamentals,
            )
            import tempfile, os as _os

            # Verify tables exist
            from backend.data.db import get_connection as _gc
            _conn = _gc()
            _tables = [r[0] for r in _conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            _conn.close()
            if "fundamental_data" not in _tables:
                st.error("❌ fundamental_data table does not exist — "
                         "restart the app to trigger init_fundamental_tables()")
                st.stop()

            def _fund_header(title):
                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:12px;margin:16px 0 10px 0;">
                    <span style="font-size:11px;font-weight:700;color:#5B8FA8;
                    text-transform:uppercase;letter-spacing:0.1em;white-space:nowrap;">{title}</span>
                    <div style="flex:1;height:1px;background:#D3D3D3;"></div>
                </div>
                """, unsafe_allow_html=True)

            _fund_tab1, _fund_tab2 = st.tabs(["📄 Upload PDF", "✏️ Manual Entry"])

            # ── PDF Upload tab ────────────────────────────────────────────────
            with _fund_tab1:
                _fund_header("Upload Financial Report")

                with st.form("fund_upload_form", clear_on_submit=True):
                    _fu_cols = st.columns([2, 2, 2, 3])
                    with _fu_cols[0]:
                        _fund_tickers = cfg["tickers"].get("long_term", []) + cfg["tickers"].get("both", [])
                        _fund_ticker = st.selectbox("Ticker", sorted(set(_fund_tickers)), key="fund_ticker")
                    with _fu_cols[1]:
                        _fund_period = st.text_input("Period (e.g. 2024-H1, 2023-A)", key="fund_period")
                    with _fu_cols[2]:
                        _fund_rtype = st.selectbox("Report type", ["annual", "semi-annual", "quarterly"], key="fund_rtype")
                    with _fu_cols[3]:
                        _fund_file = st.file_uploader("PDF file", type=["pdf"], key="fund_file")
                    _fund_submit = st.form_submit_button("📤 Process Report")

                _existing_fund = _get_latest_fund(_fund_ticker)
                if _existing_fund:
                    _col_warn, _col_clear = st.columns([3, 1])
                    with _col_warn:
                        st.warning(
                            f"⚠️ Existing data found for {_fund_ticker} "
                            f"({_existing_fund.get('period', '?')}). "
                            f"Uploading will overwrite it."
                        )
                    with _col_clear:
                        if st.button("🗑️ Clear Existing", key="clear_fund"):
                            _clr_conn = _gc()
                            _clr_conn.execute(
                                "DELETE FROM fundamental_data WHERE ticker=?",
                                (_fund_ticker,)
                            )
                            _clr_conn.commit()
                            _clr_conn.close()
                            st.success("Cleared. Now upload the PDF.")
                            st.rerun()

                if _fund_submit and _fund_file and _fund_ticker and _fund_period:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as _tmp:
                        _tmp.write(_fund_file.read())
                        _tmp_path = _tmp.name
                    try:
                        progress = st.progress(0, "Extracting text from PDF...")
                        result = process_pdf(_tmp_path)

                        ocr_count   = result.get('ocr_pages', 0)
                        table_count = result.get('table_pages', 0)
                        text_count  = result['total_pages'] - ocr_count - table_count
                        st.write(
                            f"DEBUG — Pages: {result['total_pages']} total | "
                            f"{text_count} text | {table_count} table-extracted | {ocr_count} OCR"
                        )
                        st.write(f"DEBUG — Chunks generated: {result['total_chunks']}")
                        if ocr_count > 0:
                            st.info(f"ℹ️ {ocr_count} image-based pages were processed via OCR. "
                                    f"Accuracy may vary — verify key figures after extraction.")

                        if result["total_chunks"] == 0:
                            st.error(
                                "❌ Could not extract text from PDF. "
                                "The PDF may be scanned/image-based. "
                                "Try a searchable PDF."
                            )
                            st.stop()

                        progress.progress(40, f"Extracted {result['total_pages']} pages...")

                        _save_chunks(
                            ticker=_fund_ticker,
                            period=_fund_period,
                            source_file=_fund_file.name,
                            chunks=result["chunks"]
                        )
                        st.write(f"DEBUG — Chunks saved to DB ✓")
                        progress.progress(60, "Chunks saved. Extracting KPIs with AI...")

                        full_text = "\n\n".join(p["text"] for p in result["pages"])
                        st.write(f"DEBUG — Full text length: {len(full_text)} chars")
                        st.write(f"DEBUG — Pages passed to LLM extractor: {len(result['pages'])}")
                        _table_pages = [p for p in result['pages'] if 'table' in p.get('method', '')]
                        if _table_pages:
                            st.write("DEBUG — Sample table page example:")
                            st.code(_table_pages[0]['text'][:800])

                        kpis = extract_kpis_from_text(
                            ticker=_fund_ticker,
                            full_text=full_text,
                            period=_fund_period,
                            source_file=_fund_file.name,
                            pages=result["pages"]
                        )
                        st.write("DEBUG — KPIs returned by LLM:")
                        st.json(kpis)

                        if not kpis or kpis == {"source_file": _fund_file.name,
                                                 "raw_summary": "KPI extraction failed — manual entry required."}:
                            st.error("❌ LLM failed to extract KPIs. Check Groq API key and logs.")
                        else:
                            _upsert_fundamentals(_fund_ticker, _fund_period, kpis)
                            st.write("DEBUG — upsert_fundamentals called ✓")
                            saved = _get_latest_fund(_fund_ticker)
                            st.write(f"DEBUG — Verified in DB: {bool(saved)}")
                            if saved:
                                st.success(f"✅ KPIs saved for {_fund_ticker} {_fund_period}")
                            else:
                                st.error("❌ upsert ran but DB still empty — check UNIQUE constraint")

                        progress.progress(90, "Saving report metadata...")
                        import datetime as _dt
                        _upsert_report_meta(
                            ticker=_fund_ticker,
                            period=_fund_period,
                            report_type=_fund_rtype,
                            source_file=_fund_file.name,
                            upload_date=str(_dt.date.today()),
                            pages=result["total_pages"],
                            chunks=result["total_chunks"]
                        )
                        progress.progress(100, "Done!")

                    except Exception as _fu_err:
                        st.error(f"❌ Pipeline error: {_fu_err}")
                        import traceback
                        st.code(traceback.format_exc())
                    finally:
                        _os.unlink(_tmp_path)

                _fund_header("Coverage Status")
                _all_fund_df = _get_all_fund()
                _lt_tickers = sorted(set(cfg["tickers"].get("long_term", []) + cfg["tickers"].get("both", [])))

                if not _all_fund_df.empty:
                    _cov_cols = st.columns(min(len(_lt_tickers), 6))
                    for _ci, _ctk in enumerate(_lt_tickers):
                        _tk_rows = _all_fund_df[_all_fund_df["ticker"] == _ctk]
                        if not _tk_rows.empty:
                            _latest_period = _tk_rows.sort_values("extracted_at", ascending=False).iloc[0]["period"]
                            _cov_cols[_ci % 6].markdown(
                                f"<div style='background:#00A86B15;border:1px solid #00A86B;"
                                f"border-radius:6px;padding:8px 10px;text-align:center;'>"
                                f"<b>{_ctk}</b><br><span style='font-size:10px;color:#6B7B8D;'>{_latest_period}</span></div>",
                                unsafe_allow_html=True
                            )
                        else:
                            _cov_cols[_ci % 6].markdown(
                                f"<div style='background:#EF444415;border:1px solid #EF4444;"
                                f"border-radius:6px;padding:8px 10px;text-align:center;'>"
                                f"<b>{_ctk}</b><br><span style='font-size:10px;color:#6B7B8D;'>No data</span></div>",
                                unsafe_allow_html=True
                            )
                else:
                    st.info("No fundamental reports uploaded yet. Use the form above to add your first report.")

                if not _all_fund_df.empty:
                    _fund_header("All Fundamental Data")
                    _disp_fund_cols = ["ticker", "period", "report_type", "report_date",
                                       "revenue", "net_profit", "net_margin", "eps",
                                       "roe", "roa", "debt_to_equity", "current_ratio",
                                       "pe_ratio", "currency"]
                    _avail_fund = [c for c in _disp_fund_cols if c in _all_fund_df.columns]
                    st.dataframe(_all_fund_df[_avail_fund], use_container_width=True, hide_index=True)

                _rpt_list = _get_report_list()
                if not _rpt_list.empty:
                    with st.expander("Report Upload History", expanded=False):
                        st.dataframe(_rpt_list, use_container_width=True, hide_index=True)

            # ── Manual Entry tab ─────────────────────────────────────────────
            with _fund_tab2:
                st.markdown("#### ✏️ Manual Fundamentals Entry")
                st.caption("Use this when PDF extraction fails or for quick updates.")

                _man_conn = _gc()
                _wl_tickers_df = pd.read_sql(
                    "SELECT ticker, name FROM watchlist WHERE active=1 ORDER BY ticker",
                    _man_conn
                )
                _ticker_options = [
                    f"{r['ticker']} — {r['name']}" for _, r in _wl_tickers_df.iterrows()
                ] if not _wl_tickers_df.empty else sorted(set(
                    cfg["tickers"].get("long_term", []) + cfg["tickers"].get("both", [])
                ))

                _man_selected = st.selectbox("Select Ticker", _ticker_options, key="manual_ticker_sel")
                _man_ticker = _man_selected.split(" — ")[0] if _man_selected else None

                if _man_ticker:
                    _man_existing = _man_conn.execute(
                        "SELECT * FROM fundamental_data WHERE ticker=? ORDER BY period DESC LIMIT 1",
                        (_man_ticker,)
                    ).fetchone()
                    _man_cols = [d[0] for d in _man_conn.execute(
                        "PRAGMA table_info(fundamental_data)"
                    ).fetchall()]
                    _man_dict = dict(zip(_man_cols, _man_existing)) if _man_existing else {}

                    # ── Existing periods for this ticker ──────────────────
                    _existing_periods_df = pd.read_sql(
                        "SELECT period, period_type, fiscal_year, report_date "
                        "FROM fundamental_data WHERE ticker=? ORDER BY period DESC",
                        _man_conn, params=(_man_ticker,)
                    )
                    if not _existing_periods_df.empty:
                        st.caption(
                            "**Saved periods:** " +
                            " · ".join(_existing_periods_df["period"].tolist())
                        )

                    with st.form("manual_fund_form"):
                        st.markdown("**📅 Report Info**")
                        _pc1, _pc2, _pc3, _pc4 = st.columns([1, 1.5, 1.5, 1])
                        with _pc1:
                            _existing_fy = _man_dict.get("fiscal_year") or (
                                int(_man_dict["period"][:4])
                                if _man_dict.get("period") and len(_man_dict["period"]) >= 4 else 2025
                            )
                            _man_fy = st.number_input("Fiscal Year",
                                value=int(_existing_fy), min_value=2000, max_value=2100, step=1)
                        with _pc2:
                            _pt_opts = [
                                "FY — Full Year",
                                "Q1 — 3 Months",
                                "H1 — 6 Months",
                                "9M — 9 Months",
                            ]
                            _existing_pt = _man_dict.get("period_type", "FY") or "FY"
                            _pt_opt_map  = {"FY": "FY — Full Year", "A": "FY — Full Year",
                                            "Q1": "Q1 — 3 Months",
                                            "H1": "H1 — 6 Months",
                                            "9M": "9M — 9 Months"}
                            _pt_default  = _pt_opt_map.get(_existing_pt, "FY — Full Year")
                            _man_pt_label = st.selectbox("Period Type", _pt_opts,
                                index=_pt_opts.index(_pt_default) if _pt_default in _pt_opts else 0,
                                help="FY=annual report | Q1=3m | H1=6m cumulative | 9M=9m cumulative")
                        with _pc3:
                            _man_rdate = st.text_input("Report Date",
                                value=_man_dict.get("report_date", "") or "",
                                placeholder="YYYY-MM-DD")
                        with _pc4:
                            _cur_opts = ["EGP", "USD", "EUR"]
                            _cur_val  = _man_dict.get("currency", "EGP") or "EGP"
                            _man_currency = st.selectbox("Currency", _cur_opts,
                                index=_cur_opts.index(_cur_val) if _cur_val in _cur_opts else 0)

                        # Derive period / period_type / quarter
                        _PERIOD_MAP = {
                            "FY — Full Year": ("FY", None, "FY"),
                            "Q1 — 3 Months":  ("Q1", 1,    "Q1"),
                            "H1 — 6 Months":  ("H1", 2,    "H1"),
                            "9M — 9 Months":  ("9M", 3,    "9M"),
                        }
                        _man_period_code, _man_quarter, _man_period_suffix = \
                            _PERIOD_MAP.get(_man_pt_label, ("FY", None, "FY"))
                        _man_period_type = _man_period_code
                        _man_period      = f"{_man_fy}-{_man_period_suffix}"
                        st.caption(f"Period string: **{_man_period}**")

                        _units_label = st.selectbox(
                            "Statement Units",
                            options=[
                                "Millions (EGP M)      — enter as shown",
                                "Thousands (EGP 000s)  — will divide by 1,000",
                                "Billions (EGP B)      — will multiply by 1,000",
                            ],
                            index=0,
                            help="Check the top of your financial statement for the unit"
                        )
                        if "Thousands" in _units_label:
                            _units_divisor = 1_000
                        elif "Billions" in _units_label:
                            _units_divisor = 0.001
                        else:
                            _units_divisor = 1

                        st.markdown("**📊 Income Statement** (in currency millions)")
                        _is1, _is2, _is3, _is4 = st.columns(4)
                        with _is1:
                            _man_revenue    = st.number_input("Revenue", value=float(_man_dict.get("revenue") or 0), format="%.2f")
                            _man_rev_growth = st.number_input("Revenue Growth %", value=float(_man_dict.get("revenue_growth") or 0), format="%.2f")
                        with _is2:
                            _man_net_profit = st.number_input("Net Profit (attrib. to shareholders)", value=float(_man_dict.get("net_profit") or 0), format="%.2f")
                            _man_net_margin = st.number_input("Net Margin %", value=float(_man_dict.get("net_margin") or 0), format="%.2f")
                        with _is3:
                            _man_ebitda = st.number_input("EBITDA", value=float(_man_dict.get("ebitda") or 0), format="%.2f")
                            _man_eps    = st.number_input("EPS (per share)", value=float(_man_dict.get("eps") or 0), format="%.4f")
                        with _is4:
                            _man_int_inc   = st.number_input("Interest Income", value=float(_man_dict.get("interest_income") or 0), format="%.2f")
                            _man_int2rev   = st.number_input("Interest/Revenue % (Shariah)", value=float(_man_dict.get("interest_to_rev") or 0), format="%.4f",
                                help="Key Shariah metric — must be < 5% to pass screening")

                        st.markdown("**🏦 Balance Sheet**")
                        _bs1, _bs2, _bs3, _bs4 = st.columns(4)
                        with _bs1:
                            _man_assets = st.number_input("Total Assets", value=float(_man_dict.get("total_assets") or 0), format="%.2f")
                            _man_debt   = st.number_input("Total Debt", value=float(_man_dict.get("total_debt") or 0), format="%.2f")
                        with _bs2:
                            _man_equity = st.number_input("Equity (attrib. to owners)", value=float(_man_dict.get("equity") or 0), format="%.2f")
                            _man_de     = st.number_input("Debt/Equity Ratio", value=float(_man_dict.get("debt_to_equity") or 0), format="%.3f")
                        with _bs3:
                            _man_curr_ratio = st.number_input("Current Ratio", value=float(_man_dict.get("current_ratio") or 0), format="%.3f")
                            _man_roe        = st.number_input("ROE %", value=float(_man_dict.get("roe") or 0), format="%.2f")
                        with _bs4:
                            _man_roa = st.number_input("ROA %", value=float(_man_dict.get("roa") or 0), format="%.2f")
                            _man_div = st.number_input("Dividend Per Share", value=float(_man_dict.get("dividend_per_share") or 0), format="%.4f")

                        st.markdown("**📝 Summary**")
                        _man_summary = st.text_area("Raw Summary (for AI context)",
                            value=_man_dict.get("raw_summary", "") or "",
                            height=100,
                            placeholder="Brief summary of financial performance, key highlights, outlook...")

                        _man_save = st.form_submit_button("💾 Save Fundamentals", use_container_width=True, type="primary")

                        if _man_save:
                            # Apply units conversion to monetary fields only
                            _d = _units_divisor
                            _sv_revenue    = round(_man_revenue    / _d, 2) if _man_revenue    else None
                            _sv_net_profit = round(_man_net_profit / _d, 2) if _man_net_profit else None
                            _sv_ebitda     = round(_man_ebitda     / _d, 2) if _man_ebitda     else None
                            _sv_assets     = round(_man_assets     / _d, 2) if _man_assets     else None
                            _sv_debt       = round(_man_debt       / _d, 2) if _man_debt       else None
                            _sv_equity     = round(_man_equity     / _d, 2) if _man_equity     else None
                            _sv_int_inc    = round(_man_int_inc    / _d, 2) if _man_int_inc    else None

                            # Ratios and percentages use converted figures but are never divided themselves
                            _net_margin = _man_net_margin or (round(_sv_net_profit / _sv_revenue * 100, 2) if _sv_revenue and _sv_net_profit else 0.0)
                            _roe        = _man_roe or (round(_sv_net_profit / _sv_equity * 100, 2) if _sv_equity and _sv_net_profit else 0.0)
                            _roa        = _man_roa or (round(_sv_net_profit / _sv_assets * 100, 2) if _sv_assets and _sv_net_profit else 0.0)
                            _de         = _man_de or (round(_sv_debt / _sv_equity, 3) if _sv_equity and _sv_debt else 0.0)
                            _int2rev    = _man_int2rev or (round(_sv_int_inc / _sv_revenue * 100, 4) if _sv_revenue and _sv_int_inc else 0.0)
                            _shariah_pass = (_int2rev < 5.0) if _int2rev else None

                            from backend.data.fundamental_db import manual_upsert_fundamentals as _muf
                            _ok = _muf(
                                ticker=_man_ticker,
                                period=_man_period,
                                period_type=_man_period_type,
                                quarter=_man_quarter,
                                fiscal_year=int(_man_fy),
                                currency=_man_currency,
                                report_type="annual" if _man_period_type == "FY" else "quarterly",
                                report_date=_man_rdate or None,
                                revenue=_sv_revenue,
                                revenue_growth=_man_rev_growth or None,
                                net_profit=_sv_net_profit,
                                net_margin=_net_margin or None,
                                eps=_man_eps or None,
                                ebitda=_sv_ebitda,
                                total_assets=_sv_assets,
                                total_debt=_sv_debt,
                                equity=_sv_equity,
                                debt_to_equity=_de or None,
                                current_ratio=_man_curr_ratio or None,
                                roe=_roe or None,
                                roa=_roa or None,
                                interest_income=_sv_int_inc,
                                interest_to_rev=_int2rev or None,
                                dividend_per_share=_man_div or None,
                                raw_summary=_man_summary or None,
                                source_file="manual_entry",
                            )
                            if _ok:
                                if _shariah_pass is True:
                                    st.success(f"✅ {_man_ticker} {_man_period} saved! Shariah: PASS (interest/rev = {_int2rev:.4f}% < 5%)")
                                elif _shariah_pass is False:
                                    st.warning(f"⚠️ {_man_ticker} {_man_period} saved but Shariah: FAIL (interest/rev = {_int2rev:.4f}% ≥ 5%)")
                                else:
                                    st.success(f"✅ {_man_ticker} {_man_period} fundamentals saved!")
                            else:
                                st.error(f"❌ Save failed — check logs")

                _man_conn.close()

        except Exception as _e:
            st.error(f"Fundamentals tab error: {_e}")

    # ─────────────────────────────────────────────────────────────────────────
    # SUBTAB 6 — Recommendations
    # ─────────────────────────────────────────────────────────────────────────
    with _lt_sub_rec:
        try:
            from backend.agents.longterm_agent import run_signals as _run_signals

            def _section_header(title):
                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:12px;margin:16px 0 10px 0;">
                    <span style="font-size:11px;font-weight:700;color:#5B8FA8;
                    text-transform:uppercase;letter-spacing:0.1em;white-space:nowrap;">{title}</span>
                    <div style="flex:1;height:1px;background:#D3D3D3;"></div>
                </div>
                """, unsafe_allow_html=True)

            # ── Run Signal Analysis button (2-pass: technical then LLM) ─────
            from backend.agents.longterm_agent import run_llm_descriptions as _run_llm_desc
            from backend.data.lt_db import get_positions as _get_pos

            if st.button("🔄 Run Signal Analysis", key="btn_run_signals",
                         type="primary", use_container_width=False):

                _open_pos = _get_pos()
                _open_pos = (_open_pos[_open_pos["status"] == "Open"]
                             if not _open_pos.empty else _open_pos)
                _active_tickers = _open_pos["ticker"].tolist() if not _open_pos.empty else []

                if not _active_tickers:
                    st.warning("No open positions found.")
                else:
                    # ── Pass 1: Technical calculations (fast, no LLM) ─────────
                    _step1 = st.status(
                        f"⚙️ Step 1/2 — Technical calculations "
                        f"for {len(_active_tickers)} tickers...",
                        expanded=True,
                    )
                    with _step1:
                        try:
                            _pass1_results = _run_signals(
                                tickers=_active_tickers, skip_llm=True
                            )
                            _p1_ok  = [t for t, v in _pass1_results.items() if "error" not in v]
                            _p1_err = [t for t, v in _pass1_results.items() if "error" in v]
                            _step1.update(
                                label=(
                                    f"✅ Step 1/2 — Technical done "
                                    f"({len(_p1_ok)} signals"
                                    + (f", {len(_p1_err)} failed" if _p1_err else "")
                                    + ")"
                                ),
                                state="complete",
                                expanded=False,
                            )
                        except Exception as _p1_err_ex:
                            _step1.update(
                                label=f"❌ Step 1/2 — Error: {_p1_err_ex}",
                                state="error",
                            )
                            st.stop()

                    # ── Pass 2: LLM descriptions ───────────────────────────────
                    _step2 = st.status(
                        f"🤖 Step 2/2 — Generating AI descriptions "
                        f"(~{len(_active_tickers) * 8}s)...",
                        expanded=True,
                    )
                    with _step2:
                        try:
                            _llm_results = _run_llm_desc(tickers=_active_tickers)
                            _l_ok   = sum(1 for v in _llm_results.values() if v == "ok")
                            _l_skip = sum(1 for v in _llm_results.values() if "skipped" in v)
                            _l_err  = sum(1 for v in _llm_results.values() if "error" in v)
                            _step2.update(
                                label=(
                                    f"✅ Step 2/2 — AI descriptions complete "
                                    f"({_l_ok} generated, {_l_skip} skipped"
                                    + (f", {_l_err} errors" if _l_err else "")
                                    + ")"
                                ),
                                state="complete" if _l_err == 0 else "error",
                                expanded=False,
                            )
                        except Exception as _p2_err_ex:
                            _step2.update(
                                label=f"⚠️ Step 2/2 — AI failed: {_p2_err_ex}",
                                state="error",
                            )

                    st.success("🎯 Analysis complete! Signals updated below.")
                    st.rerun()

            # ── Load latest signals (hidden while running) ────────────────────
            _sig_df = pd.DataFrame()
            if not st.session_state.get("lt_signals_running", False):
                _sig_df = _lt_signals()

            if _sig_df.empty:
                if not st.session_state.get("lt_signals_running", False):
                    st.info("No signals yet — click 'Run Signal Analysis' to generate.")
            else:
                st.markdown(
                    f"<p style='font-size:11px;color:#6B7B8D;margin-bottom:12px;'>"
                    f"Last run: {_sig_df['run_date'].max()} · "
                    f"{len(_sig_df)} tickers analysed</p>",
                    unsafe_allow_html=True,
                )

                # ── Capital Allocator ─────────────────────────────────────────
                st.markdown("<div style='margin:12px 0 4px 0;'></div>", unsafe_allow_html=True)
                _section_header("Capital Allocator")

                alloc_col1, alloc_col2 = st.columns([1, 3])
                with alloc_col1:
                    capital_input = st.number_input(
                        "Available capital to deploy (EGP)",
                        min_value=0.0,
                        value=0.0,
                        step=1000.0,
                        format="%.2f",
                        key="capital_allocator_input",
                        help="Enter the amount you want to invest. The engine allocates it across BUY/ACCUMULATE signals weighted by score.",
                    )

                MIN_TICKET = 2000.0
                TIER_CAPS  = {"full": 1.00, "half": 0.60, "starter": 0.30}

                if capital_input > 0:
                    def _tier(df, tier):
                        return df[df["deploy_tier"].fillna("wait") == tier].copy() if "deploy_tier" in df.columns else pd.DataFrame()

                    full_df    = _tier(_sig_df, "full")
                    half_df    = _tier(_sig_df, "half")
                    starter_df = _tier(_sig_df, "starter")

                    if not full_df.empty:
                        active_df   = full_df
                        active_tier = "full"
                        tier_label  = "Full Deploy"
                        tier_color  = "#00A86B"
                    elif not half_df.empty:
                        active_df   = half_df
                        active_tier = "half"
                        tier_label  = "Half Now (60% cap)"
                        tier_color  = "#5B8FA8"
                    elif not starter_df.empty:
                        active_df   = starter_df
                        active_tier = "starter"
                        tier_label  = "Starter Only (30% cap)"
                        tier_color  = "#F59E0B"
                    else:
                        active_df   = pd.DataFrame()
                        active_tier = "wait"
                        tier_label  = "Wait"
                        tier_color  = "#B8B8B6"

                    if active_df.empty:
                        st.markdown(
                            f"<div style='background:#F8FAFC;border:1px solid #D3D3D3;"
                            f"border-left:3px solid #B8B8B6;border-radius:6px;"
                            f"padding:10px 14px;'>"
                            f"<p style='margin:0;font-size:13px;color:#6B7B8D;'>"
                            f"No actionable signals — all tickers on <b>Wait</b>. "
                            f"Preserve EGP {capital_input:,.2f} as dry powder.</p></div>",
                            unsafe_allow_html=True)
                    else:
                        cap_pct        = TIER_CAPS.get(active_tier, 0)
                        capped_capital = round(capital_input * cap_pct, 2)
                        dry_powder     = round(capital_input - capped_capital, 2)

                        active_df = active_df.copy()
                        active_df["score"]     = active_df["score"].fillna(0).astype(float)
                        active_df["price_val"] = active_df["price"].fillna(0).astype(float)
                        active_df = active_df.sort_values(
                            ["score", "current_allocation_pct"],
                            ascending=[False, True]
                        ).reset_index(drop=True)

                        # ── Minimum ticket enforcement ──
                        deployed_df = active_df.copy()
                        skipped     = []

                        while True:
                            n = len(deployed_df)
                            if n == 0:
                                break
                            total_score = deployed_df["score"].sum()
                            if total_score == 0:
                                break
                            deployed_df["weight"]        = deployed_df["score"] / total_score
                            deployed_df["allocated_egp"] = (deployed_df["weight"] * capped_capital).round(2)
                            below_min = deployed_df[deployed_df["allocated_egp"] < MIN_TICKET]
                            if below_min.empty:
                                break
                            drop_idx = below_min.index[-1]
                            skipped.append(deployed_df.loc[drop_idx, "ticker"])
                            deployed_df = deployed_df.drop(drop_idx).reset_index(drop=True)

                        if not deployed_df.empty:
                            deployed_df["shares_to_buy"] = (
                                deployed_df["allocated_egp"] /
                                deployed_df["price_val"].replace(0, float("nan"))
                            ).fillna(0).apply(lambda x: int(x))
                            deployed_df["actual_cost"] = (
                                deployed_df["shares_to_buy"] * deployed_df["price_val"]
                            ).round(2)
                            total_deployed = deployed_df["actual_cost"].sum()
                            remaining_cash = round(capital_input - total_deployed, 2)
                        else:
                            total_deployed = 0.0
                            remaining_cash = capital_input

                        # ── Tier banner ──
                        st.markdown(
                            f"<div style='background:#F8FAFC;border-left:3px solid {tier_color};"
                            f"border-radius:4px;padding:6px 12px;margin-bottom:10px;'>"
                            f"<span style='font-size:11px;font-weight:700;color:{tier_color};"
                            f"text-transform:uppercase;letter-spacing:0.05em;'>{tier_label}</span>"
                            f"<span style='font-size:11px;color:#6B7B8D;margin-left:10px;'>"
                            f"Max deployable: EGP {capped_capital:,.2f} ({cap_pct*100:.0f}% of input) "
                            f"— EGP {dry_powder:,.2f} reserved as dry powder</span></div>",
                            unsafe_allow_html=True)

                        sm1, sm2 = st.columns(2)
                        sm1.metric("Total Deployed", f"EGP {total_deployed:,.2f}")
                        sm2.metric("Remaining Cash", f"EGP {remaining_cash:,.2f}")

                        st.markdown("<div style='margin:8px 0;'></div>", unsafe_allow_html=True)

                        if not deployed_df.empty:
                            for _, arow in deployed_df.iterrows():
                                _dl = str(arow.get("deploy_label") or "")
                                _dc = {"Full deploy": "#00A86B", "Half now": "#5B8FA8", "Starter only": "#F59E0B"}.get(_dl, "#6B7B8D")

                                ac1, ac2, ac3, ac4 = st.columns([1, 1.5, 1.5, 1.5])
                                ac1.markdown(
                                    f"<p style='font-size:14px;font-weight:700;color:#2C3E50;margin:4px 0;'>"
                                    f"{arow['ticker']}</p>",
                                    unsafe_allow_html=True)
                                ac2.markdown(
                                    f"<span style='background:{_dc}22;color:{_dc};"
                                    f"border:1px solid {_dc}55;padding:2px 8px;"
                                    f"border-radius:3px;font-size:10px;font-weight:700;'>{_dl}</span>",
                                    unsafe_allow_html=True)
                                ac3.markdown(
                                    f"<p style='font-size:10px;color:#6B7B8D;margin:0;'>SHARES</p>"
                                    f"<p style='font-size:14px;font-weight:700;color:#5B8FA8;margin:0;'>"
                                    f"{int(arow['shares_to_buy'])} shares</p>",
                                    unsafe_allow_html=True)
                                ac4.markdown(
                                    f"<p style='font-size:10px;color:#6B7B8D;margin:0;'>COST</p>"
                                    f"<p style='font-size:13px;font-weight:600;color:#00A86B;margin:0;'>"
                                    f"EGP {arow['actual_cost']:,.2f}</p>",
                                    unsafe_allow_html=True)
                                st.markdown(
                                    "<hr style='border:none;border-top:1px solid #F0F0F0;margin:2px 0;'/>",
                                    unsafe_allow_html=True)

                        if skipped:
                            st.markdown(
                                f"<p style='font-size:11px;color:#EF4444;margin:6px 0 2px 0;'>"
                                f"⚠ Skipped (below EGP {MIN_TICKET:,.0f} minimum): "
                                f"{', '.join(skipped)}</p>",
                                unsafe_allow_html=True)

                        st.markdown(
                            f"<p style='font-size:11px;color:#6B7B8D;margin:6px 0 0 0;'>"
                            f"Min EGP {MIN_TICKET:,.0f}/ticker enforced. Shares rounded down — never overspend. "
                            f"Always verify price on Thndr before placing orders.</p>",
                            unsafe_allow_html=True)

                st.markdown("<div style='margin:12px 0;'></div>", unsafe_allow_html=True)
                _section_header("Signals")

                # ── Signal cards: one per ticker ──────────────────────────────
                def _cell(col, label, value, color="#2C3E50", size="13px"):
                    col.markdown(
                        f"<p style='font-size:10px;color:#6B7B8D;margin:0 0 1px 0;"
                        f"text-transform:uppercase;letter-spacing:0.04em;'>{label}</p>"
                        f"<p style='font-size:{size};font-weight:600;color:{color};"
                        f"margin:0;'>{value}</p>",
                        unsafe_allow_html=True
                    )

                for _, row in _sig_df.iterrows():
                    sig   = str(row.get("signal", "HOLD"))
                    score = int(row.get("score", 0))

                    if sig in ("BUY", "ACCUMULATE"):
                        sig_color = "#00A86B"; sig_border = "#00A86B"; sig_bg = "#00A86B15"
                    elif sig in ("TRIM_PEAK", "SELL", "REDUCE"):
                        sig_color = "#EF4444"; sig_border = "#EF4444"; sig_bg = "#EF444415"
                    else:
                        sig_color = "#F59E0B"; sig_border = "#F59E0B"; sig_bg = "#F59E0B15"

                    profit_pct   = float(row.get("profit_pct") or 0)
                    profit_color = "#00A86B" if profit_pct >= 0 else "#EF4444"
                    conf         = str(row.get("forecast_confidence") or "LOW")
                    conf_color   = {"HIGH": "#00A86B", "MEDIUM": "#F59E0B", "LOW": "#EF4444"}.get(conf, "#6B7B8D")
                    action       = str(row.get("action") or "—")
                    ticker_sym   = str(row.get("ticker", ""))

                    # ── Parse enhanced analysis ──
                    _enh = {}
                    try:
                        _ej = row.get("enhanced_json")
                        if _ej and str(_ej) not in ("None", "nan", ""):
                            _enh = json.loads(_ej)
                    except Exception:
                        pass

                    _fv  = _enh.get("fair_value", {}) or {}
                    _div = _enh.get("divergence",  {}) or {}
                    _vol = _enh.get("volume",       {}) or {}
                    _mtf = _enh.get("mtf",          {}) or {}

                    fv_mid        = _fv.get("fv_mid")
                    fv_upside     = _fv.get("upside_pct")
                    val_status    = _fv.get("valuation_status", "NO_DATA")
                    val_color     = {
                        "UNDERVALUED":    "#00A86B",
                        "FAIR_VALUE":     "#5B8FA8",
                        "SLIGHTLY_RICH":  "#F59E0B",
                        "OVERVALUED":     "#EF4444",
                        "NO_DATA":        "#B8B8B6",
                    }.get(val_status, "#B8B8B6")

                    daily_rsi  = _mtf.get("rsi_daily")
                    weekly_rsi = _mtf.get("rsi_weekly")
                    mtf_agree  = _mtf.get("mtf_agreement", False)
                    conf_boost = int(_mtf.get("confidence_boost", 0))
                    vol_signal = _vol.get("signal", "NEUTRAL")
                    vol_color  = {
                        "CONFIRMED_UP":   "#00A86B",
                        "WEAK_UP":        "#A3C4A8",
                        "CONFIRMED_DOWN": "#EF4444",
                        "WEAK_DOWN":      "#F59E0B",
                        "NEUTRAL":        "#B8B8B6",
                    }.get(vol_signal, "#B8B8B6")
                    div_signal = _div.get("divergence", "NONE")
                    div_color  = {
                        "BULLISH": "#00A86B",
                        "BEARISH": "#EF4444",
                        "NONE":    "#B8B8B6",
                    }.get(div_signal, "#B8B8B6")

                    score_bar = (
                        f"<div style='background:#E2E8F0;border-radius:4px;height:5px;"
                        f"width:60px;display:inline-block;vertical-align:middle;'>"
                        f"<div style='background:{sig_color};height:5px;border-radius:4px;"
                        f"width:{score}%;'></div></div>"
                    )

                    st.markdown(
                        f"<div style='border-left:3px solid {sig_border};"
                        f"padding-left:10px;margin:4px 0 2px 0;'>",
                        unsafe_allow_html=True
                    )

                    # ── Header row ──
                    h1, h2, h3, h4, h5 = st.columns([1.2, 1.4, 2, 1.5, 3])
                    h1.markdown(
                        f"<p style='font-size:15px;font-weight:700;color:#2C3E50;"
                        f"margin:2px 0;'>{ticker_sym}</p>",
                        unsafe_allow_html=True)
                    h2.markdown(
                        f"<span style='background:{sig_bg};color:{sig_color};"
                        f"border:1px solid {sig_border};padding:2px 8px;"
                        f"border-radius:4px;font-size:11px;font-weight:700;'>{sig}</span>",
                        unsafe_allow_html=True)
                    h3.markdown(
                        f"<div style='margin:4px 0;'>{score_bar}"
                        f"<span style='font-size:11px;color:#6B7B8D;margin-left:6px;'>"
                        f"{score}/100"
                        + (f" <span style='color:#00A86B;font-size:10px;'>+{conf_boost} MTF</span>" if conf_boost > 0 else "")
                        + f"</span></div>",
                        unsafe_allow_html=True)
                    h4.markdown(
                        f"<span style='font-size:11px;color:#6B7B8D;'>"
                        f"Action: <b style='color:#2C3E50;'>{action}</b></span>",
                        unsafe_allow_html=True)
                    h5.markdown(
                        f"<p style='font-size:11px;color:#2C3E50;margin:2px 0;"
                        f"line-height:1.5;border-left:2px solid {sig_border};"
                        f"padding-left:8px;'>"
                        f"{str(row.get('description') or '—')}</p>",
                        unsafe_allow_html=True)

                    # ── Metric grid: Cost | Price | P/L | Fair Value | Upside | Valuation ──
                    st.markdown("<div style='margin:4px 0;'></div>", unsafe_allow_html=True)
                    d1, d2, d3, d4, d5, d6 = st.columns(6)
                    _cell(d1, "Avg Cost",       f"EGP {float(row.get('avg_cost') or 0):.2f}")
                    _cell(d2, "Current Price",  f"EGP {float(row.get('price') or 0):.2f}")
                    _cell(d3, "P/L on Cost ①",  f"{profit_pct:+.2f}%", profit_color)
                    _cell(d4, "Fair Value",
                          f"EGP {fv_mid:.2f}" if fv_mid else "—",
                          val_color)
                    _cell(d5, "Upside to FV",
                          f"{fv_upside:+.1f}%" if fv_upside is not None else "—",
                          "#00A86B" if (fv_upside or 0) >= 0 else "#EF4444")
                    _cell(d6, "Valuation",
                          val_status.replace("_", " "), val_color, "11px")

                    # ── Technical snapshot row ──
                    st.markdown("<div style='margin:6px 0 2px 0;'></div>", unsafe_allow_html=True)
                    t1, t2, t3, t4, t5, t6 = st.columns(6)
                    _cell(t1, "RSI (Daily)",
                          f"{daily_rsi:.1f}" if daily_rsi else "—",
                          "#00A86B" if daily_rsi and 40 <= daily_rsi <= 70
                          else ("#EF4444" if daily_rsi and daily_rsi > 70 else "#F59E0B"))
                    _cell(t2, "RSI (Weekly)",
                          f"{weekly_rsi:.1f}" if weekly_rsi else "—",
                          "#00A86B" if weekly_rsi and 40 <= weekly_rsi <= 70
                          else ("#EF4444" if weekly_rsi and weekly_rsi > 70 else "#F59E0B"))
                    t3.markdown(
                        f"<p style='font-size:10px;color:#6B7B8D;margin:0 0 1px 0;"
                        f"text-transform:uppercase;letter-spacing:0.04em;'>MTF Agree</p>"
                        f"<span style='background:{'#00A86B' if mtf_agree else '#E2E8F0'}22;"
                        f"color:{'#00A86B' if mtf_agree else '#B8B8B6'};"
                        f"border:1px solid {'#00A86B' if mtf_agree else '#E2E8F0'}55;"
                        f"padding:1px 6px;border-radius:3px;font-size:10px;font-weight:700;'>"
                        f"{'YES' if mtf_agree else 'NO'}</span>",
                        unsafe_allow_html=True)
                    t4.markdown(
                        f"<p style='font-size:10px;color:#6B7B8D;margin:0 0 1px 0;"
                        f"text-transform:uppercase;letter-spacing:0.04em;'>Volume</p>"
                        f"<span style='color:{vol_color};font-size:10px;font-weight:700;'>"
                        f"{vol_signal.replace('_', ' ')}</span>",
                        unsafe_allow_html=True)
                    t5.markdown(
                        f"<p style='font-size:10px;color:#6B7B8D;margin:0 0 1px 0;"
                        f"text-transform:uppercase;letter-spacing:0.04em;'>RSI Div</p>"
                        f"<span style='color:{div_color};font-size:10px;font-weight:700;'>"
                        f"{div_signal}</span>",
                        unsafe_allow_html=True)
                    def _safe_float(v):
                        try:
                            f = float(v)
                            return f if f and not (f != f) else None
                        except (TypeError, ValueError):
                            return None

                    _ez_low  = _safe_float(row.get("entry_zone_low")  or (_enh.get("entry_zone_low")))
                    _ez_high = _safe_float(row.get("entry_zone_high") or (_enh.get("entry_zone_high")))
                    _ez_mid  = _safe_float(row.get("entry_zone_mid")  or (_enh.get("entry_zone_mid")))

                    if _ez_low and _ez_high and _ez_low != _ez_high:
                        _entry_display = f"EGP {_ez_low:.2f} – {_ez_high:.2f}"
                        _entry_sub     = f"Mid: EGP {_ez_mid:.2f}" if _ez_mid else ""
                    elif _ez_mid:
                        _entry_display = f"EGP {_ez_mid:.2f}"
                        _entry_sub     = "Entry trigger"
                    else:
                        _entry_display = "—"
                        _entry_sub     = ""

                    _ez_color = "#00A86B" if _ez_mid else "#6B7B8D"
                    t6.markdown(
                        f"<p style='font-size:10px;color:#6B7B8D;margin:0 0 1px 0;"
                        f"text-transform:uppercase;letter-spacing:0.04em;'>Entry Zone</p>"
                        f"<p style='font-size:11px;font-weight:600;color:{_ez_color};"
                        f"margin:0;'>{_entry_display}</p>"
                        + (f"<p style='font-size:10px;color:#6B7B8D;margin:0;'>{_entry_sub}</p>"
                           if _entry_sub else ""),
                        unsafe_allow_html=True
                    )

                    st.markdown("</div>", unsafe_allow_html=True)

                    # ── Deploy pill ──
                    _deploy_label = str(row.get("deploy_label") or "")
                    _deploy_note  = str(row.get("deploy_note") or "")
                    _deploy_color = {
                        "Full deploy":  "#00A86B",
                        "Half now":     "#5B8FA8",
                        "Starter only": "#F59E0B",
                        "Reduce":       "#EF4444",
                        "Wait":         "#B8B8B6",
                    }.get(_deploy_label, "#B8B8B6")

                    if _deploy_label:
                        st.markdown(
                            f"<div style='background:#F8FAFC;border-radius:4px;padding:6px 10px;"
                            f"margin:4px 0 0 0;border-left:3px solid {_deploy_color};'>"
                            f"<span style='font-size:10px;font-weight:700;color:{_deploy_color};"
                            f"text-transform:uppercase;letter-spacing:0.05em;'>{_deploy_label}</span>"
                            f"<span style='font-size:11px;color:#6B7B8D;margin-left:8px;'>{_deploy_note}</span>"
                            f"</div>",
                            unsafe_allow_html=True)

                    # ── Deep-dive expander ──
                    with st.expander(f"🔍 {ticker_sym} — Deep Dive"):
                        dd1, dd2, dd3 = st.columns(3)

                        with dd1:
                            st.markdown("**Fundamentals**")
                            try:
                                from backend.data.fundamental_db import get_best_fundamentals as _gbf
                                _kpis = _gbf(ticker_sym) or {}
                            except Exception:
                                _kpis = {}
                            if _kpis:
                                _cur         = _kpis.get("currency", "EGP")
                                _fmt         = lambda v, d=2: f"{float(v):.{d}f}" if v is not None else "N/A"
                                _period_lbl  = _kpis.get("_period_label", _kpis.get("period", "?"))
                                _quality     = _kpis.get("_data_quality", "")
                                _annualized  = _kpis.get("_annualized", False)
                                _factor      = _kpis.get("_annualization_factor", 1.0)
                                _q_color     = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}.get(_quality, "⚪")
                                _cur_flag    = "🇺🇸 USD" if _cur == "USD" else "🇪🇬 EGP"
                                st.caption(
                                    f"📅 {_period_lbl} | "
                                    f"{_cur_flag} | "
                                    f"{_q_color} {_quality} confidence"
                                )
                                if _annualized:
                                    st.caption(
                                        f"⚠️ Quarterly data annualized ×{_factor:.2f} — "
                                        f"upload annual report when available for exact figures"
                                    )
                                st.markdown(
                                    f"<p style='margin:2px 0;font-size:12px;'>Revenue: <b>{_fmt(_kpis.get('revenue'))}M</b></p>"
                                    f"<p style='margin:2px 0;font-size:12px;'>Net Profit: <b>{_fmt(_kpis.get('net_profit'))}M</b></p>"
                                    f"<p style='margin:2px 0;font-size:12px;'>ROE: <b>{_fmt(_kpis.get('roe'))}%</b></p>"
                                    f"<p style='margin:2px 0;font-size:12px;'>D/E: <b>{_fmt(_kpis.get('debt_to_equity'),3)}</b></p>"
                                    f"<p style='margin:2px 0;font-size:12px;'>EPS: <b>{_fmt(_kpis.get('eps'),4)} {_cur}</b></p>",
                                    unsafe_allow_html=True)
                                _itr = _kpis.get("interest_to_rev")
                                if _itr is not None:
                                    _shariah_ok = float(_itr) < 5.0
                                    _sc = "#00A86B" if _shariah_ok else "#EF4444"
                                    _sl = "PASS" if _shariah_ok else "REVIEW"
                                    st.markdown(
                                        f"<p style='margin:4px 0;font-size:12px;'>"
                                        f"Interest/Rev: <b>{_fmt(_itr,3)}%</b> "
                                        f"<span style='color:{_sc};font-weight:700;'>({_sl})</span></p>",
                                        unsafe_allow_html=True)
                            else:
                                st.markdown("<small style='color:#B8B8B6;'>No fundamentals uploaded yet</small>", unsafe_allow_html=True)

                        with dd2:
                            st.markdown("**Technical Detail**")
                            e20  = _mtf.get("ema20")
                            e50  = _mtf.get("ema50")
                            e200 = _mtf.get("ema200")
                            ema_al = _mtf.get("bullish_ema_alignment", False)
                            st.markdown(
                                f"<p style='margin:2px 0;font-size:12px;'>EMA 20: <b>{'EGP '+str(round(e20,2)) if e20 else 'N/A'}</b></p>"
                                f"<p style='margin:2px 0;font-size:12px;'>EMA 50: <b>{'EGP '+str(round(e50,2)) if e50 else 'N/A'}</b></p>"
                                f"<p style='margin:2px 0;font-size:12px;'>EMA 200: <b>{'EGP '+str(round(e200,2)) if e200 else 'N/A'}</b></p>"
                                f"<p style='margin:2px 0;font-size:12px;'>EMA Aligned: <b style='color:{'#00A86B' if ema_al else '#EF4444'}'>{'YES' if ema_al else 'NO'}</b></p>"
                                f"<p style='margin:2px 0;font-size:12px;'>RSI Daily: <b>{f'{daily_rsi:.1f}' if daily_rsi else 'N/A'}</b></p>"
                                f"<p style='margin:2px 0;font-size:12px;'>RSI Weekly: <b>{f'{weekly_rsi:.1f}' if weekly_rsi else 'N/A'}</b></p>",
                                unsafe_allow_html=True)
                            st.markdown(
                                f"<p style='margin:2px 0;font-size:12px;'>"
                                f"Divergence: <b style='color:{div_color};'>{div_signal}</b></p>"
                                f"<p style='margin:2px 0;font-size:12px;font-size:11px;color:#6B7B8D;'>"
                                f"{_div.get('detail','')}</p>"
                                f"<p style='margin:4px 0;font-size:12px;'>"
                                f"Volume: <b style='color:{vol_color};'>{vol_signal.replace('_',' ')}</b></p>"
                                f"<p style='margin:0;font-size:11px;color:#6B7B8D;'>{_vol.get('detail','')}</p>",
                                unsafe_allow_html=True)
                            _fib_zone = str(row.get("fib_zone") or "—")
                            st.markdown(
                                f"<p style='margin:6px 0 2px 0;font-size:12px;'>Fib Zone: <b>{_fib_zone}</b></p>",
                                unsafe_allow_html=True)

                        with dd3:
                            st.markdown("**Targets & Fair Value**")
                            if fv_mid:
                                _fv_low  = _fv.get("fv_low",  "?")
                                _fv_high = _fv.get("fv_high", "?")
                                _pe_used = _fv.get("pe_used",  "?")
                                _eps_used = _fv.get("eps_used","?")
                                st.markdown(
                                    f"<p style='margin:2px 0;font-size:12px;'>FV Low: <b>EGP {_fv_low}</b></p>"
                                    f"<p style='margin:2px 0;font-size:12px;'>FV Mid: <b>EGP {fv_mid}</b></p>"
                                    f"<p style='margin:2px 0;font-size:12px;'>FV High: <b>EGP {_fv_high}</b></p>"
                                    f"<p style='margin:2px 0;font-size:11px;color:#6B7B8D;'>"
                                    f"P/E used: {_pe_used}x | EPS: {_eps_used}</p>",
                                    unsafe_allow_html=True)
                            else:
                                st.markdown("<small style='color:#B8B8B6;'>No EPS data — upload fundamentals</small>", unsafe_allow_html=True)

                            _quality_note = _fv.get("quality_note", "")
                            if _quality_note:
                                st.caption(_quality_note)

                            _t1  = _safe_float(row.get("target_1m")  or _enh.get("target_1m"))
                            _t6  = _safe_float(row.get("target_6m")  or _enh.get("target_6m"))
                            _t12 = _safe_float(row.get("target_12m") or _enh.get("target_12m"))
                            _cur_p = _safe_float(row.get("price") or row.get("close")) or 0

                            def _fmt_tgt(price, cur):
                                if not price or not cur:
                                    return "—", "—"
                                pct  = round((price - cur) / cur * 100, 1)
                                sign = "+" if pct >= 0 else ""
                                col  = "#00A86B" if pct >= 0 else "#EF4444"
                                return f"EGP {price:.2f}", f"<span style='color:{col};'>{sign}{pct:.1f}%</span>"

                            _t1s,  _p1s  = _fmt_tgt(_t1,  _cur_p)
                            _t6s,  _p6s  = _fmt_tgt(_t6,  _cur_p)
                            _t12s, _p12s = _fmt_tgt(_t12, _cur_p)
                            st.markdown(
                                f"<p style='margin:6px 0 2px 0;font-size:12px;'>Target 1M: "
                                f"<b>{_t1s}</b> {_p1s}</p>"
                                f"<p style='margin:2px 0;font-size:12px;'>Target 6M: "
                                f"<b>{_t6s}</b> {_p6s}</p>"
                                f"<p style='margin:2px 0;font-size:12px;'>Target 12M: "
                                f"<b>{_t12s}</b> {_p12s}</p>",
                                unsafe_allow_html=True)
                            _ez_low  = _safe_float(row.get("entry_zone_low")  or _enh.get("entry_zone_low"))
                            _ez_high = _safe_float(row.get("entry_zone_high") or _enh.get("entry_zone_high"))
                            _ez_mid  = _safe_float(row.get("entry_zone_mid")  or _enh.get("entry_zone_mid"))
                            if _ez_low and _ez_high:
                                if abs(_ez_high - _ez_low) > 0.10:
                                    st.markdown(
                                        f"<p style='margin:6px 0 2px 0;font-size:12px;'>"
                                        f"Entry Zone: <b style='color:#00A86B;'>"
                                        f"EGP {_ez_low:.2f} – {_ez_high:.2f}</b></p>"
                                        + (f"<p style='margin:0;font-size:11px;color:#6B7B8D;'>"
                                           f"Mid: EGP {_ez_mid:.2f}</p>" if _ez_mid else ""),
                                        unsafe_allow_html=True)
                                else:
                                    st.markdown(
                                        f"<p style='margin:6px 0 2px 0;font-size:12px;'>"
                                        f"Entry Zone: <b style='color:#00A86B;'>EGP {_ez_mid:.2f}</b></p>",
                                        unsafe_allow_html=True)
                                    st.caption("(zone too narrow — signals pending recalc)")
                            elif _ez_mid:
                                st.markdown(
                                    f"<p style='margin:6px 0 2px 0;font-size:12px;'>"
                                    f"Entry Zone: <b style='color:#00A86B;'>EGP {_ez_mid:.2f}</b></p>",
                                    unsafe_allow_html=True)
                            if mtf_agree:
                                st.markdown(
                                    "<p style='margin:6px 0;font-size:11px;"
                                    "color:#00A86B;font-weight:700;'>"
                                    "✓ Daily + Weekly MTF aligned — high-conviction setup</p>",
                                    unsafe_allow_html=True)

                    st.markdown(
                        "<hr style='border:none;border-top:2px solid #D3D3D3;margin:12px 0;'/>",
                        unsafe_allow_html=True)

                st.markdown(
                    "<p style='font-size:11px;color:#6B7B8D;margin:8px 0 16px 0;'>"
                    "① <b>P/L on Cost</b>: (current price − avg cost) ÷ avg cost. "
                    "Reflects YOUR entry, not today's market change. "
                    "Fair Value uses sector P/E × EPS from uploaded fundamentals.</p>",
                    unsafe_allow_html=True)

                # ── Full signals table ────────────────────────────────────────
                with st.expander("📋 Full Signals Table"):
                    _disp_cols = ["ticker", "signal", "action", "score",
                                  "avg_cost", "price", "profit_pct",
                                  "target_1m", "target_6m", "fib_zone",
                                  "forecast_confidence", "run_date"]
                    _avail = [c for c in _disp_cols if c in _sig_df.columns]
                    st.dataframe(_sig_df[_avail], use_container_width=True, hide_index=True)

        except Exception as _e:
            st.error(f"Recommendations tab error: {_e}")

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

    st.subheader("🔧 System Status")
    col1, col2 = st.columns(2)
    with col1:
        try:
            import pytesseract
            pytesseract.get_tesseract_version()
            st.success("✅ Tesseract OCR — Available")
        except Exception:
            st.error("❌ Tesseract OCR — Not installed")
            st.caption("Install from: https://github.com/UB-Mannheim/tesseract/wiki")
    with col2:
        try:
            from pdf2image import convert_from_path  # noqa: F401
            st.success("✅ pdf2image / poppler — Available")
        except Exception:
            st.error("❌ pdf2image — Not installed")
            st.caption("Run: pip install pdf2image  +  install poppler")

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

    st.markdown("---")
    st.subheader("📋 Watchlist Manager")

    _wl_conn = get_connection()
    _wl_df = pd.read_sql(
        "SELECT ticker, name, yahoo_code, sector, market, shariah, active, notes "
        "FROM watchlist ORDER BY active DESC, ticker",
        _wl_conn
    )

    if not _wl_df.empty:
        _wl_disp = _wl_df.copy()
        _wl_disp["shariah"] = _wl_disp["shariah"].apply(lambda x: "✅" if x else "❌")
        _wl_disp["active"]  = _wl_disp["active"].apply(lambda x: "🟢" if x else "⚫")
        st.dataframe(
            _wl_disp.rename(columns={
                "ticker": "Ticker", "name": "Company", "yahoo_code": "Yahoo Code",
                "sector": "Sector", "market": "Market",
                "shariah": "Shariah", "active": "Active", "notes": "Notes"
            }),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No tickers in watchlist yet.")

    st.markdown("#### ➕ Add Ticker")
    with st.form("add_ticker_form"):
        _at_c1, _at_c2, _at_c3 = st.columns(3)
        with _at_c1:
            _new_ticker  = st.text_input("Ticker (EGX code)", placeholder="e.g. COMI").upper().strip()
            _new_name    = st.text_input("Company Name", placeholder="e.g. Commercial International Bank")
        with _at_c2:
            _new_yahoo   = st.text_input("Yahoo Finance Code", placeholder="e.g. COMI.CA")
            _new_sector  = st.selectbox("Sector", [
                "Financials", "Industrials", "Materials", "Energy",
                "Consumer", "Real Estate", "Healthcare", "Technology",
                "Utilities", "Telecom", "Other"
            ])
        with _at_c3:
            _new_market  = st.selectbox("Market", ["EGX", "ADX", "NYSE", "Other"])
            _new_shariah = st.checkbox("Shariah Compliant", value=True)
            _new_notes   = st.text_input("Notes (optional)")
        _at_submit = st.form_submit_button("➕ Add to Watchlist", use_container_width=True)
        if _at_submit:
            if not _new_ticker or not _new_name:
                st.error("Ticker and Company Name are required.")
            else:
                try:
                    _wl_conn.execute("""
                        INSERT OR REPLACE INTO watchlist
                        (ticker, name, yahoo_code, sector, market, shariah, active, notes)
                        VALUES (?,?,?,?,?,?,1,?)
                    """, (_new_ticker, _new_name, _new_yahoo, _new_sector,
                          _new_market, int(_new_shariah), _new_notes))
                    _wl_conn.commit()
                    st.success(f"✅ {_new_ticker} — {_new_name} added to watchlist!")
                    st.rerun()
                except Exception as _e:
                    st.error(f"Error: {_e}")

    st.markdown("#### ✏️ Edit or Remove Ticker")

    _SECTORS = [
        "Financials", "Industrials", "Materials", "Energy",
        "Consumer", "Real Estate", "Healthcare", "Technology",
        "Utilities", "Telecom", "Other"
    ]
    _MARKETS = ["EGX", "ADX", "NYSE", "Other"]

    _wl_tickers_list = [r[0] for r in _wl_conn.execute(
        "SELECT ticker FROM watchlist ORDER BY ticker"
    ).fetchall()]

    _edit_ticker = st.selectbox("Select Ticker to Edit", _wl_tickers_list, key="edit_sel")

    if _edit_ticker:
        _wl_row = _wl_conn.execute(
            "SELECT ticker, name, yahoo_code, sector, market, "
            "shariah, active, notes FROM watchlist WHERE ticker=?",
            (_edit_ticker,)
        ).fetchone()

        if _wl_row:
            st.markdown(f"**Editing: {_wl_row[0]} — {_wl_row[1]}**")

            with st.form("edit_ticker_form"):
                _ec1, _ec2, _ec3 = st.columns(3)

                with _ec1:
                    _edit_name  = st.text_input("Company Name",  value=_wl_row[1] or "")
                    _edit_yahoo = st.text_input("Yahoo Finance Code", value=_wl_row[2] or "",
                                                placeholder="e.g. COMI.CA")

                with _ec2:
                    _cur_sector = _wl_row[3] or "Other"
                    _sector_idx = _SECTORS.index(_cur_sector) if _cur_sector in _SECTORS else len(_SECTORS) - 1
                    _edit_sector = st.selectbox("Sector", _SECTORS,
                                                index=_sector_idx, key="edit_sector_sel")

                    _cur_market = _wl_row[4] or "EGX"
                    _market_idx = _MARKETS.index(_cur_market) if _cur_market in _MARKETS else 0
                    _edit_market = st.selectbox("Market", _MARKETS,
                                                index=_market_idx, key="edit_market_sel")

                with _ec3:
                    _edit_shariah = st.checkbox("Shariah Compliant", value=bool(_wl_row[5]),
                                                key="edit_shariah_cb")
                    _edit_active  = st.checkbox("Active", value=bool(_wl_row[6]),
                                                key="edit_active_cb")
                    _edit_notes   = st.text_input("Notes", value=_wl_row[7] or "")

                _save_col, _del_col = st.columns([3, 1])
                with _save_col:
                    _save_edit = st.form_submit_button("💾 Save Changes",
                                                       use_container_width=True,
                                                       type="primary")
                with _del_col:
                    _delete_edit = st.form_submit_button("🗑️ Delete",
                                                         use_container_width=True,
                                                         type="secondary")

                if _save_edit:
                    try:
                        _wl_conn.execute("""
                            UPDATE watchlist
                            SET name=?, yahoo_code=?, sector=?, market=?,
                                shariah=?, active=?, notes=?
                            WHERE ticker=?
                        """, (
                            _edit_name, _edit_yahoo, _edit_sector, _edit_market,
                            int(_edit_shariah), int(_edit_active),
                            _edit_notes, _edit_ticker
                        ))
                        _wl_conn.commit()
                        st.success(
                            f"✅ {_edit_ticker} updated — "
                            f"Sector: {_edit_sector} | "
                            f"Shariah: {'✅' if _edit_shariah else '❌'} | "
                            f"Active: {'🟢' if _edit_active else '⚫'}"
                        )
                        st.rerun()
                    except Exception as _upd_err:
                        st.error(f"❌ Update failed: {_upd_err}")

                if _delete_edit:
                    try:
                        _wl_conn.execute(
                            "DELETE FROM watchlist WHERE ticker=?", (_edit_ticker,)
                        )
                        _wl_conn.commit()
                        st.success(f"🗑️ {_edit_ticker} deleted from watchlist.")
                        st.rerun()
                    except Exception as _del_err:
                        st.error(f"❌ Delete failed: {_del_err}")

    _wl_conn.close()

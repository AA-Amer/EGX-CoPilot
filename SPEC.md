# EGX Copilot — Project Specification
**Last updated: 2026-04-16**

---

## What this is
AI multi-agent investment copilot for the Egyptian Stock Exchange (EGX).
Shariah-compliant. Two wallets. Free stack only — no paid APIs or subscriptions.

---

## Architecture
```
Streamlit UI → Python Agents → Analysis Engines → Free Data Sources
```
Single-process app — no separate backend server needed for MVP.

---

## UI
- Framework: Streamlit (Python, runs locally)
- Entry point: `streamlit run app.py`
- **Main tabs:** Chat / Long-Term Wallet / Swing Scanner / Settings
- **Sidebar:** wallet summary, top movers, watchlist with daily change % and wallet badge (LT/SW/Both)
- Future upgrade path: React frontend (designs already completed)

### Long-Term Wallet — Subtabs
| # | Subtab | Description |
|---|---|---|
| 1 | 📋 Transactions | Full transaction log with filters + Add Transaction form |
| 2 | 📊 Positions Summary | Open/closed positions table + portfolio metrics |
| 3 | 📈 KPIs Dashboard | XIRR, real gain, concentration, allocation chart |
| 4 | 🌡️ Inflation | CPI tracker, monthly input, cumulative index |
| 5 | 🤖 Recommendations | AI signals per ticker, run signal analysis button |

---

## Two wallets
- **Long-term wallet:** buy & hold, fundamental analysis, 6+ month horizon
- **Swing trading wallet:** T+2 compounding, top 3 tickers per cycle, daily signals
- **Cash opportunities:** reserved for surprise entries
- All percentages configurable from Settings tab (written to `config.json`)

### Default wallet allocation
- Long-term: 60%
- Swing: 30%
- Cash: 10%

---

## Swing trading rules
- Signal score minimum: 70/100 across 6 indicators (RSI, EMA9/21, MACD, Volume, Support, BB)
- Minimum trade size: EGP 2,000 (broker fee floor)
- Max concurrent positions: 3 (top 3 by signal score)
- Allocation mode: `score_weighted` (configurable: equal / top_heavy)
- Step target per trade: 3.5%
- Stop loss per trade: 2.5%
- Minimum R:R ratio: 1:1.2 (hard gate — trade blocked if below)
- Monthly target: 10% on swing wallet
- T+2 settlement strictly enforced — no re-entry on unsettled cash
- Max combined loss across all positions: 5% of swing wallet
- Sector cap: max 2 picks from same sector per cycle
- Signal scan runs at 17:30 EET + re-check at 09:45 EET

## Long-term wallet rules
- Max single stock allocation: 20% of LT wallet
- Max sector allocation: 40% of LT wallet
- Annual return target: 25%
- Minimum holding period: 6 months (180 days)

---

## Shariah compliance (5 screens — applied before any signal is shown)
1. Business activity — no conventional banking, alcohol, tobacco, gambling, weapons
2. Interest-bearing debt / total assets < 33%
3. Interest income / total revenue < 5% (purification required if 3–5%)
4. Accounts receivable / total assets < 49%
5. Cross-reference EGX Islamic Index + AAOIFI standards

---

## Data stack (100% free)
- **Prices:** yfinance daily, collected at 15:05 EET via scheduler
  - Most EGX tickers require long ISIN codes (not simple `.CA` suffix)
  - Symbol overrides defined in `price_collector.py`
  - Period = `1d`, upsert via `INSERT OR REPLACE`
  - Backup: Stooq via pandas-datareader
- **Historical prices:** imported from Investing.com into SQLite (already populated)
- **News/sentiment:** Google News RSS + yfinance ticker.news (Arabic supported)
- **Macro:** FRED API (free key) + IMF DataMapper API + CBE website
- **Egypt market hours:** Sun–Thu; weekend = Friday (4) + Saturday (5) — scheduler skips these

### yfinance Symbol Overrides (price_collector.py)
```python
_SYMBOL_OVERRIDES = {
    "ORAS": "EGS95001C011.CA",   # delisted on simple Yahoo ticker
    "AMOC": "EGS380P1C010.CA",
    "MICH": "EGS38211C016.CA",
    "SWDY": "EGS3G0Z1C014.CA",
    "MPCI": "EGS38351C010.CA",
    "ORWE": "EGS33041C012.CA",
    "SUGR": "EGS30201C015.CA",
    "ABUK": "EGS38191C010.CA",
    "OLFI": "EGS30AL1C012.CA",
}
EGX30_SYMBOL = "EGX30.CA"
```

---

## AI / LLM stack (100% free)
- **Primary:** Groq free tier — `llama-3.3-70b-versatile`
  - Get free key at console.groq.com
  - Used for: news analysis, Arabic sentiment, signal scoring, structured extraction, chat
  - Note: `llama-3.1-70b-versatile` was decommissioned — always use `llama-3.3-70b-versatile`
- **Fallback:** Gemini 2.0 Flash (free key at aistudio.google.com)
- **Future:** Claude Sonnet 4.6 — configured but disabled (`enabled: false` in config.json)
  - Activate by setting `enabled: true` and adding `ANTHROPIC_API_KEY` to `.env`
- All providers use OpenAI-compatible client — switching is one config line
- Task routing in `config.json` controls which provider handles each task type
- `simple_chat` task routes to Groq (not Ollama — Ollama not installed in this setup)

---

## Database
- **Engine:** SQLite at path from `.env` → `DB_PATH=D:/egx_copilot.db`
- **NOT** in the repo — external file, gitignored

### Tables

#### Prices (populated from Investing.com + yfinance daily)
| Table | Key columns |
|---|---|
| `prices` | ticker, date, open, high, low, close, volume, source — UNIQUE(ticker, date) |
| `manual_prices` | ticker, date, close, entered_by |
| `collection_log` | run_at, tickers_ok, tickers_fail, notes |

#### Long-Term Wallet (created by `init_lt_tables()` in db.py)
| Table | Key columns |
|---|---|
| `lt_transactions` | id, date, category, ticker, quantity, fulfillment_price, fees, dividend_tax, actual_price_per_share, total_amount, year, quarter, fx_rate, usd_value, net_wallet_impact, external_capital_impact, notes |
| `lt_positions` | id, ticker UNIQUE, total_shares, total_cost_net, weighted_avg_cost, realized_pl, dividends_net, status, last_updated |
| `lt_signals` | id, run_date, ticker, avg_cost, price, signal, action, score, position_size_pct, current_allocation_pct, recommended_shares, recommended_capital, suggested_buy_price, profit_pct, sell_price, fib_zone, swing_high, swing_low, target_1m, target_6m, target_12m, exp_return_1m, exp_return_6m, exp_return_12m, forecast_confidence, description — UNIQUE(run_date, ticker) |
| `lt_purification` | id, ticker, year, quarter, quarter_start, quarter_end, daily_haram_rate, share_days, purification_amount, purification_rounded, status, paid_amount, outstanding, quarter_closed — UNIQUE(ticker, year, quarter) |
| `inflation_data` | id, month_year UNIQUE, headline_mom, cumulative_index, cumulative_pct |

---

## Long-Term Wallet — Transaction categories
| Category | Net Wallet Impact | External Capital Impact |
|---|---|---|
| Buy | -(qty × price + fees) | 0 |
| Sell | +(qty × price - fees) | 0 |
| Dividend | +(qty × price) | 0 |
| Top-Up | +(qty × price) | -(total incl. fees) |
| Subscription | 0 | -(total) |

Position recalculation (`recalculate_positions()`) uses FIFO cost basis and rebuilds `lt_positions` from scratch on every transaction save.

---

## Current LT wallet — Open positions (as of 2026-04-16)
| Ticker | Shares | Avg Cost | Allocation |
|---|---|---|---|
| MPCI | 97 | £146.01 | 20.29% |
| AMOC | 1920 | £7.90 | 19.80% |
| ORWE | 484 | £23.08 | 13.72% |
| MICH | 303 | £31.98 | 13.00% |
| ORAS | 18 | £466.36 | 12.90% |
| OLFI | 495 | £21.13 | 12.78% |
| SUGR | 80 | £48.70 | 4.70% |
| SWDY | 28 | £79.78 | 2.81% |

Closed: ABUK (realized +£45.88), BSB (realized -£5.47)

Total invested: £77,287 | Current worth: £81,968 | Net profit: £4,681 (6.06%) | XIRR: 77.91%

---

## Inflation data (monthly CPI, base Nov 2025)
| Month | m/m % | Cumulative Index |
|---|---|---|
| Nov 2025 | 0.30% | 1.003000 |
| Dec 2025 | 0.20% | 1.005006 |
| Jan 2026 | 1.20% | 1.017066 |
| Feb 2026 | 2.80% | 1.045544 |
| Mar 2026 | 3.20% | 1.079001 |

---

## 6 Agents (Python classes, called from Streamlit directly)
1. **Orchestrator** — routes user messages to right agent
2. **Long-term advisor** — signal generation, Fibonacci zones, targets, buy/hold/trim recommendations
3. **Swing trading advisor** — TA signals, entry/exit, T+2 tracking
4. **Portfolio monitor** — positions, P&L, drift, rebalancing
5. **Risk & allocation engine** — VaR, Kelly, position sizing, guardrails
6. **Memory agent** — user goals, risk profile, trade history (stored in local JSON)
7. **Market data agent** — yfinance wrapper, news ingestion, macro feed

---

## Project structure
```
egx-copilot/
├── app.py                          ← Streamlit entry point
├── backend/
│   ├── agents/
│   │   ├── orchestrator.py
│   │   ├── swing_agent.py
│   │   ├── longterm_agent.py       ← LT signal generation via LLM
│   │   ├── risk_engine.py
│   │   ├── memory_agent.py
│   │   └── market_data_agent.py
│   ├── analysis/
│   │   ├── technical.py            ← RSI, EMA, MACD, BB, Volume ratio
│   │   ├── fundamental.py          ← yfinance financials
│   │   ├── sentiment.py            ← Google RSS + Arabic NLP
│   │   └── shariah.py              ← 5-screen compliance checker
│   └── data/
│       ├── config_loader.py        ← load_config() with lru_cache
│       ├── db.py                   ← init_db(), init_lt_tables(), get_connection()
│       ├── fetcher.py              ← reads from SQLite (not yfinance directly)
│       ├── llm_client.py           ← ask_llm() multi-provider router
│       ├── lt_db.py                ← all LT wallet DB operations
│       ├── lt_seed.py              ← one-time seed script (run manually)
│       ├── price_collector.py      ← collect_today() with symbol overrides + upsert
│       └── scheduler.py            ← runs at 15:05 EET, skips Fri+Sat
├── data/
│   └── memory.json                 ← user goals, trade history (gitignored)
├── scripts/
│   └── collect_prices.py
├── config.json                     ← all settings, edited from Settings tab
├── SPEC.md
├── requirements.txt
├── .env                            ← gitignored
└── .env.example
```

---

## Environment variables (.env)
```
GROQ_API_KEY=your_key_here
GEMINI_API_KEY=your_key_here        # optional fallback
DB_PATH=D:/egx_copilot.db
APP_ENV=development
```

---

## Key requirements (requirements.txt)
```
streamlit>=1.12.0
yfinance>=0.2.38
pandas>=2.2.0
openai>=1.30.0
python-dotenv>=1.0.0
plotly>=5.22.0
ta>=0.11.0
schedule>=1.2.0
pydantic>=2.7.0
```

---

## Known issues & fixes
| Issue | Fix |
|---|---|
| `applymap` AttributeError on pandas 2.2+ | Replace `.applymap()` with `.map()` — pure rename |
| yfinance simple `.CA` format returns 0 rows for most EGX tickers | Use long ISIN codes in `_SYMBOL_OVERRIDES` |
| Groq `llama-3.1-70b-versatile` decommissioned | Use `llama-3.3-70b-versatile` |
| `simple_chat` routed to Ollama (not installed) | Set task routing to `groq` in `config.json` |
| `rowid` KeyError in manual entries | Use `SELECT rowid as rowid` explicit alias in SQL |
| Python 3.9.7 (Anaconda) blocked by Streamlit | Use Python 3.11 venv |

---

## How to run
```bash
# Activate venv (Git Bash)
source venv/Scripts/activate

# First time only — seed LT wallet data
python backend/data/lt_seed.py

# Run app
streamlit run app.py

# Manual price collection (runs automatically via scheduler at 15:05 EET)
python scripts/collect_prices.py
```

---

## Repo
- GitHub: https://github.com/AA-Amer/EGX-CoPilot (public)
- DB file is external to repo (D:/egx_copilot.db) — never committed

# EGX Copilot — Project Specification

## What this is
AI multi-agent investment copilot for the Egyptian Stock Exchange (EGX).
Shariah-compliant. Two wallets. Free stack only — no paid APIs or subscriptions.

## Architecture
Streamlit UI → Python Agents → Analysis Engines → Free Data Sources
Single-process app — no separate backend server needed for MVP.

## UI
- Framework: Streamlit (Python, runs locally)
- Tabs: Chat / Swing Scanner / Portfolio / Settings
- Sidebar: wallet summary, top movers, watchlist
- Future upgrade path: React frontend (designs already completed)

## Two wallets
- Long-term wallet: buy & hold, fundamental analysis, 6+ month horizon
- Swing trading wallet: T+2 compounding, top 3 tickers per cycle, daily signals
- Cash opportunities: reserved for surprise entries
- All percentages configurable from Settings tab (written to config.json)

## Default wallet allocation
- Long-term: 60%
- Swing: 30%
- Cash: 10%

## Swing trading rules
- Signal score minimum: 70/100 across 6 indicators (RSI, EMA9/21, MACD, Volume, Support, BB)
- Minimum trade size: EGP 2,000 (broker fee floor)
- Max concurrent positions: 3 (top 3 by signal score)
- Allocation mode: score_weighted (configurable: equal / top_heavy)
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

## Shariah compliance (5 screens — applied before any signal is shown)
1. Business activity — no conventional banking, alcohol, tobacco, gambling, weapons
2. Interest-bearing debt / total assets < 33%
3. Interest income / total revenue < 5% (purification required if 3–5%)
4. Accounts receivable / total assets < 49%
5. Cross-reference EGX Islamic Index + AAOIFI standards

## Data stack (100% free)
- Prices: yfinance, EGX tickers use .CA suffix (e.g. SWDY → SWDY.CA)
- Prices are 15-min delayed — acceptable for daily signal generation
- Backup: Stooq via pandas-datareader
- News/sentiment: Google News RSS + yfinance ticker.news (Arabic supported)
- Macro: FRED API (free key) + IMF DataMapper API + CBE website
- User verifies live price on their broker before placing any order

## AI / LLM stack (100% free)
- Primary: Groq free tier — Llama 3.1 70B (best quality, very fast)
  - Get free key at console.groq.com
  - Used for: news analysis, Arabic sentiment, signal scoring, structured extraction
- Fallback 1: Ollama local — Llama 3.1 8B (no key, fully offline)
  - Used for: simple chat replies when Groq is unavailable
  - Install: https://ollama.com then `ollama pull llama3.1:8b`
- Fallback 2: Gemini 2.0 Flash (free key at aistudio.google.com)
- Future: Claude Sonnet 4.6 — configured but disabled (enabled: false in config.json)
  - Activate by setting enabled: true and adding ANTHROPIC_API_KEY to .env
- All providers use OpenAI-compatible client — switching is one config line
- Task routing in config.json controls which provider handles each task type

## 6 Agents (Python classes, called from Streamlit directly)
1. Orchestrator — routes user messages to right agent
2. Long-term advisor — DCF, fundamentals, sector rotation
3. Swing trading advisor — TA signals, entry/exit, T+2 tracking
4. Portfolio monitor — positions, P&L, drift, rebalancing
5. Risk & allocation engine — VaR, Kelly, position sizing, guardrails
6. Memory agent — user goals, risk profile, trade history (stored in local JSON)
7. Market data agent — yfinance wrapper, news ingestion, macro feed

## Project structure
```
egx-copilot/
├── app.py                    ← Streamlit entry point (run with: streamlit run app.py)
├── backend/
│   ├── agents/
│   │   ├── orchestrator.py
│   │   ├── swing_agent.py
│   │   ├── longterm_agent.py
│   │   ├── risk_engine.py
│   │   ├── memory_agent.py
│   │   └── market_data_agent.py
│   ├── analysis/
│   │   ├── technical.py      ← RSI, EMA, MACD, BB, Volume ratio
│   │   ├── fundamental.py    ← yfinance financials
│   │   ├── sentiment.py      ← Google RSS + Arabic NLP
│   │   └── shariah.py        ← 5-screen compliance checker
│   └── data/
│       ├── config_loader.py  ← load_config() with lru_cache
│       ├── fetcher.py        ← yfinance .CA wrapper
│       └── llm_client.py     ← ask_llm() multi-provider router
├── data/
│   └── memory.json           ← user goals, trade history (gitignored)
├── config.json               ← all settings, edited from Settings tab
├── SPEC.md
├── requirements.txt
├── .env                      ← gitignored
└── .env.example
```

## How to run
```bash
pip install -r requirements.txt
streamlit run app.py
```

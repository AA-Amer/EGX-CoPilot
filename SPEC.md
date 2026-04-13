# EGX Copilot — Project Specification

## What this is
AI multi-agent investment copilot for the Egyptian Stock Exchange (EGX).
Shariah-compliant. Two wallets. Free data stack only.

## Architecture
5 layers: UI → Orchestrator → 6 Agents → 5 Analysis Engines → Data Sources
See full system diagram in conversation history on claude.ai.

## Two wallets
- Long-term wallet: buy & hold, fundamental analysis, 6+ month horizon
- Swing trading wallet: T+2 compounding, top 3 tickers per cycle, daily signals
- Cash opportunities: reserved for surprise entries

## Default wallet allocation
- Long-term: 60%
- Swing: 30%
- Cash: 10%
All percentages configurable via settings UI (config.json).

## Swing trading rules
- Signal score minimum: 70/100 across 6 indicators (RSI, EMA9/21, MACD, Volume, Support, BB)
- Minimum trade size: EGP 2,000 (broker fee floor)
- Max concurrent positions: 3 (top 3 by signal score)
- Step target per trade: 3.5%
- Stop loss per trade: 2.5%
- Minimum R:R ratio: 1:1.2 (hard gate, trade blocked if below)
- Monthly target: 10% on swing wallet
- T+2 settlement strictly enforced — no re-entry on unsettled cash
- Max combined loss across all positions: 5% of swing wallet
- Sector cap: max 2 picks from same sector per cycle
- Signal runs at 17:30 EET (end of day) + re-check at 09:45 EET

## Long-term wallet rules
- Max single stock allocation: 20% of LT wallet
- Max sector allocation: 40% of LT wallet
- Annual return target: 25%
- Minimum holding period: 6 months

## Shariah compliance (5 screens)
1. Business activity — no conventional banking, alcohol, tobacco, gambling, weapons
2. Interest-bearing debt / total assets < 33%
3. Interest income / total revenue < 5% (purification required if 3–5%)
4. Accounts receivable / total assets < 49%
5. Cross-reference EGX Islamic Index + AAOIFI standards

## Data stack (100% free)
- Prices: yfinance Python library, EGX tickers use .CA suffix (e.g. COMI.CA)
- Prices are 15-min delayed — acceptable for daily signal generation
- Backup: Stooq via pandas-datareader
- News/sentiment: Google News RSS + yfinance ticker.news (Arabic supported)
- Macro: FRED API (free key) + IMF DataMapper API + CBE website
- No live price feed needed — user verifies on broker before order execution

## EGX ticker format for yfinance
ORAS → EGS95001C011.CA
AMOC → EGS380P1C010.CA
MICH → EGS38211C016.CA
SWDY → EGS3G0Z1C014.CA
MPCI → EGS38351C010.CA
ORWE → EGS33041C012.CA
SUGR → EGS30201C015.CA
ABUK → EGS38191C010.CA
OLFI → EGS30AL1C012.CA

## 6 Agents
1. Orchestrator — routes queries, synthesizes responses
2. Long-term advisor — DCF, fundamentals, sector rotation
3. Swing trading advisor — TA signals, entry/exit, T+2 tracking
4. Portfolio monitor — positions, P&L, drift, rebalancing
5. Risk & allocation engine — VaR, Kelly, position sizing, guardrails
6. Memory agent — user goals, risk profile, trade history
7. Market data agent — yfinance wrapper, news ingestion, macro feed

## Tech stack
- Backend: Python, FastAPI
- Frontend: React (Vite)
- AI: Anthropic Claude API (claude-sonnet-4-5)
- Data: yfinance, pandas, pandas-datareader
- Config: config.json (editable from settings UI)
- No paid data subscriptions

## Project structure
egx-copilot/
├── backend/
│   ├── agents/
│   │   ├── orchestrator.py
│   │   ├── swing_agent.py
│   │   ├── longterm_agent.py
│   │   ├── risk_engine.py
│   │   ├── memory_agent.py
│   │   └── market_data_agent.py
│   ├── analysis/
│   │   ├── technical.py
│   │   ├── fundamental.py
│   │   ├── sentiment.py
│   │   └── shariah.py
│   ├── data/
│   │   ├── fetcher.py
│   │   ├── scheduler.py
│   │   └── config.json
│   ├── api/
│   │   └── routes.py
│   └── main.py
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── Chat.jsx
│   │   │   ├── Sidebar.jsx
│   │   │   ├── SwingPanel.jsx
│   │   │   ├── SettingsPage.jsx
│   │   │   └── StockCard.jsx
│   │   └── App.jsx
│   └── package.json
├── SPEC.md        ← this file
├── config.json
├── requirements.txt
└── .env.example
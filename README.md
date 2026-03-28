# Dave's AI Trader 🤖📈

A fully automated paper trading engine that runs **20 concurrent strategies** on S&P 500 stocks, powered by Claude AI. Each strategy manages its own $1,000 account, makes daily buy/sell decisions, and competes on a live leaderboard.

---

## What It Does

- Runs daily via **GitHub Actions** (pre-market + post-market schedule)
- Fetches live KPI data from **Yahoo Finance** and macro data from **FRED**
- Feeds signals to **Claude (claude-sonnet-4-6)** for trade decisions
- Tracks P&L, holdings, and transactions per strategy in CSV files
- Publishes a ranked leaderboard after every run

---

## Repository Structure

```
daves_ai_trader/
├── strategy_runner.py        # Main engine — runs all 20 strategies
├── equity_kpi_analyzer.py    # Fetches fundamentals + technicals for universe
├── universe_manager.py       # Manages S&P 500 constituent list (cached weekly)
├── update_quotes.py          # Refresh live prices mid-day (no trading logic)
├── reset_accounts.py         # Hard reset all accounts to $1,000
├── down_day_analyzer.py      # Research tool: what held up on bad market days?
├── run_daily.bat             # Windows local runner (pre/post-market aware)
├── deploy_from_github.sh     # Linux deploy + dry-run validator
├── .github/workflows/
│   └── daily_trading.yml     # GitHub Actions schedule (UTC)
├── account_XX.csv            # Per-strategy account state
├── holdings_XX.csv           # Per-strategy open positions
├── transactions_XX.csv       # Per-strategy trade log
├── leaderboard.csv           # Daily ranked results
├── equity_kpi_results.csv    # Today's scored universe (200 tickers)
├── universe_cache.json       # Weekly S&P 500 constituent cache
└── news_macro_cache.json     # Daily news analysis cache (strategies 19 & 20)
```

---

## The 20 Strategies

| ID | Name | Style | Risk | Key Signal |
|----|------|-------|------|------------|
| 01 | Momentum / trend following | momentum | high | Golden cross + strong RSI |
| 02 | Mean reversion | contrarian | med | RSI < 38 oversold |
| 03 | Value investing | value | low | Low P/E + high margin |
| 04 | Quality growth (GARP) | growth | med | EPS growth + margin + P/E ≤ 50 |
| 05 | Sector rotation | macro | med | Top sector composite score |
| 06 | Low volatility / defensive | defensive | low | Beta < 1.0 + stable earnings |
| 07 | Earnings surprise (PEAD) | event | high | Profitable + fwd EPS growth + positive abnormal return |
| 08 | Dividend growth | income | low | High yield + growing payout |
| 09 | Insider buying signal | alt-data | med | Net C-suite open-market buys |
| 10 | Macro-regime adaptive | macro | med | VIX-driven aggressive/defensive switch |
| 11 | Large-cap value (Fama-French) | academic | med | Bottom-third market cap + low P/E |
| 12 | Momentum (Asness/AQR) | academic | high | Golden cross + ≥5% below 52w high |
| 13 | Quality / profitability | academic | low | High gross margin + low beta |
| 14 | Passive S&P 500 benchmark | passive | low | Buy-and-hold top market cap |
| 15 | Noise chaser *(degenerate baseline)* | speculative | high | Pure abnormal return chasing |
| 16 | Estimate revision momentum | growth | med | Upward EPS estimate revisions |
| 17 | High-beta quality growth | growth | high | Beta 1.5–2.5 + strong fundamentals |
| 18 | Capex beneficiary / semis | thematic | high | IT sector + high margin + golden cross |
| 19 | News macro catalyst | macro | med | RSS headlines → Claude macro themes → sector scoring |
| 20 | News sentiment momentum | alt-data | med | KPI quality gate + news sentiment overlay |

Strategy 14 (passive) never calls Claude — it's a pure code-driven buy-and-hold benchmark. Strategy 15 (noise chaser) is an intentional degenerate baseline to demonstrate that chasing daily movers loses money.

---

## Daily Schedule (GitHub Actions)

| Stage | UTC Cron | Local (ET Winter) | What Runs |
|-------|----------|-------------------|-----------|
| Pre-market | `30 11 * * 2-6` | 6:30 AM | Strategies 19 & 20 only (news cache built) |
| Post-market | `30 21 * * 1-5` | 4:30 PM | KPI analyser + all 20 strategies |

The news cache from Stage 1 is reused in Stage 2 — Claude is called only once per day for macro analysis regardless of how many strategies consume it.

> **Note:** GitHub Actions cron runs on UTC with no automatic DST adjustment. In EDT (summer) the post-market run lands at 5:30 PM ET.

---

## Setup

### Requirements

```bash
pip install -r requirements.txt
```

Key dependencies: `yfinance`, `pandas`, `numpy`, `requests`, `fredapi` (optional)

### Secrets (GitHub Actions)

| Secret | Required | Purpose |
|--------|----------|---------|
| `ANTHROPIC_API_KEY` | Yes | Claude trade decisions + news analysis |
| `FRED_API_KEY` | Optional | GDP / CPI / Fed rate macro data (free at fred.stlouisfed.org) |

### First Run

```bash
# 1. Initialise all 20 strategy accounts
python strategy_runner.py --init

# 2. Fetch KPI data for the universe
python equity_kpi_analyzer.py --universe

# 3. Dry-run to preview decisions without executing trades
python strategy_runner.py --dry-run

# 4. Run all strategies for real
python strategy_runner.py
```

### Single Strategy

```bash
python strategy_runner.py --strategy 18    # run only strategy 18
python strategy_runner.py --strategy 19    # news macro only
```

### Windows Local Runner

`run_daily.bat` auto-detects the time and runs the appropriate stage:

```bat
run_daily.bat           # auto: news before 9 AM, full after 4 PM
run_daily.bat --news    # force pre-market news run
run_daily.bat --full    # force full post-market run
run_daily.bat --all     # run both stages sequentially
```

### Refresh Live Quotes (No Trading)

```bash
python update_quotes.py           # refresh all strategy portfolio values
python update_quotes.py --show    # + print per-position detail
```

### Reset Everything

```bash
python reset_accounts.py          # prompts for confirmation
python reset_accounts.py --yes    # skip prompt (CI use)
```

---

## KPI Scoring System

`equity_kpi_analyzer.py` scores each stock 0–100 across three tiers:

| Tier | Weight | Signals |
|------|--------|---------|
| Tier 1 — Fundamentals | 60% | Net profit margin, EPS growth, current ratio, GDP, CPI, Fed rate, VIX |
| Tier 2 — Technicals | 30% | RSI-14, 50/200-day MA (golden/death cross), abnormal return |
| Tier 3 — Sentiment | 10% | Analyst consensus, net insider share transactions |

The **composite score** (0–100) is the weighted average of available tiers. Strategy scoring functions each apply additional filters on top of this score.

---

## News Infrastructure (Strategies 19 & 20)

Free RSS feeds are polled from Reuters, AP, Yahoo Finance, CNBC, and MarketWatch. Claude analyzes up to 60 deduplicated headlines and returns structured JSON identifying:

- **Market regime** (RISK-ON / RISK-OFF / NEUTRAL)
- **Dominant macro themes** with beneficiary and loser sectors
- **Company-specific catalysts** (earnings beats, upgrades, scandals)
- **Trade ideas**

The result is cached to `news_macro_cache.json` and reused across both strategies and both run stages — no duplicate API calls.

---

## Leaderboard

`leaderboard.csv` is updated after every run and committed back to the repo:

```
Rank  ID   Strategy                             Total      P&L    Return
1     18   Capex beneficiary / semis         $1,243.10  +$243.10   +24.31%
2     04   Quality growth (GARP)             $1,187.40  +$187.40   +18.74%
...
20    15   Noise chaser                        $821.30  -$178.70   -17.87%
```

---

## Research Tools

**`down_day_analyzer.py`** — identifies which stocks held up (or gained) on a down market day and explains why (sector, beta, momentum, dividends, fundamentals, insider activity). Useful for finding defensive characteristics before the next downturn.

```bash
python down_day_analyzer.py
```

---

## Architecture Notes

- All strategies share a single `equity_kpi_results.csv` input — the KPI analyser runs once and all strategy scoring functions read the same file
- Each strategy has independent CSV files: `account_XX.csv`, `holdings_XX.csv`, `transactions_XX.csv`
- Strategy 14 (passive) never calls Claude — trade logic runs entirely in Python
- Holdings values are always marked-to-market at current prices (not stale cost basis) before any trade decision
- `eps_revision_pct` is computed by diffing today's `eps_growth_fwd` against yesterday's snapshot — powers Strategy 16
- `universe_cache.json` is refreshed weekly; `news_macro_cache.json` is refreshed daily

---

## Author

Dave Skura, 2026

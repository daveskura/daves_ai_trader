# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dave's AI Trader is a fully automated paper trading engine that runs 15-20 concurrent strategies on S&P 500 stocks, powered by Claude AI. Each strategy manages its own $1,000 account, makes daily buy/sell decisions, and competes on a live leaderboard. Data is persisted in MySQL. Runs are scheduled daily via GitHub Actions.

## Common Commands

```bash
# Run all strategies
python strategy_runner.py

# Run a single strategy
python strategy_runner.py --strategy 02

# Dry-run (preview trades, no execution)
python strategy_runner.py --dry-run

# Initialize all strategy accounts (first-time setup)
python strategy_runner.py --init

# Fetch and score KPI data for the full universe
python equity_kpi_analyzer.py --universe

# Refresh live quotes and mark holdings to market (no trading)
python update_quotes.py
python update_quotes.py --strategy 03

# Show leaderboard
python show_results.py

# View run logs from MySQL
python show_logs.py

# Validate system
python strategy_runner.py --validate

# Print news briefing
python strategy_runner.py --news-brief

# Test MySQL connection
python test_db_connect.py
```

### Deployment runners

```bash
# Linux (auto-detects pre/post-market window)
./run_daily.sh           # auto-detect
./run_daily.sh --news    # force pre-market news run
./run_daily.sh --full    # force post-market full run
./run_daily.sh --all     # run both stages sequentially

# Windows
run_daily.bat            # same flags apply
```

## Architecture

### Daily Run Stages

| Stage | When (ET) | What Runs |
|-------|-----------|-----------|
| Pre-market | 6:30 AM | Strategies 19 & 20 (news cache built) |
| Post-market | 4:30 PM | KPI analyzer + all strategies |

News cache from Stage 1 is reused in Stage 2 — Claude is called only once for macro analysis per day.

### Data Flow

1. **Universe** (`universe_manager.py`) — fetches S&P 500 constituents from Wikipedia, ranks by market cap, caches weekly. Default: top 200 stocks.
2. **KPI Analysis** (`equity_kpi_analyzer.py`) — scores each stock 0–100 across three tiers: Fundamentals 60% (margins, EPS, macro), Technicals 30% (RSI, golden/death cross, abnormal return), Sentiment 10% (analyst, insider). Writes to `equity_kpi_results` MySQL table.
3. **Quote Updates** (`update_quotes.py`) — marks all holdings to market using live yfinance prices, recomputes abnormal_return. No trading logic — purely data refresh.
4. **Strategy Execution** (`strategy_runner.py`) — for each active strategy: reads account/holdings/KPI from MySQL → runs strategy-specific `score_*()` function to rank candidates → calls Claude with the ranked list + strategy description → Claude returns buy/sell/hold JSON → trades are executed → leaderboard updated.
5. **News** (Strategies 19 & 20) — polls RSS feeds (Reuters, AP, CNBC, etc.), batches up to 60 headlines, calls Claude for regime/sector scoring, caches result in `news_macro_cache` table.

### MySQL Schema

- `accounts` — cash, total value, P&L per strategy
- `holdings` — open positions (ticker, shares, avg_cost, marked_price)
- `transactions` — full trade log
- `leaderboard` / `leaderboard_history` — daily ranked results
- `equity_kpi_results` — daily KPI scores per ticker
- `news_macro_cache` — daily cached news/macro analysis
- `run_logs` — structured event log (stage, strategy, level, message)
- `universe_cache` — cached S&P 500 constituent list

### Active Strategies (constants.py)

| ID | Name | Style |
|----|------|-------|
| 02 | Mean reversion | Contrarian (RSI < 38) |
| 03 | Value investing | Low P/E + high margin |
| 06 | Low volatility / defensive | Beta < 1.0, stable earnings |
| 07 | Earnings surprise (PEAD) | EPS growth + abnormal return |
| 08 | Dividend growth | High yield + growing payout |
| 09 | Insider buying | Net C-suite open-market buys |
| 10 | Macro-regime adaptive | VIX-driven aggressive/defensive switch |
| 11 | S&P 500 value tilt | Small-cap + low P/E |
| 12 | Momentum (AQR) | Golden cross + 5% below 52w high |
| 13 | Quality / profitability | High gross margin + low beta |
| 14 | Passive S&P 500 benchmark | Buy-and-hold, **no Claude calls** |
| 18 | Capex / semis thematic | IT sector + high margin + golden cross |
| 19 | News macro catalyst | RSS → Claude → sector scoring |
| 20 | News sentiment momentum | KPI gate + news sentiment overlay |
| 21 | Defense & war economy | Defense, energy, cybersecurity |

Strategy 14 is a pure code-driven benchmark — it never calls Claude.

### Key Design Patterns

- **Shared KPI table:** All strategies read the same `equity_kpi_results` table. KPI analyzer runs once per day.
- **Per-strategy isolation:** Each strategy has independent account, holdings, and transaction records.
- **Mark-to-market first:** Holdings are always updated to live prices before any trade decision.
- **Stop-loss enforcement:** 20% hard stop on all positions, enforced before trading logic runs.
- **Hold period gating:** Each strategy has a minimum holding period (e.g., mean reversion: 3 days, value: 10 days, dividend: 15 days), defined in `HOLD_PERIODS` in `constants.py`.
- **News caching:** Strategies 19 and 20 share one Claude news call per day via `news_macro_cache`.
- **UTF-8:** All scripts include `sys.stdout.reconfigure(encoding='utf-8')` for Windows compatibility.
- **Retry decorator:** API calls use exponential backoff.
- **Logging:** All events go to `run_logs` MySQL table; yfinance/peewee DEBUG chatter is silenced.

## Environment Variables

Loaded from `.env` (not committed):

```
ANTHROPIC_API_KEY    # Required
FRED_API_KEY         # Optional (macro data via fredapi)
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD  # MySQL connection
GIT_TOKEN            # GitHub token (CI/CD)
```

## Key Files

| File | Role |
|------|------|
| `strategy_runner.py` | Main orchestrator: `main()`, `score_*()` per strategy, `ask_claude()`, `update_leaderboard()` |
| `equity_kpi_analyzer.py` | Daily scoring engine; computes composite KPI score 0–100 per ticker |
| `universe_manager.py` | Universe curation; `get_universe()`, weekly cache, 50-ticker hardcoded fallback |
| `update_quotes.py` | Mark-to-market refresh; recomputes abnormal_return |
| `db.py` | All MySQL persistence (20+ public functions) |
| `db_logger.py` | `Logger` class writing to `run_logs` table |
| `constants.py` | `STRATEGIES` registry, `HOLD_PERIODS`, `MACRO_THEME_MAP` |
| `show_results.py` | Leaderboard viewer |
| `down_day_analyzer.py` | Research tool for identifying defensive stocks on down days |

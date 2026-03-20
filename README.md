# Dave's AI Trader

An automated paper trading system that runs 20 competing investment strategies daily, powered by Claude AI. Every weekday the system scores the S&P 500 universe, asks Claude to make buy/sell decisions for each strategy, and produces a leaderboard showing which approach is winning.

**Goal:** Start with $1,000 per strategy and see which one doubles first.

---

## Strategies

| ID | Name | Style | Risk |
|----|------|-------|------|
| 01 | Momentum / trend following | Momentum | High |
| 02 | Mean reversion (oversold bounce) | Contrarian | Med |
| 03 | Value investing | Value | Low |
| 04 | Quality growth (GARP) | Growth | Med |
| 05 | Sector rotation | Macro | Med |
| 06 | Low volatility / defensive | Defensive | Low |
| 07 | Earnings surprise (PEAD) | Event-driven | High |
| 08 | Dividend growth | Income | Low |
| 09 | Insider buying signal | Alt-data | Med |
| 10 | Macro-regime adaptive | Macro | Med |
| 11 | Large-cap value (Fama-French) | Academic | Med |
| 12 | Momentum (academic / Asness) | Academic | High |
| 13 | Quality / profitability (Novy-Marx) | Academic | Low |
| 14 | Passive S&P 500 benchmark | Passive | Low |
| 15 | Noise chaser *(degenerate baseline)* | Speculative | High |
| 16 | Estimate revision momentum | Growth | Med |
| 17 | High-beta quality growth | Growth | High |
| 18 | Capex beneficiary / semis | Thematic | High |
| 19 | News macro catalyst | Macro | Med |
| 20 | News sentiment momentum | Alt-data | Med |

Strategy 14 is a buy-and-hold passive baseline — if none of the active strategies beat it over time, that's a valuable finding in itself. Strategy 15 is a deliberately degenerate noise-chaser that serves as a lower bound; it demonstrates why chasing yesterday's price movers loses money.

Strategies 19 and 20 are news-driven. They fetch live RSS headlines each morning, ask Claude to identify the dominant macro theme (oil shock, deregulation, trade war, etc.), and score the universe by sector alignment. The analysis is cached once per day so there are no duplicate API calls.

---

## How it works

The pipeline runs in two stages each weekday:

```
Stage 1 — Pre-market (6:30 AM ET):
  1. strategy_runner.py --strategy 19   — news macro catalyst
  2. strategy_runner.py --strategy 20   — news sentiment momentum
     ↳ Fetches RSS headlines, calls Claude once, saves news_macro_cache.json

Stage 2 — Post-market (4:30 PM ET):
  1. equity_kpi_analyzer.py --universe  — scores top 200 S&P 500 stocks on 15+ KPIs
  2. strategy_runner.py                 — runs all 20 strategies, asks Claude for decisions
     ↳ Strategies 19 & 20 reuse the morning cache — no duplicate Claude call
  3. leaderboard.csv                    — updated with current rankings
  4. Results committed back to repo
```

Each strategy has its own isolated account:
- `account_XX.csv` — cash, holdings value, total
- `holdings_XX.csv` — current positions
- `transactions_XX.csv` — full trade ledger

---

## KPIs tracked

| Tier | Factor | KPI |
|------|--------|-----|
| 1 (60%) | Financial performance | Net profit margin, EPS growth, current ratio |
| 1 (60%) | Macro | GDP growth, CPI, Fed rate change |
| 1 (60%) | Sentiment | VIX |
| 2 (30%) | Technical | RSI-14, 50/200-day MA crossover, abnormal return |
| 2 (30%) | Sector | Relative sector performance |
| 3 (10%) | Alternative | Net insider transactions, analyst consensus |

Additional KPIs used by newer strategies: dividend yield, 5-year average dividend yield, beta, market cap, % from 52-week high, EPS revision delta.

---

## Files

| File | Purpose |
|------|---------|
| `equity_kpi_analyzer.py` | Daily KPI scorer for S&P 500 universe |
| `universe_manager.py` | Weekly refresh of S&P 500 constituent list |
| `strategy_runner.py` | Multi-strategy trading engine (all 20 strategies) |
| `news_strategy.py` | News infrastructure — RSS fetching, Claude macro analysis, caching |
| `ai_trader.py` | Core Claude API interaction and trade execution helpers |
| `leaderboard.html` | Dashboard — drag leaderboard.csv onto it |
| `leaderboard.csv` | Daily rankings output (auto-updated) |
| `account_XX.csv` | Per-strategy account snapshot |
| `holdings_XX.csv` | Per-strategy current positions |
| `transactions_XX.csv` | Per-strategy trade history |
| `news_macro_cache.json` | Daily news analysis cache (reused across Stage 1 → Stage 2) |
| `universe_cache.json` | Weekly S&P 500 constituent list cache |
| `run_daily.bat` | Windows local runner with pre/post-market stage detection |
| `.github/workflows/daily_trading.yml` | GitHub Actions two-stage schedule |

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/daves_ai_trader.git
cd daves_ai_trader
```

### 2. Install dependencies

```bash
pip install yfinance pandas fredapi ta requests python-dotenv
```

### 3. Add API keys

In your repo on GitHub — go to **Settings > Secrets and variables > Actions** and add:
- `ANTHROPIC_API_KEY` — from [console.anthropic.com](https://console.anthropic.com)
- `FRED_API_KEY` *(optional)* — from [fred.stlouisfed.org](https://fred.stlouisfed.org)

For local runs, create a `.env` file in the project root:

```
ANTHROPIC_API_KEY=your_key_here
FRED_API_KEY=your_key_here
```

### 4. Initialise account files

```bash
python strategy_runner.py --init
git add .
git commit -m "Initialise strategy accounts"
git push
```

### 5. That's it

GitHub Actions runs automatically — Stage 1 at 6:30 AM ET and Stage 2 at 4:30 PM ET on weekdays. Check the **Actions** tab in your repo to see each run. Results are committed back — pull them down any time to see the latest leaderboard.

---

## Viewing results

Open `leaderboard.html` in any browser and drag `leaderboard.csv` onto it. No server needed — it runs entirely locally.

---

## Running locally

**Linux / macOS:**

```bash
python equity_kpi_analyzer.py --universe   # update KPIs
python strategy_runner.py                  # run all 20 strategies
python strategy_runner.py --strategy 19    # single strategy
python strategy_runner.py --dry-run        # preview only
python strategy_runner.py --force-init     # reset everything
```

**Windows:**

```bat
run_daily.bat           :: auto-detects pre/post-market from system time
run_daily.bat --news    :: Stage 1 only (strategies 19 & 20)
run_daily.bat --full    :: Stage 2 only (full pipeline)
run_daily.bat --all     :: both stages back-to-back
```

---

## Trading rules

- Starting capital: **$1,000 per strategy**
- Commission: **$4.95 flat + 0.05% spread** per trade
- Max positions: **3 per strategy** (2 for strategy 18)
- Max single position: **60% of portfolio** (70% for strategy 18)
- Cash reserve: **5% minimum**
- Stop-loss: **20% below average cost**
- Sell rule: only sell if score drops significantly, stop-loss triggers, or a 10+ point better opportunity exists

---

## Tech stack

- **Python 3.11** · **yfinance** · **pandas** · **ta** · **requests** · **python-dotenv**
- **FRED API** — macroeconomic data
- **Claude API** (Anthropic) — trading decisions and macro news analysis
- **GitHub Actions** — two-stage daily scheduling

---

## Disclaimer

This is a paper trading system for educational and research purposes only. No real money is involved. Nothing here constitutes financial advice.

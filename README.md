# Dave's AI Trader

An automated paper trading system that runs 14 competing investment strategies daily, powered by Claude AI. Every weekday the system scores the S&P 500 universe, asks Claude to make buy/sell decisions for each strategy, and produces a leaderboard showing which approach is winning.

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
| 11 | Small-cap value (Fama-French) | Academic | Med |
| 12 | Momentum (academic / Asness) | Academic | High |
| 13 | Quality / profitability (Novy-Marx) | Academic | Low |
| 14 | Passive S&P 500 benchmark | Passive | Low |

Strategy 14 is a buy-and-hold passive baseline. If none of the active strategies beat it over time, that's a valuable finding in itself.

---

## How it works

```
Every weekday at 4:30 PM ET (GitHub Actions):
  1. equity_kpi_analyzer.py   — scores top 200 S&P 500 stocks on 15+ KPIs
  2. strategy_runner.py       — runs all 14 strategies, asks Claude for decisions
  3. leaderboard.csv          — updated with current rankings
  4. Results committed back   — leaderboard.html shows the results
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

---

## Files

| File | Purpose |
|------|---------|
| `equity_kpi_analyzer.py` | Daily KPI scorer for S&P 500 universe |
| `universe_manager.py` | Weekly refresh of S&P 500 constituent list |
| `strategy_runner.py` | Multi-strategy trading engine |
| `leaderboard.html` | Dashboard — drag leaderboard.csv onto it |
| `leaderboard.csv` | Daily rankings output (auto-updated) |
| `account_XX.csv` | Per-strategy account snapshot |
| `holdings_XX.csv` | Per-strategy current positions |
| `transactions_XX.csv` | Per-strategy trade history |
| `.github/workflows/daily_trading.yml` | GitHub Actions schedule |

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

### 3. Add API keys as GitHub Secrets

In your repo on GitHub:
- Go to **Settings > Secrets and variables > Actions**
- Add `ANTHROPIC_API_KEY` — from [console.anthropic.com](https://console.anthropic.com)
- Add `FRED_API_KEY` *(optional)* — from [fred.stlouisfed.org](https://fred.stlouisfed.org)

### 4. Initialise account files

```bash
python strategy_runner.py --init
git add .
git commit -m "Initialise strategy accounts"
git push
```

### 5. That's it

GitHub Actions will run automatically every weekday at 4:30 PM ET. Check the **Actions** tab in your repo to see each run. Results are committed back to the repo — pull them down any time to see the latest leaderboard.

---

## Viewing results

Open `leaderboard.html` in any browser and drag `leaderboard.csv` onto it. No server needed — it runs entirely locally.

---

## Running locally

```bash
# Full daily run
python equity_kpi_analyzer.py --universe
python strategy_runner.py

# Preview without trading
python strategy_runner.py --dry-run

# Single strategy only
python strategy_runner.py --strategy 03

# Reset everything
python strategy_runner.py --init --force-init
```

---

## Trading rules

- Starting capital: **$1,000 per strategy**
- Commission: **$4.95 flat + 0.05% spread** per trade
- Max positions: **3 per strategy**
- Max single position: **60% of portfolio**
- Cash reserve: **5% minimum**
- Stop-loss: **20% below average cost**
- Sell rule: only sell if score drops below 50, stop-loss triggered, or a 10+ point better opportunity exists

---

## Tech stack

- **Python 3.9+**
- **yfinance** — price and fundamental data
- **pandas** — data processing
- **ta** — technical indicators (RSI, moving averages)
- **FRED API** — macroeconomic data (GDP, CPI, Fed rate)
- **Claude API** (Anthropic) — trading decisions
- **GitHub Actions** — daily scheduling

---

## Disclaimer

This is a paper trading system for educational and research purposes only. No real money is involved. Nothing here constitutes financial advice.

Dave's AI Trader
An automated paper trading system that runs 20 competing investment strategies daily, powered by Claude AI. Every weekday the system scores the S&P 500 universe, asks Claude to make buy/sell decisions for each strategy, and produces a leaderboard showing which approach is winning.
Goal: Start with $1,000 per strategy and see which one doubles first.

Strategies
IDNameStyleRisk01Momentum / trend followingMomentumHigh02Mean reversion (oversold bounce)ContrarianMed03Value investingValueLow04Quality growth (GARP)GrowthMed05Sector rotationMacroMed06Low volatility / defensiveDefensiveLow07Earnings surprise (PEAD)Event-drivenHigh08Dividend growthIncomeLow09Insider buying signalAlt-dataMed10Macro-regime adaptiveMacroMed11Large-cap value (Fama-French)AcademicMed12Momentum (academic / Asness)AcademicHigh13Quality / profitability (Novy-Marx)AcademicLow14Passive S&P 500 benchmarkPassiveLow15Noise chaser (degenerate baseline)SpeculativeHigh16Estimate revision momentumGrowthMed17High-beta quality growthGrowthHigh18Capex beneficiary / semisThematicHigh19News macro catalystMacroMed20News sentiment momentumAlt-dataMed
Strategy 14 is a buy-and-hold passive baseline — if none of the active strategies beat it over time, that's a valuable finding in itself. Strategy 15 is a deliberately degenerate noise-chaser that serves as a lower bound; it demonstrates why chasing yesterday's price movers loses money.
Strategies 19 and 20 are news-driven. They fetch live RSS headlines each morning, ask Claude to identify the dominant macro theme (oil shock, deregulation, trade war, etc.), and score the universe by sector alignment. The analysis is cached once per day so there are no duplicate API calls.

How it works
The pipeline runs in two stages each weekday:
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
Each strategy has its own isolated account:

account_XX.csv — cash, holdings value, total
holdings_XX.csv — current positions
transactions_XX.csv — full trade ledger


KPIs tracked
TierFactorKPI1 (60%)Financial performanceNet profit margin, EPS growth, current ratio1 (60%)MacroGDP growth, CPI, Fed rate change1 (60%)SentimentVIX2 (30%)TechnicalRSI-14, 50/200-day MA crossover, abnormal return2 (30%)SectorRelative sector performance3 (10%)AlternativeNet insider transactions, analyst consensus
Additional KPIs used by newer strategies: dividend yield, 5-year average dividend yield, beta, market cap, % from 52-week high, EPS revision delta.

Files
FilePurposeequity_kpi_analyzer.pyDaily KPI scorer for S&P 500 universeuniverse_manager.pyWeekly refresh of S&P 500 constituent liststrategy_runner.pyMulti-strategy trading engine (all 20 strategies)news_strategy.pyNews infrastructure — RSS fetching, Claude macro analysis, cachingai_trader.pyCore Claude API interaction and trade execution helpersleaderboard.htmlDashboard — drag leaderboard.csv onto itleaderboard.csvDaily rankings output (auto-updated)account_XX.csvPer-strategy account snapshotholdings_XX.csvPer-strategy current positionstransactions_XX.csvPer-strategy trade historynews_macro_cache.jsonDaily news analysis cache (reused across Stage 1 → Stage 2)universe_cache.jsonWeekly S&P 500 constituent list cacherun_daily.batWindows local runner with pre/post-market stage detection.github/workflows/daily_trading.ymlGitHub Actions two-stage schedule

Setup
1. Clone the repo
bashgit clone https://github.com/YOUR_USERNAME/daves_ai_trader.git
cd daves_ai_trader
2. Install dependencies
bashpip install yfinance pandas fredapi ta requests python-dotenv
```

### 3. Add API keys as GitHub Secrets

In your repo on GitHub — go to **Settings > Secrets and variables > Actions** and add:
- `ANTHROPIC_API_KEY` — from [console.anthropic.com](https://console.anthropic.com)
- `FRED_API_KEY` *(optional)* — from [fred.stlouisfed.org](https://fred.stlouisfed.org)

For local runs, create a `.env` file in the project root:
```
ANTHROPIC_API_KEY=your_key_here
FRED_API_KEY=your_key_here
4. Initialise account files
bashpython strategy_runner.py --init
git add .
git commit -m "Initialise strategy accounts"
git push
5. That's it
GitHub Actions runs automatically — Stage 1 at 6:30 AM ET and Stage 2 at 4:30 PM ET on weekdays. Results are committed back to the repo; pull them down any time to see the latest leaderboard.

Viewing results
Open leaderboard.html in any browser and drag leaderboard.csv onto it. No server needed.

Running locally
Linux / macOS:
bashpython equity_kpi_analyzer.py --universe  # update KPIs
python strategy_runner.py                 # run all 20 strategies
python strategy_runner.py --strategy 19   # single strategy
python strategy_runner.py --dry-run       # preview only
python strategy_runner.py --force-init    # reset everything
Windows:
batrun_daily.bat           :: auto-detects pre/post-market from system time
run_daily.bat --news    :: Stage 1 only (strategies 19 & 20)
run_daily.bat --full    :: Stage 2 only (full pipeline)
run_daily.bat --all     :: both stages back-to-back

Trading rules

Starting capital: $1,000 per strategy
Commission: $4.95 flat + 0.05% spread per trade
Max positions: 3 per strategy (2 for strategy 18)
Max single position: 60% of portfolio (70% for strategy 18)
Cash reserve: 5% minimum
Stop-loss: 20% below average cost
Sell rule: only sell if score drops significantly, stop-loss triggers, or a 10+ point better opportunity exists


Tech stack

Python 3.11 · yfinance · pandas · ta · requests · python-dotenv
FRED API — macroeconomic data
Claude API (Anthropic) — trading decisions and macro news analysis
GitHub Actions — two-stage daily scheduling
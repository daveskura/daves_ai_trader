"""
strategy_runner.py  —  Multi-strategy paper trading engine
Runs 13 active strategies + 1 passive benchmark concurrently.
Each strategy has its own account, holdings, and transactions CSV.
Produces a daily leaderboard CSV for the dashboard.

Usage:
    python strategy_runner.py                  # run all strategies
    python strategy_runner.py --dry-run        # preview decisions only
    python strategy_runner.py --strategy 02    # run one strategy only
    python strategy_runner.py --init           # initialise all account files fresh

Active strategies: 02, 03, 06, 07, 08, 09, 10, 11, 12, 13, 14, 18, 19, 20, 21

Changes vs original:
    FIX-1  Model string updated to claude-sonnet-4-6
    FIX-2  buy()  — holdings_value after purchase uses current market prices for
                    ALL positions, not a hybrid of market price + stale cost basis
    FIX-3  sell() — holdings_value after sale uses current market prices for
                    remaining positions, not stale cost basis
    FIX-4  Strategy 14 (passive) no longer calls Claude; buy-and-hold executed
                    entirely in code — no wasted API tokens
    FIX-5  Strategy 11 (small-cap value) cap threshold replaced with dynamic
                    bottom-third-of-universe threshold so it has actual candidates
    FIX-6  max_tokens raised from 600 to 1200; truncated responses are logged
    FIX-7  score_quality_growth: skip stocks with negative trailing EPS
    FIX-8  score_academic_momentum: genuine crash-protection pullback filter —
                    stock must be >= 5% below 52-week high
    FIX-9  score_dividend_growth: uses actual dividend_yield from KPI data
                    when available, falls back to proxy otherwise
    FIX-10 score_earnings_surprise: PEAD threshold tightened — abnormal_return
                    raised from 0.5% to 2% and min fwd EPS growth raised to 20%
                    to filter genuine post-earnings drift from daily noise
    RETIRED Strategy 01 (Momentum/trend) — overlaps with S12 (Asness academic
                    momentum) which is strictly better: same signal + crash
                    protection. Running both wastes tokens on identical picks.
    RETIRED Strategy 04 (Quality growth / GARP) — overlaps with S13 (Novy-Marx
                    quality/profitability). S13 has tighter academic grounding;
                    S04 was a looser duplicate.
    RETIRED Strategy 05 (Sector rotation) — picks the strongest sector by avg
                    composite score but has no individual stock filter, producing
                    a noisy, undifferentiated candidate list. Low signal/noise.
    RETIRED Strategy 15 (Noise chaser) — intentional degenerate baseline that
                    buys yesterday's biggest movers with no fundamental filter.
                    Useful as a concept; wastes real API tokens to demonstrate
                    the obvious. Removed entirely.
    RETIRED Strategy 16 (Estimate revision momentum) — the eps_revision_pct
                    delta column is rarely populated in practice (requires two
                    consecutive KPI runs with differing analyst data). In the
                    absence of real revision data the fallback collapses into a
                    weaker clone of S13. Removed until the KPI analyser can
                    reliably supply the delta field.
    RETIRED Strategy 17 (High-beta quality growth) — high-beta (1.5-2.5)
                    filter is extremely restrictive in the S&P 500 universe;
                    most candidates that pass are also caught by S18 (capex
                    beneficiary / semis) with a stronger sector thesis. Duplicate
                    coverage with extra volatility and no incremental edge.
    NEW-19 Strategy 19 "News macro catalyst" — fetches RSS headlines daily, asks
                    Claude to identify dominant macro themes, scores universe stocks
                    by sector alignment with those themes. Cache in news_macro_cache.json
                    so Claude is called only once per day regardless of run count.
    NEW-20 Strategy 20 "News sentiment momentum" — overlays today's news sentiment
                    on top of KPI composite scores. Requires minimum quality gate
                    (CS > 40, positive EPS) then amplifies signal with macro sector
                    tailwinds and company-specific catalysts from headlines.
    NEW-21 Strategy 21 "Defense & war economy" — targets defense primes, energy,
                    and cybersecurity stocks that benefit from sustained US military
                    spending. Scores on: sector membership, net margin, EPS growth,
                    golden cross, and a backlog/contract proxy via abnormal return.
"""

import sys, os, re, json, csv, math, argparse, urllib.error, urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from collections import defaultdict

_urllib_req = urllib.request   # alias used by news infrastructure

# ── UTF-8 output (Windows fix) ──────────────────────────────────────────────
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# ── Load .env if present ────────────────────────────────────────────────────
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            v = v.strip().strip('"').strip("'")   # remove surrounding quotes
            os.environ.setdefault(k.strip(), v)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL             = "claude-sonnet-4-6"          # FIX-1: corrected model string
STARTING_CASH     = 1000.00
COMMISSION        = 4.95          # flat per trade
SPREAD_PCT        = 0.0005        # 0.05% spread
ACCOUNT_NUM       = "123456789"
BASE_DIR          = Path(__file__).parent

# ── Strategy definitions ─────────────────────────────────────────────────────
STRATEGIES = [
    # id,  name,                          style,         risk,   description
    ("02", "Mean reversion",               "contrarian",  "med",
     "Buy oversold stocks (RSI < 38). Sell when RSI recovers above 50."),
    ("03", "Value investing",              "value",       "low",
     "Buy fundamentally cheap stocks: low P/E, low P/B. Hold until fair value."),
    ("06", "Low volatility / defensive",   "defensive",   "low",
     "Lowest beta stocks with stable earnings. Hold through high-VIX environments."),
    ("07", "Earnings surprise (PEAD)",     "event",       "high",
     "Buy profitable stocks with strong forward EPS growth AND a positive abnormal return today — a proxy for post-earnings drift."),
    ("08", "Dividend growth",              "income",      "low",
     "Companies with strong dividend yield and growing payout. Uses actual yield data when available."),
    ("09", "Insider buying signal",        "alt-data",    "med",
     "Follow C-suite open-market purchases. Strong predictor of 6-12 month outperformance."),
    ("10", "Macro-regime adaptive",        "macro",       "med",
     "Switch between aggressive and defensive posture based on VIX, GDP trend, Fed direction."),
    ("11", "Large-cap value (Fama-French)","academic",    "med",
     "Fama-French value factor within S&P 500: bottom third by market cap in universe + low P/E + high margin."),  # FIX-5
    ("12", "Momentum (academic / Asness)", "academic",    "high",
     "Cliff Asness AQR-style momentum: golden cross + RSI not overbought + must be >=5% below 52w high (crash protection)."),  # FIX-8
    ("13", "Quality / profitability",      "academic",    "low",
     "Novy-Marx gross profitability factor: high gross margin, low debt, stable ROE."),
    ("14", "Passive S&P 500 benchmark",    "passive",     "low",
     "Buy and hold the top market-cap stocks. No active trading. No Claude call. Reality-check baseline."),
    ("18", "Capex beneficiary / semis",    "thematic",    "high",  # NEW-18
     "Semiconductor and hardware infrastructure stocks that benefit when hyperscaler capex "
     "accelerates. Scores on: high net margin, high EPS growth, golden cross, and "
     "Information Technology sector membership. Targets picks like NVDA before they run."),
    ("19", "News macro catalyst",          "macro",       "med",   # NEW-19
     "Fetches today's financial & world headlines via RSS, asks Claude to identify the "
     "dominant macro theme (oil shock, deregulation, trade war, etc.), then scores universe "
     "stocks by sector alignment. Cached once per day — no duplicate API calls."),
    ("20", "News sentiment momentum",      "alt-data",    "med",   # NEW-20
     "Overlays today's news sentiment on KPI composite scores. Requires CS > 40 and positive "
     "EPS as a quality gate, then amplifies with macro sector tailwinds and company-specific "
     "catalysts (earnings beats, scandals, upgrades) found in today's headlines."),
    ("21", "Defense & war economy",        "thematic",    "med",   # NEW-21
     "Targets defense primes (LMT, RTX, NOC, GD, LHX), energy majors, and cybersecurity "
     "stocks that benefit from sustained US military spending in the Middle East. "
     "Scores on: sector/sub-industry membership, net margin, EPS growth, golden cross, "
     "and contract-flow proxy via abnormal return. Hold with conviction — defense budgets "
     "are multi-year commitments. Sell only on death cross or margin collapse."),
]

# ── File helpers ─────────────────────────────────────────────────────────────

def acct_file(sid):   return BASE_DIR / f"account_{sid}.csv"
def hold_file(sid):   return BASE_DIR / f"holdings_{sid}.csv"
def txn_file(sid):    return BASE_DIR / f"transactions_{sid}.csv"
def leader_file():    return BASE_DIR / "leaderboard.csv"

def read_csv(path):
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def read_account(sid):
    rows = read_csv(acct_file(sid))
    if not rows:
        return {"account": ACCOUNT_NUM, "strategy_id": sid,
                "cash": STARTING_CASH, "holdings_value": 0.0, "total": STARTING_CASH,
                "start_date": date.today().isoformat(), "trades": 0}
    r = rows[0]
    r["cash"]           = float(r["cash"])
    r["holdings_value"] = float(r["holdings_value"])
    r["total"]          = float(r["total"])
    r["trades"]         = int(r.get("trades", 0))
    return r

def save_account(sid, acct):
    write_csv(acct_file(sid), [acct],
              ["account","strategy_id","cash","holdings_value","total","start_date","trades"])

def read_holdings(sid):
    rows = read_csv(hold_file(sid))
    for r in rows:
        r["shares"]    = float(r["shares"])
        r["avg_cost"]  = float(r["avg_cost"])
        r["cost_basis"]= float(r["cost_basis"])
    return rows

def save_holdings(sid, holdings):
    write_csv(hold_file(sid), holdings,
              ["ticker","shares","avg_cost","cost_basis","purchase_date","strategy_id"])

def append_txn(sid, txn):
    path = txn_file(sid)
    fieldnames = ["date","strategy_id","action","ticker","shares","price",
                  "commission","net_amount","cash_after","reason"]
    write_header = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
        w.writerow(txn)

# ── Holdings value helper ─────────────────────────────────────────────────────

def calc_holdings_value(holdings, kmap):
    """
    FIX-2 / FIX-3: Compute total holdings market value using current prices for
    ALL positions. Falls back to avg_cost if a ticker isn't in kmap.
    """
    total = 0.0
    for h in holdings:
        price = kmap.get(h["ticker"], {}).get("current_price") or float(h["avg_cost"])
        total += h["shares"] * price
    return round(total, 2)

# ── KPI data loader ───────────────────────────────────────────────────────────

def load_kpi(kpi_path="equity_kpi_results.csv"):
    path = BASE_DIR / kpi_path
    if not path.exists():
        print(f"  [WARN] KPI file not found: {path}")
        return [], {}
    rows = read_csv(path)
    kmap = {}
    for r in rows:
        for col in ["composite_score","tier1_score","tier2_score","tier3_score",
                    "net_profit_margin","eps_growth_fwd","eps_ttm","eps_forward",
                    "pe_ratio","current_price","rsi_14","ma_50","ma_200","beta",
                    "market_cap","pct_from_52w_high","abnormal_return",
                    "net_insider_shares","vix",
                    "dividend_yield","five_year_avg_dividend_yield",
                    "eps_revision_pct"]:   # NEW-16: revision delta
            if col in r:
                try:    r[col] = float(r[col])
                except: r[col] = 0.0
        kmap[r["ticker"]] = r
    return rows, kmap

# ── Strategy scoring functions ────────────────────────────────────────────────
# Each returns a sorted list of (ticker, score, reason) tuples.

def score_mean_reversion(rows, kmap):
    """Buy oversold (RSI < 38). The lower the RSI the better."""
    out = []
    for r in rows:
        rsi = r.get("rsi_14", 50)
        if rsi > 38: continue
        score = (40 - rsi) * 2 + r.get("composite_score", 0) * 0.3
        out.append((r["ticker"], round(score, 2), f"Oversold RSI={rsi:.1f}"))
    return sorted(out, key=lambda x: -x[1])

def score_value(rows, kmap):
    """Low P/E + high profit margin."""
    out = []
    for r in rows:
        pe  = r.get("pe_ratio", 999)
        npm = r.get("net_profit_margin", 0)
        if pe <= 0 or pe > 25: continue
        score = (25 - pe) * 2 + npm * 100
        out.append((r["ticker"], round(score, 2), f"P/E={pe:.1f} margin={npm*100:.1f}%"))
    return sorted(out, key=lambda x: -x[1])

def score_low_volatility(rows, kmap):
    """Lowest beta + decent composite score."""
    out = []
    for r in rows:
        beta = r.get("beta", 1)
        if beta <= 0 or beta > 1.0: continue
        score = (1.5 - beta) * 40 + r.get("composite_score", 0) * 0.4
        out.append((r["ticker"], round(score, 2), f"Low beta={beta:.2f}"))
    return sorted(out, key=lambda x: -x[1])

def score_earnings_surprise(rows, kmap):
    """
    PEAD proxy: requires BOTH strong forward EPS growth AND a meaningful positive
    abnormal return today (market reacting to an earnings beat).
    FIX-10: tightened thresholds — abnormal_return raised from 0.5% to 2% and
    min fwd EPS growth raised from 10% to 20%. A 0.5% daily move is routine noise;
    2%+ on a stock with 20%+ forward growth is a genuine PEAD signal. Reduces
    false positives and commission drag from churning on noise days.
    """
    out = []
    for r in rows:
        eps_ttm = r.get("eps_ttm", 0) or 0
        eps_g   = r.get("eps_growth_fwd", 0)
        ab      = r.get("abnormal_return", 0)
        # profitable company + strong forward growth + meaningful market reaction
        if eps_ttm <= 0: continue
        if eps_g < 0.20: continue   # FIX-10: raised from 0.10
        if ab <= 0.02:   continue   # FIX-10: raised from 0.005 (0.5% → 2%)
        score = eps_g * 150 + ab * 200 + r.get("composite_score", 0) * 0.2
        out.append((r["ticker"], round(score, 2),
                    f"PEAD: fwd_growth={eps_g*100:.1f}% ab_ret={ab*100:.2f}%"))
    return sorted(out, key=lambda x: -x[1])

def score_dividend_growth(rows, kmap):
    """
    FIX-9: Use actual dividend_yield and five_year_avg_dividend_yield when
    present in the KPI file (requires updated equity_kpi_analyzer).
    Falls back to the original low-beta/low-PE proxy if data is absent.
    """
    out = []
    for r in rows:
        beta = r.get("beta", 1)
        pe   = r.get("pe_ratio", 999)
        npm  = r.get("net_profit_margin", 0)
        dy   = r.get("dividend_yield", 0) or 0
        dy5  = r.get("five_year_avg_dividend_yield", 0) or 0

        if beta > 1.2: continue

        if dy > 0:
            # Real dividend path — reward high yield, growing payout, reasonable P/E
            pe_pen = max(0, pe - 35) * 0.3 if pe > 0 else 0
            score  = dy * 400 + max(0, dy - dy5) * 200 + npm * 40 - pe_pen
            reason = f"Div yield={dy*100:.2f}% 5yr={dy5*100:.2f}% beta={beta:.2f}"
        else:
            # Proxy path (when dividend data unavailable)
            if pe <= 0 or pe > 30: continue
            score  = (1.5 - beta) * 20 + (30 - pe) + npm * 60
            reason = f"Div proxy: beta={beta:.2f} P/E={pe:.1f}"

        out.append((r["ticker"], round(score, 2), reason))
    return sorted(out, key=lambda x: -x[1])

def score_insider_buying(rows, kmap):
    """Positive net insider shares = confidence signal."""
    out = []
    for r in rows:
        ins = r.get("net_insider_shares", 0)
        if ins <= 0: continue
        score = math.log1p(ins) * 5 + r.get("composite_score", 0) * 0.4
        out.append((r["ticker"], round(score, 2), f"Net insider buy={int(ins):,} shares"))
    return sorted(out, key=lambda x: -x[1])

def score_macro_adaptive(rows, kmap):
    """Switch between aggressive (low VIX) and defensive (high VIX)."""
    vix = rows[0].get("vix", 20) if rows else 20
    out = []
    for r in rows:
        beta = r.get("beta", 1)
        cs   = r.get("composite_score", 0)
        if vix > 25:
            if beta > 1.0: continue
            score = cs * 0.5 + (1.5 - beta) * 30
            label = f"Defensive (VIX={vix:.1f})"
        else:
            score = cs * 0.8 + r.get("abnormal_return", 0) * 100
            label = f"Aggressive (VIX={vix:.1f})"
        out.append((r["ticker"], round(score, 2), label))
    return sorted(out, key=lambda x: -x[1])

def score_large_cap_value(rows, kmap):
    """
    FIX-5: Original $10B cap threshold excluded every S&P 500 stock.
    Now dynamically computes the bottom-third market cap within the loaded
    universe so there are always real candidates. Low P/E + high margin filter
    applies the Fama-French value tilt to the relatively smaller large-caps.
    """
    caps = sorted([r.get("market_cap", 0) for r in rows if r.get("market_cap", 0) > 0])
    if not caps:
        return []
    cap_threshold = caps[len(caps) // 3]   # bottom third of universe by market cap

    out = []
    for r in rows:
        cap = r.get("market_cap", 1e15)
        pe  = r.get("pe_ratio", 999)
        npm = r.get("net_profit_margin", 0)
        if cap > cap_threshold: continue
        if pe <= 0 or pe > 20: continue
        score = (cap_threshold - cap) / 1e9 * 0.01 + (20 - pe) * 2 + npm * 50
        out.append((r["ticker"], round(score, 2),
                    f"Value tilt: cap=${cap/1e9:.0f}B P/E={pe:.1f} margin={npm*100:.1f}%"))
    return sorted(out, key=lambda x: -x[1])

def score_academic_momentum(rows, kmap):
    """
    FIX-8: Asness-style momentum with genuine crash-protection pullback filter.
    Requires the stock to be AT LEAST 5% below its 52-week high (pct_from_52w_high
    <= -0.05). This ensures we buy momentum with upside room, not stocks already
    at the top. Differentiates S12 from S01 which has no pullback requirement.
    """
    out = []
    for r in rows:
        cs            = r.get("composite_score", 0)
        rsi           = r.get("rsi_14", 50)
        beta          = r.get("beta", 1)
        pct_from_high = r.get("pct_from_52w_high", 0)  # negative = below 52w high

        if r.get("ma_signal","") != "BULLISH (Golden Cross)": continue
        if rsi > 75:           continue   # skip overbought
        if beta > 1.8:         continue   # skip extreme beta
        if pct_from_high > -0.05: continue  # FIX-8: must be >=5% below 52w high

        score = cs * 0.6 + (100 - rsi) * 0.2 + r.get("abnormal_return", 0) * 100
        out.append((r["ticker"], round(score, 2),
                    f"Asness: RSI={rsi:.1f} beta={beta:.2f} from52wh={pct_from_high*100:.1f}%"))
    return sorted(out, key=lambda x: -x[1])

def score_quality_profitability(rows, kmap):
    """Novy-Marx: high gross margin + low beta + positive EPS growth."""
    out = []
    for r in rows:
        npm  = r.get("net_profit_margin", 0)
        beta = r.get("beta", 1)
        eps_g= r.get("eps_growth_fwd", 0)
        if npm < 0.15: continue
        if beta > 1.3: continue
        score = npm * 100 + (1.5 - beta) * 20 + eps_g * 30
        out.append((r["ticker"], round(score, 2),
                    f"Quality: margin={npm*100:.1f}% beta={beta:.2f}"))
    return sorted(out, key=lambda x: -x[1])

def score_passive(rows, kmap):
    """Passive benchmark: rank by market cap for buy-and-hold selection.
    FIX-4: run_strategy short-circuits for sid==14 and never calls Claude.
    """
    out = []
    for r in rows:
        cap = r.get("market_cap", 0)
        out.append((r["ticker"], cap, "Passive hold: largest market cap"))
    return sorted(out, key=lambda x: -x[1])

def score_capex_beneficiary(rows, kmap):
    """
    NEW-18: Capex Beneficiary / Semiconductor Infrastructure.
    Targets IT-sector stocks (semis, hardware, cloud infra) that benefit from
    hyperscaler data-centre buildout.  Scoring heavily weights:
      - Net margin (semis have exceptional margins at scale: NVDA ~55%)
      - Forward EPS growth (capex waves flow through to earnings)
      - Golden cross (institutional accumulation already underway)
      - Positive abnormal return (market is beginning to price it in)
    P/E guard is deliberately relaxed (≤ 80) because structural growers trade
    at premium multiples before the market fully understands the thesis.
    """
    CAPEX_SECTORS = {
        "Information Technology",
        "Communication Services",   # hyperscalers: GOOGL, META
    }
    CAPEX_KEYWORDS = {   # sub-industry / name hints for hardware/semi companies
        "Semiconductor", "semiconductor",
        "Hardware", "hardware",
        "Electronic", "electronic",
        "Network", "network",
        "Storage", "storage",
        "Circuit", "circuit",
    }

    out = []
    for r in rows:
        sector  = r.get("sector", "") or ""
        name    = r.get("ticker", "")        # we use ticker as a crude proxy here
        eps_ttm = r.get("eps_ttm", 0) or 0
        eps_fwd = r.get("eps_growth_fwd", 0) or 0
        npm     = r.get("net_profit_margin", 0) or 0
        pe      = r.get("pe_ratio", 999) or 999
        ma_sig  = r.get("ma_signal", "")
        ab      = r.get("abnormal_return", 0) or 0
        cs      = r.get("composite_score", 0)
        beta    = r.get("beta", 1.0) or 1.0
        rsi     = r.get("rsi_14", 50) or 50

        if sector not in CAPEX_SECTORS:      continue
        if eps_ttm <= 0:                     continue   # profitable only
        if eps_fwd < 0.08:                   continue   # needs growth
        if npm < 0.12:                       continue   # decent margins
        if pe > 80 or pe <= 0:               continue   # valuation sanity check
        if "BULLISH" not in ma_sig:          continue   # uptrend required
        if rsi > 82:                         continue   # skip parabolic

        # Core score: margin quality + growth + trend confirmation
        score = (npm * 120 +               # margin quality (NVDA ~55% → huge weight)
                 eps_fwd * 100 +           # forward growth acceleration
                 ab * 150 +                # market beginning to price it in
                 cs * 0.25 +              # composite health
                 min(beta, 2.0) * 8)      # beta bonus (up to 2.0)

        reason = (f"Capex beneficiary: sector={sector}  "
                  f"margin={npm*100:.1f}%  fwd_growth={eps_fwd*100:.1f}%  "
                  f"P/E={pe:.1f}  beta={beta:.2f}")
        out.append((r["ticker"], round(score, 2), reason))
    return sorted(out, key=lambda x: -x[1])

# ── News strategy infrastructure (strategies 19 & 20) ────────────────────────
# RSS news sources — all free, no API key required
NEWS_FEEDS = [
    ("Reuters Business",  "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters Markets",   "https://feeds.reuters.com/reuters/financialsNews"),
    ("Reuters World",     "https://feeds.reuters.com/Reuters/worldNews"),
    ("AP Business",       "https://feeds.apnews.com/rss/apf-business"),
    ("AP Top News",       "https://feeds.apnews.com/rss/apf-topnews"),
    ("Yahoo Finance",     "https://finance.yahoo.com/news/rssindex"),
    ("CNBC Economy",      "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("MarketWatch",       "https://feeds.marketwatch.com/marketwatch/topstories/"),
]

# Macro theme id → (beneficiary sectors, loser sectors) — used alongside Claude's output
MACRO_THEME_MAP = {
    "oil_supply_shock":         (["Energy","Industrials"],                         ["Consumer Discretionary","Consumer Staples","Utilities"]),
    "oil_demand_drop":          (["Consumer Discretionary","Consumer Staples"],    ["Energy"]),
    "rate_hike":                (["Financial Services"],                           ["Real Estate","Utilities","Consumer Discretionary"]),
    "rate_cut":                 (["Real Estate","Utilities","Technology"],         ["Financial Services"]),
    "inflation_surge":          (["Energy","Materials","Consumer Staples"],        ["Technology","Consumer Discretionary","Real Estate"]),
    "china_trade_tension":      (["Industrials","Energy","Materials"],             ["Technology","Consumer Discretionary"]),
    "geopolitical_conflict":    (["Energy","Industrials","Healthcare"],            ["Consumer Discretionary","Communication Services"]),
    "bank_deregulation":        (["Financial Services"],                           []),
    "bank_regulation":          ([],                                               ["Financial Services"]),
    "ai_investment_boom":       (["Technology","Communication Services"],          []),
    "tech_selloff":             (["Energy","Consumer Staples","Healthcare"],       ["Technology","Communication Services"]),
    "recession_fear":           (["Consumer Staples","Healthcare","Utilities"],    ["Consumer Discretionary","Industrials","Technology"]),
    "strong_jobs":              (["Consumer Discretionary","Financial Services"],  ["Real Estate","Utilities"]),
    "weak_jobs":                (["Real Estate","Utilities","Technology"],         ["Financial Services","Industrials"]),
    "consumer_confidence_drop": (["Consumer Staples","Healthcare","Utilities"],    ["Consumer Discretionary"]),
    "dollar_strength":          (["Financial Services","Consumer Staples"],        ["Technology","Industrials","Energy"]),
    "dollar_weakness":          (["Technology","Industrials","Energy","Materials"],[]),
}

NEWS_CACHE_FILE = BASE_DIR / "news_macro_cache.json"


def _fetch_rss(url: str, timeout: int = 8) -> list[dict]:
    """Fetch an RSS feed and return list of {title, summary}. No external deps."""
    try:
        req = _urllib_req.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with _urllib_req.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        items = []
        titles = re.findall(r"<title[^>]*><!\[CDATA\[(.*?)\]\]></title>", raw, re.DOTALL)
        if not titles:
            titles = re.findall(r"<title[^>]*>(.*?)</title>", raw, re.DOTALL)
        descs = re.findall(r"<description[^>]*><!\[CDATA\[(.*?)\]\]></description>", raw, re.DOTALL)
        if not descs:
            descs = re.findall(r"<description[^>]*>(.*?)</description>", raw, re.DOTALL)
        for i, t in enumerate(titles[1:16]):
            clean = re.sub(r"<[^>]+>", "", t).strip()
            desc  = re.sub(r"<[^>]+>", "", descs[i] if i < len(descs) else "").strip()[:250]
            if clean:
                items.append({"title": clean, "summary": desc})
        return items
    except Exception:
        return []


def _gather_headlines(verbose: bool = True) -> list[dict]:
    """Fetch and deduplicate headlines from all NEWS_FEEDS."""
    if verbose:
        print("  [NEWS] Fetching headlines...", end=" ", flush=True)
    all_items, seen = [], set()
    for name, url in NEWS_FEEDS:
        for item in _fetch_rss(url):
            key = item["title"].lower()[:60]
            if key and key not in seen:
                seen.add(key)
                item["source"] = name
                all_items.append(item)
    if verbose:
        print(f"{len(all_items)} unique headlines from {len(NEWS_FEEDS)} sources")
    return all_items


def _load_news_cache() -> dict | None:
    """Return today's cached macro analysis, or None if stale/missing."""
    if not NEWS_CACHE_FILE.exists():
        return None
    try:
        data = json.loads(NEWS_CACHE_FILE.read_text(encoding="utf-8"))
        if data.get("analysis_date") == date.today().isoformat():
            return data
    except Exception:
        pass
    return None


def _save_news_cache(data: dict):
    try:
        NEWS_CACHE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"  [NEWS] Warning: could not save cache — {e}")


def get_news_macro_analysis(force_refresh: bool = False, verbose: bool = True) -> dict | None:
    """
    Return today's macro analysis dict, cached after first call each day.
    Fetches RSS headlines then asks Claude once to identify themes & catalysts.
    Saves result to news_macro_cache.json so all subsequent strategy runs reuse it.
    """
    if not force_refresh:
        cached = _load_news_cache()
        if cached:
            if verbose:
                print(f"  [NEWS] Using cached analysis: "
                      f"regime={cached.get('market_regime','?')}  "
                      f"themes={len(cached.get('dominant_themes',[]))}")
            return cached

    headlines = _gather_headlines(verbose=verbose)
    if not headlines:
        print("  [NEWS] No headlines fetched — strategies 19/20 skipped today")
        return None

    headline_text = "\n".join(
        f"[{h.get('source','')}] {h['title']} — {h.get('summary','')[:150]}"
        for h in headlines[:60]
    )

    prompt = f"""Today is {date.today().isoformat()}.

Here are today's top financial and world news headlines:

{headline_text}

Analyze these headlines and return ONLY valid JSON (no preamble, no markdown):

{{
  "analysis_date": "{date.today().isoformat()}",
  "market_regime": "RISK-ON or RISK-OFF or NEUTRAL",
  "regime_confidence": 7,
  "regime_reasoning": "2-3 sentence explanation of overall market tone",
  "dominant_themes": [
    {{
      "theme_id": "geopolitical_conflict",
      "theme_name": "Human readable name",
      "description": "2-3 sentences on why this matters today",
      "strength": 8,
      "duration_outlook": "1-3 days or 1-2 weeks or 1-3 months or structural",
      "beneficiary_sectors": ["Energy"],
      "loser_sectors": ["Consumer Discretionary"],
      "supporting_headlines": ["headline 1"]
    }}
  ],
  "company_catalysts": [
    {{
      "ticker": "FDX",
      "catalyst_type": "earnings_beat or earnings_miss or upgrade or downgrade or guidance_up or guidance_down or ma or scandal or regulatory or other",
      "sentiment": "positive or negative or neutral",
      "magnitude": 8,
      "description": "What happened in one sentence"
    }}
  ],
  "macro_risk_factors": ["Risk factor 1"],
  "trade_ideas": [
    {{
      "idea": "Short description",
      "rationale": "Why",
      "sectors": ["Energy"],
      "direction": "long or short",
      "timeframe": "today or week or month"
    }}
  ]
}}

Only include themes genuinely visible in today's headlines. Do not invent news.
1-3 dominant themes max. company_catalysts only for tickers explicitly named."""

    if verbose:
        print("  [NEWS] Asking Claude to identify macro themes...", end=" ", flush=True)

    payload = json.dumps({
        "model": MODEL, "max_tokens": 2000,
        "system": "You are a senior macro strategist. Respond only with valid JSON.",
        "messages": [{"role": "user", "content": prompt}]
    }).encode("utf-8")

    req = _urllib_req.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={"Content-Type": "application/json",
                 "x-api-key": ANTHROPIC_API_KEY,
                 "anthropic-version": "2023-06-01"},
        method="POST",
    )
    try:
        with _urllib_req.urlopen(req, timeout=90) as resp:
            resp_data = json.loads(resp.read().decode("utf-8"))
        text = resp_data["content"][0]["text"].strip()
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
        analysis = json.loads(text)
        analysis["headline_count"] = len(headlines)
        analysis["fetched_at"] = datetime.now(timezone.utc).isoformat()
        _save_news_cache(analysis)
        if verbose:
            print(f"done  (regime={analysis.get('market_regime','?')}  "
                  f"themes={len(analysis.get('dominant_themes',[]))}  "
                  f"catalysts={len(analysis.get('company_catalysts',[]))})")
        return analysis
    except json.JSONDecodeError as e:
        print(f"\n  [NEWS] JSON parse error: {e}")
        return None
    except Exception as e:
        print(f"\n  [NEWS] API error: {e}")
        return None


def score_news_macro(rows: list, kmap: dict, macro: dict) -> list[tuple]:
    """
    Strategy 19 — News Macro Catalyst scorer.
    Scores each stock by sector alignment with today's dominant macro themes.
    Theme strength, duration, and market regime all adjust the final score.
    """
    if not macro or not macro.get("dominant_themes"):
        return []

    regime = macro.get("market_regime", "NEUTRAL")
    sector_scores: dict[str, float] = defaultdict(float)
    sector_labels: dict[str, list]  = defaultdict(list)

    for theme in macro.get("dominant_themes", []):
        strength = theme.get("strength", 5) / 10.0
        dur_mult = {"1-3 days": 0.8, "1-2 weeks": 1.0,
                    "1-3 months": 1.2, "structural": 1.5}.get(
                        theme.get("duration_outlook", "1-2 weeks"), 1.0)

        # Merge Claude's sectors with our built-in theme map for extra coverage
        builtin  = MACRO_THEME_MAP.get(theme.get("theme_id", ""), ([], []))
        ben_secs = list(set(theme.get("beneficiary_sectors", []) + builtin[0]))
        los_secs = list(set(theme.get("loser_sectors", []) + builtin[1]))

        for sec in ben_secs:
            sector_scores[sec] += strength * dur_mult * 10
            sector_labels[sec].append(f"{theme['theme_name']}(+)")
        for sec in los_secs:
            sector_scores[sec] -= strength * dur_mult * 10
            sector_labels[sec].append(f"{theme['theme_name']}(-)")

    # Per-company catalysts from news
    catalyst_map: dict[str, tuple] = {}
    for cat in macro.get("company_catalysts", []):
        ticker = (cat.get("ticker") or "").upper()
        if not ticker: continue
        mag  = cat.get("magnitude", 5) / 10.0
        sign = (1  if cat.get("sentiment") == "positive" else
               -1  if cat.get("sentiment") == "negative" else 0)
        if sign:
            catalyst_map[ticker] = (sign * mag * 15, cat.get("description", "")[:80])

    out = []
    for r in rows:
        ticker = r["ticker"]
        sector = r.get("sector", "Unknown")
        beta   = r.get("beta", 1.0) or 1.0
        cs     = r.get("composite_score", 0) or 0

        sec_adj = sector_scores.get(sector, 0.0)

        # Regime modifier
        if regime == "RISK-OFF":
            if beta > 1.3:  sec_adj -= 5
            elif beta < 0.8: sec_adj += 3
        elif regime == "RISK-ON":
            if beta > 1.2:  sec_adj += 3

        cat_score, cat_desc = catalyst_map.get(ticker, (0, ""))
        score = sec_adj + cs * 0.3 + cat_score

        if score <= 2 and not cat_score:
            continue

        labels = " ".join(sector_labels.get(sector, []))
        reason = f"News macro: {sector} adj={sec_adj:+.1f} regime={regime}"
        if labels:   reason += f" [{labels}]"
        if cat_desc: reason += f" | catalyst: {cat_desc}"

        out.append((ticker, round(score, 2), reason))

    return sorted(out, key=lambda x: -x[1])


def score_news_sentiment(rows: list, kmap: dict, macro: dict) -> list[tuple]:
    """
    Strategy 20 — News Sentiment Momentum scorer.
    Quality gate (CS > 40, positive EPS) then amplified by macro sector tailwinds
    and company-specific catalysts. Fundamentals + news together.
    """
    if not macro:
        return []

    sector_scores: dict[str, float] = defaultdict(float)
    for theme in macro.get("dominant_themes", []):
        strength = theme.get("strength", 5) / 10.0
        builtin  = MACRO_THEME_MAP.get(theme.get("theme_id", ""), ([], []))
        for sec in set(theme.get("beneficiary_sectors", []) + builtin[0]):
            sector_scores[sec] += strength * 8
        for sec in set(theme.get("loser_sectors", []) + builtin[1]):
            sector_scores[sec] -= strength * 8

    catalyst_map: dict[str, tuple] = {}
    for cat in macro.get("company_catalysts", []):
        ticker = (cat.get("ticker") or "").upper()
        if not ticker: continue
        mag  = cat.get("magnitude", 5) / 10.0
        sign = (1  if cat.get("sentiment") == "positive" else
               -1  if cat.get("sentiment") == "negative" else 0)
        if sign:
            catalyst_map[ticker] = (sign * mag * 20, cat.get("description", "")[:80])

    out = []
    for r in rows:
        ticker = r["ticker"]
        cs     = r.get("composite_score", 0) or 0
        eps_g  = r.get("eps_growth_fwd", 0) or 0
        npm    = r.get("net_profit_margin", 0) or 0
        sector = r.get("sector", "Unknown")

        if cs < 40: continue                         # quality gate
        if (r.get("eps_ttm") or 0) < 0: continue    # must be profitable

        sec_adj           = sector_scores.get(sector, 0.0)
        cat_adj, cat_desc = catalyst_map.get(ticker, (0, ""))
        score = cs * 0.5 + sec_adj + cat_adj + eps_g * 30 + npm * 20

        if score <= 15 and not cat_adj:
            continue

        parts = [f"Sentiment: CS={cs:.0f}"]
        if sec_adj:  parts.append(f"sector_adj={sec_adj:+.1f}")
        if cat_adj:  parts.append(f"catalyst={cat_adj:+.1f} ({cat_desc})")
        parts.append(f"[{sector}]")

        out.append((ticker, round(score, 2), "  ".join(parts)))

    return sorted(out, key=lambda x: -x[1])


def _build_news_prompt(strategy_id, strategy_name, strategy_desc,
                       candidates, holdings, acct, today, macro):
    """Build the Claude trading prompt for news-driven strategies 19 & 20."""
    hold_str = "\n".join(
        f"  {h['ticker']}: {h['shares']:.4f} sh  cost ${h['cost_basis']:.2f}  avg ${h['avg_cost']:.4f}"
        for h in holdings
    ) or "  (none)"

    cand_str = "\n".join(
        f"  {t}: score={s:.1f}  {r}"
        for t, s, r in candidates[:12]
    )

    regime          = macro.get("market_regime", "NEUTRAL")
    regime_conf     = macro.get("regime_confidence", 5)
    regime_reasoning = macro.get("regime_reasoning", "")

    themes_str = ""
    for th in macro.get("dominant_themes", [])[:3]:
        themes_str += (
            f"\n  • {th.get('theme_name','')} "
            f"(strength {th.get('strength',5)}/10, {th.get('duration_outlook','?')})\n"
            f"    Tailwind: {', '.join(th.get('beneficiary_sectors',[]) or ['none'])}\n"
            f"    Headwind: {', '.join(th.get('loser_sectors',[]) or ['none'])}\n"
            f"    {th.get('description','')}"
        )

    cats_str = ""
    for cat in macro.get("company_catalysts", [])[:6]:
        sign = "+" if cat.get("sentiment") == "positive" else ("-" if cat.get("sentiment") == "negative" else "~")
        cats_str += (f"\n  [{sign}] {cat.get('ticker','?')}: "
                     f"{cat.get('description','')} (mag {cat.get('magnitude',5)}/10)")

    ideas_str = ""
    for idea in macro.get("trade_ideas", [])[:3]:
        ideas_str += (f"\n  • {idea.get('idea','')} "
                      f"[{idea.get('direction','?').upper()}, {idea.get('timeframe','?')}]: "
                      f"{idea.get('rationale','')}")

    return f"""You are managing a paper trading account for strategy "{strategy_id} - {strategy_name}".

Strategy: {strategy_desc}

=== TODAY'S NEWS-DRIVEN MACRO CONTEXT ({today}) ===
Market Regime: {regime} (confidence {regime_conf}/10)
{regime_reasoning}

Dominant macro themes:{themes_str if themes_str else chr(10) + "  None — treat as NEUTRAL"}

Company catalysts from headlines:{cats_str if cats_str else chr(10) + "  None identified"}

Trade ideas:{ideas_str if ideas_str else chr(10) + "  None"}

=== ACCOUNT ===
Cash ${acct['cash']:.2f}  Holdings ${acct['holdings_value']:.2f}  Total ${acct['cash']+acct['holdings_value']:.2f}
Goal ${STARTING_CASH*2:.2f}  |  Trades so far: {acct['trades']}

Current holdings:
{hold_str}

Top-ranked candidates (news-macro scoring):
{cand_str}

=== RULES ===
- Commission $4.95 + 0.05% spread per trade
- Max 3 positions, max 60% per stock, keep ≥5% cash reserve
- BUY: sector aligns with bullish theme AND KPI score > 40
- SELL: sector is a headwind loser OR negative company catalyst OR >20% loss from avg cost
- HOLD: signal strength < 5/10 or ambiguous — don't overtrade noise
- RISK-OFF regime: prefer beta < 1.0 even in tailwind sectors
- Strong company catalyst (earnings beat, guidance raise) overrides sector headwinds

Respond ONLY with a JSON object. No explanation outside the JSON.
{{
  "actions": [
    {{"type": "BUY",  "ticker": "XOM",  "reason": "Energy tailwind from oil shock"}},
    {{"type": "SELL", "ticker": "NVDA", "reason": "Tech headwind + RISK-OFF regime"}},
    {{"type": "HOLD", "ticker": "JPM",  "reason": "Deregulation tailwind, awaiting confirmation"}}
  ],
  "summary": "one sentence: what news theme drove today's decisions"
}}"""


def print_news_briefing(macro: dict):
    """Print a formatted daily news macro briefing."""
    if not macro:
        return
    print("\n" + "─"*65)
    print("  TODAY'S NEWS MACRO BRIEFING")
    print(f"  {macro.get('analysis_date','?')}  |  "
          f"{macro.get('headline_count','?')} headlines analyzed")
    print("─"*65)
    regime = macro.get("market_regime", "NEUTRAL")
    sym    = {"RISK-ON": "▲", "RISK-OFF": "▼", "NEUTRAL": "─"}.get(regime, "─")
    print(f"  {sym} {regime}  (confidence {macro.get('regime_confidence',5)}/10)")
    print(f"  {macro.get('regime_reasoning','')}")
    for i, th in enumerate(macro.get("dominant_themes", []), 1):
        print(f"\n  Theme {i}: {th.get('theme_name','')}  "
              f"[{th.get('strength',5)}/10  {th.get('duration_outlook','?')}]")
        print(f"    {th.get('description','')}")
        if th.get("beneficiary_sectors"):
            print(f"    Tailwind: {', '.join(th['beneficiary_sectors'])}")
        if th.get("loser_sectors"):
            print(f"    Headwind: {', '.join(th['loser_sectors'])}")
    for cat in macro.get("company_catalysts", []):
        sym2 = {"positive": "▲", "negative": "▼"}.get(cat.get("sentiment",""), "─")
        print(f"  {sym2} {cat.get('ticker','?'):<6}  "
              f"[{cat.get('catalyst_type','')}]  {cat.get('description','')[:70]}")
    print("─"*65)


def score_defense_war_economy(rows, kmap):
    """
    NEW-21: Defense & War Economy.

    Targets the beneficiaries of sustained US military spending:
      - Defense primes & contractors (LMT, RTX, NOC, GD, LHX, BA, HWM, TDG, HII)
      - Cybersecurity (CRWD, PANW, FTNT) — wartime elevates state-sponsored threats
      - Energy majors (XOM, CVX, COP, XLE proxies) — Middle East conflict = supply risk

    Scoring weights:
      - Sector / ticker whitelist membership (hard gate)
      - Net profit margin (defense primes have very stable, contract-locked margins)
      - Forward EPS growth (backlog conversion proxy)
      - Golden cross (institutional accumulation already underway)
      - Abnormal return bonus (contract-announcement days produce real spikes)
      - RSI guard: skip if overbought (> 78) — don't chase a spike

    Hold philosophy: defense budgets are multi-year — turnover should be LOW.
    Only sell on death cross, margin collapse below 8%, or EPS growth turning negative.
    """
    # Tickers with direct defense / war-economy exposure in the universe
    DEFENSE_TICKERS = {
        # Defense primes & Tier-1 contractors
        "LMT", "RTX", "NOC", "GD", "LHX", "BA", "HWM", "TDG",
        # Cyber (DoD's fastest-growing budget line)
        "CRWD", "PANW", "FTNT",
        # Industrials with large DoD exposure
        "GE", "GEV", "HON", "CAT", "EMR",
        # Energy — geopolitical risk premium
        "XOM", "CVX", "COP", "OXY", "SLB", "BKR", "EOG",
    }
    DEFENSE_SECTORS = {"Industrials", "Energy", "Information Technology"}

    out = []
    for r in rows:
        ticker  = r["ticker"]
        sector  = r.get("sector", "") or ""
        eps_ttm = r.get("eps_ttm", 0) or 0
        eps_fwd = r.get("eps_growth_fwd", 0) or 0
        npm     = r.get("net_profit_margin", 0) or 0
        ma_sig  = r.get("ma_signal", "")
        ab      = r.get("abnormal_return", 0) or 0
        cs      = r.get("composite_score", 0)
        rsi     = r.get("rsi_14", 50) or 50
        beta    = r.get("beta", 1.0) or 1.0

        # Hard gates: must be a known defense/war-economy ticker OR be in a
        # qualifying sector with a direct ticker-level signal.
        in_whitelist = ticker in DEFENSE_TICKERS
        in_sector    = sector in DEFENSE_SECTORS

        if not in_whitelist and not in_sector:
            continue
        if eps_ttm <= 0:              continue   # must be profitable
        if npm < 0.08:                continue   # minimum margin quality
        if "BULLISH" not in ma_sig:   continue   # uptrend required
        if rsi > 78:                  continue   # skip overbought

        # Whitelist bonus: known defense prime gets a structural premium
        whitelist_bonus = 15 if in_whitelist else 0

        # Cyber stocks get a moderate extra bonus (fastest DoD budget growth)
        cyber_bonus = 10 if ticker in {"CRWD", "PANW", "FTNT"} else 0

        # Core score
        score = (
            npm * 100 +              # margin quality (defense margins are sticky)
            eps_fwd * 80 +           # backlog/contract conversion growth
            ab * 120 +               # contract-announcement momentum
            cs * 0.25 +              # composite KPI health
            whitelist_bonus +
            cyber_bonus
        )

        reason = (
            f"Defense/war economy: {ticker}  "
            f"margin={npm*100:.1f}%  fwd_growth={eps_fwd*100:.1f}%  "
            f"RSI={rsi:.1f}  beta={beta:.2f}"
        )
        out.append((ticker, round(score, 2), reason))

    return sorted(out, key=lambda x: -x[1])


SCORE_FN = {
    "02": score_mean_reversion,
    "03": score_value,
    "06": score_low_volatility,
    "07": score_earnings_surprise,
    "08": score_dividend_growth,
    "09": score_insider_buying,
    "10": score_macro_adaptive,
    "11": score_large_cap_value,        # FIX-5
    "12": score_academic_momentum,      # FIX-8
    "13": score_quality_profitability,
    "14": score_passive,
    "18": score_capex_beneficiary,      # NEW-18
    "19": score_news_macro,             # NEW-19
    "20": score_news_sentiment,         # NEW-20
    "21": score_defense_war_economy,    # NEW-21
}

# ── Trade execution helpers ───────────────────────────────────────────────────

def buy(sid, ticker, price, cash_available, reason, today, kmap, dry_run=False):
    """Buy as many shares as cash allows (up to 60% of total account).
    FIX-2: recalculate holdings_value using market prices for ALL positions.
    """
    acct     = read_account(sid)
    holdings = read_holdings(sid)
    total    = acct["cash"] + acct["holdings_value"]
    max_pos  = total * 0.60
    existing = next((h for h in holdings if h["ticker"] == ticker), None)
    already_invested = float(existing["cost_basis"]) if existing else 0
    spend = min(cash_available * 0.90, max_pos - already_invested)
    spend = max(spend, 0)
    if spend < price + COMMISSION:
        return False, "Insufficient funds"
    shares   = round((spend - COMMISSION) / (price * (1 + SPREAD_PCT)), 6)
    net_cost = round(shares * price * (1 + SPREAD_PCT) + COMMISSION, 4)

    if dry_run:
        print(f"    [DRY-RUN] BUY  {shares:.4f} x {ticker} @ ${price:.2f}  cost=${net_cost:.2f}")
        return True, "dry-run"

    acct["cash"] = round(acct["cash"] - net_cost, 4)
    acct["trades"] += 1
    if existing:
        new_total_shares  = existing["shares"] + shares
        new_cost_basis    = existing["cost_basis"] + net_cost
        existing["shares"]    = round(new_total_shares, 6)
        existing["avg_cost"]  = round(new_cost_basis / new_total_shares, 4)
        existing["cost_basis"]= round(new_cost_basis, 4)
    else:
        holdings.append({"ticker": ticker, "shares": round(shares, 6),
                         "avg_cost": round(net_cost / shares, 4),
                         "cost_basis": round(net_cost, 4),
                         "purchase_date": today, "strategy_id": sid})

    # FIX-2: use market prices for all positions
    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"]          = round(acct["cash"] + acct["holdings_value"], 2)
    save_account(sid, acct)
    save_holdings(sid, holdings)
    append_txn(sid, {"date": today, "strategy_id": sid, "action": "BUY",
                     "ticker": ticker, "shares": round(shares, 6),
                     "price": round(price, 4), "commission": COMMISSION,
                     "net_amount": round(-net_cost, 4),
                     "cash_after": round(acct["cash"], 4), "reason": reason})
    return True, f"Bought {shares:.4f} shares @ ${price:.2f}"

def sell(sid, ticker, price, reason, today, kmap, dry_run=False):
    """Sell the full position in ticker.
    FIX-3: recalculate holdings_value using market prices for remaining positions.
    """
    holdings = read_holdings(sid)
    pos = next((h for h in holdings if h["ticker"] == ticker), None)
    if not pos:
        return False, "No position"
    shares   = pos["shares"]
    proceeds = round(shares * price * (1 - SPREAD_PCT) - COMMISSION, 4)
    if proceeds <= 0:
        return False, f"Position too small to sell after commission (proceeds=${proceeds:.2f})"
    if dry_run:
        print(f"    [DRY-RUN] SELL {shares:.4f} x {ticker} @ ${price:.2f}  proceeds=${proceeds:.2f}")
        return True, "dry-run"
    acct = read_account(sid)
    acct["cash"]   = round(acct["cash"] + proceeds, 4)
    acct["trades"] += 1
    holdings = [h for h in holdings if h["ticker"] != ticker]
    # FIX-3: use market prices for remaining positions
    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"]          = round(acct["cash"] + acct["holdings_value"], 2)
    save_account(sid, acct)
    save_holdings(sid, holdings)
    append_txn(sid, {"date": today, "strategy_id": sid, "action": "SELL",
                     "ticker": ticker, "shares": round(shares, 6),
                     "price": round(price, 4), "commission": COMMISSION,
                     "net_amount": round(proceeds, 4),
                     "cash_after": round(acct["cash"], 4), "reason": reason})
    return True, f"Sold {shares:.4f} shares @ ${price:.2f}"

# ── Passive strategy (no Claude call) ────────────────────────────────────────

def run_passive(sid, rows, kmap, today, dry_run=False):
    """
    FIX-4: Passive strategy runs entirely in code — no Claude API call.
    Buys the top-2 market-cap stocks on day 1 and holds them. Only ever buys
    new positions if a slot is empty and cash is available.
    """
    acct     = read_account(sid)
    holdings = read_holdings(sid)

    # Refresh holdings values with current prices
    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"]          = round(acct["cash"] + acct["holdings_value"], 2)
    save_account(sid, acct)

    held = {h["ticker"] for h in holdings}
    top2 = [t for t, _, _ in score_passive(rows, kmap)[:2]]

    for ticker in top2:
        if ticker in held:
            continue
        if len(holdings) >= 2:
            break
        price = kmap.get(ticker, {}).get("current_price", 0)
        if price <= 0:
            continue
        spend = acct["cash"] * 0.48   # roughly half of cash per slot
        if spend < price + COMMISSION:
            continue
        ok, msg = buy(sid, ticker, price, spend, "Passive: buy top market-cap", today, kmap, dry_run)
        print(f"       BUY  {ticker}: {msg}")
        acct     = read_account(sid)
        holdings = read_holdings(sid)

    print(f"       Cash: ${acct['cash']:>9.2f}  |  "
          f"Holdings: ${acct['holdings_value']:>9.2f}  |  "
          f"Total: ${acct['total']:>9.2f}  |  "
          f"Positions: {[h['ticker'] for h in holdings]}")

# ── Claude API call ───────────────────────────────────────────────────────────

def ask_claude(strategy_id, strategy_name, strategy_desc, candidates, holdings, acct, today):
    """Ask Claude to decide which of the top candidates to buy/sell.
    FIX-6: max_tokens raised to 1200; truncated responses logged with raw text.
    """
    try:
        import urllib.request
    except ImportError:
        return None, "urllib not available"

    hold_str = "\n".join(
        f"  {h['ticker']}: {h['shares']:.4f} shares, cost basis ${h['cost_basis']:.2f}, avg ${h['avg_cost']:.4f}"
        for h in holdings
    ) or "  (none)"

    cand_str = "\n".join(
        f"  {t}: score={s:.1f}  {r}"
        for t, s, r in candidates[:10]
    )

    if strategy_id == "18":
        prompt = f"""You are managing a paper trading account for strategy "18 - Capex beneficiary / semis".

Strategy description: {strategy_desc}

THESIS: Hyperscalers (Microsoft, Google, Amazon, Meta) are in a multi-year AI infrastructure
buildout, spending hundreds of billions on data centres. Every dollar of hyperscaler capex
flows upstream to GPU makers, network chip designers, PCB manufacturers, power management ICs,
and storage. You are positioned to capture that upstream demand wave BEFORE the market fully
prices it in.

The scoring function has already filtered for: IT/Comms sector, profitable, expanding margins,
golden cross uptrend, and reasonable valuation (P/E ≤ 80). Your job is to pick the best 1-2
positions from the candidates and hold them with conviction while the thesis plays out.
Concentration is intentional — diversification dilutes the thesis.

Account:
  Cash available: ${acct['cash']:.2f}
  Holdings value: ${acct['holdings_value']:.2f}
  Total: ${acct['cash']+acct['holdings_value']:.2f}
  Goal: ${STARTING_CASH*2:.2f} (double the money)
  Trades so far: {acct['trades']}

Current holdings:
{hold_str}

Top-ranked capex beneficiary candidates today (IT/Comms sector, scored by margin × growth × trend):
{cand_str}

Rules:
- Commission is $4.95 per trade + 0.05% spread
- Max 2 positions (concentrated conviction)
- Max 70% of portfolio in any one stock (thesis warrants concentration)
- Keep at least 5% cash reserve
- Hold with conviction — only sell if: MA turns bearish (death cross), net margin drops below 10%,
  forward EPS growth turns negative, or the AI capex cycle shows clear signs of ending

Respond ONLY with a JSON object. No explanation outside the JSON.
{{
  "actions": [
    {{"type": "BUY",  "ticker": "NVDA", "reason": "why"}},
    {{"type": "SELL", "ticker": "XYZ",  "reason": "why"}},
    {{"type": "HOLD", "ticker": "AMAT", "reason": "why"}}
  ],
  "summary": "one sentence summary of today's decisions"
}}
"""
    elif strategy_id == "21":
        prompt = f"""You are managing a paper trading account for strategy "21 - Defense & war economy".

Strategy description: {strategy_desc}

THESIS: The US has been spending ~$1B per week sustaining military operations in the Middle East.
That spending flows directly into defense primes (LMT, RTX, NOC, GD, LHX), industrial suppliers
(GE, HON, CAT), cybersecurity firms (CRWD, PANW, FTNT — DoD's fastest-growing budget line), and
energy majors (XOM, CVX, COP — Middle East conflict = sustained supply risk premium).
Defense contracts are multi-year. Backlogs take years to convert. This is a HOLD-with-conviction
strategy — do NOT churn positions on day-to-day volatility.

Account:
  Cash available: ${acct['cash']:.2f}
  Holdings value: ${acct['holdings_value']:.2f}
  Total: ${acct['cash']+acct['holdings_value']:.2f}
  Goal: ${STARTING_CASH*2:.2f} (double the money)
  Trades so far: {acct['trades']}

Current holdings:
{hold_str}

Top-ranked war-economy candidates today (scored by margin × growth × trend):
{cand_str}

Rules:
- Commission is $4.95 per trade + 0.05% spread
- Max 3 positions at once
- Max 60% of portfolio in any one stock
- Keep at least 5% cash reserve
- HOLD with conviction — only sell if: MA turns bearish (death cross), net margin drops below 8%,
  or forward EPS growth turns negative. Normal 10-15% drawdowns are EXPECTED — do not sell on them.
- BUY the top-scoring candidate if a slot is open and candidate score > 20
- Prefer defense primes (LMT, RTX, NOC, GD, LHX) and cyber (CRWD, PANW, FTNT) over pure energy
  when scores are close — defense has more direct budget exposure

Respond ONLY with a JSON object. No explanation outside the JSON.
{{
  "actions": [
    {{"type": "BUY",  "ticker": "LMT",  "reason": "why"}},
    {{"type": "SELL", "ticker": "XYZ",  "reason": "why"}},
    {{"type": "HOLD", "ticker": "RTX",  "reason": "why"}}
  ],
  "summary": "one sentence summary of today's decisions"
}}
"""
    else:
        prompt = f"""You are managing a paper trading account for strategy "{strategy_id} - {strategy_name}".

Strategy description: {strategy_desc}

Account:
  Cash available: ${acct['cash']:.2f}
  Holdings value: ${acct['holdings_value']:.2f}
  Total: ${acct['cash']+acct['holdings_value']:.2f}
  Goal: ${STARTING_CASH*2:.2f} (double the money)
  Trades so far: {acct['trades']}

Current holdings:
{hold_str}

Top-ranked candidates today (by strategy scoring):
{cand_str}

Rules:
- Commission is $4.95 per trade + 0.05% spread
- Max 3 positions at once
- Max 60% of portfolio in any one stock
- Keep at least 5% cash reserve
- Only sell if: stock dropped >20% from your avg cost, score is genuinely terrible, or a clearly better opportunity exists (10+ point score gap)

Respond ONLY with a JSON object. No explanation outside the JSON.
{{
  "actions": [
    {{"type": "BUY",  "ticker": "AAPL", "reason": "why"}},
    {{"type": "SELL", "ticker": "XYZ",  "reason": "why"}},
    {{"type": "HOLD", "ticker": "MU",   "reason": "why"}}
  ],
  "summary": "one sentence summary of today's decisions"
}}
"""
    payload = json.dumps({
        "model":      MODEL,
        "max_tokens": 1200,          # FIX-6: raised from 600
        "messages":   [{"role": "user", "content": prompt}]
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "Content-Type":      "application/json",
            "x-api-key":         ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    text = ""
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        text = data["content"][0]["text"].strip()

        # FIX-6: detect and log response truncation
        if data.get("stop_reason") == "max_tokens":
            print(f"       [WARN] Claude response truncated (hit max_tokens=1200). "
                  f"Partial text: {text[:200]}...")
            return None, "Response truncated — increase max_tokens or shorten prompt"

        # strip markdown fences if present
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$",        "", text)
        return json.loads(text), None

    except json.JSONDecodeError as e:
        print(f"       [WARN] JSON parse error: {e}")
        print(f"       Raw response ({len(text)} chars): {text[:300]}")
        return None, f"JSON parse error: {e}"
    except urllib.error.HTTPError as e:
        body = ""
        try:    body = e.read().decode("utf-8")
        except: pass
        print(f"       [WARN] HTTP {e.code} from Anthropic API: {body[:300]}")
        return None, f"HTTP {e.code}: {body[:200]}"
    except Exception as e:
        return None, str(e)

# ── Leaderboard ───────────────────────────────────────────────────────────────

def update_leaderboard(today, kmap=None):
    """Build and write the daily leaderboard CSV.
    If kmap is provided, refreshes holdings_value for every strategy at current
    market prices before reading — so strategies that were skipped today (no
    candidates) still show up-to-date portfolio values rather than yesterday's.
    """
    if kmap:
        for sid, *_ in STRATEGIES:
            acct     = read_account(sid)
            holdings = read_holdings(sid)
            if holdings:
                acct["holdings_value"] = calc_holdings_value(holdings, kmap)
                acct["total"]          = round(acct["cash"] + acct["holdings_value"], 2)
                save_account(sid, acct)

    rows = []
    for sid, name, style, risk, desc in STRATEGIES:
        acct  = read_account(sid)
        total = acct["cash"] + acct["holdings_value"]
        pnl   = total - STARTING_CASH
        pct   = (total / STARTING_CASH - 1) * 100
        rows.append({
            "date":           today,
            "strategy_id":    sid,
            "strategy_name":  name,
            "style":          style,
            "risk":           risk,
            "cash":           round(acct["cash"], 2),
            "holdings_value": round(acct["holdings_value"], 2),
            "total":          round(total, 2),
            "pnl":            round(pnl, 2),
            "pct_return":     round(pct, 2),
            "trades":         acct["trades"],
        })
    rows.sort(key=lambda x: -x["total"])
    for i, r in enumerate(rows):
        r["rank"] = i + 1
    write_csv(leader_file(), rows,
              ["rank","date","strategy_id","strategy_name","style","risk",
               "cash","holdings_value","total","pnl","pct_return","trades"])
    return rows

# ── Initialise account files ──────────────────────────────────────────────────

def init_accounts(force=False):
    print("\n" + "-"*55)
    print("  INITIALISING STRATEGY ACCOUNTS")
    print("-"*55)
    today = date.today().isoformat()
    for sid, name, style, risk, desc in STRATEGIES:
        path = acct_file(sid)
        if path.exists() and not force:
            print(f"  [{sid}] {name[:40]} — already exists, skipping")
            continue
        acct = {"account": ACCOUNT_NUM, "strategy_id": sid,
                "cash": STARTING_CASH, "holdings_value": 0.0, "total": STARTING_CASH,
                "start_date": today, "trades": 0}
        save_account(sid, acct)
        save_holdings(sid, [])
        print(f"  [{sid}] {name[:40]} — created  ${STARTING_CASH:.2f}")
    print("-"*55)
    print("  Done. Run without --init to start trading.")
    print("-"*55 + "\n")

# ── Run one strategy ──────────────────────────────────────────────────────────

def run_strategy(sid, name, style, risk, desc, rows, kmap, today, dry_run=False):
    print(f"\n  [{sid}] {name}")
    print(f"       Style: {style}  |  Risk: {risk}")

    # FIX-4: passive strategy bypasses Claude entirely
    if sid == "14":
        run_passive(sid, rows, kmap, today, dry_run)
        return

    # NEW-19/20: news-driven strategies — fetch macro analysis then use news prompt
    if sid in ("19", "20"):
        macro = get_news_macro_analysis(verbose=True)
        if not macro:
            print("       No news macro analysis available today — skipping")
            return
        print_news_briefing(macro)
        acct     = read_account(sid)
        holdings = read_holdings(sid)
        acct["holdings_value"] = calc_holdings_value(holdings, kmap)
        acct["total"]          = round(acct["cash"] + acct["holdings_value"], 2)
        save_account(sid, acct)
        print(f"       Cash: ${acct['cash']:>9.2f}  |  "
              f"Holdings: ${acct['holdings_value']:>9.2f}  |  "
              f"Total: ${acct['total']:>9.2f}")
        score_fn   = SCORE_FN.get(sid)
        candidates = score_fn(rows, kmap, macro) if score_fn else []
        if not candidates:
            print("       No candidates matched today's news themes")
            return
        print(f"       Top candidate: {candidates[0][0]} (score {candidates[0][1]:.1f})")
        prompt = _build_news_prompt(sid, name, desc, candidates, holdings, acct, today, macro)
        payload = json.dumps({"model": MODEL, "max_tokens": 1200,
                               "messages": [{"role": "user", "content": prompt}]}).encode("utf-8")
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages", data=payload,
            headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                resp_data = json.loads(resp.read().decode("utf-8"))
            text = resp_data["content"][0]["text"].strip()
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
            decision = json.loads(text)
        except Exception as e:
            print(f"       Claude error: {e}")
            return
        print(f"       Summary: {decision.get('summary','')}")
        for action in decision.get("actions", []):
            atype  = action.get("type","").upper()
            ticker = action.get("ticker","")
            reason = action.get("reason","")
            if not ticker or ticker not in kmap: continue
            price = kmap[ticker].get("current_price", 0)
            if price <= 0: continue
            if atype == "BUY":
                ok, msg = buy(sid, ticker, price, acct["cash"], reason, today, kmap, dry_run)
                print(f"       BUY  {ticker}: {msg}")
                acct = read_account(sid)
            elif atype == "SELL":
                ok, msg = sell(sid, ticker, price, reason, today, kmap, dry_run)
                print(f"       SELL {ticker}: {msg}")
                acct = read_account(sid)
            elif atype == "HOLD":
                print(f"       HOLD {ticker}: {reason[:60]}")
        return

    acct     = read_account(sid)
    holdings = read_holdings(sid)

    # Refresh holdings values at today's market prices
    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"]          = round(acct["cash"] + acct["holdings_value"], 2)
    save_account(sid, acct)

    print(f"       Cash: ${acct['cash']:>9.2f}  |  "
          f"Holdings: ${acct['holdings_value']:>9.2f}  |  "
          f"Total: ${acct['total']:>9.2f}")

    score_fn = SCORE_FN.get(sid)
    if not score_fn or not rows:
        print("       No KPI data or scoring function — skipping")
        return

    candidates = score_fn(rows, kmap)
    if not candidates:
        print("       No candidates matched this strategy's filters today")
        return

    print(f"       Top candidate: {candidates[0][0]} (score {candidates[0][1]:.1f})")

    decision, err = ask_claude(sid, name, desc, candidates, holdings, acct, today)
    if err:
        print(f"       Claude error: {err}")
        return

    print(f"       Summary: {decision.get('summary','')}")

    for action in decision.get("actions", []):
        atype  = action.get("type","").upper()
        ticker = action.get("ticker","")
        reason = action.get("reason","")
        if not ticker or ticker not in kmap:
            continue
        price = kmap[ticker].get("current_price", 0)
        if price <= 0:
            continue

        if atype == "BUY":
            ok, msg = buy(sid, ticker, price, acct["cash"], reason, today, kmap, dry_run)
            print(f"       BUY  {ticker}: {msg}")
            acct = read_account(sid)   # reload after mutation
        elif atype == "SELL":
            ok, msg = sell(sid, ticker, price, reason, today, kmap, dry_run)
            print(f"       SELL {ticker}: {msg}")
            acct = read_account(sid)   # reload after mutation
        elif atype == "HOLD":
            print(f"       HOLD {ticker}: {reason[:60]}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Multi-strategy paper trading runner")
    parser.add_argument("--dry-run",    action="store_true", help="Preview trades, don't execute")
    parser.add_argument("--init",       action="store_true", help="Initialise all account files")
    parser.add_argument("--force-init", action="store_true", help="Re-initialise (resets all accounts!)")
    parser.add_argument("--strategy",   type=str,            help="Run only this strategy ID, e.g. 01")
    parser.add_argument("--kpi-file",   type=str, default="equity_kpi_results.csv")
    args = parser.parse_args()

    today = date.today().isoformat()

    if args.init or args.force_init:
        init_accounts(force=args.force_init)
        return

    if not ANTHROPIC_API_KEY:
        print("\n  ERROR: ANTHROPIC_API_KEY not set.")
        print("  Add it to your .env file or export it as an environment variable.\n")
        return

    print("\n" + "="*65)
    print("  MULTI-STRATEGY PAPER TRADING ENGINE")
    print(f"  Date: {today}  |  Strategies: {len(STRATEGIES)}  |  Goal: $2,000 each")
    print(f"  Model: {MODEL}")
    if args.dry_run:
        print("  *** DRY RUN — no trades will be executed ***")
    print("="*65)

    rows, kmap = load_kpi(args.kpi_file)
    if not rows:
        print("\n  ERROR: No KPI data found. Run equity_kpi_analyzer.py first.\n")
        return

    print(f"\n  Loaded {len(rows)} tickers from KPI file.")

    run_ids = [args.strategy] if args.strategy else [s[0] for s in STRATEGIES]

    for sid, name, style, risk, desc in STRATEGIES:
        if sid not in run_ids:
            continue
        run_strategy(sid, name, style, risk, desc, rows, kmap, today, dry_run=args.dry_run)

    print("\n" + "-"*65)
    print("  LEADERBOARD")
    print("-"*65)
    lb = update_leaderboard(today, kmap=kmap)
    print(f"  {'Rank':<5} {'ID':<4} {'Strategy':<35} {'Total':>9} {'P&L':>9} {'Return':>8}")
    print(f"  {'-'*4} {'-'*4} {'-'*35} {'-'*9} {'-'*9} {'-'*8}")
    for r in lb:
        sign = "+" if r["pnl"] >= 0 else ""
        print(f"  {r['rank']:<5} {r['strategy_id']:<4} {r['strategy_name'][:35]:<35} "
              f"${r['total']:>8.2f} {sign}${r['pnl']:>8.2f} {sign}{r['pct_return']:>7.2f}%")
    print("-"*65)
    print(f"  Leaderboard saved to: {leader_file().name}")
    print("="*65 + "\n")


if __name__ == "__main__":
    main()

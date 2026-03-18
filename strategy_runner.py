"""
strategy_runner.py  —  Multi-strategy paper trading engine
Runs 14 active strategies + 1 passive benchmark concurrently.
Each strategy has its own account, holdings, and transactions CSV.
Produces a daily leaderboard CSV for the dashboard.

Usage:
    python strategy_runner.py                  # run all strategies
    python strategy_runner.py --dry-run        # preview decisions only
    python strategy_runner.py --strategy 01    # run one strategy only
    python strategy_runner.py --init           # initialise all account files fresh

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
    NEW-15 Strategy 15 "Noise chaser" — explicit degenerate baseline that buys
                    the largest abnormal movers with no fundamental filter
"""

import sys, os, re, json, csv, math, argparse, urllib.error
from datetime import date
from pathlib import Path

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
            os.environ.setdefault(k.strip(), v.strip())

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
    ("01", "Momentum / trend following",   "momentum",    "high",
     "Buy the strongest 3-12 month performers. Sell when trend breaks (RSI drops, MA death cross)."),
    ("02", "Mean reversion",               "contrarian",  "med",
     "Buy oversold stocks (RSI < 38). Sell when RSI recovers above 50."),
    ("03", "Value investing",              "value",       "low",
     "Buy fundamentally cheap stocks: low P/E, low P/B. Hold until fair value."),
    ("04", "Quality growth (GARP)",        "growth",      "med",
     "Growth at a reasonable price. High margins, strong EPS growth, not overvalued. Only profitable companies (positive trailing EPS)."),
    ("05", "Sector rotation",              "macro",       "med",
     "Concentrate in the strongest-performing sector based on relative sector composite scores."),
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
    ("15", "Noise chaser",                 "speculative", "high",  # NEW-15
     "Chase the largest single-day abnormal price movers with zero fundamental filter. "
     "Explicit degenerate baseline — demonstrates why noise-chasing loses money over time."),
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
                    "dividend_yield","five_year_avg_dividend_yield"]:   # FIX-9
            if col in r:
                try:    r[col] = float(r[col])
                except: r[col] = 0.0
        kmap[r["ticker"]] = r
    return rows, kmap

# ── Strategy scoring functions ────────────────────────────────────────────────
# Each returns a sorted list of (ticker, score, reason) tuples.

def score_momentum(rows, kmap):
    """High composite score + bullish MA + strong abnormal return."""
    out = []
    for r in rows:
        if r.get("ma_signal","") != "BULLISH (Golden Cross)": continue
        rsi_contrib = min(r.get("rsi_14", 50), 70)   # cap so overbought doesn't score higher
        score = (r.get("composite_score", 0) * 0.5 +
                 rsi_contrib * 0.3 +
                 r.get("abnormal_return", 0) * 200)
        out.append((r["ticker"], round(score, 2), "Strong momentum + golden cross"))
    return sorted(out, key=lambda x: -x[1])

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

def score_quality_growth(rows, kmap):
    """High EPS growth + high margin + P/E not insane.
    FIX-7: skip loss-making companies (negative trailing EPS) to prevent
    artificially inflated growth scores for firms recovering from a large loss.
    """
    out = []
    for r in rows:
        eps_ttm = r.get("eps_ttm", 0) or 0
        if eps_ttm < 0:           # FIX-7: skip loss-makers
            continue
        eps_g = r.get("eps_growth_fwd", 0)
        npm   = r.get("net_profit_margin", 0)
        pe    = r.get("pe_ratio", 999)
        if eps_g <= 0.05 or pe > 50: continue
        score = eps_g * 60 + npm * 80 + r.get("composite_score", 0) * 0.3
        out.append((r["ticker"], round(score, 2),
                    f"EPS growth={eps_g*100:.1f}% margin={npm*100:.1f}%"))
    return sorted(out, key=lambda x: -x[1])

def score_sector_rotation(rows, kmap):
    """Best composite score within the strongest sector (by avg composite score)."""
    sector_scores = {}
    for r in rows:
        sec = r.get("sector", "Unknown")
        sector_scores.setdefault(sec, []).append(r.get("composite_score", 0))
    sector_avg  = {s: sum(v)/len(v) for s, v in sector_scores.items()}
    best_sector = max(sector_avg, key=sector_avg.get)
    out = []
    for r in rows:
        if r.get("sector","") != best_sector: continue
        out.append((r["ticker"], round(r.get("composite_score", 0), 2),
                    f"Top sector: {best_sector}"))
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
    PEAD proxy: requires BOTH strong forward EPS growth AND a positive abnormal
    return today (market reacting to an earnings beat). Original version used only
    abnormal_return which is plain noise-chasing — this version requires the
    underlying fundamental signal to be present too.
    """
    out = []
    for r in rows:
        eps_ttm = r.get("eps_ttm", 0) or 0
        eps_g   = r.get("eps_growth_fwd", 0)
        ab      = r.get("abnormal_return", 0)
        # profitable company + strong forward growth + positive market reaction
        if eps_ttm <= 0: continue
        if eps_g < 0.10: continue
        if ab <= 0.005:  continue
        score = eps_g * 150 + ab * 200 + r.get("composite_score", 0) * 0.2
        out.append((r["ticker"], round(score, 2),
                    f"PEAD proxy: fwd_growth={eps_g*100:.1f}% ab_ret={ab*100:.2f}%"))
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

def score_noise_chaser(rows, kmap):
    """
    NEW-15: Explicit degenerate strategy — noise chaser.
    Buys whichever stocks had the largest POSITIVE abnormal return today with
    zero fundamental filter. This is what Strategy 07 was accidentally doing.
    The market has already priced in the move; buying now captures only noise.
    Expected to underperform most other strategies over time — a useful lower bound.
    """
    out = []
    for r in rows:
        ab = r.get("abnormal_return", 0)
        if ab <= 0.005: continue   # only upside movers (we can only go long)
        score = ab * 500           # pure noise signal, zero fundamental weighting
        out.append((r["ticker"], round(score, 2),
                    f"Noise chase: abnormal_return={ab*100:.2f}% (no fundamentals)"))
    return sorted(out, key=lambda x: -x[1])

SCORE_FN = {
    "01": score_momentum,
    "02": score_mean_reversion,
    "03": score_value,
    "04": score_quality_growth,
    "05": score_sector_rotation,
    "06": score_low_volatility,
    "07": score_earnings_surprise,
    "08": score_dividend_growth,
    "09": score_insider_buying,
    "10": score_macro_adaptive,
    "11": score_large_cap_value,        # FIX-5
    "12": score_academic_momentum,      # FIX-8
    "13": score_quality_profitability,
    "14": score_passive,
    "15": score_noise_chaser,           # NEW-15
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

    # Strategy 15 gets a purpose-built aggressive prompt — it should chase noise,
    # not reason conservatively. Using the standard hold-biased prompt would cause
    # Claude to rationally avoid buying and the degenerate behaviour wouldn't show.
    if strategy_id == "15":
        prompt = f"""You are managing a paper trading account for strategy "15 - Noise chaser".
This is an EXPERIMENTAL DEGENERATE strategy designed to demonstrate that chasing
yesterday's price movers loses money. Your job is to act as the worst possible trader:
buy the #1 highest abnormal-return stock from the candidate list every single day,
regardless of fundamentals, valuation, or risk. Never hold cash beyond the 5% reserve.
Sell any existing position if a higher abnormal-return candidate exists today.

Account:
  Cash available: ${acct['cash']:.2f}
  Holdings value: ${acct['holdings_value']:.2f}
  Total: ${acct['cash']+acct['holdings_value']:.2f}
  Trades so far: {acct['trades']}

Current holdings:
{hold_str}

Top noise candidates today (ranked by abnormal return — pure price chasing):
{cand_str}

Rules:
- Commission is $4.95 per trade + 0.05% spread
- Max 3 positions, max 60% per stock, keep 5% cash
- BUY the #1 candidate if you have a free position slot
- SELL any holding if its ticker is not in the top 3 candidates today
- Do NOT apply fundamental reasoning — this strategy intentionally ignores it

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

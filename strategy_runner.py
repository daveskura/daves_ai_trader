"""
strategy_runner.py  —  Multi-strategy paper trading engine
Runs 13 active strategies + 1 passive benchmark concurrently.
Each strategy has its own account, holdings, and transactions CSV.
Produces a daily leaderboard CSV for the dashboard.

Usage:
    python strategy_runner.py                  # run all strategies
    python strategy_runner.py --dry-run        # preview decisions only
    python strategy_runner.py --strategy 01    # run one strategy only
    python strategy_runner.py --init           # initialise all account files fresh
"""

import sys, os, re, json, csv, math, argparse
from datetime import datetime, date
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
MODEL             = "claude-sonnet-4-5-20250929"
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
     "Buy oversold stocks (RSI < 30). Sell when RSI recovers above 50."),
    ("03", "Value investing",              "value",       "low",
     "Buy fundamentally cheap stocks: low P/E, low P/B. Hold until fair value."),
    ("04", "Quality growth (GARP)",        "growth",      "med",
     "Growth at a reasonable price. High margins, strong EPS growth, not overvalued."),
    ("05", "Sector rotation",              "macro",       "med",
     "Concentrate in the strongest-performing sector based on relative sector ETF performance."),
    ("06", "Low volatility / defensive",   "defensive",   "low",
     "Lowest beta stocks with stable earnings. Hold through high-VIX environments."),
    ("07", "Earnings surprise (PEAD)",     "event",       "high",
     "Buy stocks with strong positive EPS surprise. Ride the post-earnings drift."),
    ("08", "Dividend growth",              "income",      "low",
     "Companies with strong dividends and growing payout. Preserve capital with income."),
    ("09", "Insider buying signal",        "alt-data",    "med",
     "Follow C-suite open-market purchases. Strong predictor of 6-12 month outperformance."),
    ("10", "Macro-regime adaptive",        "macro",       "med",
     "Switch between aggressive and defensive posture based on VIX, GDP trend, Fed direction."),
    ("11", "Small-cap value (Fama-French)","academic",    "med",
     "Fama-French factor investing: small market cap + low price-to-book. 13-14% historical CAGR."),
    ("12", "Momentum (academic / Asness)", "academic",    "high",
     "Cliff Asness AQR-style momentum: top 6-12 month performers with crash protection rules."),
    ("13", "Quality / profitability",      "academic",    "low",
     "Novy-Marx gross profitability factor: high gross margin, low debt, stable ROE."),
    ("14", "Passive S&P 500 benchmark",    "passive",     "low",
     "Buy and hold the top market-cap stocks. No active trading. Reality-check baseline."),
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

# ── KPI data loader ──────────────────────────────────────────────────────────

def load_kpi(kpi_path="equity_kpi_results.csv"):
    path = BASE_DIR / kpi_path
    if not path.exists():
        print(f"  [WARN] KPI file not found: {path}")
        return [], {}
    rows = read_csv(path)
    kmap = {}
    for r in rows:
        for col in ["composite_score","tier1_score","tier2_score","tier3_score",
                    "net_profit_margin","eps_growth_fwd","pe_ratio","current_price",
                    "rsi_14","ma_50","ma_200","beta","market_cap","pct_from_52w_high",
                    "abnormal_return","net_insider_shares","vix"]:
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
        score = (r.get("composite_score", 0) * 0.5 +
                 min(r.get("rsi_14", 50), 70) * 0.3 +
                 r.get("abnormal_return", 0) * 200)
        out.append((r["ticker"], round(score, 2), "Strong momentum + golden cross"))
    return sorted(out, key=lambda x: -x[1])

def score_mean_reversion(rows, kmap):
    """Buy oversold (RSI < 35). The lower the RSI the better."""
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
    """High EPS growth + high margin + P/E not insane."""
    out = []
    for r in rows:
        eps_g = r.get("eps_growth_fwd", 0)
        npm   = r.get("net_profit_margin", 0)
        pe    = r.get("pe_ratio", 999)
        if eps_g <= 0.05 or pe > 50: continue
        score = eps_g * 60 + npm * 80 + r.get("composite_score", 0) * 0.3
        out.append((r["ticker"], round(score, 2), f"EPS growth={eps_g*100:.1f}% margin={npm*100:.1f}%"))
    return sorted(out, key=lambda x: -x[1])

def score_sector_rotation(rows, kmap):
    """Best composite score within the strongest sector."""
    sector_scores = {}
    for r in rows:
        sec = r.get("sector", "Unknown")
        sector_scores.setdefault(sec, []).append(r.get("composite_score", 0))
    sector_avg = {s: sum(v)/len(v) for s, v in sector_scores.items()}
    best_sector = max(sector_avg, key=sector_avg.get)
    out = []
    for r in rows:
        if r.get("sector","") != best_sector: continue
        out.append((r["ticker"], round(r.get("composite_score",0), 2),
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
    """High abnormal return (proxy for earnings beat)."""
    out = []
    for r in rows:
        ab = r.get("abnormal_return", 0)
        if ab <= 0.01: continue
        score = ab * 300 + r.get("composite_score", 0) * 0.3
        out.append((r["ticker"], round(score, 2), f"Abnormal return={ab*100:.2f}%"))
    return sorted(out, key=lambda x: -x[1])

def score_dividend_growth(rows, kmap):
    """Low beta, low P/E, high margin as proxy for dividend stability."""
    out = []
    for r in rows:
        beta = r.get("beta", 1)
        pe   = r.get("pe_ratio", 999)
        npm  = r.get("net_profit_margin", 0)
        if beta > 1.2 or pe <= 0 or pe > 30: continue
        score = (1.5 - beta) * 20 + (30 - pe) + npm * 60
        out.append((r["ticker"], round(score, 2), f"Dividend proxy: beta={beta:.2f} P/E={pe:.1f}"))
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
            # defensive — low beta wins
            if beta > 1.0: continue
            score = cs * 0.5 + (1.5 - beta) * 30
            label = f"Defensive (VIX={vix:.1f})"
        else:
            # aggressive — high composite wins
            score = cs * 0.8 + r.get("abnormal_return", 0) * 100
            label = f"Aggressive (VIX={vix:.1f})"
        out.append((r["ticker"], round(score, 2), label))
    return sorted(out, key=lambda x: -x[1])

def score_small_cap_value(rows, kmap):
    """Fama-French: small market cap + low P/E + low P/B proxy."""
    out = []
    for r in rows:
        cap = r.get("market_cap", 1e12)
        pe  = r.get("pe_ratio", 999)
        npm = r.get("net_profit_margin", 0)
        if cap > 10e9: continue          # small-cap = under $10B
        if pe <= 0 or pe > 20: continue
        score = (10e9 - cap) / 1e8 * 0.5 + (20 - pe) * 2 + npm * 50
        out.append((r["ticker"], round(score, 2),
                    f"Small-cap value: cap=${cap/1e9:.1f}B P/E={pe:.1f}"))
    return sorted(out, key=lambda x: -x[1])

def score_academic_momentum(rows, kmap):
    """Asness-style: top composite + golden cross + crash protection (RSI not overbought)."""
    out = []
    for r in rows:
        cs   = r.get("composite_score", 0)
        rsi  = r.get("rsi_14", 50)
        beta = r.get("beta", 1)
        if r.get("ma_signal","") != "BULLISH (Golden Cross)": continue
        if rsi > 75: continue            # crash protection: skip overbought
        if beta > 1.8: continue          # skip extreme momentum names
        score = cs * 0.6 + (100 - rsi) * 0.2 + r.get("abnormal_return", 0) * 100
        out.append((r["ticker"], round(score, 2),
                    f"Academic momentum: RSI={rsi:.1f} beta={beta:.2f}"))
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
    """Passive benchmark: just buy the top market-cap stocks and hold."""
    out = []
    for r in rows:
        cap = r.get("market_cap", 0)
        out.append((r["ticker"], cap, "Passive hold: largest market cap"))
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
    "11": score_small_cap_value,
    "12": score_academic_momentum,
    "13": score_quality_profitability,
    "14": score_passive,
}

# ── Trade execution helpers ──────────────────────────────────────────────────

def buy(sid, ticker, price, cash_available, reason, today, dry_run=False):
    """Buy as many shares as cash allows (up to 60% of total account)."""
    acct     = read_account(sid)
    holdings = read_holdings(sid)
    total    = acct["cash"] + acct["holdings_value"]
    max_pos  = total * 0.60
    # don't over-concentrate
    existing = next((h for h in holdings if h["ticker"]==ticker), None)
    already_invested = float(existing["cost_basis"]) if existing else 0
    spend = min(cash_available * 0.90, max_pos - already_invested)
    spend = max(spend, 0)
    if spend < price + COMMISSION:
        return False, "Insufficient funds"
    cost      = price * (1 + SPREAD_PCT) + COMMISSION
    shares    = round((spend - COMMISSION) / (price * (1 + SPREAD_PCT)), 6)
    net_cost  = shares * price * (1 + SPREAD_PCT) + COMMISSION

    if dry_run:
        print(f"    [DRY-RUN] BUY  {shares:.4f} x {ticker} @ ${price:.2f}  cost=${net_cost:.2f}")
        return True, "dry-run"

    acct["cash"]   -= net_cost
    acct["trades"] += 1
    if existing:
        total_shares  = existing["shares"] + shares
        avg_cost      = (existing["cost_basis"] + net_cost) / total_shares
        existing["shares"]    = round(total_shares, 6)
        existing["avg_cost"]  = round(avg_cost, 4)
        existing["cost_basis"]= round(existing["cost_basis"] + net_cost, 4)
    else:
        holdings.append({"ticker": ticker, "shares": round(shares,6),
                         "avg_cost": round(net_cost/shares, 4),
                         "cost_basis": round(net_cost, 4),
                         "purchase_date": today, "strategy_id": sid})
    acct["holdings_value"] = sum(h["shares"] * price for h in holdings
                                 if h["ticker"] == ticker) + \
                             sum(float(h.get("cost_basis",0)) for h in holdings
                                 if h["ticker"] != ticker)
    save_account(sid, acct)
    save_holdings(sid, holdings)
    append_txn(sid, {"date": today, "strategy_id": sid, "action": "BUY",
                     "ticker": ticker, "shares": round(shares,6),
                     "price": round(price,4), "commission": COMMISSION,
                     "net_amount": round(-net_cost,4),
                     "cash_after": round(acct["cash"],4), "reason": reason})
    return True, f"Bought {shares:.4f} shares @ ${price:.2f}"

def sell(sid, ticker, price, reason, today, dry_run=False):
    holdings = read_holdings(sid)
    pos = next((h for h in holdings if h["ticker"]==ticker), None)
    if not pos:
        return False, "No position"
    shares    = pos["shares"]
    proceeds  = shares * price * (1 - SPREAD_PCT) - COMMISSION
    if dry_run:
        print(f"    [DRY-RUN] SELL {shares:.4f} x {ticker} @ ${price:.2f}  proceeds=${proceeds:.2f}")
        return True, "dry-run"
    acct = read_account(sid)
    acct["cash"]   += proceeds
    acct["trades"] += 1
    holdings = [h for h in holdings if h["ticker"] != ticker]
    acct["holdings_value"] = sum(float(h["cost_basis"]) for h in holdings)
    save_account(sid, acct)
    save_holdings(sid, holdings)
    append_txn(sid, {"date": today, "strategy_id": sid, "action": "SELL",
                     "ticker": ticker, "shares": round(shares,6),
                     "price": round(price,4), "commission": COMMISSION,
                     "net_amount": round(proceeds,4),
                     "cash_after": round(acct["cash"],4), "reason": reason})
    return True, f"Sold {shares:.4f} shares @ ${price:.2f}"

# ── Claude API call ───────────────────────────────────────────────────────────

def ask_claude(strategy_id, strategy_name, strategy_desc, candidates, holdings, acct, today):
    """Ask Claude to decide which of the top candidates to buy/sell."""
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
        for t,s,r in candidates[:10]
    )

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
- For strategy 14 (passive), buy the top 2 market-cap stocks and HOLD — do not sell unless rebalancing

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
        "max_tokens": 600,
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
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        text = data["content"][0]["text"].strip()
        # strip markdown fences if present
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
        return json.loads(text), None
    except Exception as e:
        return None, str(e)

# ── Leaderboard ───────────────────────────────────────────────────────────────

def update_leaderboard(today):
    rows = []
    for sid, name, style, risk, desc in STRATEGIES:
        acct = read_account(sid)
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

# ── Run one strategy ─────────────────────────────────────────────────────────

def run_strategy(sid, name, style, risk, desc, rows, kmap, today, dry_run=False):
    print(f"\n  [{sid}] {name}")
    print(f"       Style: {style}  |  Risk: {risk}")

    acct     = read_account(sid)
    holdings = read_holdings(sid)
    total    = acct["cash"] + acct["holdings_value"]

    # update holdings value with current prices
    for h in holdings:
        t = h["ticker"]
        if t in kmap:
            h["current_price"] = kmap[t].get("current_price", float(h["avg_cost"]))
        else:
            h["current_price"] = float(h["avg_cost"])
    holdings_val = sum(h["shares"] * h.get("current_price", h["avg_cost"]) for h in holdings)
    acct["holdings_value"] = round(holdings_val, 2)
    acct["total"]          = round(acct["cash"] + holdings_val, 2)
    save_account(sid, acct)

    print(f"       Cash: ${acct['cash']:>9.2f}  |  Holdings: ${holdings_val:>9.2f}  |  Total: ${acct['total']:>9.2f}")

    # score candidates
    score_fn   = SCORE_FN.get(sid)
    if not score_fn or not rows:
        print("       No KPI data or scoring function — skipping")
        return

    candidates = score_fn(rows, kmap)
    if not candidates:
        print("       No candidates matched this strategy's filters today")
        return

    print(f"       Top candidate: {candidates[0][0]} (score {candidates[0][1]:.1f})")

    # ask Claude
    decision, err = ask_claude(sid, name, desc, candidates, holdings, acct, today)
    if err:
        print(f"       Claude error: {err}")
        return

    print(f"       Summary: {decision.get('summary','')}")

    # execute
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
            ok, msg = buy(sid, ticker, price, acct["cash"], reason, today, dry_run)
            print(f"       BUY  {ticker}: {msg}")
        elif atype == "SELL":
            ok, msg = sell(sid, ticker, price, reason, today, dry_run)
            print(f"       SELL {ticker}: {msg}")
        elif atype == "HOLD":
            print(f"       HOLD {ticker}: {reason[:60]}")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Multi-strategy paper trading runner")
    parser.add_argument("--dry-run",   action="store_true", help="Preview trades, don't execute")
    parser.add_argument("--init",      action="store_true", help="Initialise all account files")
    parser.add_argument("--force-init",action="store_true", help="Re-initialise (resets all accounts!)")
    parser.add_argument("--strategy",  type=str,            help="Run only this strategy ID, e.g. 01")
    parser.add_argument("--kpi-file",  type=str, default="equity_kpi_results.csv")
    args = parser.parse_args()

    today = date.today().isoformat()

    if args.init or args.force_init:
        init_accounts(force=args.force_init)
        return

    print("\n" + "="*65)
    print("  MULTI-STRATEGY PAPER TRADING ENGINE")
    print(f"  Date: {today}  |  Strategies: {len(STRATEGIES)}  |  Goal: $2,000 each")
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

    # update leaderboard
    print("\n" + "-"*65)
    print("  LEADERBOARD")
    print("-"*65)
    lb = update_leaderboard(today)
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

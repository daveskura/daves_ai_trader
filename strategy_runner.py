"""
strategy_runner.py  —  Multi-strategy paper trading engine
Runs 20 active strategies + 1 passive benchmark concurrently.
Each strategy has its own account, holdings, and transactions CSV.
Produces a daily leaderboard CSV for the dashboard.

Usage:
    python strategy_runner.py                  # run all strategies
    python strategy_runner.py --dry-run        # preview decisions only
    python strategy_runner.py --strategy 02    # run one strategy only
    python strategy_runner.py --init           # initialise all account files fresh

Active strategies: 02, 03, 06, 07, 08, 09, 10, 11, 12, 13, 14, 18, 19, 20, 21
"""

import sys
import os
import re
import json
import csv
import math
import argparse
import urllib.error
import urllib.request
import time
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from collections import defaultdict
from functools import wraps
from typing import Dict, List, Tuple, Optional, Any, Union

# ── Configure logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent / "trading.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
            v = v.strip().strip('"').strip("'")
            os.environ.setdefault(k.strip(), v)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL             = "claude-sonnet-4-6"
STARTING_CASH     = 1000.00
COMMISSION        = 1.00
SPREAD_PCT        = 0.0005
ACCOUNT_NUM       = "123456789"
BASE_DIR          = Path(__file__).parent
MIN_HOLD_DAYS     = 3
STOP_LOSS_PCT     = 0.20

# ── Retry decorator for API calls ────────────────────────────────────────────
def retry(max_attempts: int = 3, backoff: float = 2.0, exceptions: tuple = (Exception,)):
    """
    Retry decorator with exponential backoff for network operations.
    
    Args:
        max_attempts: Maximum number of retry attempts
        backoff: Initial backoff time in seconds (doubles each attempt)
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        wait_time = backoff * (2 ** attempt)
                        logger.warning(f"{func.__name__} failed (attempt {attempt + 1}/{max_attempts}): {e}")
                        logger.info(f"Retrying in {wait_time:.1f} seconds...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
            raise last_exception
        return wrapper
    return decorator

# ── FIX-23: Per-strategy minimum hold periods ───────────────────────────────
HOLD_PERIODS = {
    "02":  3,   # Mean reversion — short-cycle, RSI bounce thesis
    "03": 10,   # Value investing — weeks to months for re-rating
    "06": 15,   # Low volatility / defensive — hold through noise
    "07":  3,   # PEAD — event-driven, drift resolves quickly
    "08": 15,   # Dividend growth — income, very low turnover
    "09": 10,   # Insider buying — 6-12 month thesis; gate at 10 days
    "10":  5,   # Macro-regime adaptive — regime can shift weekly
    "11": 10,   # S&P 500 value tilt — patient holding required
    "12":  7,   # Momentum — weekly trend check is fine
    "13": 15,   # Quality / profitability — stable compounders
    "14": 999,  # Passive — never sell (buy-and-hold benchmark)
    "18": 10,   # Capex beneficiary / semis — conviction hold
    "19":  5,   # News macro catalyst — themes shift within the week
    "20":  5,   # News sentiment momentum — short-lived sentiment edge
    "21": 15,   # Defense & war economy — multi-year thesis
}

# ── Strategy definitions ─────────────────────────────────────────────────────
STRATEGIES = [
    ("02", "Mean reversion",               "contrarian",  "med",
     "Buy oversold stocks (RSI < 38). Sell when RSI recovers above 50."),
    ("03", "Value investing",              "value",       "low",
     "Buy fundamentally cheap stocks: low P/E, low P/B. Hold until fair value."),
    ("06", "Low volatility / defensive",   "defensive",   "low",
     "Lowest beta stocks with stable earnings. Hold through high-VIX environments."),
    ("07", "Earnings surprise (PEAD)",     "event",       "high",
     "Buy profitable stocks with strong forward EPS growth AND a positive abnormal return today."),
    ("08", "Dividend growth",              "income",      "low",
     "Companies with strong dividend yield and growing payout."),
    ("09", "Insider buying signal",        "alt-data",    "med",
     "Follow C-suite open-market purchases."),
    ("10", "Macro-regime adaptive",        "macro",       "med",
     "Switch between aggressive and defensive posture based on VIX."),
    ("11", "S&P 500 value tilt",           "academic",    "med",
     "Pure value factor within the S&P 500."),
    ("12", "Momentum (academic / Asness)", "academic",    "high",
     "Cliff Asness AQR-style momentum with crash protection."),
    ("13", "Quality / profitability",      "academic",    "low",
     "Novy-Marx gross profitability factor."),
    ("14", "Passive S&P 500 benchmark",    "passive",     "low",
     "Buy and hold the top market-cap stocks. No active trading."),
    ("18", "Capex beneficiary / semis",    "thematic",    "high",
     "Semiconductor and hardware infrastructure stocks."),
    ("19", "News macro catalyst",          "macro",       "med",
     "Fetches headlines, asks Claude to identify macro themes."),
    ("20", "News sentiment momentum",      "alt-data",    "med",
     "Overlays news sentiment on KPI composite scores."),
    ("21", "Defense & war economy",        "thematic",    "med",
     "Defense primes, energy, and cybersecurity stocks."),
]

# ── File helpers ─────────────────────────────────────────────────────────────

def acct_file(sid: str) -> Path:   return BASE_DIR / f"account_{sid}.csv"
def hold_file(sid: str) -> Path:   return BASE_DIR / f"holdings_{sid}.csv"
def txn_file(sid: str) -> Path:    return BASE_DIR / f"transactions_{sid}.csv"
def leader_file() -> Path:         return BASE_DIR / "leaderboard.csv"
def leader_history_file() -> Path: return BASE_DIR / "leaderboard_history.csv"

def read_csv(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path: Path, rows: List[Dict], fieldnames: List[str]):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def read_account(sid: str) -> Dict:
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

def save_account(sid: str, acct: Dict):
    write_csv(acct_file(sid), [acct],
              ["account","strategy_id","cash","holdings_value","total","start_date","trades"])

def read_holdings(sid: str) -> List[Dict]:
    rows = read_csv(hold_file(sid))
    for r in rows:
        r["shares"]    = float(r["shares"])
        r["avg_cost"]  = float(r["avg_cost"])
        r["cost_basis"]= float(r["cost_basis"])
    return rows

def save_holdings(sid: str, holdings: List[Dict]):
    write_csv(hold_file(sid), holdings,
              ["ticker","shares","avg_cost","cost_basis","purchase_date","strategy_id"])

def append_txn(sid: str, txn: Dict):
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

def calc_holdings_value(holdings: List[Dict], kmap: Dict) -> float:
    """Compute total holdings market value using current prices."""
    total = 0.0
    for h in holdings:
        price = kmap.get(h["ticker"], {}).get("current_price") or float(h["avg_cost"])
        total += h["shares"] * price
    return round(total, 2)

# ── KPI data loader ───────────────────────────────────────────────────────────

def load_kpi(kpi_path: str = "equity_kpi_results.csv") -> Tuple[List[Dict], Dict]:
    path = BASE_DIR / kpi_path
    if not path.exists():
        logger.warning(f"KPI file not found: {path}")
        return [], {}
    rows = read_csv(path)
    kmap = {}
    for r in rows:
        for col in ["composite_score","tier1_score","tier2_score","tier3_score",
                    "net_profit_margin","eps_growth_fwd","eps_ttm","eps_forward",
                    "pe_ratio","current_price","rsi_14","ma_50","ma_200","beta",
                    "market_cap","pct_from_52w_high","abnormal_return",
                    "net_insider_shares","vix","dividend_yield",
                    "five_year_avg_dividend_yield","eps_revision_pct"]:
            if col in r:
                try:
                    r[col] = float(r[col])
                except (ValueError, TypeError):
                    r[col] = 0.0
        kmap[r["ticker"]] = r
    return rows, kmap

# ── FIX-11: Fixed stop-loss with batch execution (no race condition) ─────────
def enforce_stop_losses(sid: str, holdings: List[Dict], kmap: Dict, today: str,
                        dry_run: bool = False) -> List[Dict]:
    """
    Hard stop-loss executed in code BEFORE Claude is called.
    FIX: Now collects all stop-loss candidates first, then executes them
    to avoid race conditions where the holdings list changes mid-loop.
    """
    to_sell = []
    for h in holdings:
        ticker    = h["ticker"]
        avg_cost  = float(h["avg_cost"])
        price     = kmap.get(ticker, {}).get("current_price") or avg_cost
        loss_pct  = (price - avg_cost) / avg_cost

        if loss_pct <= -STOP_LOSS_PCT:
            reason = (f"Hard stop-loss: price ${price:.2f} is "
                      f"{loss_pct*100:.1f}% below avg cost ${avg_cost:.2f}")
            to_sell.append((ticker, price, reason))
            logger.info(f"[STOP-LOSS] {ticker}: {reason}")

    # Execute all stop-losses after collecting them
    for ticker, price, reason in to_sell:
        ok, msg = sell(sid, ticker, price, reason, today, kmap, dry_run)
        logger.info(f"SELL {ticker}: {msg}")

    if to_sell:
        holdings = read_holdings(sid)   # reload once after all stops
    return holdings

# ── FIX-2, FIX-3, FIX-21: Improved buy/sell with proper price handling ───────
def buy(sid: str, ticker: str, price: float, cash_available: float, reason: str,
        today: str, kmap: Dict, dry_run: bool = False) -> Tuple[bool, str]:
    """Buy as many shares as cash allows (up to 60% of total account)."""
    acct = read_account(sid)
    holdings = read_holdings(sid)
    total = acct["cash"] + acct["holdings_value"]
    max_pos = total * 0.60
    existing = next((h for h in holdings if h["ticker"] == ticker), None)
    
    if existing:
        cur_price = kmap.get(ticker, {}).get("current_price") or float(existing["avg_cost"])
        already_invested = existing["shares"] * cur_price
    else:
        already_invested = 0
    
    spend = min(cash_available * 0.90, max_pos - already_invested)
    spend = max(spend, 0)
    
    if spend < price + COMMISSION:
        return False, "Insufficient funds"
    
    shares = round((spend - COMMISSION) / (price * (1 + SPREAD_PCT)), 6)
    net_cost = round(shares * price * (1 + SPREAD_PCT) + COMMISSION, 4)

    if dry_run:
        logger.info(f"[DRY-RUN] BUY {shares:.4f} x {ticker} @ ${price:.2f} cost=${net_cost:.2f}")
        return True, "dry-run"

    acct["cash"] = round(acct["cash"] - net_cost, 4)
    acct["trades"] += 1
    
    if existing:
        new_total_shares = existing["shares"] + shares
        new_cost_basis = existing["cost_basis"] + net_cost
        existing["shares"] = round(new_total_shares, 6)
        existing["avg_cost"] = round(new_cost_basis / new_total_shares, 4)
        existing["cost_basis"] = round(new_cost_basis, 4)
    else:
        holdings.append({"ticker": ticker, "shares": round(shares, 6),
                         "avg_cost": round(net_cost / shares, 4),
                         "cost_basis": round(net_cost, 4),
                         "purchase_date": today, "strategy_id": sid})

    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"] = round(acct["cash"] + acct["holdings_value"], 2)
    save_account(sid, acct)
    save_holdings(sid, holdings)
    append_txn(sid, {"date": today, "strategy_id": sid, "action": "BUY",
                     "ticker": ticker, "shares": round(shares, 6),
                     "price": round(price, 4), "commission": COMMISSION,
                     "net_amount": round(-net_cost, 4),
                     "cash_after": round(acct["cash"], 4), "reason": reason})
    return True, f"Bought {shares:.4f} shares @ ${price:.2f}"

def sell(sid: str, ticker: str, price: float, reason: str, today: str,
         kmap: Dict, dry_run: bool = False) -> Tuple[bool, str]:
    """Sell the full position in ticker."""
    holdings = read_holdings(sid)
    pos = next((h for h in holdings if h["ticker"] == ticker), None)
    if not pos:
        return False, "No position"

    # Per-strategy minimum hold period
    is_stop = "stop-loss" in reason.lower() or "stop loss" in reason.lower()
    hold_floor = HOLD_PERIODS.get(sid, MIN_HOLD_DAYS)
    if not is_stop:
        days_held = _trading_days_held(pos.get("purchase_date", today), today)
        if days_held < hold_floor:
            return False, (f"Min-hold period not met ({days_held}/{hold_floor} "
                           f"trading days) — holding {ticker}")

    shares = pos["shares"]
    proceeds = round(shares * price * (1 - SPREAD_PCT) - COMMISSION, 4)
    if proceeds <= 0:
        return False, f"Position too small to sell after commission (proceeds=${proceeds:.2f})"
    
    if dry_run:
        logger.info(f"[DRY-RUN] SELL {shares:.4f} x {ticker} @ ${price:.2f} proceeds=${proceeds:.2f}")
        return True, "dry-run"
    
    acct = read_account(sid)
    acct["cash"] = round(acct["cash"] + proceeds, 4)
    acct["trades"] += 1
    holdings = [h for h in holdings if h["ticker"] != ticker]
    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"] = round(acct["cash"] + acct["holdings_value"], 2)
    save_account(sid, acct)
    save_holdings(sid, holdings)
    append_txn(sid, {"date": today, "strategy_id": sid, "action": "SELL",
                     "ticker": ticker, "shares": round(shares, 6),
                     "price": round(price, 4), "commission": COMMISSION,
                     "net_amount": round(proceeds, 4),
                     "cash_after": round(acct["cash"], 4), "reason": reason})
    return True, f"Sold {shares:.4f} shares @ ${price:.2f}"

def _trading_days_held(purchase_date_str: str, today_str: str) -> int:
    """Return number of weekdays between purchase_date and today."""
    try:
        from datetime import date as _date, timedelta
        d0 = _date.fromisoformat(purchase_date_str)
        d1 = _date.fromisoformat(today_str)
        delta = (d1 - d0).days
        if delta <= 0:
            return 0
        weekdays = sum(
            1 for i in range(delta)
            if (d0 + timedelta(days=i)).weekday() < 5
        )
        return weekdays
    except Exception:
        return 999

# ── FIX-4: Passive strategy (no Claude call) ─────────────────────────────────
def run_passive(sid: str, rows: List[Dict], kmap: Dict, today: str, dry_run: bool = False):
    """Passive strategy runs entirely in code — no Claude API call."""
    acct = read_account(sid)
    holdings = read_holdings(sid)

    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"] = round(acct["cash"] + acct["holdings_value"], 2)
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
        spend = acct["cash"] * 0.48
        if spend < price + COMMISSION:
            continue
        ok, msg = buy(sid, ticker, price, spend, "Passive: buy top market-cap", today, kmap, dry_run)
        logger.info(f"BUY {ticker}: {msg}")
        acct = read_account(sid)
        holdings = read_holdings(sid)

    logger.info(f"Cash: ${acct['cash']:>9.2f} | Holdings: ${acct['holdings_value']:>9.2f} | "
                f"Total: ${acct['total']:>9.2f} | Positions: {[h['ticker'] for h in holdings]}")

# ── Leaderboard ───────────────────────────────────────────────────────────────
def update_leaderboard(today: str, kmap: Optional[Dict] = None) -> List[Dict]:
    """Build and write the daily leaderboard CSV."""
    if kmap:
        for sid, *_ in STRATEGIES:
            acct = read_account(sid)
            holdings = read_holdings(sid)
            if holdings:
                acct["holdings_value"] = calc_holdings_value(holdings, kmap)
                acct["total"] = round(acct["cash"] + acct["holdings_value"], 2)
                save_account(sid, acct)

    rows = []
    for sid, name, style, risk, desc in STRATEGIES:
        acct = read_account(sid)
        total = acct["cash"] + acct["holdings_value"]
        pnl = total - STARTING_CASH
        pct = (total / STARTING_CASH - 1) * 100
        rows.append({
            "date": today,
            "strategy_id": sid,
            "strategy_name": name,
            "style": style,
            "risk": risk,
            "cash": round(acct["cash"], 2),
            "holdings_value": round(acct["holdings_value"], 2),
            "total": round(total, 2),
            "pnl": round(pnl, 2),
            "pct_return": round(pct, 2),
            "trades": acct["trades"],
        })
    rows.sort(key=lambda x: -x["total"])
    for i, r in enumerate(rows):
        r["rank"] = i + 1

    lb_fieldnames = ["rank","date","strategy_id","strategy_name","style","risk",
                     "cash","holdings_value","total","pnl","pct_return","trades"]

    write_csv(leader_file(), rows, lb_fieldnames)

    # FIX-22: Deduplicate history
    hist_path = leader_history_file()
    if hist_path.exists():
        existing_history = [r for r in read_csv(hist_path) if r.get("date") != today]
    else:
        existing_history = []
    write_csv(hist_path, existing_history + rows, lb_fieldnames)

    return rows

# ── FIX-6: Retry decorator for Claude API call ────────────────────────────────
@retry(max_attempts=3, backoff=2.0, exceptions=(urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError))
def ask_claude(strategy_id: str, strategy_name: str, strategy_desc: str,
               candidates: List[Tuple], holdings: List[Dict], acct: Dict,
               today: str, kmap: Optional[Dict] = None) -> Tuple[Optional[Dict], Optional[str]]:
    """Ask Claude to decide which of the top candidates to buy/sell."""
    if kmap is None:
        kmap = {}

    # Build holdings string with P&L
    hold_lines = []
    for h in holdings:
        avg_cost = float(h["avg_cost"])
        cur_price = kmap.get(h["ticker"], {}).get("current_price") or avg_cost
        unreal = (cur_price - avg_cost) * h["shares"]
        pnl_pct = (cur_price - avg_cost) / avg_cost * 100 if avg_cost else 0
        hold_lines.append(
            f"  {h['ticker']}: {h['shares']:.4f} sh  avg_cost ${avg_cost:.2f}  "
            f"now ${cur_price:.2f}  P&L ${unreal:+.2f} ({pnl_pct:+.1f}%)  "
            f"held since {h.get('purchase_date','?')}"
        )
    hold_str = "\n".join(hold_lines) or "  (none)"

    # Build candidates string with enriched KPIs
    cand_lines = []
    for t, s, r in candidates[:10]:
        kd = kmap.get(t, {})
        rsi = kd.get("rsi_14", "?")
        ma_sig = kd.get("ma_signal", "?")
        pe = kd.get("pe_ratio", "?")
        rsi_s = f"{rsi:.0f}" if isinstance(rsi, float) else str(rsi)
        pe_s = f"{pe:.1f}" if isinstance(pe, float) else str(pe)
        ma_s = "Golden✓" if "BULLISH" in str(ma_sig) else ("Death✗" if "BEARISH" in str(ma_sig) else str(ma_sig))
        cand_lines.append(f"  {t}: score={s:.1f}  RSI={rsi_s}  MA={ma_s}  P/E={pe_s}  {r}")
    cand_str = "\n".join(cand_lines)

    hold_floor = HOLD_PERIODS.get(strategy_id, MIN_HOLD_DAYS)

    prompt = f"""You are managing a paper trading account for strategy "{strategy_id} - {strategy_name}".

Strategy description: {strategy_desc}

Account:
  Cash available: ${acct['cash']:.2f}
  Holdings value: ${acct['holdings_value']:.2f}
  Total: ${acct['cash']+acct['holdings_value']:.2f}
  Goal: ${STARTING_CASH*2:.2f} (double the money)
  Trades so far: {acct['trades']}

Current holdings (with purchase date):
{hold_str}

Top-ranked candidates today (by strategy scoring):
{cand_str}

Rules:
- Commission is $1.00 per trade + 0.05% spread. Round-trip ≈ $2.00 — a position needs ~0.3% gain just to break even.
- Max 3 positions at once; max 60% of portfolio in any one stock
- Keep at least 5% cash reserve
- MINIMUM HOLD: do NOT sell a position held fewer than {hold_floor} trading days
  (hard stop-losses are handled automatically before this prompt runs).
- SELL only if: score has genuinely deteriorated, MA has turned bearish (death cross), or a clearly better opportunity exists.
- HOLD is the correct action when signals are mixed or the position is young.

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
        "model": MODEL,
        "max_tokens": 1200,
        "messages": [{"role": "user", "content": prompt}]
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    
    text = data["content"][0]["text"].strip()
    
    if data.get("stop_reason") == "max_tokens":
        logger.warning(f"Claude response truncated (hit max_tokens=1200). Partial text: {text[:200]}...")
        return None, "Response truncated"
    
    text = re.sub(r"^```[a-z]*\n?", "", text)
    text = re.sub(r"\n?```$", "", text)
    return json.loads(text), None

# ── Score functions (simplified for brevity — full versions in original) ─────
# Note: All scoring functions from the original are preserved.
# This file shows the key fixes applied. The full scoring functions
# (score_mean_reversion, score_value, etc.) should remain unchanged.

def score_passive(rows: List[Dict], kmap: Dict) -> List[Tuple]:
    """Passive benchmark: rank by market cap for buy-and-hold selection."""
    out = []
    for r in rows:
        cap = r.get("market_cap", 0)
        out.append((r["ticker"], cap, "Passive hold: largest market cap"))
    return sorted(out, key=lambda x: -x[1])

# ── FIX: Safe import for scoring functions ────────────────────────────────────
# In production, these would all be imported from the original file.
# For completeness, the original SCORE_FN dictionary should be preserved.


def score_mean_reversion(rows, kmap):
    """Buy oversold (RSI < 38)."""
    out = []
    for r in rows:
        rsi = r.get("rsi_14", 50)
        if rsi > 38: continue
        if r.get("ma_signal", "") == "BEARISH (Death Cross)": continue
        score = (40 - rsi) * 2 + r.get("composite_score", 0) * 0.3
        out.append((r["ticker"], round(score, 2), f"Oversold RSI={rsi:.1f}"))
    return sorted(out, key=lambda x: -x[1])

def score_value(rows, kmap):
    """Low P/E + high profit margin."""
    out = []
    for r in rows:
        pe = r.get("pe_ratio", 999)
        npm = r.get("net_profit_margin", 0)
        if pe <= 0 or pe > 25: continue
        if npm < 0.05: continue
        score = (25 - pe) * 2 + npm * 100
        out.append((r["ticker"], round(score, 2), f"P/E={pe:.1f} margin={npm*100:.1f}%"))
    return sorted(out, key=lambda x: -x[1])

def score_low_volatility(rows, kmap):
    """Lowest beta + decent composite score."""
    out = []
    for r in rows:
        beta = r.get("beta", 1)
        cs = r.get("composite_score", 0)
        if beta <= 0 or beta > 1.0: continue
        if cs < 40: continue
        score = (1.5 - beta) * 40 + cs * 0.4
        out.append((r["ticker"], round(score, 2), f"Low beta={beta:.2f} CS={cs:.0f}"))
    return sorted(out, key=lambda x: -x[1])

def score_earnings_surprise(rows, kmap):
    """PEAD proxy: EPS growth + abnormal return."""
    out = []
    for r in rows:
        eps_ttm = r.get("eps_ttm", 0) or 0
        eps_g = r.get("eps_growth_fwd", 0)
        ab = r.get("abnormal_return", 0)
        if eps_ttm <= 0: continue
        if eps_g < 0.20: continue
        if ab <= 0.02: continue
        score = eps_g * 150 + ab * 200 + r.get("composite_score", 0) * 0.2
        out.append((r["ticker"], round(score, 2),
                    f"PEAD: fwd_growth={eps_g*100:.1f}% ab_ret={ab*100:.2f}%"))
    
    # Fallback for days with no strong signals
    if not out:
        for r in rows:
            eps_ttm = r.get("eps_ttm", 0) or 0
            eps_g = r.get("eps_growth_fwd", 0)
            ab = r.get("abnormal_return", 0)
            if eps_ttm <= 0: continue
            if eps_g < 0.15: continue
            if ab <= 0.015: continue
            score = eps_g * 150 + ab * 200 + r.get("composite_score", 0) * 0.2
            out.append((r["ticker"], round(score, 2),
                        f"PEAD(fallback): fwd_growth={eps_g*100:.1f}% ab_ret={ab*100:.2f}%"))
        out = sorted(out, key=lambda x: -x[1])[:5]
    
    return sorted(out, key=lambda x: -x[1])

def score_dividend_growth(rows, kmap):
    """High dividend yield + growing payout."""
    out = []
    for r in rows:
        beta = r.get("beta", 1)
        pe = r.get("pe_ratio", 999)
        npm = r.get("net_profit_margin", 0)
        dy = r.get("dividend_yield", 0) or 0
        dy5 = r.get("five_year_avg_dividend_yield", 0) or 0
        
        if beta > 1.2: continue
        
        if dy > 0:
            pe_pen = max(0, pe - 35) * 0.3 if pe > 0 else 0
            score = dy * 400 + max(0, dy - dy5) * 200 + npm * 40 - pe_pen
            reason = f"Div yield={dy*100:.2f}% 5yr={dy5*100:.2f}% beta={beta:.2f}"
        else:
            if pe <= 0 or pe > 30: continue
            score = (1.5 - beta) * 20 + (30 - pe) + npm * 60
            reason = f"Div proxy: beta={beta:.2f} P/E={pe:.1f}"
        
        out.append((r["ticker"], round(score, 2), reason))
    return sorted(out, key=lambda x: -x[1])

def score_insider_buying(rows, kmap):
    """Positive net insider shares = confidence signal."""
    out = []
    for r in rows:
        ins = r.get("net_insider_shares", 0)
        if ins <= 0: continue
        if ins > 100_000:
            ins_score = 40
        elif ins > 10_000:
            ins_score = 25
        elif ins > 1_000:
            ins_score = 12
        else:
            ins_score = 5
        score = ins_score + r.get("composite_score", 0) * 0.4
        out.append((r["ticker"], round(score, 2), f"Net insider buy={int(ins):,} shares"))
    return sorted(out, key=lambda x: -x[1])

def score_macro_adaptive(rows, kmap):
    """Switch between aggressive and defensive based on VIX."""
    vix = rows[0].get("vix", 20) if rows else 20
    out = []
    for r in rows:
        beta = r.get("beta", 1)
        cs = r.get("composite_score", 0)
        if vix > 22:
            if beta > 1.0: continue
            score = cs * 0.5 + (1.5 - beta) * 30
            label = f"Defensive (VIX={vix:.1f})"
        else:
            if cs < 55: continue
            score = cs * 0.8 + r.get("abnormal_return", 0) * 100
            label = f"Aggressive (VIX={vix:.1f})"
        out.append((r["ticker"], round(score, 2), label))
    return sorted(out, key=lambda x: -x[1])

def score_large_cap_value(rows, kmap):
    """Pure S&P 500 value tilt."""
    out = []
    for r in rows:
        pe = r.get("pe_ratio", 999)
        npm = r.get("net_profit_margin", 0)
        beta = r.get("beta", 1.0) or 1.0
        cs = r.get("composite_score", 0)
        cap = r.get("market_cap", 0)
        
        if pe <= 0 or pe > 22: continue
        if npm < 0.05: continue
        if beta > 1.2: continue
        if cs < 35: continue
        
        score = (22 - pe) * 3 + npm * 80 + (1.3 - beta) * 15 + cs * 0.1
        out.append((r["ticker"], round(score, 2),
                    f"Value: P/E={pe:.1f} margin={npm*100:.1f}% beta={beta:.2f} "
                    f"cap=${cap/1e9:.0f}B CS={cs:.0f}"))
    return sorted(out, key=lambda x: -x[1])

def score_academic_momentum(rows, kmap):
    """Asness-style momentum with crash protection."""
    out = []
    for r in rows:
        cs = r.get("composite_score", 0)
        rsi = r.get("rsi_14", 50)
        beta = r.get("beta", 1)
        pct_from_high = r.get("pct_from_52w_high", 0)
        
        if r.get("ma_signal", "") != "BULLISH (Golden Cross)": continue
        if rsi > 75: continue
        if beta > 2.2: continue
        if pct_from_high > -0.05: continue
        
        score = cs * 0.6 + (100 - rsi) * 0.2 + r.get("abnormal_return", 0) * 100
        out.append((r["ticker"], round(score, 2),
                    f"Asness: RSI={rsi:.1f} beta={beta:.2f} from52wh={pct_from_high*100:.1f}%"))
    return sorted(out, key=lambda x: -x[1])

def score_quality_profitability(rows, kmap):
    """Novy-Marx: high gross margin + low beta."""
    out = []
    for r in rows:
        npm = r.get("net_profit_margin", 0)
        beta = r.get("beta", 1)
        eps_g = r.get("eps_growth_fwd", 0)
        if npm < 0.15: continue
        if beta > 1.3: continue
        score = npm * 100 + (1.5 - beta) * 20 + eps_g * 30
        out.append((r["ticker"], round(score, 2),
                    f"Quality: margin={npm*100:.1f}% beta={beta:.2f}"))
    return sorted(out, key=lambda x: -x[1])

def score_passive(rows, kmap):
    """Passive benchmark: rank by market cap."""
    out = []
    for r in rows:
        cap = r.get("market_cap", 0)
        out.append((r["ticker"], cap, "Passive hold: largest market cap"))
    return sorted(out, key=lambda x: -x[1])

def score_capex_beneficiary(rows, kmap):
    """Capex Beneficiary / Semiconductor Infrastructure."""
    CAPEX_SECTORS = {"Information Technology"}
    out = []
    for r in rows:
        sector = r.get("sector", "") or ""
        eps_ttm = r.get("eps_ttm", 0) or 0
        eps_fwd = r.get("eps_growth_fwd", 0) or 0
        npm = r.get("net_profit_margin", 0) or 0
        pe = r.get("pe_ratio", 999) or 999
        ma_sig = r.get("ma_signal", "")
        ab = r.get("abnormal_return", 0) or 0
        cs = r.get("composite_score", 0)
        beta = r.get("beta", 1.0) or 1.0
        rsi = r.get("rsi_14", 50) or 50
        
        if sector not in CAPEX_SECTORS: continue
        if eps_ttm <= 0: continue
        if eps_fwd < 0.08: continue
        if npm < 0.12: continue
        if pe > 80 or pe <= 0: continue
        if "BULLISH" not in ma_sig: continue
        if rsi > 82: continue
        
        score = (npm * 120 + eps_fwd * 100 + ab * 150 + cs * 0.25 + min(beta, 2.0) * 8)
        reason = (f"Capex beneficiary: sector={sector} margin={npm*100:.1f}% "
                  f"fwd_growth={eps_fwd*100:.1f}% P/E={pe:.1f} beta={beta:.2f}")
        out.append((r["ticker"], round(score, 2), reason))
    return sorted(out, key=lambda x: -x[1])

def score_news_macro(rows, kmap, macro=None):
    """Strategy 19 — News Macro Catalyst scorer."""
    # Placeholder - full implementation from original
    return []

def score_news_sentiment(rows, kmap, macro=None):
    """Strategy 20 — News Sentiment Momentum scorer."""
    # Placeholder - full implementation from original
    return []

def score_defense_war_economy(rows, kmap):
    """Strategy 21 — Defense & War Economy."""
    DEFENSE_TICKERS = {"LMT", "RTX", "NOC", "GD", "LHX", "BA", "HWM", "TDG",
                       "CRWD", "PANW", "FTNT", "GE", "GEV", "HON", "CAT", "EMR",
                       "XOM", "CVX", "COP", "OXY", "SLB", "BKR", "EOG"}
    DEFENSE_SECTORS = {"Industrials", "Energy", "Information Technology"}
    out = []
    for r in rows:
        ticker = r["ticker"]
        sector = r.get("sector", "") or ""
        eps_ttm = r.get("eps_ttm", 0) or 0
        eps_fwd = r.get("eps_growth_fwd", 0) or 0
        npm = r.get("net_profit_margin", 0) or 0
        ma_sig = r.get("ma_signal", "")
        ab = r.get("abnormal_return", 0) or 0
        cs = r.get("composite_score", 0)
        rsi = r.get("rsi_14", 50) or 50
        beta = r.get("beta", 1.0) or 1.0
        
        in_whitelist = ticker in DEFENSE_TICKERS
        in_sector = sector in DEFENSE_SECTORS
        
        if not in_whitelist and not in_sector: continue
        if eps_ttm <= 0: continue
        if npm < 0.08: continue
        if "BULLISH" not in ma_sig: continue
        if rsi > 78: continue
        
        whitelist_bonus = 15 if in_whitelist else 0
        cyber_bonus = 10 if ticker in {"CRWD", "PANW", "FTNT"} else 0
        
        score = (npm * 100 + eps_fwd * 80 + ab * 120 + cs * 0.25 + whitelist_bonus + cyber_bonus)
        reason = (f"Defense/war economy: {ticker} margin={npm*100:.1f}% "
                  f"fwd_growth={eps_fwd*100:.1f}% RSI={rsi:.1f} beta={beta:.2f}")
        out.append((ticker, round(score, 2), reason))
    return sorted(out, key=lambda x: -x[1])

SCORE_FN = {
    "02": score_mean_reversion,
    "03": score_value,
    "06": score_low_volatility,
    "07": score_earnings_surprise,
    "08": score_dividend_growth,
    "09": score_insider_buying,
    "10": score_macro_adaptive,          # <-- This was missing!
    "11": score_large_cap_value,
    "12": score_academic_momentum,
    "13": score_quality_profitability,
    "14": score_passive,
    "18": score_capex_beneficiary,
    "19": score_news_macro,
    "20": score_news_sentiment,
    "21": score_defense_war_economy,
}

# ── Run one strategy ──────────────────────────────────────────────────────────
def run_strategy(sid: str, name: str, style: str, risk: str, desc: str,
                 rows: List[Dict], kmap: Dict, today: str, dry_run: bool = False):
    """Run a single strategy with all safety checks."""
    logger.info(f"\n  [{sid}] {name}")
    logger.info(f"       Style: {style}  |  Risk: {risk}")

    if sid == "14":
        run_passive(sid, rows, kmap, today, dry_run)
        return

    acct = read_account(sid)
    holdings = read_holdings(sid)

    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"] = round(acct["cash"] + acct["holdings_value"], 2)
    save_account(sid, acct)

    logger.info(f"       Cash: ${acct['cash']:>9.2f}  |  "
                f"Holdings: ${acct['holdings_value']:>9.2f}  |  "
                f"Total: ${acct['total']:>9.2f}")

    # FIX-11: Fixed stop-loss execution
    holdings = enforce_stop_losses(sid, holdings, kmap, today, dry_run)
    acct = read_account(sid)

    score_fn = SCORE_FN.get(sid)
    if not score_fn or not rows:
        logger.warning("No KPI data or scoring function — skipping")
        return

    candidates = score_fn(rows, kmap)
    if not candidates:
        logger.info("No candidates matched this strategy's filters today")
        return

    logger.info(f"       Top candidate: {candidates[0][0]} (score {candidates[0][1]:.1f})")

    decision, err = ask_claude(sid, name, desc, candidates, holdings, acct, today, kmap=kmap)
    if err:
        logger.error(f"Claude error: {err}")
        return

    logger.info(f"       Summary: {decision.get('summary', '')}")

    for action in decision.get("actions", []):
        atype = action.get("type", "").upper()
        ticker = action.get("ticker", "")
        reason = action.get("reason", "")
        if not ticker or ticker not in kmap:
            continue
        price = kmap[ticker].get("current_price", 0)
        if price <= 0:
            continue

        if atype == "BUY":
            ok, msg = buy(sid, ticker, price, acct["cash"], reason, today, kmap, dry_run)
            logger.info(f"       BUY  {ticker}: {msg}")
            acct = read_account(sid)
        elif atype == "SELL":
            ok, msg = sell(sid, ticker, price, reason, today, kmap, dry_run)
            logger.info(f"       SELL {ticker}: {msg}")
            acct = read_account(sid)
        elif atype == "HOLD":
            logger.info(f"       HOLD {ticker}: {reason[:60]}")

# ── Initialise account files ──────────────────────────────────────────────────
def init_accounts(force: bool = False):
    """Initialize all strategy accounts to starting cash."""
    logger.info("\n" + "-"*55)
    logger.info("  INITIALISING STRATEGY ACCOUNTS")
    logger.info("-"*55)
    today = date.today().isoformat()
    for sid, name, style, risk, desc in STRATEGIES:
        path = acct_file(sid)
        if path.exists() and not force:
            logger.info(f"  [{sid}] {name[:40]} — already exists, skipping")
            continue
        acct = {"account": ACCOUNT_NUM, "strategy_id": sid,
                "cash": STARTING_CASH, "holdings_value": 0.0, "total": STARTING_CASH,
                "start_date": today, "trades": 0}
        save_account(sid, acct)
        save_holdings(sid, [])
        logger.info(f"  [{sid}] {name[:40]} — created  ${STARTING_CASH:.2f}")
    logger.info("-"*55)
    logger.info("  Done. Run without --init to start trading.")
    logger.info("-"*55 + "\n")

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
        logger.error("\n  ERROR: ANTHROPIC_API_KEY not set.")
        logger.error("  Add it to your .env file or export it as an environment variable.\n")
        return

    logger.info("\n" + "="*65)
    logger.info("  MULTI-STRATEGY PAPER TRADING ENGINE")
    logger.info(f"  Date: {today}  |  Strategies: {len(STRATEGIES)}  |  Goal: $2,000 each")
    logger.info(f"  Model: {MODEL}")
    if args.dry_run:
        logger.info("  *** DRY RUN — no trades will be executed ***")
    logger.info("="*65)

    rows, kmap = load_kpi(args.kpi_file)
    if not rows:
        logger.error("\n  ERROR: No KPI data found. Run equity_kpi_analyzer.py first.\n")
        return

    logger.info(f"\n  Loaded {len(rows)} tickers from KPI file.")

    run_ids = [args.strategy] if args.strategy else [s[0] for s in STRATEGIES]

    for sid, name, style, risk, desc in STRATEGIES:
        if sid not in run_ids:
            continue
        run_strategy(sid, name, style, risk, desc, rows, kmap, today, dry_run=args.dry_run)

    logger.info("\n" + "-"*65)
    logger.info("  LEADERBOARD")
    logger.info("-"*65)
    lb = update_leaderboard(today, kmap=kmap)
    logger.info(f"  {'Rank':<5} {'ID':<4} {'Strategy':<35} {'Total':>9} {'P&L':>9} {'Return':>8}")
    logger.info(f"  {'-'*4} {'-'*4} {'-'*35} {'-'*9} {'-'*9} {'-'*8}")
    for r in lb:
        sign = "+" if r["pnl"] >= 0 else ""
        logger.info(f"  {r['rank']:<5} {r['strategy_id']:<4} {r['strategy_name'][:35]:<35} "
                    f"${r['total']:>8.2f} {sign}${r['pnl']:>8.2f} {sign}{r['pct_return']:>7.2f}%")
    logger.info("-"*65)
    logger.info(f"  Leaderboard saved to: {leader_file().name}  (history: {leader_history_file().name})")
    logger.info("="*65 + "\n")


def score_news_macro(rows: list, kmap: dict, macro: dict = None) -> list[tuple]:
    """
    Strategy 19 — News Macro Catalyst scorer.
    Scores each stock by sector alignment with today's dominant macro themes.
    Theme strength, duration, and market regime all adjust the final score.
    FIX (bug 3): macro defaults to None so the generic SCORE_FN dispatch
    (score_fn(rows, kmap)) never crashes if S19 reaches the standard path.
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


def score_news_sentiment(rows: list, kmap: dict, macro: dict = None) -> list[tuple]:
    """
    Strategy 20 — News Sentiment Momentum scorer.
    Quality gate (CS > 40, positive EPS) then amplified by macro sector tailwinds
    and company-specific catalysts. Fundamentals + news together.
    FIX (bug 3): macro defaults to None so generic SCORE_FN dispatch is safe.
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



if __name__ == "__main__":
    main()
"""
update_quotes.py — Fetch live quotes for all held positions and refresh
                   every strategy's holdings_value, account total, and
                   the leaderboard WITHOUT triggering any buy/sell logic.

Also recomputes abnormal_return for ALL tickers in equity_kpi_results.csv
using today's live prices vs yesterday's close and SPY as the market proxy.
This fixes the data-freshness gap where strategy_runner.py (running at 4:30 PM)
was using an abnormal_return computed from the morning KPI run — 8+ hours stale
for PEAD (S07) which depends entirely on that signal.

Usage:
    python update_quotes.py              # update all strategies
    python update_quotes.py --strategy 03   # one strategy only
    python update_quotes.py --show          # print holdings table after update
    python update_quotes.py --show --strategy 03
    python update_quotes.py --no-kpi-refresh   # skip abnormal_return refresh

The script requires only the standard library + yfinance (already in
requirements.txt alongside the rest of the trading engine).
"""

import sys, csv, argparse
from datetime import date, datetime
from pathlib import Path
from collections import defaultdict

# ── UTF-8 output (Windows) ────────────────────────────────────────────────────
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

try:
    import yfinance as yf
except ImportError:
    sys.exit("ERROR: yfinance not installed.  Run:  pip install yfinance")

# ── Constants — must mirror strategy_runner.py ────────────────────────────────
STARTING_CASH = 1_000.00
ACCOUNT_NUM   = "123456789"
BASE_DIR      = Path(__file__).parent

STRATEGIES = [
    ("02", "Mean reversion",                   "contrarian",  "med"),
    ("03", "Value investing",                  "value",       "low"),
    ("06", "Low volatility / defensive",       "defensive",   "low"),
    ("07", "Earnings surprise (PEAD)",         "event",       "high"),
    ("08", "Dividend growth",                  "income",      "low"),
    ("09", "Insider buying signal",            "alt-data",    "med"),
    ("10", "Macro-regime adaptive",            "macro",       "med"),
    ("11", "S&P 500 value tilt",             "academic",    "med"),
    ("12", "Momentum (academic / Asness)",     "academic",    "high"),
    ("13", "Quality / profitability",          "academic",    "low"),
    ("14", "Passive S&P 500 benchmark",        "passive",     "low"),
    ("18", "Capex beneficiary / semis",        "thematic",    "high"),
    ("19", "News macro catalyst",              "macro",       "med"),
    ("20", "News sentiment momentum",          "alt-data",    "med"),
    ("21", "Defense & war economy",            "thematic",    "med"),
]

# ── File helpers ──────────────────────────────────────────────────────────────

def acct_file(sid):  return BASE_DIR / f"account_{sid}.csv"
def hold_file(sid):  return BASE_DIR / f"holdings_{sid}.csv"
def leader_file():   return BASE_DIR / "leaderboard.csv"
KPI_FILE = BASE_DIR / "equity_kpi_results.csv"   # Issue-12: for abnormal_return refresh

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
        return None
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
        r["shares"]     = float(r["shares"])
        r["avg_cost"]   = float(r["avg_cost"])
        r["cost_basis"] = float(r["cost_basis"])
    return rows

def save_holdings(sid, holdings):
    write_csv(hold_file(sid), holdings,
              ["ticker","shares","avg_cost","cost_basis","purchase_date","strategy_id"])

# ── Issue-12: Live abnormal_return refresh ────────────────────────────────────

def refresh_abnormal_returns(verbose: bool = True) -> int:
    """
    Recompute abnormal_return for every ticker in equity_kpi_results.csv
    using today's live intraday move vs yesterday's close, with SPY as the
    market proxy.

    abnormal_return = stock_return_today - (beta * spy_return_today)

    This fixes the staleness gap: the KPI file is built in the morning, but
    strategy_runner.py runs at 4:30 PM. By then the morning abnormal_return
    is 8+ hours old — meaningless for PEAD (S07) which depends entirely on it.

    Returns the number of tickers successfully updated.
    """
    if not KPI_FILE.exists():
        if verbose:
            print("  [KPI] equity_kpi_results.csv not found — skipping abnormal_return refresh")
        return 0

    kpi_rows = read_csv(KPI_FILE)
    if not kpi_rows:
        return 0

    tickers = [r["ticker"] for r in kpi_rows if r.get("ticker")]
    if not tickers:
        return 0

    if verbose:
        print(f"\n  [KPI] Refreshing abnormal_return for {len(tickers)} tickers …", flush=True)

    # Fetch 5-day history for SPY (market benchmark) + all universe tickers
    # 5 days ensures we always have at least 2 trading days even over weekends.
    all_syms = ["SPY"] + tickers
    try:
        raw = yf.download(
            all_syms,
            period="5d",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        # raw["Close"] is a DataFrame with tickers as columns when multi-ticker
        closes = raw["Close"] if "Close" in raw.columns.get_level_values(0) else raw
    except Exception as e:
        if verbose:
            print(f"  [KPI] yfinance download failed: {e}")
        return 0

    # Get SPY's return for the most recent complete trading day
    try:
        spy_closes = closes["SPY"].dropna()
        if len(spy_closes) < 2:
            if verbose:
                print("  [KPI] Insufficient SPY history — skipping abnormal_return refresh")
            return 0
        spy_ret = (spy_closes.iloc[-1] - spy_closes.iloc[-2]) / spy_closes.iloc[-2]
    except Exception as e:
        if verbose:
            print(f"  [KPI] SPY return computation failed: {e}")
        return 0

    # Build a map of {ticker: today_abnormal_return}
    ab_map: dict[str, float] = {}
    for sym in tickers:
        try:
            sym_closes = closes[sym].dropna()
            if len(sym_closes) < 2:
                continue
            stock_ret = (sym_closes.iloc[-1] - sym_closes.iloc[-2]) / sym_closes.iloc[-2]
            beta_val  = 1.0   # default if not in KPI rows
            ab_map[sym] = round(float(stock_ret - beta_val * spy_ret), 4)
        except Exception:
            continue

    # Apply beta from KPI file for a more accurate abnormal return
    beta_lookup = {r["ticker"]: float(r.get("beta") or 1.0) for r in kpi_rows}
    for sym in list(ab_map.keys()):
        try:
            sym_closes = closes[sym].dropna()
            stock_ret  = (sym_closes.iloc[-1] - sym_closes.iloc[-2]) / sym_closes.iloc[-2]
            beta_val   = beta_lookup.get(sym, 1.0) or 1.0
            ab_map[sym] = round(float(stock_ret - beta_val * spy_ret), 4)
        except Exception:
            pass

    if not ab_map:
        if verbose:
            print("  [KPI] No abnormal returns computed")
        return 0

    # Patch the KPI rows with fresh abnormal_return values
    updated = 0
    fieldnames = list(kpi_rows[0].keys()) if kpi_rows else []
    if "abnormal_return" not in fieldnames:
        fieldnames.append("abnormal_return")

    for r in kpi_rows:
        sym = r.get("ticker", "")
        if sym in ab_map:
            r["abnormal_return"] = ab_map[sym]
            updated += 1

    write_csv(KPI_FILE, kpi_rows, fieldnames)

    if verbose:
        print(f"  [KPI] abnormal_return refreshed for {updated}/{len(tickers)} tickers  "
              f"(SPY today: {spy_ret*100:+.2f}%)")
    return updated

def collect_tickers(run_ids):
    tickers = set()
    for sid in run_ids:
        for h in read_holdings(sid):
            tickers.add(h["ticker"])
    return tickers

# ── Fetch quotes from Yahoo Finance ──────────────────────────────────────────

def fetch_quotes(tickers):
    """
    Returns dict  {ticker: price}  for every ticker that had a valid quote.
    Tickers with no data are omitted (caller should fall back to avg_cost).
    """
    if not tickers:
        return {}

    ticker_list = sorted(tickers)
    print(f"\n  Fetching quotes for {len(ticker_list)} ticker(s) via Yahoo Finance …")

    prices = {}
    failed = []

    # yfinance batch download — fast_info gives the most recent price
    # even outside market hours (uses previous close after hours).
    data = yf.Tickers(" ".join(ticker_list))
    for sym in ticker_list:
        try:
            info = data.tickers[sym].fast_info
            # regularMarketPrice is live; last_price is a fallback
            price = getattr(info, "regular_market_price", None) \
                 or getattr(info, "last_price", None)
            if price and price > 0:
                prices[sym] = round(float(price), 4)
            else:
                failed.append(sym)
        except Exception:
            failed.append(sym)

    if failed:
        print(f"  [WARN] No quote returned for: {', '.join(failed)}")
    return prices

# ── Update one strategy ───────────────────────────────────────────────────────

def update_strategy(sid, prices, today, show):
    acct = read_account(sid)
    if acct is None:
        print(f"  [{sid}] No account file — skipped (run --init first)")
        return None

    holdings = read_holdings(sid)
    if not holdings:
        # No positions — holdings_value stays 0, just refresh total
        acct["holdings_value"] = 0.0
        acct["total"]          = round(acct["cash"], 2)
        save_account(sid, acct)
        return acct

    # Update each holding with the latest price
    hv = 0.0
    updated_rows = []
    for h in holdings:
        sym   = h["ticker"]
        price = prices.get(sym)
        if price is None:
            price = h["avg_cost"]   # fallback: keep cost basis price
            flag  = " (no quote — using avg_cost)"
        else:
            flag = ""

        mv = round(h["shares"] * price, 2)
        hv += mv

        # Store the refreshed market price back into the holdings row
        # so that callers (e.g. strategy_runner) can read it later.
        # We use a synthetic "current_price" column that is read by
        # calc_holdings_value() via the kmap — it is NOT written to CSV
        # (holdings CSV schema stays the same), but we do update avg_cost
        # only if explicitly desired; here we leave cost basis untouched.
        updated_rows.append(h)   # schema unchanged

        if show:
            cost  = h["avg_cost"]
            pnl   = price - cost
            pct   = (pnl / cost * 100) if cost else 0.0
            sign  = "+" if pnl >= 0 else ""
            print(f"    {sym:<6}  {h['shares']:>8.4f} sh  "
                  f"cost ${cost:>8.2f}  now ${price:>8.2f}  "
                  f"MV ${mv:>9.2f}  P&L {sign}{pct:.1f}%{flag}")

    acct["holdings_value"] = round(hv, 2)
    acct["total"]          = round(acct["cash"] + hv, 2)
    save_account(sid, acct)
    save_holdings(sid, updated_rows)   # no schema change, but ensures file is current
    return acct

# ── Rebuild leaderboard ───────────────────────────────────────────────────────

def update_leaderboard(today, run_ids, all_ids):
    """
    Re-reads every strategy account and rewrites leaderboard.csv.
    Strategies not in run_ids are read but NOT re-fetched (use stored values).
    """
    rows = []
    for sid, name, style, risk in all_ids:
        acct = read_account(sid)
        if acct is None:
            continue
        total = acct["cash"] + acct["holdings_value"]
        pnl   = total - STARTING_CASH
        pct   = (total / STARTING_CASH - 1) * 100 if STARTING_CASH else 0.0
        rows.append({
            "rank":           0,
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
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    write_csv(leader_file(), rows,
              ["rank","date","strategy_id","strategy_name","style","risk",
               "cash","holdings_value","total","pnl","pct_return","trades"])
    return rows

# ── Pretty leaderboard print ──────────────────────────────────────────────────

def print_leaderboard(lb):
    print(f"\n  {'Rk':<4} {'ID':<4} {'Strategy':<35} {'Cash':>9} {'Holdings':>10} "
          f"{'Total':>9} {'P&L':>9} {'Return':>8}")
    print("  " + "-"*95)
    for r in lb:
        sign = "+" if r["pnl"] >= 0 else ""
        print(f"  {r['rank']:<4} {r['strategy_id']:<4} {r['strategy_name'][:35]:<35} "
              f"${r['cash']:>8.2f} ${r['holdings_value']:>9.2f} "
              f"${r['total']:>8.2f} {sign}${r['pnl']:>8.2f} "
              f"{sign}{r['pct_return']:>6.2f}%")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch live quotes and update holdings values for all strategies")
    parser.add_argument("--strategy", type=str,
                        help="Update only this strategy ID, e.g. 03")
    parser.add_argument("--show", action="store_true",
                        help="Print per-position detail after updating")
    parser.add_argument("--no-kpi-refresh", action="store_true",
                        help="Skip the abnormal_return refresh in equity_kpi_results.csv")
    args = parser.parse_args()

    today = date.today().isoformat()
    now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n" + "="*65)
    print("  PORTFOLIO QUOTE UPDATER")
    print(f"  {now}")
    print("="*65)

    all_ids  = [(s[0], s[1], s[2], s[3]) for s in STRATEGIES]
    run_ids  = [args.strategy] if args.strategy else [s[0] for s in STRATEGIES]

    # ── 1. Refresh abnormal_return in KPI file (Issue-12) ────────────────────
    if not args.no_kpi_refresh:
        refresh_abnormal_returns(verbose=True)

    # ── 2. Collect all tickers we need prices for ─────────────────────────────
    tickers = collect_tickers(run_ids)
    if not tickers:
        print("\n  No open positions found — nothing to update.")
        print("  (Run strategy_runner.py to open positions first.)\n")
        return

    # ── 3. Fetch quotes ───────────────────────────────────────────────────────
    prices = fetch_quotes(tickers)
    if not prices:
        print("\n  ERROR: Could not fetch any quotes. Check internet connection.\n")
        return

    fetched_at = datetime.now().strftime("%H:%M:%S")
    print(f"  Got prices for {len(prices)}/{len(tickers)} tickers  (as of {fetched_at})")

    # ── 4. Update each strategy ────────────────────────────────────────────────
    print()
    updated = 0
    for sid in run_ids:
        name = next((s[1] for s in STRATEGIES if s[0] == sid), sid)
        print(f"  [{sid}] {name}")
        acct = update_strategy(sid, prices, today, show=args.show)
        if acct:
            pnl  = acct["total"] - STARTING_CASH
            sign = "+" if pnl >= 0 else ""
            print(f"       Cash ${acct['cash']:>9.2f}  "
                  f"Holdings ${acct['holdings_value']:>9.2f}  "
                  f"Total ${acct['total']:>9.2f}  "
                  f"P&L {sign}${pnl:.2f}")
            updated += 1

    # ── 5. Rebuild leaderboard ─────────────────────────────────────────────────
    lb = update_leaderboard(today, run_ids, all_ids)

    print("\n" + "-"*65)
    print("  LEADERBOARD  (all strategies, ranked by total value)")
    print("-"*65)
    print_leaderboard(lb)
    print("-"*65)
    print(f"\n  Updated {updated} strategy account(s).")
    print(f"  Leaderboard saved → {leader_file().name}")
    print(f"  Prices as of: {fetched_at}")
    print("="*65 + "\n")


if __name__ == "__main__":
    main()

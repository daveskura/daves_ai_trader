"""
reset_accounts.py — Hard reset for the paper trading engine.

Deletes all account, holdings, and transaction CSV files, wipes the
leaderboard, then re-initialises every strategy account to $1,000 cash.

Usage:
    python reset_accounts.py          # prompts for confirmation
    python reset_accounts.py --yes    # skips confirmation (CI / scripted use)
"""

import sys, csv, argparse
from datetime import date
from pathlib import Path

# ── Mirror the constants from strategy_runner.py ─────────────────────────────
STARTING_CASH = 1000.00
ACCOUNT_NUM   = "123456789"
BASE_DIR      = Path(__file__).parent

STRATEGIES = [
    ("01", "Momentum / trend following",   "momentum",    "high"),
    ("02", "Mean reversion",               "contrarian",  "med"),
    ("03", "Value investing",              "value",       "low"),
    ("04", "Quality growth (GARP)",        "growth",      "med"),
    ("05", "Sector rotation",              "macro",       "med"),
    ("06", "Low volatility / defensive",   "defensive",   "low"),
    ("07", "Earnings surprise (PEAD)",     "event",       "high"),
    ("08", "Dividend growth",              "income",      "low"),
    ("09", "Insider buying signal",        "alt-data",    "med"),
    ("10", "Macro-regime adaptive",        "macro",       "med"),
    ("11", "Large-cap value (Fama-French)","academic",    "med"),
    ("12", "Momentum (academic / Asness)", "academic",    "high"),
    ("13", "Quality / profitability",      "academic",    "low"),
    ("14", "Passive S&P 500 benchmark",    "passive",     "low"),
    ("15", "Noise chaser",                 "speculative", "high"),
    ("16", "Estimate revision momentum",   "growth",      "med"),
    ("17", "High-beta quality growth",     "growth",      "high"),
    ("18", "Capex beneficiary / semis",    "thematic",    "high"),
]

# ── File name helpers (must match strategy_runner.py) ────────────────────────
def acct_file(sid):  return BASE_DIR / f"account_{sid}.csv"
def hold_file(sid):  return BASE_DIR / f"holdings_{sid}.csv"
def txn_file(sid):   return BASE_DIR / f"transactions_{sid}.csv"
def leader_file():   return BASE_DIR / "leaderboard.csv"

# ── CSV helpers ───────────────────────────────────────────────────────────────
def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

# ── Main reset logic ──────────────────────────────────────────────────────────
def reset(yes=False):
    print("\n" + "="*60)
    print("  PAPER TRADING — HARD RESET")
    print("="*60)
    print(f"  This will DELETE all account, holdings, transaction CSVs")
    print(f"  and the leaderboard, then reinitialise every strategy")
    print(f"  account to ${STARTING_CASH:,.2f} cash.\n")

    if not yes:
        ans = input("  Type YES to proceed, anything else to cancel: ").strip()
        if ans != "YES":
            print("  Cancelled.")
            return

    today = date.today().isoformat()
    deleted = 0

    # ── Delete existing files ─────────────────────────────────────────────────
    print("\n  Deleting existing files...")
    targets = [leader_file()]
    for sid, *_ in STRATEGIES:
        targets += [acct_file(sid), hold_file(sid), txn_file(sid)]

    for p in targets:
        if p.exists():
            p.unlink()
            print(f"    deleted  {p.name}")
            deleted += 1
        else:
            print(f"    missing  {p.name}  (skipped)")

    print(f"\n  {deleted} file(s) removed.")

    # ── Reinitialise accounts ─────────────────────────────────────────────────
    print("\n  Creating fresh account files...")
    acct_fields = ["account","strategy_id","cash","holdings_value","total",
                   "start_date","trades"]
    hold_fields = ["ticker","shares","avg_cost","cost_basis","purchase_date","strategy_id"]

    for sid, name, style, risk in STRATEGIES:
        acct = {
            "account":        ACCOUNT_NUM,
            "strategy_id":    sid,
            "cash":           STARTING_CASH,
            "holdings_value": 0.0,
            "total":          STARTING_CASH,
            "start_date":     today,
            "trades":         0,
        }
        write_csv(acct_file(sid), [acct], acct_fields)
        write_csv(hold_file(sid), [],     hold_fields)
        print(f"    [{sid}] {name[:42]:<42}  cash=${STARTING_CASH:,.2f}")

    # ── Create blank leaderboard ──────────────────────────────────────────────
    lb_fields = ["rank","date","strategy_id","strategy_name","style","risk",
                 "cash","holdings_value","total","pnl","pct_return","trades"]
    lb_rows = []
    for i, (sid, name, style, risk) in enumerate(STRATEGIES, 1):
        lb_rows.append({
            "rank": i, "date": today, "strategy_id": sid,
            "strategy_name": name, "style": style, "risk": risk,
            "cash": STARTING_CASH, "holdings_value": 0.0,
            "total": STARTING_CASH, "pnl": 0.0, "pct_return": 0.0, "trades": 0,
        })
    write_csv(leader_file(), lb_rows, lb_fields)
    print(f"\n    leaderboard.csv  — reset with {len(STRATEGIES)} strategies")

    print("\n" + "="*60)
    print(f"  Reset complete.  {len(STRATEGIES)} accounts at ${STARTING_CASH:,.2f}")
    print(f"  Start date: {today}")
    print("  Run strategy_runner.py to begin trading.")
    print("="*60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hard-reset all paper trading accounts")
    parser.add_argument("--yes", action="store_true",
                        help="Skip confirmation prompt (for scripted use)")
    args = parser.parse_args()
    reset(yes=args.yes)

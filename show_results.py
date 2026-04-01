# -*- coding: utf-8 -*-
"""
show_results.py
Pretty-prints the latest leaderboard snapshot from MySQL.
Run after test_run.bat to inspect results without needing a MySQL client.

Usage:
    python show_results.py
    python show_results.py --history   # show all dates, not just latest
"""
import sys
import argparse
from db import get_connection

def main():
    parser = argparse.ArgumentParser(description="Show leaderboard results from MySQL")
    parser.add_argument("--history", action="store_true",
                        help="Show all historical dates, not just the latest")
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    if args.history:
        cur.execute(
            "SELECT lb_date, rank_pos, strategy_id, strategy_name, "
            "       total, pnl, pct_return, trades "
            "FROM leaderboard "
            "ORDER BY lb_date DESC, rank_pos ASC"
        )
    else:
        cur.execute(
            "SELECT lb_date, rank_pos, strategy_id, strategy_name, "
            "       total, pnl, pct_return, trades "
            "FROM leaderboard "
            "WHERE lb_date = (SELECT MAX(lb_date) FROM leaderboard) "
            "ORDER BY rank_pos ASC"
        )

    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No leaderboard data found. Run test_run.bat first.")
        sys.exit(1)

    print()
    print(f"  {'Date':<12} {'Rk':<4} {'ID':<4} {'Strategy':<35} "
          f"{'Total':>9} {'P&L':>9} {'Return':>8} {'Trades':>7}")
    print("  " + "-" * 92)
    current_date = None
    for lb_date, rank, sid, name, total, pnl, pct, trades in rows:
        if lb_date != current_date:
            if current_date is not None:
                print()
            current_date = lb_date
        sign = "+" if float(pnl) >= 0 else ""
        print(f"  {str(lb_date):<12} {rank:<4} {sid:<4} {name[:35]:<35} "
              f"${float(total):>8.2f} {sign}${float(pnl):>8.2f} "
              f"{sign}{float(pct):>7.2f}% {int(trades):>7}")
    print()

if __name__ == "__main__":
    main()

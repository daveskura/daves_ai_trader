# -*- coding: utf-8 -*-
"""
show_logs.py
Pretty-prints run_logs from MySQL — the structured equivalent of daily_run.log.

Usage:
    python3 show_logs.py                        # last 50 rows, all levels
    python3 show_logs.py --n 100                # last 100 rows
    python3 show_logs.py --level WARNING        # warnings and above only
    python3 show_logs.py --stage full           # full-pipeline stage only
    python3 show_logs.py --strategy 07          # one strategy only
    python3 show_logs.py --summary              # daily summary table (7 days)
    python3 show_logs.py --summary --days 30    # summary over 30 days
"""
import sys
import argparse

LEVEL_COLORS = {
    "DEBUG":    "\033[90m",   # dark grey
    "INFO":     "\033[0m",    # normal
    "WARNING":  "\033[93m",   # yellow
    "ERROR":    "\033[91m",   # red
    "CRITICAL": "\033[95m",   # magenta
}
RESET = "\033[0m"

def colorize(level: str, text: str) -> str:
    """Wrap text in ANSI color if the terminal supports it."""
    if not sys.stdout.isatty():
        return text
    return LEVEL_COLORS.get(level, "") + text + RESET


def show_tail(args):
    from db_logger import tail_logs
    rows = tail_logs(
        n=args.n,
        stage=args.stage or None,
        strategy_id=args.strategy or None,
        min_level=args.level.upper(),
    )

    if not rows:
        print("No log entries found matching your filters.")
        return

    # Print newest-last so the most recent line is at the bottom (like `tail -f`)
    rows = list(reversed(rows))

    print()
    print(f"  {'Timestamp':<24} {'Stage':<8} {'Strat':<6} {'Level':<9} Message")
    print("  " + "-" * 100)
    for r in rows:
        ts       = str(r["logged_at"])[:23]
        stage    = (r["run_stage"]   or "")[:8]
        strat    = (r["strategy_id"] or "")[:6]
        level    = str(r["level"])
        msg      = str(r["message"])
        line = (f"  {ts:<24} {stage:<8} {strat:<6} "
                f"{colorize(level, f'{level:<9}')} {msg}")
        print(line)
    print()
    print(f"  ({len(rows)} row(s) shown)")
    print()


def show_summary(args):
    from db_logger import run_summary
    rows = run_summary(days=args.days)

    if not rows:
        print("No log data found.")
        return

    print()
    print(f"  {'Date':<12} {'Stage':<10} {'INFO':>6} {'WARN':>6} {'ERROR':>6} {'CRIT':>6}")
    print("  " + "-" * 50)
    for r in rows:
        warn  = int(r["warn_count"]     or 0)
        error = int(r["error_count"]    or 0)
        crit  = int(r["critical_count"] or 0)
        warn_s  = colorize("WARNING",  str(warn))  if warn  else str(warn)
        error_s = colorize("ERROR",    str(error)) if error else str(error)
        crit_s  = colorize("CRITICAL", str(crit))  if crit  else str(crit)
        print(f"  {str(r['log_date']):<12} {(r['run_stage'] or ''):<10} "
              f"{int(r['info_count'] or 0):>6} {warn_s:>6} {error_s:>6} {crit_s:>6}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Show run_logs from MySQL")
    parser.add_argument("--n",        type=int, default=50,
                        help="Number of recent log rows to show (default: 50)")
    parser.add_argument("--level",    default="DEBUG",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Minimum log level to display (default: DEBUG)")
    parser.add_argument("--stage",    default="",
                        help="Filter by run stage: news | full | kpi | quotes | …")
    parser.add_argument("--strategy", default="",
                        help="Filter by strategy ID, e.g. 07")
    parser.add_argument("--summary",  action="store_true",
                        help="Show daily error/warning summary instead of raw rows")
    parser.add_argument("--days",     type=int, default=7,
                        help="Days of history for --summary (default: 7)")
    args = parser.parse_args()

    try:
        from db import get_connection
        # Verify connectivity without running DDL — schema creation belongs
        # only in the scripts that write data, not in read-only viewers.
        conn = get_connection()
        conn.close()
    except Exception as e:
        print(f"ERROR: Could not connect to MySQL: {e}")
        print("Check DB_HOST / DB_USER / DB_PASSWORD in your .env file.")
        sys.exit(1)

    if args.summary:
        show_summary(args)
    else:
        show_tail(args)


if __name__ == "__main__":
    main()

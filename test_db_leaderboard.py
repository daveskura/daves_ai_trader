# -*- coding: utf-8 -*-
"""
test_db_leaderboard.py
Called by test_run.bat to verify leaderboard rows exist after a full run.
Exit 0 = rows found, Exit 1 = empty (something went wrong).
"""
import sys
try:
    from db import get_connection
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM leaderboard")
    n = cur.fetchone()[0]
    conn.close()
    if n > 0:
        print(f"  {n} leaderboard row(s) found in MySQL -- OK")
        sys.exit(0)
    else:
        print("  ERROR: leaderboard table is empty after full run.")
        sys.exit(1)
except Exception as e:
    print(f"  FAILED: {e}")
    sys.exit(1)

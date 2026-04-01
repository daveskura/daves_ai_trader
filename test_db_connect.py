# -*- coding: utf-8 -*-
"""
test_db_connect.py
Called by test_run.bat to verify MySQL connectivity and initialise schema.
Exit 0 = OK, Exit 1 = failed.
"""
import sys
try:
    from db import get_connection, init_schema
    init_schema()
    conn = get_connection()
    conn.close()
    print("  MySQL connection OK, schema ready.")
    sys.exit(0)
except Exception as e:
    print(f"  FAILED: {e}")
    sys.exit(1)

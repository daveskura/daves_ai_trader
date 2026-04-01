# -*- coding: utf-8 -*-
"""
db_logger.py — Structured MySQL logging for the paper-trading engine.

Adds a `run_logs` table to the existing schema and exposes a Logger class
that can be used from any script.  Falls back to stderr silently if the DB
is unavailable so it never breaks a running strategy.

Usage (in Python scripts):
    from db_logger import Logger
    log = Logger(run_stage="full", strategy_id="07")
    log.info("Bought 10 shares of AAPL")
    log.warning("KPI file is stale")
    log.error("Claude API timeout")
    log.end(status="ok")   # or "error"

Usage (from the shell via the CLI wrapper):
    python3 db_logger.py --stage news --level INFO  --message "Stage 1 complete"
    python3 db_logger.py --stage full --level ERROR --message "Strategy 19 failed"

Query helpers (import and call directly):
    from db_logger import tail_logs, run_summary
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Schema extension — appended to db.init_schema() via monkey-patch below
# ---------------------------------------------------------------------------

_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS run_logs (
    id           BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    logged_at    DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    run_stage    VARCHAR(20)      NOT NULL DEFAULT '',
    strategy_id  VARCHAR(8)       NOT NULL DEFAULT '',
    level        ENUM('DEBUG','INFO','WARNING','ERROR','CRITICAL')
                                  NOT NULL DEFAULT 'INFO',
    message      TEXT             NOT NULL,
    extra        JSON,
    INDEX idx_logged_at   (logged_at),
    INDEX idx_stage       (run_stage),
    INDEX idx_strategy    (strategy_id),
    INDEX idx_level       (level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _ensure_log_table():
    """Create run_logs if it doesn't exist. Safe to call repeatedly."""
    try:
        from db import get_connection
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(_LOG_TABLE_SQL)
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass   # DB unavailable — callers fall back to stderr


# ---------------------------------------------------------------------------
# Core Logger class
# ---------------------------------------------------------------------------

class Logger:
    """
    Thin structured logger that writes to the run_logs MySQL table.

    Parameters
    ----------
    run_stage : str
        Label for the pipeline stage, e.g. "news", "full", "kpi", "quotes".
    strategy_id : str
        Strategy being processed, e.g. "07". Leave blank for pipeline-level logs.
    echo : bool
        If True (default), also print to stdout so terminal output is preserved.

    Implementation notes
    --------------------
    Rows are buffered in-process and flushed in a single INSERT batch whenever
    the buffer reaches FLUSH_EVERY rows, or explicitly via flush().  This avoids
    one connect/disconnect cycle per log line.

    DB errors are retried on each flush attempt rather than permanently silencing
    the logger — a transient connection drop will recover automatically.
    """

    LEVELS     = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
    FLUSH_EVERY = 20   # batch size before an automatic flush

    def __init__(
        self,
        run_stage: str = "",
        strategy_id: str = "",
        echo: bool = True,
        min_level: str = "DEBUG",
    ):
        self.run_stage   = run_stage
        self.strategy_id = strategy_id
        self.echo        = echo
        self.min_level   = min_level
        self._buffer: list = []   # pending rows not yet flushed to DB
        self._warned = False      # have we already printed a DB-error warning?
        _ensure_log_table()

    # ------------------------------------------------------------------ public

    def debug(self, msg: str, **extra):    self._log("DEBUG",    msg, extra)
    def info(self, msg: str, **extra):     self._log("INFO",     msg, extra)
    def warning(self, msg: str, **extra):  self._log("WARNING",  msg, extra)
    def error(self, msg: str, **extra):    self._log("ERROR",    msg, extra)
    def critical(self, msg: str, **extra): self._log("CRITICAL", msg, extra)

    def end(self, status: str = "ok", msg: str = ""):
        """Log a run-end marker and flush any buffered rows."""
        level = "INFO" if status == "ok" else "ERROR"
        self._log(level, msg or f"Run finished — status={status}",
                  {"run_status": status})
        self.flush()

    def flush(self):
        """Write all buffered rows to MySQL in one INSERT batch."""
        if not self._buffer:
            return
        rows, self._buffer = self._buffer, []
        self._batch_insert(rows)

    # ----------------------------------------------------------------- private

    def _log(self, level: str, message: str, extra: dict | None = None):
        if self.LEVELS.get(level, 0) < self.LEVELS.get(self.min_level, 0):
            return

        now = datetime.now(timezone.utc)
        ts  = now.strftime("%Y-%m-%d %H:%M:%S")

        if self.echo:
            prefix = f"[{ts}] [{level:<8}]"
            if self.run_stage:
                prefix += f" [{self.run_stage}]"
            if self.strategy_id:
                prefix += f" [S{self.strategy_id}]"
            print(f"{prefix} {message}", flush=True)

        self._buffer.append((now, level, message, extra or {}))
        if len(self._buffer) >= self.FLUSH_EVERY:
            self.flush()

    def _batch_insert(self, rows: list):
        import json as _json
        params = [
            (
                ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                self.run_stage,
                self.strategy_id,
                level,
                message[:4000],
                _json.dumps(extra) if extra else None,
            )
            for ts, level, message, extra in rows
        ]
        try:
            from db import get_connection
            conn = get_connection()
            try:
                cur = conn.cursor()
                cur.executemany(
                    """
                    INSERT INTO run_logs
                        (logged_at, run_stage, strategy_id, level, message, extra)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    params,
                )
                conn.commit()
                self._warned = False   # DB recovered — reset warning flag
            finally:
                conn.close()
        except Exception as exc:
            # Warn on first failure and on recovery; don't spam on every flush
            if not self._warned:
                print(
                    f"[db_logger] WARNING: Cannot write to run_logs: {exc}",
                    file=sys.stderr,
                )
                self._warned = True
            # Don't re-buffer — rows are lost for this flush, but the
            # logger stays alive and will retry on the next flush attempt.


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def tail_logs(
    n: int = 50,
    stage: Optional[str] = None,
    strategy_id: Optional[str] = None,
    min_level: str = "DEBUG",
) -> list[dict]:
    """
    Return the last *n* log rows as dicts, newest first.

    Example:
        from db_logger import tail_logs
        for row in tail_logs(20, min_level="WARNING"):
            print(row["logged_at"], row["level"], row["message"])
    """
    from db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        wheres = ["level >= %s"]
        params: list = [min_level]

        # MySQL ENUM comparison works, but it's cleaner to use a subquery for
        # ordering by severity — instead just filter by name with IN().
        level_order = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        min_idx = level_order.index(min_level.upper()) if min_level.upper() in level_order else 0
        levels_to_include = level_order[min_idx:]
        wheres = [f"level IN ({','.join(['%s']*len(levels_to_include))})"]
        params = list(levels_to_include)

        if stage:
            wheres.append("run_stage = %s")
            params.append(stage)
        if strategy_id:
            wheres.append("strategy_id = %s")
            params.append(strategy_id)

        where_clause = " AND ".join(wheres)
        params.append(n)
        cur.execute(
            f"SELECT * FROM run_logs WHERE {where_clause} ORDER BY logged_at DESC LIMIT %s",
            params,
        )
        return cur.fetchall()
    finally:
        conn.close()


def run_summary(days: int = 7) -> list[dict]:
    """
    Return a daily summary: counts of INFO / WARNING / ERROR per stage.

    Example:
        from db_logger import run_summary
        for row in run_summary(days=14):
            print(row)
    """
    from db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                DATE(logged_at)  AS log_date,
                run_stage,
                SUM(level = 'INFO')     AS info_count,
                SUM(level = 'WARNING')  AS warn_count,
                SUM(level = 'ERROR')    AS error_count,
                SUM(level = 'CRITICAL') AS critical_count
            FROM run_logs
            WHERE logged_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
            GROUP BY log_date, run_stage
            ORDER BY log_date DESC, run_stage
            """,
            (days,),
        )
        return cur.fetchall()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI shim — called from run_daily.sh
# ---------------------------------------------------------------------------

def _cli():
    parser = argparse.ArgumentParser(
        description="Write a single log entry to the run_logs MySQL table."
    )
    parser.add_argument("--stage",    default="",    help="Run stage label (news/full/kpi/…)")
    parser.add_argument("--strategy", default="",    help="Strategy ID, e.g. 07")
    parser.add_argument("--level",    default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--message",  required=True, help="Log message text")
    parser.add_argument("--no-echo",  action="store_true",
                        help="Suppress stdout (useful when the shell already echoed the text)")
    args = parser.parse_args()

    log = Logger(
        run_stage=args.stage,
        strategy_id=args.strategy,
        echo=not args.no_echo,
    )
    log._log(args.level, args.message)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _cli()

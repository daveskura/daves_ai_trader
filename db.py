# -*- coding: utf-8 -*-
"""
db.py -- MySQL data layer for the multi-strategy paper trading engine.

Replaces all CSV file I/O in strategy_runner.py, update_quotes.py,
reset_accounts.py, and equity_kpi_analyzer.py with MySQL queries.

Connection settings are read from environment variables (or .env):
    DB_HOST      -- default: localhost
    DB_PORT      -- default: 3306
    DB_NAME      -- default: paper_trading
    DB_USER      -- default: trading
    DB_PASSWORD  -- required

Install the driver once:
    pip install mysql-connector-python

Public API (mirrors the original CSV helpers exactly):
    get_connection()
    init_schema()              -- create tables if they don't exist
    read_account(sid)
    save_account(sid, acct)
    read_holdings(sid)
    save_holdings(sid, holdings)
    append_txn(sid, txn)
    read_leaderboard()
    write_leaderboard(rows)
    read_leaderboard_history(since_date=None)
    append_leaderboard_history(rows)
    read_kpi_rows()
    write_kpi_rows(rows, fieldnames)
    update_kpi_abnormal_returns(ab_map)
    reset_all(starting_cash, strategies, account_num)
    write_down_day_results(analysis_date, market_return_pct, vix, rows)
    read_down_day_results(analysis_date=None, days=30)
    write_universe_cache(tickers, mode, sector_breakdown=None)
    read_universe_cache(mode, max_age_days)
    write_news_macro_cache(data)
    read_news_macro_cache(analysis_date=None)
    read_news_macro_cache_latest()
"""

import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

# -- Load .env if present (mirrors strategy_runner.py) ------------------------
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            _v = _v.strip().strip('"').strip("'")
            os.environ.setdefault(_k.strip(), _v)

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    sys.exit(
        "ERROR: mysql-connector-python not installed.\n"
        "Run:  pip install mysql-connector-python"
    )

# -- Connection settings -------------------------------------------------------
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "port":     int(os.environ.get("DB_PORT", "3306")),
    "database": os.environ.get("DB_NAME",     "paper_trading"),
    "user":     os.environ.get("DB_USER",     "trading"),
    "password": os.environ.get("DB_PASSWORD", ""),
    "charset":  "utf8mb4",
    "autocommit": False,
    "connection_timeout": 10,
}


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------

def _safe_float(v) -> Optional[float]:
    """Convert a value to float, returning None if not possible."""
    if v is None:
        return None
    try:
        import math as _math
        f = float(v)
        return None if _math.isnan(f) or _math.isinf(f) else round(f, 6)
    except (TypeError, ValueError):
        return None


def get_connection():
    """Return a new MySQL connection. Caller is responsible for closing it."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except MySQLError as e:
        raise ConnectionError(
            f"Cannot connect to MySQL ({DB_CONFIG['host']}:{DB_CONFIG['port']} "
            f"db={DB_CONFIG['database']} user={DB_CONFIG['user']}): {e}"
        ) from e


# -----------------------------------------------------------------------------
# Schema
# -----------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
    strategy_id       VARCHAR(8)     NOT NULL PRIMARY KEY,
    account           VARCHAR(20)    NOT NULL,
    cash              DECIMAL(14,4)  NOT NULL DEFAULT 0,
    holdings_value    DECIMAL(14,4)  NOT NULL DEFAULT 0,
    total             DECIMAL(14,4)  NOT NULL DEFAULT 0,
    start_date        DATE           NOT NULL,
    trades            INT            NOT NULL DEFAULT 0,
    updated_at        TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP
                                     ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS holdings (
    id            BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    strategy_id   VARCHAR(8)       NOT NULL,
    ticker        VARCHAR(16)      NOT NULL,
    shares        DECIMAL(18,6)    NOT NULL,
    avg_cost      DECIMAL(14,4)    NOT NULL,
    cost_basis    DECIMAL(14,4)    NOT NULL,
    purchase_date DATE             NOT NULL,
    UNIQUE KEY uq_strategy_ticker (strategy_id, ticker),
    INDEX idx_strategy (strategy_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS transactions (
    id            BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    txn_date      DATE             NOT NULL,
    strategy_id   VARCHAR(8)       NOT NULL,
    action        VARCHAR(8)       NOT NULL,
    ticker        VARCHAR(16)      NOT NULL,
    shares        DECIMAL(18,6)    NOT NULL,
    price         DECIMAL(14,4)    NOT NULL,
    commission    DECIMAL(10,4)    NOT NULL DEFAULT 0,
    net_amount    DECIMAL(14,4)    NOT NULL,
    cash_after    DECIMAL(14,4)    NOT NULL,
    reason        VARCHAR(500)     NOT NULL DEFAULT '',
    created_at    TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_strategy_date (strategy_id, txn_date),
    INDEX idx_date (txn_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS leaderboard (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    lb_date         DATE            NOT NULL,
    rank_pos        INT             NOT NULL,
    strategy_id     VARCHAR(8)      NOT NULL,
    strategy_name   VARCHAR(80)     NOT NULL,
    style           VARCHAR(30)     NOT NULL,
    risk            VARCHAR(10)     NOT NULL,
    cash            DECIMAL(14,4)   NOT NULL,
    holdings_value  DECIMAL(14,4)   NOT NULL,
    total           DECIMAL(14,4)   NOT NULL,
    pnl             DECIMAL(14,4)   NOT NULL,
    pct_return      DECIMAL(10,4)   NOT NULL,
    trades          INT             NOT NULL DEFAULT 0,
    UNIQUE KEY uq_date_strategy (lb_date, strategy_id),
    INDEX idx_date (lb_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS equity_kpi (
    ticker                     VARCHAR(16)   NOT NULL PRIMARY KEY,
    composite_score            DOUBLE,
    tier1_score                DOUBLE,
    tier2_score                DOUBLE,
    tier3_score                DOUBLE,
    net_profit_margin          DOUBLE,
    eps_growth_fwd             DOUBLE,
    eps_ttm                    DOUBLE,
    eps_forward                DOUBLE,
    pe_ratio                   DOUBLE,
    current_price              DOUBLE,
    rsi_14                     DOUBLE,
    ma_50                      DOUBLE,
    ma_200                     DOUBLE,
    ma_signal                  VARCHAR(60),
    beta                       DOUBLE,
    market_cap                 DOUBLE,
    pct_from_52w_high          DOUBLE,
    abnormal_return            DOUBLE,
    net_insider_shares         DOUBLE,
    vix                        DOUBLE,
    dividend_yield             DOUBLE,
    five_year_avg_dividend_yield DOUBLE,
    eps_revision_pct           DOUBLE,
    sector                     VARCHAR(60),
    extra_json                 JSON,
    updated_at                 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS down_day_results (
    id                  BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    analysis_date       DATE             NOT NULL,
    market_return_pct   DECIMAL(8,4)     NOT NULL,
    vix                 DECIMAL(6,2),
    ticker              VARCHAR(16)      NOT NULL,
    name                VARCHAR(100),
    sector              VARCHAR(60),
    industry            VARCHAR(100),
    day_return_pct      DECIMAL(8,4),
    abnormal_return_pct DECIMAL(8,4),
    actually_gained     TINYINT(1),
    beta                DECIMAL(6,3),
    pe_ratio            DECIMAL(10,2),
    forward_pe          DECIMAL(10,2),
    net_margin_pct      DECIMAL(8,2),
    dividend_yield_pct  DECIMAL(8,4),
    market_cap_B        DECIMAL(12,2),
    short_pct_float     DECIMAL(8,4),
    pct_from_52w_high   DECIMAL(8,4),
    ma_signal           VARCHAR(10),
    debt_to_equity      DECIMAL(10,4),
    current_ratio       DECIMAL(8,4),
    insider_pct         DECIMAL(8,4),
    analyst_rating      DECIMAL(4,2),
    created_at          TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_date_ticker (analysis_date, ticker),
    INDEX idx_date (analysis_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS universe_cache (
    id              BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    refreshed       DATE             NOT NULL,
    mode            VARCHAR(20)      NOT NULL DEFAULT 'balanced',
    n               INT              NOT NULL,
    tickers_json    JSON             NOT NULL,
    sector_breakdown_json JSON,
    created_at      TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_refreshed_mode (refreshed, mode),
    INDEX idx_refreshed (refreshed)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS news_macro_cache (
    id              BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    analysis_date   DATE             NOT NULL UNIQUE,
    market_regime   VARCHAR(20)      NOT NULL DEFAULT 'NEUTRAL',
    themes_json     JSON             NOT NULL,
    catalysts_json  JSON             NOT NULL,
    raw_json        JSON             NOT NULL,
    created_at      TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_date (analysis_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# Known columns in equity_kpi (beyond ticker + updated_at)
_KPI_NUMERIC_COLS = [
    "composite_score", "tier1_score", "tier2_score", "tier3_score",
    "net_profit_margin", "eps_growth_fwd", "eps_ttm", "eps_forward",
    "pe_ratio", "current_price", "rsi_14", "ma_50", "ma_200",
    "beta", "market_cap", "pct_from_52w_high", "abnormal_return",
    "net_insider_shares", "vix", "dividend_yield",
    "five_year_avg_dividend_yield", "eps_revision_pct",
]
_KPI_STR_COLS = ["ma_signal", "sector"]
_KPI_ALL_TYPED = set(_KPI_NUMERIC_COLS + _KPI_STR_COLS)


def init_schema():
    """Create all tables if they don't already exist. Safe to call repeatedly."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        for statement in _SCHEMA_SQL.strip().split(";"):
            statement = statement.strip()
            if statement:
                cur.execute(statement)
        conn.commit()
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Accounts
# -----------------------------------------------------------------------------

def read_account(sid: str) -> Dict:
    """Return the account row for strategy sid as a plain dict."""
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM accounts WHERE strategy_id = %s", (sid,))
        row = cur.fetchone()
        if row is None:
            return {}
        return {
            "account":        row["account"],
            "strategy_id":    row["strategy_id"],
            "cash":           float(row["cash"]),
            "holdings_value": float(row["holdings_value"]),
            "total":          float(row["total"]),
            "start_date":     str(row["start_date"]),
            "trades":         int(row["trades"]),
        }
    finally:
        conn.close()


def save_account(sid: str, acct: Dict):
    """Upsert the account row for strategy sid."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO accounts
                (strategy_id, account, cash, holdings_value, total, start_date, trades)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                account        = VALUES(account),
                cash           = VALUES(cash),
                holdings_value = VALUES(holdings_value),
                total          = VALUES(total),
                start_date     = VALUES(start_date),
                trades         = VALUES(trades)
            """,
            (
                sid,
                acct.get("account", ""),
                round(float(acct.get("cash", 0)), 4),
                round(float(acct.get("holdings_value", 0)), 4),
                round(float(acct.get("total", 0)), 4),
                acct.get("start_date", date.today().isoformat()),
                int(acct.get("trades", 0)),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Holdings
# -----------------------------------------------------------------------------

def read_holdings(sid: str) -> List[Dict]:
    """Return all holdings for strategy sid as a list of plain dicts."""
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT ticker, shares, avg_cost, cost_basis, purchase_date, strategy_id "
            "FROM holdings WHERE strategy_id = %s",
            (sid,),
        )
        rows = cur.fetchall()
        return [
            {
                "ticker":        r["ticker"],
                "shares":        float(r["shares"]),
                "avg_cost":      float(r["avg_cost"]),
                "cost_basis":    float(r["cost_basis"]),
                "purchase_date": str(r["purchase_date"]),
                "strategy_id":   r["strategy_id"],
            }
            for r in rows
        ]
    finally:
        conn.close()


def save_holdings(sid: str, holdings: List[Dict]):
    """
    Replace all holdings for strategy sid atomically.
    Deletes existing rows then inserts the new list in one transaction.
    Rolls back fully if the INSERT fails so holdings are never left empty.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM holdings WHERE strategy_id = %s", (sid,))
        if holdings:
            cur.executemany(
                """
                INSERT INTO holdings
                    (strategy_id, ticker, shares, avg_cost, cost_basis, purchase_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        sid,
                        h["ticker"],
                        round(float(h["shares"]), 6),
                        round(float(h["avg_cost"]), 4),
                        round(float(h["cost_basis"]), 4),
                        h.get("purchase_date", date.today().isoformat()),
                    )
                    for h in holdings
                ],
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Transactions
# -----------------------------------------------------------------------------

def append_txn(sid: str, txn: Dict):
    """Insert one transaction row."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO transactions
                (txn_date, strategy_id, action, ticker, shares, price,
                 commission, net_amount, cash_after, reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                txn.get("date", date.today().isoformat()),
                sid,
                txn.get("action", ""),
                txn.get("ticker", ""),
                round(float(txn.get("shares", 0)), 6),
                round(float(txn.get("price", 0)), 4),
                round(float(txn.get("commission", 0)), 4),
                round(float(txn.get("net_amount", 0)), 4),
                round(float(txn.get("cash_after", 0)), 4),
                str(txn.get("reason", ""))[:500],
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Leaderboard
# -----------------------------------------------------------------------------

def _lb_row_to_dict(r: Dict) -> Dict:
    return {
        "rank":           int(r.get("rank_pos", r.get("rank", 0))),
        "date":           str(r["lb_date"]),
        "strategy_id":    r["strategy_id"],
        "strategy_name":  r["strategy_name"],
        "style":          r["style"],
        "risk":           r["risk"],
        "cash":           float(r["cash"]),
        "holdings_value": float(r["holdings_value"]),
        "total":          float(r["total"]),
        "pnl":            float(r["pnl"]),
        "pct_return":     float(r["pct_return"]),
        "trades":         int(r["trades"]),
    }


def read_leaderboard() -> List[Dict]:
    """Return the most recent leaderboard snapshot, sorted by rank."""
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT * FROM leaderboard
            WHERE lb_date = (SELECT MAX(lb_date) FROM leaderboard)
            ORDER BY rank_pos
            """
        )
        rows = cur.fetchall()
        return [_lb_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def write_leaderboard(rows: List[Dict]):
    """
    Upsert today's leaderboard rows.
    Uses ON DUPLICATE KEY UPDATE so running multiple times is idempotent.
    """
    if not rows:
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO leaderboard
                (lb_date, rank_pos, strategy_id, strategy_name, style, risk,
                 cash, holdings_value, total, pnl, pct_return, trades)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                rank_pos       = VALUES(rank_pos),
                strategy_name  = VALUES(strategy_name),
                style          = VALUES(style),
                risk           = VALUES(risk),
                cash           = VALUES(cash),
                holdings_value = VALUES(holdings_value),
                total          = VALUES(total),
                pnl            = VALUES(pnl),
                pct_return     = VALUES(pct_return),
                trades         = VALUES(trades)
            """,
            [
                (
                    r.get("date", date.today().isoformat()),
                    int(r.get("rank", r.get("rank_pos", 0))),
                    r["strategy_id"],
                    r["strategy_name"],
                    r["style"],
                    r["risk"],
                    round(float(r["cash"]), 4),
                    round(float(r["holdings_value"]), 4),
                    round(float(r["total"]), 4),
                    round(float(r["pnl"]), 4),
                    round(float(r["pct_return"]), 4),
                    int(r.get("trades", 0)),
                )
                for r in rows
            ],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def read_leaderboard_history(since_date: Optional[str] = None) -> List[Dict]:
    """Return leaderboard rows optionally filtered by date."""
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        if since_date:
            cur.execute(
                "SELECT * FROM leaderboard WHERE lb_date >= %s ORDER BY lb_date, rank_pos",
                (since_date,),
            )
        else:
            cur.execute("SELECT * FROM leaderboard ORDER BY lb_date, rank_pos")
        return [_lb_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def append_leaderboard_history(rows: List[Dict]):
    """Alias for write_leaderboard -- history is stored in the same table."""
    write_leaderboard(rows)


# -----------------------------------------------------------------------------
# Equity KPI
# -----------------------------------------------------------------------------

def read_kpi_rows() -> List[Dict]:
    """
    Return all KPI rows as plain dicts with numeric fields already cast to float.
    Column names match the original CSV fieldnames exactly.
    Any extra columns not in the schema are surfaced from the extra_json column.
    """
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM equity_kpi")
        rows = cur.fetchall()
        result = []
        for r in rows:
            row: Dict[str, Any] = {"ticker": r["ticker"]}
            for col in _KPI_NUMERIC_COLS:
                v = r.get(col)
                row[col] = float(v) if v is not None else 0.0
            for col in _KPI_STR_COLS:
                row[col] = r.get(col) or ""
            # Merge extra_json fields back in
            extra = r.get("extra_json") or {}
            if isinstance(extra, str):
                import json as _json
                try:
                    extra = _json.loads(extra)
                except Exception:
                    extra = {}
            row.update(extra)
            result.append(row)
        return result
    finally:
        conn.close()


def write_kpi_rows(rows: List[Dict], fieldnames: Optional[List[str]] = None):
    """
    Replace the entire equity_kpi table with the supplied rows.
    fieldnames is accepted for API compatibility but ignored -- we infer
    columns from the row dicts directly.
    """
    import json as _json

    if not rows:
        return

    conn = get_connection()
    try:
        cur = conn.cursor()
        # Use INSERT ... ON DUPLICATE KEY UPDATE (not DELETE+INSERT) so the table
        # is never left empty if the insert fails part-way through.
        insert_rows = []
        for r in rows:
            ticker = r.get("ticker", "")
            if not ticker:
                continue

            # Separate known schema cols from extras
            extra = {}
            numeric_vals = {}
            str_vals = {}
            for k, v in r.items():
                if k in ("ticker", "updated_at"):
                    continue
                if k in _KPI_NUMERIC_COLS:
                    try:
                        numeric_vals[k] = float(v) if v not in (None, "", "nan") else None
                    except (TypeError, ValueError):
                        numeric_vals[k] = None
                elif k in _KPI_STR_COLS:
                    str_vals[k] = str(v) if v is not None else None
                else:
                    extra[k] = v

            insert_rows.append((
                ticker,
                numeric_vals.get("composite_score"),
                numeric_vals.get("tier1_score"),
                numeric_vals.get("tier2_score"),
                numeric_vals.get("tier3_score"),
                numeric_vals.get("net_profit_margin"),
                numeric_vals.get("eps_growth_fwd"),
                numeric_vals.get("eps_ttm"),
                numeric_vals.get("eps_forward"),
                numeric_vals.get("pe_ratio"),
                numeric_vals.get("current_price"),
                numeric_vals.get("rsi_14"),
                numeric_vals.get("ma_50"),
                numeric_vals.get("ma_200"),
                str_vals.get("ma_signal"),
                numeric_vals.get("beta"),
                numeric_vals.get("market_cap"),
                numeric_vals.get("pct_from_52w_high"),
                numeric_vals.get("abnormal_return"),
                numeric_vals.get("net_insider_shares"),
                numeric_vals.get("vix"),
                numeric_vals.get("dividend_yield"),
                numeric_vals.get("five_year_avg_dividend_yield"),
                numeric_vals.get("eps_revision_pct"),
                str_vals.get("sector"),
                _json.dumps(extra) if extra else None,
            ))

        cur.executemany(
            """
            INSERT INTO equity_kpi (
                ticker, composite_score, tier1_score, tier2_score, tier3_score,
                net_profit_margin, eps_growth_fwd, eps_ttm, eps_forward,
                pe_ratio, current_price, rsi_14, ma_50, ma_200, ma_signal,
                beta, market_cap, pct_from_52w_high, abnormal_return,
                net_insider_shares, vix, dividend_yield,
                five_year_avg_dividend_yield, eps_revision_pct,
                sector, extra_json
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                composite_score              = VALUES(composite_score),
                tier1_score                  = VALUES(tier1_score),
                tier2_score                  = VALUES(tier2_score),
                tier3_score                  = VALUES(tier3_score),
                net_profit_margin            = VALUES(net_profit_margin),
                eps_growth_fwd               = VALUES(eps_growth_fwd),
                eps_ttm                      = VALUES(eps_ttm),
                eps_forward                  = VALUES(eps_forward),
                pe_ratio                     = VALUES(pe_ratio),
                current_price                = VALUES(current_price),
                rsi_14                       = VALUES(rsi_14),
                ma_50                        = VALUES(ma_50),
                ma_200                       = VALUES(ma_200),
                ma_signal                    = VALUES(ma_signal),
                beta                         = VALUES(beta),
                market_cap                   = VALUES(market_cap),
                pct_from_52w_high            = VALUES(pct_from_52w_high),
                abnormal_return              = VALUES(abnormal_return),
                net_insider_shares           = VALUES(net_insider_shares),
                vix                          = VALUES(vix),
                dividend_yield               = VALUES(dividend_yield),
                five_year_avg_dividend_yield = VALUES(five_year_avg_dividend_yield),
                eps_revision_pct             = VALUES(eps_revision_pct),
                sector                       = VALUES(sector),
                extra_json                   = VALUES(extra_json)
            """,
            insert_rows,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_kpi_abnormal_returns(ab_map: Dict[str, float]):
    """
    Patch only the abnormal_return column for the tickers in ab_map.
    Much faster than a full write_kpi_rows() when only this field changed.
    """
    if not ab_map:
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.executemany(
            "UPDATE equity_kpi SET abnormal_return = %s WHERE ticker = %s",
            [(round(v, 4), k) for k, v in ab_map.items()],
        )
        conn.commit()
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Hard reset
# -----------------------------------------------------------------------------

def reset_all(starting_cash: float, strategies: list, account_num: str):
    """
    Delete all trading data and reinitialise every strategy account.

    strategies is the STRATEGIES list from constants.py:
        [(id, name, style, risk, description), ...]
    """
    today = date.today().isoformat()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM transactions")
        cur.execute("DELETE FROM holdings")
        cur.execute("DELETE FROM leaderboard")
        cur.execute("DELETE FROM accounts")

        for sid, name, style, risk, *_ in strategies:
            cur.execute(
                """
                INSERT INTO accounts
                    (strategy_id, account, cash, holdings_value, total, start_date, trades)
                VALUES (%s, %s, %s, 0, %s, %s, 0)
                """,
                (sid, account_num, starting_cash, starting_cash, today),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Down-day results
# -----------------------------------------------------------------------------

def write_down_day_results(analysis_date: str, market_return_pct: float,
                            vix: Optional[float], rows: List[Dict]):
    """
    Upsert down-day resilience results for a given analysis date.
    Existing rows for the date are replaced atomically.
    """
    if not rows:
        return
    import json as _json

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM down_day_results WHERE analysis_date = %s", (analysis_date,))
        cur.executemany(
            """
            INSERT INTO down_day_results (
                analysis_date, market_return_pct, vix,
                ticker, name, sector, industry,
                day_return_pct, abnormal_return_pct, actually_gained,
                beta, pe_ratio, forward_pe, net_margin_pct,
                dividend_yield_pct, market_cap_B, short_pct_float,
                pct_from_52w_high, ma_signal, debt_to_equity,
                current_ratio, insider_pct, analyst_rating
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                market_return_pct   = VALUES(market_return_pct),
                vix                 = VALUES(vix),
                name                = VALUES(name),
                sector              = VALUES(sector),
                industry            = VALUES(industry),
                day_return_pct      = VALUES(day_return_pct),
                abnormal_return_pct = VALUES(abnormal_return_pct),
                actually_gained     = VALUES(actually_gained),
                beta                = VALUES(beta),
                pe_ratio            = VALUES(pe_ratio),
                forward_pe          = VALUES(forward_pe),
                net_margin_pct      = VALUES(net_margin_pct),
                dividend_yield_pct  = VALUES(dividend_yield_pct),
                market_cap_B        = VALUES(market_cap_B),
                short_pct_float     = VALUES(short_pct_float),
                pct_from_52w_high   = VALUES(pct_from_52w_high),
                ma_signal           = VALUES(ma_signal),
                debt_to_equity      = VALUES(debt_to_equity),
                current_ratio       = VALUES(current_ratio),
                insider_pct         = VALUES(insider_pct),
                analyst_rating      = VALUES(analyst_rating)
            """,
            [
                (
                    analysis_date,
                    round(float(market_return_pct), 4),
                    round(float(vix), 2) if vix is not None else None,
                    r.get("ticker", ""),
                    str(r.get("name", ""))[:100],
                    str(r.get("sector", ""))[:60],
                    str(r.get("industry", ""))[:100],
                    _safe_float(r.get("day_return_pct")),
                    _safe_float(r.get("abnormal_return_pct")),
                    1 if r.get("actually_gained") == "YES" else 0,
                    _safe_float(r.get("beta")),
                    _safe_float(r.get("pe_ratio")),
                    _safe_float(r.get("forward_pe")),
                    _safe_float(r.get("net_margin_pct")),
                    _safe_float(r.get("dividend_yield_pct")),
                    _safe_float(r.get("market_cap_B")),
                    _safe_float(r.get("short_pct_float")),
                    _safe_float(r.get("pct_from_52w_high")),
                    str(r.get("50d_vs_200d_ma", ""))[:10] or None,
                    _safe_float(r.get("debt_to_equity")),
                    _safe_float(r.get("current_ratio")),
                    _safe_float(r.get("insider_pct")),
                    _safe_float(r.get("analyst_rating")),
                )
                for r in rows
            ],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def read_down_day_results(analysis_date: Optional[str] = None,
                           days: int = 30) -> List[Dict]:
    """
    Return down-day results. If analysis_date given, return that date only.
    Otherwise return the last `days` days of results.
    """
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        if analysis_date:
            cur.execute(
                "SELECT * FROM down_day_results WHERE analysis_date = %s "
                "ORDER BY abnormal_return_pct DESC",
                (analysis_date,),
            )
        else:
            cur.execute(
                "SELECT * FROM down_day_results "
                "WHERE analysis_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY) "
                "ORDER BY analysis_date DESC, abnormal_return_pct DESC",
                (days,),
            )
        return cur.fetchall()
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Universe cache
# -----------------------------------------------------------------------------

def write_universe_cache(tickers: List[str], mode: str,
                          sector_breakdown: Optional[Dict] = None):
    """Persist the universe selection to MySQL. One row per refresh."""
    import json as _json
    from datetime import date as _date
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO universe_cache
                (refreshed, mode, n, tickers_json, sector_breakdown_json)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                n                     = VALUES(n),
                tickers_json          = VALUES(tickers_json),
                sector_breakdown_json = VALUES(sector_breakdown_json)
            """,
            (
                _date.today().isoformat(),
                mode,
                len(tickers),
                _json.dumps(tickers),
                _json.dumps(sector_breakdown) if sector_breakdown else None,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def read_universe_cache(mode: str = "balanced",
                         max_age_days: int = 7) -> Optional[Dict]:
    """
    Return the most recent universe cache row if it is within max_age_days
    and matches the requested mode. Returns None if stale or missing.
    """
    import json as _json
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT * FROM universe_cache
            WHERE mode = %s
              AND refreshed >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY refreshed DESC, id DESC
            LIMIT 1
            """,
            (mode, max_age_days),
        )
        row = cur.fetchone()
        if not row:
            return None
        tickers = _json.loads(row["tickers_json"]) if row.get("tickers_json") else []
        breakdown = _json.loads(row["sector_breakdown_json"]) if row.get("sector_breakdown_json") else {}
        return {
            "tickers":          tickers,
            "refreshed":        str(row["refreshed"]),
            "mode":             row["mode"],
            "n":                row["n"],
            "sector_breakdown": breakdown,
        }
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# News macro cache
# -----------------------------------------------------------------------------

def write_news_macro_cache(data: Dict):
    """
    Upsert today's news macro analysis to MySQL.
    Replaces any existing row for the same analysis_date.
    `data` is the full JSON dict returned by Claude.
    """
    import json as _json
    analysis_date = data.get("analysis_date") or date.today().isoformat()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO news_macro_cache
                (analysis_date, market_regime, themes_json, catalysts_json, raw_json)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                market_regime  = VALUES(market_regime),
                themes_json    = VALUES(themes_json),
                catalysts_json = VALUES(catalysts_json),
                raw_json       = VALUES(raw_json)
            """,
            (
                analysis_date,
                str(data.get("market_regime", "NEUTRAL"))[:20],
                _json.dumps(data.get("dominant_themes", [])),
                _json.dumps(data.get("company_catalysts", [])),
                _json.dumps(data),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def read_news_macro_cache(analysis_date: Optional[str] = None) -> Optional[Dict]:
    """
    Return the news macro analysis for the given date (default: today).
    Returns None if not found.
    """
    import json as _json
    target = analysis_date or date.today().isoformat()
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM news_macro_cache WHERE analysis_date = %s",
            (target,),
        )
        row = cur.fetchone()
        if not row:
            return None
        # Return the full original dict so callers need no changes
        return _json.loads(row["raw_json"])
    finally:
        conn.close()


def read_news_macro_cache_latest() -> Optional[Dict]:
    """Return the most recent news macro cache row regardless of date."""
    import json as _json
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM news_macro_cache ORDER BY analysis_date DESC LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return None
        return _json.loads(row["raw_json"])
    finally:
        conn.close()




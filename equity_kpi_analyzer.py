# -*- coding: utf-8 -*-
"""
equity_kpi_analyzer.py
======================
Daily equity KPI scoring script based on your tiered framework.
Pulls data from yfinance (free) and FRED API (free).

Usage:
    # Run on auto-selected top 200 S&P 500 stocks (recommended)
    python equity_kpi_analyzer.py --universe

    # Run on top 200, sector-balanced
    python equity_kpi_analyzer.py --universe --universe-mode balanced

    # Force-refresh the universe cache (do this weekly or on demand)
    python equity_kpi_analyzer.py --universe --refresh-universe

    # Run on specific tickers only
    python equity_kpi_analyzer.py --tickers AAPL MSFT GOOGL

    # With FRED macro data
    python equity_kpi_analyzer.py --universe --fred-key YOUR_KEY

    # Show current universe cache info
    python universe_manager.py --info

FRED API key (free): https://fred.stlouisfed.org/docs/api/api_key.html
"""

import argparse
import warnings
import datetime
import sys
import os
from pathlib import Path

import pandas as pd
import numpy as np
import yfinance as yf

warnings.filterwarnings("ignore")

# Silence yfinance/peewee DEBUG chatter -- only our messages go to run_logs
import logging as _logging
_logging.getLogger("yfinance").setLevel(_logging.WARNING)
_logging.getLogger("peewee").setLevel(_logging.WARNING)

# -- Load .env if present (mirrors strategy_runner.py) ------------------------
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            _v = _v.strip().strip('"').strip("'")   # remove surrounding quotes
            os.environ.setdefault(_k.strip(), _v)

# Force UTF-8 output on Windows to avoid Unicode errors in logs/console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from db import init_schema, write_kpi_rows as _db_write_kpi_rows, read_kpi_rows as _db_read_kpi_rows
from db_logger import Logger as _DbLogger
logger = _DbLogger(run_stage="kpi", echo=True)

# Universe manager (same directory)
try:
    from universe_manager import get_universe, show_cache_info
    UNIVERSE_AVAILABLE = True
except ImportError:
    UNIVERSE_AVAILABLE = False

# ---------------------------------------------
#  CONFIG
# ---------------------------------------------
DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "JPM", "JNJ", "XOM"]
LOOKBACK_DAYS   = 365          # history window for price-based calculations
RSI_PERIOD      = 14
MA_SHORT        = 50
MA_LONG         = 200

# Tier weights for composite score (must sum to 1.0)
TIER_WEIGHT = {1: 0.60, 2: 0.30, 3: 0.10}


# ---------------------------------------------
#  MACRO DATA  (FRED)
# ---------------------------------------------

def fetch_macro_fred(fred_key: str | None) -> dict:
    """Fetch latest macro indicators from FRED. Returns empty dict if no key."""
    macro = {}
    if not fred_key:
        logger.info("  [MACRO] No FRED key provided -- skipping macro data.")
        logger.info("          Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
        return macro

    try:
        from fredapi import Fred
        fred = Fred(api_key=fred_key)

        # GDP growth (quarterly)
        gdp = fred.get_series("GDP").dropna()
        if len(gdp) >= 2 and gdp.iloc[-2] != 0:
            _gdp_ratio = (gdp.iloc[-1] - gdp.iloc[-2]) / gdp.iloc[-2]
            if np.isfinite(_gdp_ratio):
                macro["gdp_growth_rate"] = _gdp_ratio

        # CPI inflation (monthly)
        cpi = fred.get_series("CPIAUCSL").dropna()
        if len(cpi) >= 2 and cpi.iloc[-2] != 0:
            _cpi_ratio = (cpi.iloc[-1] - cpi.iloc[-2]) / cpi.iloc[-2]
            if np.isfinite(_cpi_ratio):
                macro["inflation_rate_cpi"] = _cpi_ratio

        # Fed funds rate change
        ffr = fred.get_series("FEDFUNDS").dropna()
        if len(ffr) >= 2:
            macro["policy_rate_change"] = ffr.iloc[-1] - ffr.iloc[-2]
            macro["policy_rate_current"] = ffr.iloc[-1]

        gdp_s = f"{macro['gdp_growth_rate']:.4f}" if 'gdp_growth_rate' in macro else 'N/A'
        cpi_s = f"{macro['inflation_rate_cpi']:.4f}" if 'inflation_rate_cpi' in macro else 'N/A'
        ffr_s = f"{macro['policy_rate_current']:.2f}%" if 'policy_rate_current' in macro else 'N/A'
        logger.info(f"  [MACRO] GDP growth: {gdp_s}  |  CPI: {cpi_s}  |  Fed rate: {ffr_s}")

    except Exception as e:
        logger.error(f"  [MACRO] FRED fetch failed: {e}")

    return macro


def fetch_vix() -> float | None:
    """Fetch latest VIX close from yfinance."""
    try:
        vix = yf.Ticker("^VIX").history(period="5d")
        if not vix.empty:
            return round(vix["Close"].iloc[-1], 2)
    except Exception:
        pass
    return None


# ---------------------------------------------
#  TECHNICAL INDICATORS
# ---------------------------------------------

def compute_rsi(prices: pd.Series, period: int = 14) -> float | None:
    if len(prices) < period + 1:
        return None
    delta  = prices.diff()
    gain   = delta.clip(lower=0).rolling(period).mean()
    loss   = (-delta.clip(upper=0)).rolling(period).mean()
    rs     = gain / loss.replace(0, np.nan)
    rsi    = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    if pd.isna(val):
        return None
    return round(float(val), 2)


def compute_ma_signal(prices: pd.Series) -> dict:
    result = {"ma_50": None, "ma_200": None, "ma_signal": "insufficient data"}
    if len(prices) >= MA_LONG:
        result["ma_50"]  = round(prices.rolling(MA_SHORT).mean().iloc[-1], 2)
        result["ma_200"] = round(prices.rolling(MA_LONG).mean().iloc[-1], 2)
        if pd.notna(result["ma_50"]) and pd.notna(result["ma_200"]):
            if result["ma_50"] > result["ma_200"]:
                result["ma_signal"] = "BULLISH (Golden Cross)"
            else:
                result["ma_signal"] = "BEARISH (Death Cross)"
        else:
            result["ma_signal"] = "insufficient data"
    elif len(prices) >= MA_SHORT:
        result["ma_50"] = round(prices.rolling(MA_SHORT).mean().iloc[-1], 2)
        result["ma_signal"] = "insufficient history for 200-day MA"
    return result


def compute_abnormal_return(prices: pd.Series, market_prices: pd.Series, beta: float) -> float | None:
    """Single-day abnormal return = actual return - (beta * market return)."""
    if len(prices) < 2 or len(market_prices) < 2:
        return None
    prev_price, prev_market = prices.iloc[-2], market_prices.iloc[-2]
    if prev_price <= 0 or prev_market <= 0:
        return None
    stock_ret  = (prices.iloc[-1]        - prev_price)  / prev_price
    market_ret = (market_prices.iloc[-1] - prev_market) / prev_market
    if not (np.isfinite(stock_ret) and np.isfinite(market_ret)):
        return None
    return round(stock_ret - beta * market_ret, 4)


# ---------------------------------------------
#  PER-TICKER FUNDAMENTALS
# ---------------------------------------------

def fetch_ticker_data(ticker_symbol: str, market_prices: pd.Series) -> dict:
    row = {"ticker": ticker_symbol}

    try:
        tk   = yf.Ticker(ticker_symbol)
        info = tk.info or {}

        # -- TIER 1: Financial Performance ------------------------------
        # Net Profit Margin
        net_income = info.get("netIncomeToCommon")
        revenue    = info.get("totalRevenue")
        if net_income and revenue and revenue != 0:
            row["net_profit_margin"] = round(net_income / revenue, 4)

        # EPS Growth (TTM vs prior year via yf financials)
        row["eps_ttm"]      = info.get("trailingEps")
        row["eps_forward"]  = info.get("forwardEps")
        if row.get("eps_ttm") and row.get("eps_forward") and abs(row["eps_ttm"]) >= 0.01:
            row["eps_growth_fwd"] = round(
                (row["eps_forward"] - row["eps_ttm"]) / abs(row["eps_ttm"]), 4
            )

        # Current Ratio
        current_assets      = info.get("totalCurrentAssets")
        current_liabilities = info.get("totalCurrentLiabilities")
        if current_assets and current_liabilities and current_liabilities != 0:
            row["current_ratio"] = round(current_assets / current_liabilities, 2)

        # Bonus: P/E and market cap (useful context)
        row["pe_ratio"]    = info.get("trailingPE")
        row["market_cap"]  = info.get("marketCap")
        row["sector"]      = info.get("sector", "Unknown")
        row["beta"]        = info.get("beta", 1.0)

        # FIX-9: Dividend data for Strategy 08 (dividend growth)
        # dividendYield is always a decimal in yfinance (e.g. 0.015 = 1.5%).
        # fiveYearAvgDividendYield changed behaviour across yfinance versions:
        #   - Older versions returned a percentage (e.g. 1.5 meaning 1.5%)
        #   - Newer versions (>=0.2.x) return a decimal (e.g. 0.015)
        # We detect which format is in use at runtime: values > 0.5 are almost
        # certainly percentages (no realistic stock yields 50%+), so we divide
        # by 100. Both fields are normalised to decimal for consistent scoring.
        raw_dy  = info.get("dividendYield")
        raw_dy5 = info.get("fiveYearAvgDividendYield")
        if raw_dy is not None:
            row["dividend_yield"] = round(float(raw_dy), 6)
        if raw_dy5 is not None:
            dy5_val = float(raw_dy5)
            # If the value looks like a percentage (>0.5), convert to decimal.
            # This handles both old and new yfinance versions automatically.
            if dy5_val > 0.5:
                dy5_val = dy5_val / 100
            row["five_year_avg_dividend_yield"] = round(dy5_val, 6)

        # -- TIER 2: Technical Indicators -------------------------------
        hist = tk.history(period="1y")
        if not hist.empty:
            prices = hist["Close"]
            row["current_price"] = round(prices.iloc[-1], 2)
            row["rsi_14"]        = compute_rsi(prices, RSI_PERIOD)
            row.update(compute_ma_signal(prices))

            # Abnormal return
            beta = row.get("beta") if row.get("beta") is not None else 1.0
            row["abnormal_return"] = compute_abnormal_return(prices, market_prices, beta)

            # 52-week high/low context
            row["52w_high"] = round(prices.max(), 2)
            row["52w_low"]  = round(prices.min(), 2)
            row["pct_from_52w_high"] = round(
                (prices.iloc[-1] - prices.max()) / prices.max(), 4
            )

        # -- TIER 3: Insider & Analyst Data -----------------------------
        # Analyst ratings
        try:
            rec = tk.recommendations
            if rec is not None and not rec.empty:
                # Recent 90 days
                rec.index = pd.to_datetime(rec.index, utc=True)
                cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=90)
                recent = rec[rec.index >= cutoff]
                if not recent.empty and "To Grade" in recent.columns:
                    grade_map = {
                        "Strong Buy": 5, "Buy": 4, "Overweight": 4,
                        "Hold": 3, "Neutral": 3, "Underweight": 2,
                        "Sell": 2, "Strong Sell": 1
                    }
                    grades = recent["To Grade"].map(grade_map).dropna()
                    if not grades.empty:
                        row["analyst_consensus"] = round(grades.mean(), 2)
                        row["analyst_count"]     = len(grades)
        except Exception:
            pass

        # Insider transactions (net buy/sell shares last 6 months)
        try:
            insiders = tk.insider_transactions
            if insiders is not None and not insiders.empty:
                if "Shares" in insiders.columns and "Transaction" in insiders.columns:
                    buys  = insiders[insiders["Transaction"].str.contains("Buy",  na=False)]["Shares"].sum()
                    sells = insiders[insiders["Transaction"].str.contains("Sale", na=False)]["Shares"].sum()
                    row["net_insider_shares"] = int(buys - sells)
        except Exception:
            pass

    except Exception as e:
        row["error"] = str(e)

    return row


# ---------------------------------------------
#  SCORING ENGINE
# ---------------------------------------------

def score_ticker(row: dict, macro: dict, vix: float | None) -> dict:
    """
    Convert raw KPIs into 0-100 scores per tier, then a weighted composite.
    Each sub-score is 0-100 (100 = most bullish signal).
    """
    scores = {}

    # -- TIER 1 ----------------------------------------------------------
    t1 = []

    # Net Profit Margin (target > 20%)
    npm = row.get("net_profit_margin")
    if npm is not None:
        t1.append(min(100, max(0, npm / 0.20 * 100)))

    # EPS Forward Growth (target > 10%)
    eps_g = row.get("eps_growth_fwd")
    if eps_g is not None:
        t1.append(min(100, max(0, eps_g / 0.10 * 100)))

    # Current Ratio (target > 1.5, cap at 3.0)
    cr = row.get("current_ratio")
    if cr is not None:
        t1.append(min(100, max(0, (cr - 1.0) / 2.0 * 100)))

    # GDP Growth (target > 0.5% quarterly)
    gdp = macro.get("gdp_growth_rate")
    if gdp is not None:
        t1.append(min(100, max(0, gdp / 0.005 * 100)))

    # Inflation (ideal 1.5-2.5%; penalise outside range)
    cpi = macro.get("inflation_rate_cpi")
    if cpi is not None:
        annualised = cpi * 12
        if 0.015 <= annualised <= 0.025:
            t1.append(100)
        else:
            dist = min(abs(annualised - 0.015), abs(annualised - 0.025))
            t1.append(max(0, 100 - dist / 0.01 * 50))

    # Policy Rate Change (rate cuts = bullish)
    prc = macro.get("policy_rate_change")
    if prc is not None:
        # -0.25 pp cut -> 100, 0 -> 50, +0.25 hike -> 0
        t1.append(min(100, max(0, 50 - prc / 0.25 * 50)))

    # VIX (< 20 = 100, > 30 = 0)
    if vix is not None:
        t1.append(min(100, max(0, (30 - vix) / 10 * 100)))

    scores["tier1_score"] = round(np.mean(t1), 1) if t1 else None

    # -- TIER 2 ----------------------------------------------------------
    t2 = []

    # RSI (30-70 neutral; < 30 oversold = opportunity = high score; > 70 = low score)
    rsi = row.get("rsi_14")
    if rsi is not None:
        if rsi < 30:
            t2.append(90)          # oversold = buying opportunity
        elif rsi > 70:
            t2.append(20)          # overbought = caution
        else:
            t2.append(60)          # neutral zone

    # MA Signal
    ma_sig = row.get("ma_signal", "")
    if "BULLISH" in ma_sig:
        t2.append(85)
    elif "BEARISH" in ma_sig:
        t2.append(20)

    # Abnormal Return (positive = good signal, cap at +/-5%)
    ar = row.get("abnormal_return")
    if ar is not None and np.isfinite(ar):
        t2.append(min(100, max(0, 50 + ar / 0.05 * 50)))

    scores["tier2_score"] = round(np.mean(t2), 1) if t2 else None

    # -- TIER 3 ----------------------------------------------------------
    t3 = []

    # Analyst Consensus (1-5 scale)
    ac = row.get("analyst_consensus")
    if ac is not None:
        t3.append((ac - 1) / 4 * 100)

    # Net Insider Transactions
    ni = row.get("net_insider_shares")
    if ni is not None:
        if ni > 0:
            t3.append(80)
        elif ni == 0:
            t3.append(50)
        else:
            t3.append(30)

    scores["tier3_score"] = round(np.mean(t3), 1) if t3 else None

    # -- COMPOSITE -------------------------------------------------------
    weighted, weight_sum = 0.0, 0.0
    for tier, key in [(1, "tier1_score"), (2, "tier2_score"), (3, "tier3_score")]:
        val = scores.get(key)
        if val is not None:
            weighted    += val * TIER_WEIGHT[tier]
            weight_sum  += TIER_WEIGHT[tier]

    scores["composite_score"] = round(weighted / weight_sum, 1) if weight_sum > 0 else None

    return scores


# ---------------------------------------------
#  SIGNAL LABELS
# ---------------------------------------------

def signal_label(score: float | None) -> str:
    if score is None:
        return "N/A"
    if score >= 75: return "STRONG BUY  ^^"
    if score >= 60: return "BUY         ^"
    if score >= 45: return "HOLD        -"
    if score >= 30: return "SELL        v"
    return              "STRONG SELL vv"


def _compute_eps_revision_pct(row: dict, prev_map: dict) -> float | None:
    """
    Compute the percentage change in eps_growth_fwd vs the previous day's snapshot.
    Returns None if either value is missing/zero (not a reliable revision signal).
    Kept at module level so it can be tested independently and doesn't disappear
    if the surrounding try-block is refactored.
    """
    import math
    prev = prev_map.get(row["ticker"])
    curr = row.get("eps_growth_fwd")
    if prev is None or curr is None:
        return None
    try:
        if math.isnan(float(prev)) or math.isnan(float(curr)):
            return None
    except (TypeError, ValueError):
        return None
    if float(prev) == 0:
        return None
    return round((float(curr) - float(prev)) / abs(float(prev)), 4)


# ---------------------------------------------
#  MAIN
# ---------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Daily equity KPI scorer")

    # Ensure DB tables exist before any work — catches connection failures
    # early rather than after several minutes of data fetching.
    init_schema()

    # Universe selection (new)
    uni = parser.add_argument_group("Universe selection")
    uni.add_argument("--universe",         action="store_true",
                     help="Auto-select top N stocks from S&P 500 (recommended)")
    uni.add_argument("--universe-n",       type=int, default=200,
                     help="How many stocks to include (default: 200)")
    uni.add_argument("--universe-mode",    default="balanced",
                     choices=["balanced", "market_cap"],
                     help="balanced = sector-capped | market_cap = pure ranking")
    uni.add_argument("--max-per-sector",   type=int, default=40,
                     help="Max stocks per sector in balanced mode (default: 40)")
    uni.add_argument("--refresh-universe", action="store_true",
                     help="Force re-fetch of S&P 500 constituents and market caps")
    uni.add_argument("--universe-info",    action="store_true",
                     help="Show current universe cache info and exit")

    # Manual override
    parser.add_argument("--tickers",  nargs="+", default=None,
                        help="Explicit list of tickers (overrides --universe)")
    parser.add_argument("--fred-key", default=None,
                        help="FRED API key (free at fred.stlouisfed.org). "
                             "Defaults to FRED_API_KEY environment variable / .env file.")

    args = parser.parse_args()

    # Fall back to env var so .env file works without passing --fred-key every time
    if not args.fred_key:
        args.fred_key = os.environ.get("FRED_API_KEY") or None

    # -- Universe info only ----------------------------------------------
    if args.universe_info:
        if UNIVERSE_AVAILABLE:
            show_cache_info()
        else:
            logger.info("universe_manager.py not found in the same directory.")
        logger.flush()
        return

    # -- Resolve ticker list ---------------------------------------------
    if args.tickers:
        tickers = args.tickers
        source  = "manual"
    elif args.universe:
        if not UNIVERSE_AVAILABLE:
            logger.error("ERROR: universe_manager.py not found. Place it in the same directory.")
            logger.flush()
            sys.exit(1)
        tickers = get_universe(
            n               = args.universe_n,
            mode            = args.universe_mode,
            max_per_sector  = args.max_per_sector,
            force_refresh   = args.refresh_universe,
        )
        source = f"S&P 500 top {args.universe_n} ({args.universe_mode})"
    else:
        # Default small list if neither flag given
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "JPM", "JNJ", "XOM"]
        source  = "default sample"

    logger.info("\n" + "="*65)
    logger.info("  EQUITY KPI DAILY ANALYSER")
    logger.info(f"  Run date : {datetime.date.today()}")
    logger.info(f"  Universe : {source}  ({len(tickers)} tickers)")
    logger.info("="*65)

    # -- Macro & Market data ---------------------------------------------
    logger.info("\n[1/3] Fetching macro data...")
    macro = fetch_macro_fred(args.fred_key)

    logger.info("\n[2/3] Fetching VIX & S&P 500 benchmark...")
    vix = fetch_vix()
    logger.info(f"  VIX = {vix}")

    spx_hist = yf.Ticker("^GSPC").history(period="1y")
    market_prices = spx_hist["Close"] if not spx_hist.empty else pd.Series(dtype=float)

    # -- Per-ticker loop -------------------------------------------------
    logger.info(f"\n[3/3] Fetching data for {len(tickers)} tickers...\n")
    results = []
    for i, sym in enumerate(tickers, 1):
        logger.info(f"  [{i}/{len(tickers)}] {sym}...", end=" ", flush=True)
        row = fetch_ticker_data(sym, market_prices)
        scores = score_ticker(row, macro, vix)
        row.update(scores)
        row["vix"] = vix
        row.update({f"macro_{k}": v for k, v in macro.items()})
        results.append(row)
        logger.info(f"composite={row.get('composite_score', 'err')}")

    # -- Build DataFrame -------------------------------------------------
    df = pd.DataFrame(results)
    df["signal"] = df["composite_score"].apply(signal_label)
    df = df.sort_values("composite_score", ascending=False)

    # -- EPS revision delta (Strategy 16 signal) -------------------------
    # Compare today's eps_growth_fwd to yesterday's snapshot (if it exists).
    # Snapshot is the previous output CSV -- we read it before overwriting.
    # Build previous eps_growth_fwd map: try MySQL first, fall back to CSV backup.
    prev_map = {}
    if "eps_growth_fwd" in df.columns:
        try:
            prev_rows = _db_read_kpi_rows()
            if prev_rows:
                prev_map = {r["ticker"]: float(r["eps_growth_fwd"])
                            for r in prev_rows
                            if r.get("eps_growth_fwd") not in (None, 0.0)
                            and pd.notna(r.get("eps_growth_fwd"))}
                logger.info(f"  [S16] Loaded prior eps_growth_fwd from MySQL "
                      f"({len(prev_map)} tickers)")
        except Exception as e:
            logger.error(f"  [S16] MySQL read failed ({e}), trying CSV backup...")

        # No CSV fallback — MySQL equity_kpi table is the single source of truth

        if prev_map:
            df["eps_revision_pct"] = df.apply(
                lambda row: _compute_eps_revision_pct(row, prev_map), axis=1
            )
            n_revised = (df["eps_revision_pct"].fillna(0) > 0).sum()
            logger.info(f"  [S16] EPS revision delta computed -- "
                  f"{n_revised} tickers with upward revisions today")
        else:
            df["eps_revision_pct"] = None
            logger.warning("  [S16] No prior snapshot found -- "
                  "eps_revision_pct will populate from tomorrow's run")

    # -- Console output --------------------------------------------------
    DISPLAY_COLS = [
        "ticker", "sector", "current_price", "composite_score", "signal",
        "tier1_score", "tier2_score", "tier3_score",
        "net_profit_margin", "eps_ttm", "eps_growth_fwd", "current_ratio",
        "rsi_14", "ma_signal",
        "dividend_yield", "five_year_avg_dividend_yield",
        "analyst_consensus", "net_insider_shares",
    ]
    available = [c for c in DISPLAY_COLS if c in df.columns]

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", "{:.4f}".format)

    logger.info("\n" + "="*65)
    logger.info("  RESULTS  (sorted by composite score)")
    logger.info("="*65)
    logger.info(df[available].to_string(index=False))

    # Top 20 summary
    logger.info("\n" + "-"*65)
    logger.info("  TOP 20 SIGNALS")
    logger.info("-"*65)
    for _, r in df.head(20).iterrows():
        score = r.get("composite_score")
        score_str = f"{score:.1f}" if pd.notna(score) else " N/A"
        sector = str(r.get("sector", ""))[:20]
        logger.info(f"  {r['ticker']:<6}  {score_str:>5}/100   {r.get('signal','N/A'):<18}  {sector}")

    # Sector summary
    if "sector" in df.columns and "composite_score" in df.columns:
        logger.info("\n" + "-"*65)
        logger.info("  SECTOR AVERAGES  (composite score)")
        logger.info("-"*65)
        sec_avg = (
            df.groupby("sector")["composite_score"]
            .agg(["mean", "count"])
            .sort_values("mean", ascending=False)
        )
        for sec, row in sec_avg.iterrows():
            logger.info(f"  {str(sec):<40}  avg={row['mean']:>5.1f}  n={int(row['count'])}")

    # -- Save to MySQL ----------------------------------------------------
    rows = df.to_dict(orient="records")
    _db_write_kpi_rows(rows)
    logger.info(f"\n(ok) Full results saved to MySQL equity_kpi table ({len(rows)} tickers)")

    logger.info("="*65 + "\n")
    logger.flush()

    return df


if __name__ == "__main__":
    main()

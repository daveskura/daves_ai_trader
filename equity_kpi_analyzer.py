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

import pandas as pd
import numpy as np
import yfinance as yf

warnings.filterwarnings("ignore")

# Force UTF-8 output on Windows to avoid Unicode errors in logs/console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Universe manager (same directory)
try:
    from universe_manager import get_universe, show_cache_info
    UNIVERSE_AVAILABLE = True
except ImportError:
    UNIVERSE_AVAILABLE = False

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "JPM", "JNJ", "XOM"]
LOOKBACK_DAYS   = 365          # history window for price-based calculations
RSI_PERIOD      = 14
MA_SHORT        = 50
MA_LONG         = 200

# Tier weights for composite score (must sum to 1.0)
TIER_WEIGHT = {1: 0.60, 2: 0.30, 3: 0.10}


# ─────────────────────────────────────────────
#  MACRO DATA  (FRED)
# ─────────────────────────────────────────────

def fetch_macro_fred(fred_key: str | None) -> dict:
    """Fetch latest macro indicators from FRED. Returns empty dict if no key."""
    macro = {}
    if not fred_key:
        print("  [MACRO] No FRED key provided — skipping macro data.")
        print("          Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
        return macro

    try:
        from fredapi import Fred
        fred = Fred(api_key=fred_key)

        # GDP growth (quarterly)
        gdp = fred.get_series("GDP").dropna()
        if len(gdp) >= 2:
            macro["gdp_growth_rate"] = (gdp.iloc[-1] - gdp.iloc[-2]) / gdp.iloc[-2]

        # CPI inflation (monthly)
        cpi = fred.get_series("CPIAUCSL").dropna()
        if len(cpi) >= 2:
            macro["inflation_rate_cpi"] = (cpi.iloc[-1] - cpi.iloc[-2]) / cpi.iloc[-2]

        # Fed funds rate change
        ffr = fred.get_series("FEDFUNDS").dropna()
        if len(ffr) >= 2:
            macro["policy_rate_change"] = ffr.iloc[-1] - ffr.iloc[-2]
            macro["policy_rate_current"] = ffr.iloc[-1]

        print(f"  [MACRO] GDP growth: {macro.get('gdp_growth_rate', 'N/A'):.4f}  |  "
              f"CPI: {macro.get('inflation_rate_cpi', 'N/A'):.4f}  |  "
              f"Fed rate: {macro.get('policy_rate_current', 'N/A'):.2f}%")

    except Exception as e:
        print(f"  [MACRO] FRED fetch failed: {e}")

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


# ─────────────────────────────────────────────
#  TECHNICAL INDICATORS
# ─────────────────────────────────────────────

def compute_rsi(prices: pd.Series, period: int = 14) -> float | None:
    if len(prices) < period + 1:
        return None
    delta  = prices.diff()
    gain   = delta.clip(lower=0).rolling(period).mean()
    loss   = (-delta.clip(upper=0)).rolling(period).mean()
    rs     = gain / loss.replace(0, np.nan)
    rsi    = 100 - (100 / (1 + rs))
    return round(rsi.iloc[-1], 2)


def compute_ma_signal(prices: pd.Series) -> dict:
    result = {"ma_50": None, "ma_200": None, "ma_signal": "insufficient data"}
    if len(prices) >= MA_LONG:
        result["ma_50"]  = round(prices.rolling(MA_SHORT).mean().iloc[-1], 2)
        result["ma_200"] = round(prices.rolling(MA_LONG).mean().iloc[-1], 2)
        if result["ma_50"] > result["ma_200"]:
            result["ma_signal"] = "BULLISH (Golden Cross)"
        else:
            result["ma_signal"] = "BEARISH (Death Cross)"
    elif len(prices) >= MA_SHORT:
        result["ma_50"] = round(prices.rolling(MA_SHORT).mean().iloc[-1], 2)
        result["ma_signal"] = "insufficient history for 200-day MA"
    return result


def compute_abnormal_return(prices: pd.Series, market_prices: pd.Series, beta: float) -> float | None:
    """Single-day abnormal return = actual return - (beta * market return)."""
    if len(prices) < 2 or len(market_prices) < 2:
        return None
    stock_ret  = (prices.iloc[-1] - prices.iloc[-2]) / prices.iloc[-2]
    market_ret = (market_prices.iloc[-1] - market_prices.iloc[-2]) / market_prices.iloc[-2]
    return round(stock_ret - beta * market_ret, 4)


# ─────────────────────────────────────────────
#  PER-TICKER FUNDAMENTALS
# ─────────────────────────────────────────────

def fetch_ticker_data(ticker_symbol: str, market_prices: pd.Series) -> dict:
    row = {"ticker": ticker_symbol}

    try:
        tk   = yf.Ticker(ticker_symbol)
        info = tk.info or {}

        # ── TIER 1: Financial Performance ──────────────────────────────
        # Net Profit Margin
        net_income = info.get("netIncomeToCommon")
        revenue    = info.get("totalRevenue")
        if net_income and revenue and revenue != 0:
            row["net_profit_margin"] = round(net_income / revenue, 4)

        # EPS Growth (TTM vs prior year via yf financials)
        row["eps_ttm"]      = info.get("trailingEps")
        row["eps_forward"]  = info.get("forwardEps")
        if row.get("eps_ttm") and row.get("eps_forward") and row["eps_ttm"] != 0:
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
        # dividendYield is the trailing 12-month yield as a decimal (e.g. 0.015 = 1.5%)
        # fiveYearAvgDividendYield is reported as a percentage by yfinance (e.g. 1.5)
        # Normalise both to decimal form for consistent scoring.
        raw_dy  = info.get("dividendYield")
        raw_dy5 = info.get("fiveYearAvgDividendYield")
        if raw_dy is not None:
            row["dividend_yield"] = round(float(raw_dy), 6)
        if raw_dy5 is not None:
            # yfinance returns fiveYearAvgDividendYield as a % (e.g. 1.5 means 1.5%)
            row["five_year_avg_dividend_yield"] = round(float(raw_dy5) / 100, 6)

        # ── TIER 2: Technical Indicators ───────────────────────────────
        hist = tk.history(period="1y")
        if not hist.empty:
            prices = hist["Close"]
            row["current_price"] = round(prices.iloc[-1], 2)
            row["rsi_14"]        = compute_rsi(prices, RSI_PERIOD)
            row.update(compute_ma_signal(prices))

            # Abnormal return
            beta = row.get("beta") or 1.0
            row["abnormal_return"] = compute_abnormal_return(prices, market_prices, beta)

            # 52-week high/low context
            row["52w_high"] = round(prices.max(), 2)
            row["52w_low"]  = round(prices.min(), 2)
            row["pct_from_52w_high"] = round(
                (prices.iloc[-1] - prices.max()) / prices.max(), 4
            )

        # ── TIER 3: Insider & Analyst Data ─────────────────────────────
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


# ─────────────────────────────────────────────
#  SCORING ENGINE
# ─────────────────────────────────────────────

def score_ticker(row: dict, macro: dict, vix: float | None) -> dict:
    """
    Convert raw KPIs into 0-100 scores per tier, then a weighted composite.
    Each sub-score is 0-100 (100 = most bullish signal).
    """
    scores = {}

    # ── TIER 1 ──────────────────────────────────────────────────────────
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
        # -0.25 pp cut → 100, 0 → 50, +0.25 hike → 0
        t1.append(min(100, max(0, 50 - prc / 0.25 * 50)))

    # VIX (< 20 = 100, > 30 = 0)
    if vix is not None:
        t1.append(min(100, max(0, (30 - vix) / 10 * 100)))

    scores["tier1_score"] = round(np.mean(t1), 1) if t1 else None

    # ── TIER 2 ──────────────────────────────────────────────────────────
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

    # Abnormal Return (positive = good signal, cap at ±5%)
    ar = row.get("abnormal_return")
    if ar is not None:
        t2.append(min(100, max(0, 50 + ar / 0.05 * 50)))

    scores["tier2_score"] = round(np.mean(t2), 1) if t2 else None

    # ── TIER 3 ──────────────────────────────────────────────────────────
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

    # ── COMPOSITE ───────────────────────────────────────────────────────
    weighted, weight_sum = 0.0, 0.0
    for tier, key in [(1, "tier1_score"), (2, "tier2_score"), (3, "tier3_score")]:
        val = scores.get(key)
        if val is not None:
            weighted    += val * TIER_WEIGHT[tier]
            weight_sum  += TIER_WEIGHT[tier]

    scores["composite_score"] = round(weighted / weight_sum, 1) if weight_sum > 0 else None

    return scores


# ─────────────────────────────────────────────
#  SIGNAL LABELS
# ─────────────────────────────────────────────

def signal_label(score: float | None) -> str:
    if score is None:
        return "N/A"
    if score >= 75: return "STRONG BUY  ▲▲"
    if score >= 60: return "BUY         ▲"
    if score >= 45: return "HOLD        ─"
    if score >= 30: return "SELL        ▼"
    return              "STRONG SELL ▼▼"


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Daily equity KPI scorer")

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
                        help="FRED API key (free at fred.stlouisfed.org)")
    parser.add_argument("--output",   default="equity_kpi_results.csv",
                        help="Output CSV filename")
    args = parser.parse_args()

    # ── Universe info only ──────────────────────────────────────────────
    if args.universe_info:
        if UNIVERSE_AVAILABLE:
            show_cache_info()
        else:
            print("universe_manager.py not found in the same directory.")
        return

    # ── Resolve ticker list ─────────────────────────────────────────────
    if args.tickers:
        tickers = args.tickers
        source  = "manual"
    elif args.universe:
        if not UNIVERSE_AVAILABLE:
            print("ERROR: universe_manager.py not found. Place it in the same directory.")
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

    print("\n" + "="*65)
    print("  EQUITY KPI DAILY ANALYSER")
    print(f"  Run date : {datetime.date.today()}")
    print(f"  Universe : {source}  ({len(tickers)} tickers)")
    print("="*65)

    # ── Macro & Market data ─────────────────────────────────────────────
    print("\n[1/3] Fetching macro data...")
    macro = fetch_macro_fred(args.fred_key)

    print("\n[2/3] Fetching VIX & S&P 500 benchmark...")
    vix = fetch_vix()
    print(f"  VIX = {vix}")

    spx_hist = yf.Ticker("^GSPC").history(period="1y")
    market_prices = spx_hist["Close"] if not spx_hist.empty else pd.Series(dtype=float)

    # ── Per-ticker loop ─────────────────────────────────────────────────
    print(f"\n[3/3] Fetching data for {len(tickers)} tickers...\n")
    results = []
    for i, sym in enumerate(tickers, 1):
        print(f"  [{i}/{len(tickers)}] {sym}...", end=" ", flush=True)
        row = fetch_ticker_data(sym, market_prices)
        scores = score_ticker(row, macro, vix)
        row.update(scores)
        row["vix"] = vix
        row.update({f"macro_{k}": v for k, v in macro.items()})
        results.append(row)
        print(f"composite={row.get('composite_score', 'err')}")

    # ── Build DataFrame ─────────────────────────────────────────────────
    df = pd.DataFrame(results)
    df["signal"] = df["composite_score"].apply(signal_label)
    df = df.sort_values("composite_score", ascending=False)

    # ── Console output ──────────────────────────────────────────────────
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

    print("\n" + "="*65)
    print("  RESULTS  (sorted by composite score)")
    print("="*65)
    print(df[available].to_string(index=False))

    # Top 20 summary
    print("\n" + "─"*65)
    print("  TOP 20 SIGNALS")
    print("─"*65)
    for _, r in df.head(20).iterrows():
        score = r.get("composite_score")
        score_str = f"{score:.1f}" if pd.notna(score) else " N/A"
        sector = str(r.get("sector", ""))[:20]
        print(f"  {r['ticker']:<6}  {score_str:>5}/100   {r.get('signal','N/A'):<18}  {sector}")

    # Sector summary
    if "sector" in df.columns and "composite_score" in df.columns:
        print("\n" + "─"*65)
        print("  SECTOR AVERAGES  (composite score)")
        print("─"*65)
        sec_avg = (
            df.groupby("sector")["composite_score"]
            .agg(["mean", "count"])
            .sort_values("mean", ascending=False)
        )
        for sec, row in sec_avg.iterrows():
            print(f"  {str(sec):<40}  avg={row['mean']:>5.1f}  n={int(row['count'])}")

    # ── Save CSV ────────────────────────────────────────────────────────
    out_path = args.output
    df.to_csv(out_path, index=False)
    print(f"\n✓ Full results saved to: {out_path}")
    print("="*65 + "\n")

    return df


if __name__ == "__main__":
    main()

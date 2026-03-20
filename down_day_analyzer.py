"""
down_day_analyzer.py
====================
Identifies stocks that held up (or gained) on a down market day,
then deep-dives into WHY — sector, beta, momentum, dividends,
fundamentals, insider activity, and more.

Purpose: find patterns that predict market-down resilience in the future.

Usage:
    python down_day_analyzer.py
    python down_day_analyzer.py --date 2026-03-19   # specific date
    python down_day_analyzer.py --top 30            # analyze top N outperformers
    python down_day_analyzer.py --min-gain 0        # only stocks that actually gained
    python down_day_analyzer.py --universe universe_cache.json

Outputs:
    down_day_report_YYYY-MM-DD.csv   — full ranked data
    down_day_summary_YYYY-MM-DD.txt  — human-readable analysis
"""

import argparse
import json
import sys
import os
import datetime
import warnings
from pathlib import Path
from collections import defaultdict

warnings.filterwarnings("ignore")

try:
    import yfinance as yf
except ImportError:
    sys.exit("ERROR: yfinance not installed.  Run: pip install yfinance")

try:
    import pandas as pd
    import numpy as np
except ImportError:
    sys.exit("ERROR: pandas/numpy not installed.  Run: pip install pandas numpy")


# ── Config ─────────────────────────────────────────────────────────────────
DEFAULT_UNIVERSE_FILE = "universe_cache.json"
FALLBACK_TICKERS = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","JPM","JNJ","XOM",
    "V","UNH","AVGO","MA","PG","HD","MRK","COST","ABBV","CVX","BAC",
    "NFLX","KO","TMO","WMT","ACN","MCD","CSCO","ABT","LIN","NKE","TXN",
    "ADBE","NEE","PM","ORCL","QCOM","AMD","HON","UPS","AMGN","IBM","GE","CAT"
]
MARKET_INDEX   = "^GSPC"   # S&P 500
VIX_TICKER     = "^VIX"


# ── Load universe ───────────────────────────────────────────────────────────

def load_universe(universe_file: str) -> list[str]:
    path = Path(universe_file)
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            tickers = data.get("tickers", [])
            if tickers:
                print(f"  Loaded {len(tickers)} tickers from {universe_file} "
                      f"(refreshed {data.get('refreshed','?')})")
                return tickers
        except Exception as e:
            print(f"  Warning: could not load {universe_file}: {e}")
    print(f"  Using fallback ticker list ({len(FALLBACK_TICKERS)} tickers)")
    return FALLBACK_TICKERS


# ── Fetch today's returns ───────────────────────────────────────────────────

def fetch_returns(tickers: list[str], target_date: datetime.date) -> pd.DataFrame:
    """
    Download 5 days of price history for all tickers and compute the
    return on the target date (close vs prior close).
    Also fetches the S&P 500 for the market return benchmark.
    """
    all_tickers = tickers + [MARKET_INDEX, VIX_TICKER]
    print(f"\n  Downloading price data for {len(tickers)} stocks + market indices...")

    try:
        raw = yf.download(
            all_tickers,
            period="5d",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as e:
        sys.exit(f"  ERROR fetching price data: {e}")

    if raw.empty:
        sys.exit("  ERROR: No price data returned.")

    # Handle both single and multi-ticker response shapes
    if isinstance(raw.columns, pd.MultiIndex):
        close = raw["Close"]
    else:
        close = raw[["Close"]].rename(columns={"Close": tickers[0]})

    # Compute daily returns
    returns = close.pct_change()

    # Find the row for target_date
    returns.index = pd.to_datetime(returns.index).normalize()
    target_ts = pd.Timestamp(target_date)

    if target_ts not in returns.index:
        # Fall back to most recent trading day
        available = returns.index[returns.index <= target_ts]
        if available.empty:
            sys.exit(f"  ERROR: No data available on or before {target_date}")
        target_ts = available[-1]
        print(f"  Note: {target_date} has no data — using {target_ts.date()} instead")

    day_returns = returns.loc[target_ts].dropna()

    # Extract market and VIX
    market_ret = float(day_returns.get(MARKET_INDEX, 0))
    vix_val    = float(close.get(VIX_TICKER, pd.Series([None])).dropna().iloc[-1]) if VIX_TICKER in close.columns else None

    print(f"\n  {'─'*55}")
    print(f"  Analysis date : {target_ts.date()}")
    print(f"  S&P 500 return: {market_ret*100:+.2f}%")
    if vix_val:
        print(f"  VIX           : {vix_val:.1f}")
    print(f"  {'─'*55}")

    if market_ret >= 0:
        print(f"\n  ⚠  S&P 500 is UP {market_ret*100:+.2f}% today — this tool is designed for DOWN days.")
        print(f"     Results still show relative outperformers, which is useful on any day.")

    # Build result dataframe for stocks only
    stock_returns = {t: float(day_returns.get(t, np.nan)) for t in tickers if t in day_returns}
    df = pd.DataFrame([
        {"ticker": t, "day_return": r, "abnormal_return": r - market_ret}
        for t, r in stock_returns.items()
        if not np.isnan(r)
    ])
    df["market_return"] = market_ret
    df["vix"]           = vix_val
    df["analysis_date"] = str(target_ts.date())

    return df, market_ret, vix_val, target_ts.date()


# ── Fetch fundamentals for a batch of tickers ──────────────────────────────

def fetch_fundamentals(tickers: list[str]) -> dict[str, dict]:
    """Pull key fundamentals from yfinance for a list of tickers."""
    print(f"\n  Fetching fundamentals for {len(tickers)} outperformers...")
    result = {}
    total = len(tickers)
    for i, sym in enumerate(tickers, 1):
        print(f"    [{i}/{total}] {sym}...", end=" ", flush=True)
        try:
            info = yf.Ticker(sym).info or {}
            result[sym] = {
                # Identity
                "name":                 info.get("shortName", sym),
                "sector":               info.get("sector", "Unknown"),
                "industry":             info.get("industry", "Unknown"),
                # Valuation
                "pe_ratio":             info.get("trailingPE"),
                "forward_pe":           info.get("forwardPE"),
                "pb_ratio":             info.get("priceToBook"),
                "market_cap":           info.get("marketCap"),
                "enterprise_value":     info.get("enterpriseValue"),
                # Profitability
                "net_margin":           info.get("profitMargins"),
                "gross_margin":         info.get("grossMargins"),
                "roe":                  info.get("returnOnEquity"),
                "roa":                  info.get("returnOnAssets"),
                # Growth
                "eps_ttm":              info.get("trailingEps"),
                "eps_forward":          info.get("forwardEps"),
                "revenue_growth":       info.get("revenueGrowth"),
                "earnings_growth":      info.get("earningsGrowth"),
                # Risk
                "beta":                 info.get("beta", 1.0),
                "52w_high":             info.get("fiftyTwoWeekHigh"),
                "52w_low":              info.get("fiftyTwoWeekLow"),
                "pct_from_52w_high":    None,  # computed below
                # Income
                "dividend_yield":       info.get("dividendYield"),
                "five_yr_div_yield":    info.get("fiveYearAvgDividendYield"),
                "payout_ratio":         info.get("payoutRatio"),
                # Balance sheet
                "current_ratio":        info.get("currentRatio"),
                "debt_to_equity":       info.get("debtToEquity"),
                "total_cash":           info.get("totalCash"),
                # Momentum
                "50d_avg":              info.get("fiftyDayAverage"),
                "200d_avg":             info.get("twoHundredDayAverage"),
                "current_price":        info.get("currentPrice") or info.get("regularMarketPrice"),
                # Analyst
                "analyst_rating":       info.get("recommendationMean"),
                "num_analysts":         info.get("numberOfAnalystOpinions"),
                # Short interest
                "short_ratio":          info.get("shortRatio"),
                "short_pct_float":      info.get("shortPercentOfFloat"),
                # Ownership
                "insider_pct":          info.get("heldPercentInsiders"),
                "institution_pct":      info.get("heldPercentInstitutions"),
            }
            # Compute pct from 52w high
            price = result[sym]["current_price"]
            high  = result[sym]["52w_high"]
            if price and high and high > 0:
                result[sym]["pct_from_52w_high"] = (price - high) / high
            print("✓")
        except Exception as e:
            print(f"✗ ({e})")
            result[sym] = {"name": sym, "sector": "Unknown", "error": str(e)}
    return result


# ── Pattern analysis ────────────────────────────────────────────────────────

def analyze_patterns(df_out: pd.DataFrame, fundamentals: dict, market_ret: float) -> dict:
    """
    Look for patterns across outperforming stocks.
    Returns a dict of findings.
    """
    findings = {}

    # ── Sector distribution ──────────────────────────────────────────
    sectors = defaultdict(list)
    for _, row in df_out.iterrows():
        f = fundamentals.get(row["ticker"], {})
        sec = f.get("sector", "Unknown")
        sectors[sec].append(row["abnormal_return"])

    sector_stats = {}
    for sec, rets in sectors.items():
        sector_stats[sec] = {
            "count":    len(rets),
            "avg_abnormal": round(np.mean(rets) * 100, 2),
            "pct_positive": round(sum(r > 0 for r in rets) / len(rets) * 100, 1),
        }
    findings["sector_stats"] = dict(sorted(sector_stats.items(),
                                            key=lambda x: -x[1]["avg_abnormal"]))

    # ── Beta analysis ────────────────────────────────────────────────
    betas       = []
    div_yields  = []
    pe_ratios   = []
    short_pcts  = []
    for _, row in df_out.iterrows():
        f = fundamentals.get(row["ticker"], {})
        b = f.get("beta")
        d = f.get("dividend_yield")
        p = f.get("pe_ratio")
        s = f.get("short_pct_float")
        if b and not np.isnan(float(b)):  betas.append(float(b))
        if d and not np.isnan(float(d)):  div_yields.append(float(d))
        if p and not np.isnan(float(p)):  pe_ratios.append(float(p))
        if s and not np.isnan(float(s)):  short_pcts.append(float(s))

    findings["beta"] = {
        "avg":    round(np.mean(betas), 2) if betas else None,
        "median": round(np.median(betas), 2) if betas else None,
        "pct_below_1": round(sum(b < 1.0 for b in betas) / len(betas) * 100, 1) if betas else None,
    }
    findings["dividend"] = {
        "pct_paying_dividend": round(sum(d > 0 for d in div_yields) / len(df_out) * 100, 1) if div_yields else None,
        "avg_yield": round(np.mean([d for d in div_yields if d > 0]) * 100, 2) if div_yields else None,
    }
    findings["valuation"] = {
        "avg_pe":    round(np.mean([p for p in pe_ratios if 0 < p < 200]), 1) if pe_ratios else None,
        "median_pe": round(np.median([p for p in pe_ratios if 0 < p < 200]), 1) if pe_ratios else None,
    }
    findings["short_interest"] = {
        "avg_short_pct": round(np.mean(short_pcts) * 100, 1) if short_pcts else None,
        "note": "Low short interest → less short-covering noise; high → short squeeze candidate",
    }

    # ── MA signal ────────────────────────────────────────────────────
    golden_cross = 0
    for _, row in df_out.iterrows():
        f = fundamentals.get(row["ticker"], {})
        ma50  = f.get("50d_avg") or 0
        ma200 = f.get("200d_avg") or 0
        if ma50 > 0 and ma200 > 0 and ma50 > ma200:
            golden_cross += 1
    findings["momentum"] = {
        "pct_golden_cross": round(golden_cross / len(df_out) * 100, 1) if len(df_out) > 0 else 0,
        "note": "Golden cross = 50d MA above 200d MA (bullish uptrend)",
    }

    return findings


# ── Generate hypotheses ─────────────────────────────────────────────────────

def generate_hypotheses(findings: dict, market_ret: float, vix: float) -> list[str]:
    """Translate pattern findings into human-readable hypotheses."""
    hyps = []

    beta = findings.get("beta", {})
    if beta.get("avg") and beta["avg"] < 0.9:
        hyps.append(
            f"LOW BETA SHIELD: Outperformers had avg beta {beta['avg']:.2f} "
            f"({beta.get('pct_below_1',0):.0f}% below 1.0). "
            f"Low-beta stocks absorb less of the market's downside by design — "
            f"this is the classic defensive characteristic."
        )

    div = findings.get("dividend", {})
    if div.get("pct_paying_dividend") and div["pct_paying_dividend"] > 50:
        hyps.append(
            f"DIVIDEND FLOOR: {div['pct_paying_dividend']:.0f}% of outperformers pay dividends "
            f"(avg yield {div.get('avg_yield',0):.1f}%). "
            f"Dividend stocks attract yield-seeking buyers when rates are uncertain, "
            f"providing a buyer base that cushions selloffs."
        )

    sectors = findings.get("sector_stats", {})
    top_sectors = [(s, v) for s, v in sectors.items() if v["count"] >= 2]
    top_sectors.sort(key=lambda x: -x[1]["avg_abnormal"])
    if top_sectors:
        s, v = top_sectors[0]
        hyps.append(
            f"SECTOR ROTATION INTO '{s.upper()}': This sector had {v['count']} outperformers "
            f"with avg abnormal return +{v['avg_abnormal']:.1f}%. "
            f"Down days often see rotation from growth/risk into defensive or counter-cyclical sectors."
        )

    if vix and vix > 25:
        hyps.append(
            f"HIGH FEAR (VIX {vix:.0f}): Elevated volatility tends to benefit defensive sectors "
            f"(Utilities, Consumer Staples, Health Care) and gold/commodity-linked names "
            f"as investors seek safety."
        )

    momentum = findings.get("momentum", {})
    if momentum.get("pct_golden_cross") and momentum["pct_golden_cross"] > 60:
        hyps.append(
            f"TREND STRENGTH: {momentum['pct_golden_cross']:.0f}% of outperformers are in a golden cross "
            f"(50d > 200d MA). Stocks already in established uptrends often see "
            f"dip-buying support on broad market down days."
        )

    val = findings.get("valuation", {})
    if val.get("median_pe") and val["median_pe"] < 20:
        hyps.append(
            f"VALUE SUPPORT: Median P/E of outperformers is {val['median_pe']:.1f}x — "
            f"cheap stocks have a natural floor; value investors step in during broad selloffs "
            f"because the margin of safety is already priced in."
        )

    short = findings.get("short_interest", {})
    if short.get("avg_short_pct") and short["avg_short_pct"] > 5:
        hyps.append(
            f"SHORT SQUEEZE DYNAMIC: Avg short interest {short['avg_short_pct']:.1f}% of float "
            f"among outperformers. Heavily shorted stocks sometimes GAIN on down days as short sellers "
            f"cover (buy back) to lock in profits, creating unexpected demand."
        )

    return hyps


# ── Write report ────────────────────────────────────────────────────────────

def write_report(df: pd.DataFrame, fundamentals: dict, findings: dict,
                 hypotheses: list[str], market_ret: float, vix: float,
                 analysis_date: datetime.date, output_prefix: str):
    """Write both a CSV and a human-readable summary."""

    # ── Merge fundamentals into df ────────────────────────────────────
    rows = []
    for _, r in df.iterrows():
        f = fundamentals.get(r["ticker"], {})
        row = {
            "ticker":           r["ticker"],
            "name":             f.get("name", r["ticker"]),
            "sector":           f.get("sector", "Unknown"),
            "industry":         f.get("industry", "Unknown"),
            "day_return_pct":   round(r["day_return"] * 100, 3),
            "market_return_pct":round(r["market_return"] * 100, 3),
            "abnormal_return_pct": round(r["abnormal_return"] * 100, 3),
            "actually_gained":  "YES" if r["day_return"] > 0 else "no",
            "beta":             f.get("beta"),
            "pe_ratio":         f.get("pe_ratio"),
            "forward_pe":       f.get("forward_pe"),
            "net_margin_pct":   round(f["net_margin"] * 100, 1) if f.get("net_margin") else None,
            "dividend_yield_pct": round(f["dividend_yield"] * 100, 2) if f.get("dividend_yield") else 0,
            "market_cap_B":     round(f["market_cap"] / 1e9, 1) if f.get("market_cap") else None,
            "short_pct_float":  round(f["short_pct_float"] * 100, 1) if f.get("short_pct_float") else None,
            "pct_from_52w_high":round(f["pct_from_52w_high"] * 100, 1) if f.get("pct_from_52w_high") else None,
            "50d_vs_200d_ma":   ("GOLDEN" if (f.get("50d_avg") or 0) > (f.get("200d_avg") or 0)
                                 else "DEATH") if f.get("50d_avg") and f.get("200d_avg") else "?",
            "debt_to_equity":   f.get("debt_to_equity"),
            "current_ratio":    f.get("current_ratio"),
            "insider_pct":      round(f["insider_pct"] * 100, 1) if f.get("insider_pct") else None,
            "analyst_rating":   f.get("analyst_rating"),  # 1=strong buy .. 5=sell
        }
        rows.append(row)

    out_df = pd.DataFrame(rows).sort_values("abnormal_return_pct", ascending=False)

    csv_path = f"{output_prefix}.csv"
    out_df.to_csv(csv_path, index=False)
    print(f"\n  ✓ CSV saved: {csv_path}")

    # ── Human-readable summary ────────────────────────────────────────
    txt_path = f"{output_prefix}_summary.txt"
    lines = []
    def w(s=""): lines.append(s)

    w("=" * 70)
    w("  DOWN-DAY RESILIENCE ANALYSIS")
    w(f"  Date          : {analysis_date}")
    w(f"  S&P 500 return: {market_ret*100:+.2f}%")
    w(f"  VIX           : {vix:.1f}" if vix else "  VIX           : n/a")
    w("=" * 70)

    # Gainers
    gainers = out_df[out_df["actually_gained"] == "YES"]
    w(f"\n  STOCKS THAT ACTUALLY GAINED TODAY ({len(gainers)})")
    w("  " + "─" * 55)
    if gainers.empty:
        w("  None — market was too broad a selloff for any gains.")
    else:
        for _, r in gainers.iterrows():
            w(f"  {r['ticker']:<6}  {r['day_return_pct']:+.2f}%  "
              f"(abnormal: {r['abnormal_return_pct']:+.2f}%)  "
              f"sector={r['sector']}  beta={r['beta']}")

    # Top outperformers (even if negative, beat the market most)
    w(f"\n  TOP 20 OUTPERFORMERS (least negative / most positive vs market)")
    w("  " + "─" * 55)
    w(f"  {'Ticker':<7} {'Return':>7} {'vs S&P':>8}  {'Sector':<26} {'Beta':>5}  {'P/E':>6}  {'DivY%':>6}  MA")
    w("  " + "─" * 55)
    for _, r in out_df.head(20).iterrows():
        div_str = f"{r['dividend_yield_pct']:.1f}" if r.get("dividend_yield_pct") else "  — "
        pe_str  = f"{r['pe_ratio']:.0f}" if r.get("pe_ratio") else "  —"
        w(f"  {r['ticker']:<7} {r['day_return_pct']:>+6.2f}%  "
          f"{r['abnormal_return_pct']:>+6.2f}%  "
          f"{str(r['sector'])[:25]:<26} "
          f"{r.get('beta') or 0:>5.2f}  {pe_str:>6}  {div_str:>5}  "
          f"{r.get('50d_vs_200d_ma','?')}")

    # Pattern findings
    w("\n" + "=" * 70)
    w("  PATTERN ANALYSIS  (why did these stocks hold up?)")
    w("=" * 70)

    sec_stats = findings.get("sector_stats", {})
    w("\n  SECTOR BREAKDOWN (outperformers only):")
    for sec, stats in list(sec_stats.items())[:8]:
        bar = "█" * int(stats["count"])
        w(f"    {sec[:35]:<35}  n={stats['count']:>3}  "
          f"avg abnormal={stats['avg_abnormal']:>+5.1f}%  "
          f"{stats['pct_positive']:.0f}% gained  {bar}")

    beta = findings.get("beta", {})
    w(f"\n  BETA PROFILE:")
    w(f"    Average beta : {beta.get('avg','?')}")
    w(f"    Median beta  : {beta.get('median','?')}")
    w(f"    % below 1.0  : {beta.get('pct_below_1','?')}%")
    w(f"    Interpretation: Lower beta = less market sensitivity = better down-day performance")

    div = findings.get("dividend", {})
    w(f"\n  DIVIDEND CHARACTERISTICS:")
    w(f"    % paying dividend : {div.get('pct_paying_dividend','?')}%")
    w(f"    Avg yield (payers): {div.get('avg_yield','?')}%")

    val = findings.get("valuation", {})
    w(f"\n  VALUATION:")
    w(f"    Avg P/E    : {val.get('avg_pe','?')}x")
    w(f"    Median P/E : {val.get('median_pe','?')}x")

    mom = findings.get("momentum", {})
    w(f"\n  MOMENTUM (MA signal):")
    w(f"    % in Golden Cross : {mom.get('pct_golden_cross','?')}%")

    # Hypotheses
    w("\n" + "=" * 70)
    w("  HYPOTHESES  (potential predictive signals for future down days)")
    w("=" * 70)
    if not hypotheses:
        w("\n  No strong patterns detected — sample may be too small or market")
        w("  conditions too broad for clear factor separation today.")
    else:
        for i, h in enumerate(hypotheses, 1):
            w(f"\n  {i}. {h}")

    # Predictive checklist
    w("\n" + "=" * 70)
    w("  PREDICTIVE CHECKLIST FOR NEXT DOWN DAY")
    w("  (stocks scoring well on these factors historically outperform)")
    w("=" * 70)
    w("""
  Score 1 point for each:
    [ ] Beta < 0.80
    [ ] Sector = Utilities / Consumer Staples / Health Care
    [ ] Dividend yield > 1.5%
    [ ] P/E < 20 (value floor = natural buyer support)
    [ ] 50d MA > 200d MA (golden cross = institutional trend)
    [ ] Pct from 52w high < -15% (not already overbought)
    [ ] Short interest > 8% of float (short squeeze optionality)
    [ ] Net insider buying in last 90 days (management confidence)
    [ ] Analyst rating < 2.5 (1=strong buy, 5=sell) on consensus
    [ ] Low debt-to-equity (< 0.5) — balance sheet fortress

  Stocks scoring 6+ are historically strong down-day performers.
  Run this script daily on red days to build your pattern dataset.
    """)

    w("=" * 70)
    w(f"  Full data saved to: {csv_path}")
    w("=" * 70)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"  ✓ Summary saved: {txt_path}")

    # Also print summary to console
    print("\n" + "\n".join(lines))
    return csv_path, txt_path


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Identify down-day resilient stocks and analyze why they held up")
    parser.add_argument("--date",      default=None,
                        help="Analysis date YYYY-MM-DD (default: today)")
    parser.add_argument("--top",       type=int, default=50,
                        help="Analyze fundamentals for top N outperformers (default: 50)")
    parser.add_argument("--min-gain",  type=float, default=None,
                        help="Only include stocks with return >= this (e.g. 0 for gainers only)")
    parser.add_argument("--universe",  default=DEFAULT_UNIVERSE_FILE,
                        help=f"Universe JSON file (default: {DEFAULT_UNIVERSE_FILE})")
    parser.add_argument("--output",    default=None,
                        help="Output filename prefix (default: down_day_report_YYYY-MM-DD)")
    args = parser.parse_args()

    # Resolve target date
    if args.date:
        try:
            target_date = datetime.date.fromisoformat(args.date)
        except ValueError:
            sys.exit(f"ERROR: invalid date '{args.date}'. Use YYYY-MM-DD format.")
    else:
        target_date = datetime.date.today()

    print("\n" + "=" * 65)
    print("  DOWN-DAY RESILIENCE ANALYZER")
    print(f"  Target date: {target_date}")
    print("=" * 65)

    # Load universe
    tickers = load_universe(args.universe)

    # Fetch returns
    df, market_ret, vix, analysis_date = fetch_returns(tickers, target_date)

    if df.empty:
        sys.exit("ERROR: No return data computed.")

    print(f"\n  Returns computed for {len(df)} tickers.")
    print(f"  Gainers today (absolute) : {(df['day_return'] > 0).sum()}")
    print(f"  Outperformed market      : {(df['abnormal_return'] > 0).sum()}")

    # Filter to outperformers
    df_sorted = df.sort_values("abnormal_return", ascending=False)

    if args.min_gain is not None:
        df_filtered = df_sorted[df_sorted["day_return"] >= args.min_gain / 100]
        label = f"stocks with return >= {args.min_gain:.1f}%"
    else:
        df_filtered = df_sorted.head(args.top)
        label = f"top {args.top} outperformers"

    print(f"\n  Analyzing {len(df_filtered)} {label}...")

    if df_filtered.empty:
        print("  No stocks matched the filter. Try relaxing --min-gain or increasing --top.")
        return

    # Fetch fundamentals for outperformers
    focus_tickers = df_filtered["ticker"].tolist()
    fundamentals  = fetch_fundamentals(focus_tickers)

    # Pattern analysis
    print("\n  Running pattern analysis...")
    findings   = analyze_patterns(df_filtered, fundamentals, market_ret)
    hypotheses = generate_hypotheses(findings, market_ret, vix)

    # Output
    output_prefix = args.output or f"down_day_report_{analysis_date}"
    write_report(df_filtered, fundamentals, findings, hypotheses,
                 market_ret, vix, analysis_date, output_prefix)


if __name__ == "__main__":
    main()

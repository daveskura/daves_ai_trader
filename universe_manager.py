"""
universe_manager.py
===================
Manages the equity universe for daily KPI analysis.
Fetches S&P 500 constituents, ranks by market cap, applies
sector balancing, and caches locally with weekly auto-refresh.

Sources (all free, no API key required):
  - Wikipedia  : S&P 500 constituent list
  - yfinance   : Market cap lookup for ranking
"""

import os
import json
import datetime
import sys

# Force UTF-8 output on Windows to avoid Unicode errors in logs/console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import time

import pandas as pd
import yfinance as yf

# ── Config ──────────────────────────────────────────────────────────────────
CACHE_FILE          = "universe_cache.json"
REFRESH_DAYS        = 7          # re-fetch constituents every N days
DEFAULT_UNIVERSE_N  = 200        # how many stocks to return
MAX_PER_SECTOR      = 40         # sector cap for balanced mode
WIKI_URL            = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Fallback hardcoded top-50 in case Wikipedia/yfinance are unavailable
FALLBACK_TICKERS = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","BRK-B","JPM","LLY",
    "XOM","V","UNH","AVGO","JNJ","MA","PG","HD","MRK","COST",
    "ABBV","CVX","CRM","BAC","NFLX","PEP","KO","TMO","WMT","ACN",
    "MCD","CSCO","ABT","LIN","DHR","NKE","TXN","ADBE","NEE","PM",
    "ORCL","QCOM","AMD","HON","UPS","AMGN","IBM","GE","CAT","BA",
]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _cache_is_fresh() -> bool:
    """Return True if the cache file exists and is less than REFRESH_DAYS old."""
    if not os.path.exists(CACHE_FILE):
        return False
    mtime = datetime.datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
    age   = (datetime.datetime.now() - mtime).days
    return age < REFRESH_DAYS


def _load_cache() -> dict | None:
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return None


def _save_cache(data: dict):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"  [UNIVERSE] Warning: could not save cache — {e}")


# ── Step 1: Fetch S&P 500 constituent list ───────────────────────────────────

def _fetch_from_wikipedia() -> pd.DataFrame | None:
    """Try Wikipedia using requests with a browser User-Agent to avoid 403."""
    try:
        import requests
        from io import StringIO
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(WIKI_URL, headers=headers, timeout=15)
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text))
        df = tables[0][["Symbol", "Security", "GICS Sector", "GICS Sub-Industry"]].copy()
        df.columns = ["ticker", "name", "sector", "sub_industry"]
        df["ticker"] = df["ticker"].str.replace(".", "-", regex=False)
        print(f"  [UNIVERSE] Wikipedia OK — {len(df)} constituents.")
        return df
    except Exception as e:
        print(f"  [UNIVERSE] Wikipedia failed: {e}")
    return None


def _fetch_from_slickcharts() -> pd.DataFrame | None:
    """Scrape slickcharts.com S&P 500 list using requests."""
    import requests, re
    url = "https://www.slickcharts.com/sp500"
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        tickers = re.findall(r'/symbol/([A-Z]{1,5}(?:-[A-Z])?)"', resp.text)
        seen, unique = set(), []
        for t in tickers:
            if t not in seen:
                seen.add(t)
                unique.append(t)
        if len(unique) < 100:
            return None
        df = pd.DataFrame({
            "ticker":       unique,
            "name":         unique,
            "sector":       ["Unknown"] * len(unique),
            "sub_industry": ["Unknown"] * len(unique),
        })
        print(f"  [UNIVERSE] Slickcharts OK — {len(df)} constituents.")
        return df
    except Exception as e:
        print(f"  [UNIVERSE] Slickcharts failed: {e}")
    return None


def _fetch_from_github_csv() -> pd.DataFrame | None:
    """
    Download a maintained S&P 500 CSV from GitHub (datasets repo).
    Includes ticker + sector — no parsing library needed.
    """
    import requests
    from io import StringIO
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        df = df.rename(columns={"Symbol": "ticker", "Name": "name", "Sector": "sector"})
        df["ticker"]       = df["ticker"].str.replace(".", "-", regex=False)
        df["sub_industry"] = "Unknown"
        df = df[["ticker", "name", "sector", "sub_industry"]]
        print(f"  [UNIVERSE] GitHub CSV OK — {len(df)} constituents (with sectors).")
        return df
    except Exception as e:
        print(f"  [UNIVERSE] GitHub CSV failed: {e}")
    return None


def fetch_sp500_constituents() -> pd.DataFrame:
    """
    Pull S&P 500 tickers + sectors, trying multiple free sources in order:
      1. GitHub CSV  (most reliable, includes sectors, zero extra deps)
      2. Wikipedia   (requires lxml or html5lib)
      3. Slickcharts (web scrape fallback)
      4. Hardcoded fallback list
    """
    print("  [UNIVERSE] Fetching S&P 500 constituents...")

    for source_fn in [_fetch_from_github_csv, _fetch_from_wikipedia, _fetch_from_slickcharts]:
        result = source_fn()
        if result is not None and len(result) >= 100:
            return result

    # Last resort hardcoded fallback
    print("  [UNIVERSE] All sources failed — using hardcoded fallback list.")
    return pd.DataFrame({
        "ticker":       FALLBACK_TICKERS,
        "name":         FALLBACK_TICKERS,
        "sector":       ["Unknown"] * len(FALLBACK_TICKERS),
        "sub_industry": ["Unknown"] * len(FALLBACK_TICKERS),
    })


# ── Step 2: Enrich with market cap ───────────────────────────────────────────

def enrich_with_market_cap(df: pd.DataFrame, batch_size: int = 50) -> pd.DataFrame:
    """
    Add market_cap column by querying yfinance in batches.
    Uses yfinance download for speed; falls back to ticker-by-ticker.
    """
    print(f"  [UNIVERSE] Fetching market caps for {len(df)} tickers (batched)...")
    tickers  = df["ticker"].tolist()
    cap_map  = {}

    # Batch download is fastest — pull info via Tickers object
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        try:
            multi = yf.Tickers(" ".join(batch))
            for sym in batch:
                try:
                    info = multi.tickers[sym].info
                    cap_map[sym] = info.get("marketCap") or 0
                except Exception:
                    cap_map[sym] = 0
        except Exception:
            for sym in batch:
                cap_map[sym] = 0
        pct = min(100, int((i + batch_size) / len(tickers) * 100))
        print(f"    ...{pct}% complete", end="\r", flush=True)
        time.sleep(0.3)   # be polite to Yahoo

    print()
    df = df.copy()
    df["market_cap"] = df["ticker"].map(cap_map).fillna(0).astype(float)
    return df


# ── Step 3: Selection strategies ─────────────────────────────────────────────

def select_top_n_by_market_cap(df: pd.DataFrame, n: int) -> list[str]:
    """Pure market-cap ranking — top N stocks."""
    ranked = df.sort_values("market_cap", ascending=False).head(n)
    return ranked["ticker"].tolist()


def select_sector_balanced(df: pd.DataFrame, n: int, max_per_sector: int) -> list[str]:
    """
    Select top N stocks with a per-sector cap.
    Fills slots proportionally: each sector gets up to max_per_sector picks,
    ranked by market cap within the sector.
    """
    df_sorted = df.sort_values("market_cap", ascending=False)
    selected  = []
    sector_counts: dict[str, int] = {}

    for _, row in df_sorted.iterrows():
        sec = row["sector"]
        if sector_counts.get(sec, 0) < max_per_sector:
            selected.append(row["ticker"])
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if len(selected) >= n:
            break

    return selected


# ── Public API ────────────────────────────────────────────────────────────────

def get_universe(
    n: int = DEFAULT_UNIVERSE_N,
    mode: str = "balanced",          # "balanced" | "market_cap"
    max_per_sector: int = MAX_PER_SECTOR,
    force_refresh: bool = False,
    skip_market_cap: bool = False,
) -> list[str]:
    """
    Return a list of ticker symbols for the equity universe.

    Parameters
    ----------
    n               : Number of tickers to return (default 200)
    mode            : "balanced" applies sector cap; "market_cap" is pure ranking
    max_per_sector  : Max stocks per sector when mode="balanced"
    force_refresh   : Ignore cache and re-fetch everything
    skip_market_cap : Use Wikipedia order only (faster but less accurate ranking)

    Returns
    -------
    List of ticker strings, e.g. ["AAPL", "MSFT", ...]
    """
    print("\n" + "-" * 55)
    print("  UNIVERSE SELECTION")
    print("-" * 55)

    # ── Try cache first ──────────────────────────────────────────────
    if not force_refresh and _cache_is_fresh():
        cached = _load_cache()
        if cached and "tickers" in cached:
            tickers = cached["tickers"][:n]
            refreshed = cached.get("refreshed", "unknown")
            print(f"  [UNIVERSE] Loaded {len(tickers)} tickers from cache (refreshed {refreshed})")
            print(f"  [UNIVERSE] Next refresh in < {REFRESH_DAYS} days  |  mode={cached.get('mode','?')}")
            print("-" * 55)
            return tickers

    # ── Fetch fresh data ─────────────────────────────────────────────
    constituents = fetch_sp500_constituents()

    if skip_market_cap:
        # Wikipedia lists roughly in market-cap order already
        print("  [UNIVERSE] Skipping market-cap lookup (skip_market_cap=True)")
        constituents["market_cap"] = range(len(constituents), 0, -1)
    else:
        constituents = enrich_with_market_cap(constituents)

    # ── Apply selection strategy ─────────────────────────────────────
    if mode == "balanced":
        tickers = select_sector_balanced(constituents, n, max_per_sector)
    else:
        tickers = select_top_n_by_market_cap(constituents, n)

    # ── Show sector breakdown ────────────────────────────────────────
    selected_df = constituents[constituents["ticker"].isin(tickers)]
    breakdown   = selected_df.groupby("sector").size().sort_values(ascending=False)
    print(f"\n  [UNIVERSE] Selected {len(tickers)} tickers  |  mode={mode}")
    print("  Sector breakdown:")
    for sec, cnt in breakdown.items():
        bar = "█" * cnt
        print(f"    {sec:<40} {cnt:>3}  {bar}")

    # ── Cache results ────────────────────────────────────────────────
    cache_data = {
        "tickers":   tickers,
        "refreshed": str(datetime.date.today()),
        "mode":      mode,
        "n":         n,
        "sector_breakdown": breakdown.to_dict(),
    }
    _save_cache(cache_data)
    print(f"\n  [UNIVERSE] Saved to cache: {CACHE_FILE}")
    print("-" * 55)

    return tickers


def refresh_universe(n: int = DEFAULT_UNIVERSE_N, mode: str = "balanced") -> list[str]:
    """Force a full refresh of the universe cache."""
    return get_universe(n=n, mode=mode, force_refresh=True)


def show_cache_info():
    """Print current cache status."""
    if not os.path.exists(CACHE_FILE):
        print("No universe cache found. Run get_universe() to create one.")
        return
    cached = _load_cache()
    if not cached:
        print("Cache file exists but could not be read.")
        return
    mtime = datetime.datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
    age   = (datetime.datetime.now() - mtime).days
    print(f"\nUniverse Cache Info")
    print(f"  File       : {CACHE_FILE}")
    print(f"  Refreshed  : {cached.get('refreshed')}")
    print(f"  Age        : {age} day(s)  ({'fresh' if age < REFRESH_DAYS else 'STALE — will refresh on next run'})")
    print(f"  Tickers    : {cached.get('n')}  (mode={cached.get('mode')})")
    print(f"  Sectors    :")
    for sec, cnt in (cached.get("sector_breakdown") or {}).items():
        print(f"    {sec:<40} {cnt}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Universe manager — run standalone to refresh cache")
    parser.add_argument("--n",       type=int, default=200,        help="Universe size")
    parser.add_argument("--mode",    default="balanced",           help="balanced | market_cap")
    parser.add_argument("--refresh", action="store_true",          help="Force refresh cache")
    parser.add_argument("--info",    action="store_true",          help="Show cache info only")
    args = parser.parse_args()

    if args.info:
        show_cache_info()
    else:
        tickers = get_universe(n=args.n, mode=args.mode, force_refresh=args.refresh)
        print(f"\nFirst 20 tickers: {tickers[:20]}")

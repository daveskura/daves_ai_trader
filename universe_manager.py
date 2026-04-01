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
import sys

# Force UTF-8 output on Windows to avoid Unicode errors in logs/console
# (moved here — immediately after sys import, before any other imports)
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import json
import datetime
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

# Structured logging to MySQL run_logs table
from db_logger import Logger as _DbLogger
logger = _DbLogger(run_stage="kpi", echo=True)

# MySQL persistence — universe cache stored in DB instead of JSON file
try:
    from db import write_universe_cache as _db_write_universe,                    read_universe_cache  as _db_read_universe
    _DB_AVAILABLE = True
except ImportError:
    _DB_AVAILABLE = False

# ── Config ──────────────────────────────────────────────────────────────────
# Use an absolute path so the cache is always written next to this file,
# regardless of what directory the caller's cwd is set to.
_BASE_DIR           = Path(__file__).parent
CACHE_FILE          = str(_BASE_DIR / "universe_cache.json")
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

# Alphabet share-class deduplication: keep the higher-liquidity class only.
# Map each duplicate → preferred ticker; the duplicate is dropped from the universe.
DUPLICATE_SHARE_CLASSES: dict[str, str] = {
    "GOOG": "GOOGL",   # Alphabet C → keep A (voting rights, more widely held)
    "BRK-A": "BRK-B",  # Berkshire A → keep B (much lower price, more liquid)
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _load_cache() -> dict | None:
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return None


def _cache_is_fresh(mode: str, n: int) -> bool:
    """
    Return True only if a fresh cache exists (MySQL preferred, JSON fallback).
    Fresh = refreshed within REFRESH_DAYS, same mode, large enough n.
    """
    # Try MySQL first
    if _DB_AVAILABLE:
        try:
            row = _db_read_universe(mode=mode, max_age_days=REFRESH_DAYS)
            if row and row.get("n", 0) >= n:
                return True
        except Exception:
            pass

    # Fall back to JSON file
    cached = _load_cache()
    if not cached or "tickers" not in cached or "refreshed" not in cached:
        return False
    try:
        refreshed = datetime.date.fromisoformat(cached["refreshed"])
    except ValueError:
        return False
    age = (datetime.date.today() - refreshed).days
    if age >= REFRESH_DAYS:
        return False
    if cached.get("mode") != mode:
        return False
    if cached.get("n", 0) < n:
        return False
    return True


def _save_cache(data: dict):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"  [UNIVERSE] Warning: could not save cache — {e}")


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
        logger.info(f"  [UNIVERSE] Wikipedia OK — {len(df)} constituents.")
        return df
    except Exception as e:
        logger.error(f"  [UNIVERSE] Wikipedia failed: {e}")
    return None


def _fetch_from_slickcharts() -> pd.DataFrame | None:
    """Scrape slickcharts.com S&P 500 list using requests."""
    import requests, re
    url = "https://www.slickcharts.com/sp500"
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        # Fixed: allow up to 2 chars after the dash (e.g. BRK-B, BF-B)
        tickers = re.findall(r'/symbol/([A-Z]{1,5}(?:-[A-Z]{1,2})?)\"', resp.text)
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
        logger.info(f"  [UNIVERSE] Slickcharts OK — {len(df)} constituents.")
        return df
    except Exception as e:
        logger.error(f"  [UNIVERSE] Slickcharts failed: {e}")
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
        logger.info(f"  [UNIVERSE] GitHub CSV OK — {len(df)} constituents (with sectors).")
        return df
    except Exception as e:
        logger.error(f"  [UNIVERSE] GitHub CSV failed: {e}")
    return None


def fetch_sp500_constituents() -> pd.DataFrame:
    """
    Pull S&P 500 tickers + sectors, trying multiple free sources in order:
      1. GitHub CSV  (most reliable, includes sectors, zero extra deps)
      2. Wikipedia   (requires lxml or html5lib)
      3. Slickcharts (web scrape fallback)
      4. Hardcoded fallback list
    """
    logger.info("  [UNIVERSE] Fetching S&P 500 constituents...")

    for source_fn in [_fetch_from_github_csv, _fetch_from_wikipedia, _fetch_from_slickcharts]:
        result = source_fn()
        if result is not None and len(result) >= 100:
            return result

    # Last resort hardcoded fallback
    logger.error("  [UNIVERSE] All sources failed — using hardcoded fallback list.")
    return pd.DataFrame({
        "ticker":       FALLBACK_TICKERS,
        "name":         FALLBACK_TICKERS,
        "sector":       ["Unknown"] * len(FALLBACK_TICKERS),
        "sub_industry": ["Unknown"] * len(FALLBACK_TICKERS),
    })


def deduplicate_share_classes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate share classes (e.g. GOOG when GOOGL is present).
    The DUPLICATE_SHARE_CLASSES map defines which ticker to drop → keep.
    """
    tickers_in_df = set(df["ticker"].tolist())
    to_drop = []
    for dup, preferred in DUPLICATE_SHARE_CLASSES.items():
        if dup in tickers_in_df and preferred in tickers_in_df:
            to_drop.append(dup)
            logger.info(f"  [UNIVERSE] Dedup: dropping {dup} (keeping {preferred})")
        elif dup in tickers_in_df:
            # preferred not present — rename dup to preferred for consistency
            df.loc[df["ticker"] == dup, "ticker"] = preferred
            logger.info(f"  [UNIVERSE] Dedup: renamed {dup} → {preferred}")
    if to_drop:
        df = df[~df["ticker"].isin(to_drop)].reset_index(drop=True)
    return df


# ── Step 2: Enrich with market cap ───────────────────────────────────────────

def _fetch_market_cap_with_retry(sym: str, retries: int = 3, backoff: float = 2.0) -> float:
    """Fetch a single ticker's market cap with exponential-backoff retries."""
    for attempt in range(retries):
        try:
            info = yf.Ticker(sym).info
            return float(info.get("marketCap") or 0)
        except Exception:
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
    return 0.0


def enrich_with_market_cap(df: pd.DataFrame, batch_size: int = 50) -> pd.DataFrame:
    """
    Add market_cap column by querying yfinance in batches.
    Each ticker in a failed batch is retried individually with backoff,
    so a single rate-limit hit doesn't zero-out an entire batch.
    """
    logger.info(f"  [UNIVERSE] Fetching market caps for {len(df)} tickers (batched)...")
    tickers = df["ticker"].tolist()
    cap_map: dict[str, float] = {}

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        batch_failed: list[str] = []

        try:
            multi = yf.Tickers(" ".join(batch))
            for sym in batch:
                try:
                    info = multi.tickers[sym].info
                    cap_map[sym] = float(info.get("marketCap") or 0)
                except Exception:
                    batch_failed.append(sym)
        except Exception:
            # Entire batch request failed — retry each ticker individually
            batch_failed = batch

        # Per-ticker retry with backoff for anything that failed
        for sym in batch_failed:
            cap_map[sym] = _fetch_market_cap_with_retry(sym)

        pct = min(100, int((i + batch_size) / len(tickers) * 100))
        logger.info(f"    ...{pct}% complete", end="\r", flush=True)
        time.sleep(0.5)   # slightly more polite to Yahoo

    logger.info()
    df = df.copy()
    df["market_cap"] = df["ticker"].map(cap_map).fillna(0).astype(float)

    zero_cap = (df["market_cap"] == 0).sum()
    if zero_cap > 0:
        logger.warning(f"  [UNIVERSE] Warning: {zero_cap} ticker(s) returned market_cap=0 "
              f"(rate-limit or delisted). They will rank last.")
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

    Warns if sector caps prevent reaching n tickers.
    """
    df_sorted = df.sort_values("market_cap", ascending=False)
    selected: list[str] = []
    sector_counts: dict[str, int] = {}

    for _, row in df_sorted.iterrows():
        sec = row["sector"]
        if sector_counts.get(sec, 0) < max_per_sector:
            selected.append(row["ticker"])
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if len(selected) >= n:
            break

    if len(selected) < n:
        logger.warning(f"  [UNIVERSE] Warning: only {len(selected)} tickers selected "
              f"(requested {n}) — sector caps may be too restrictive. "
              f"Consider raising MAX_PER_SECTOR or lowering n.")

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
    skip_market_cap : Use source-list order only (faster but less accurate ranking)

    Returns
    -------
    List of ticker strings, e.g. ["AAPL", "MSFT", ...]
    """
    logger.info("\n" + "-" * 55)
    logger.info("  UNIVERSE SELECTION")
    logger.info("-" * 55)

    # ── Try cache first ──────────────────────────────────────────────
    # Cache validity is checked against the stored 'refreshed' date (not mtime),
    # and must also match the requested mode and n.
    if not force_refresh and _cache_is_fresh(mode=mode, n=n):
        # Try MySQL first, fall back to JSON file
        cached = None
        if _DB_AVAILABLE:
            try:
                cached = _db_read_universe(mode=mode, max_age_days=REFRESH_DAYS)
            except Exception:
                pass
        if not cached:
            cached = _load_cache()
        if cached and "tickers" in cached:
            tickers = cached["tickers"][:n]
            refreshed = cached.get("refreshed", "unknown")
            source = "MySQL" if _DB_AVAILABLE else "cache file"
            logger.info(f"  [UNIVERSE] Loaded {len(tickers)} tickers from {source} (refreshed {refreshed})")
            logger.info(f"  [UNIVERSE] Next refresh in < {REFRESH_DAYS} days  |  mode={cached.get('mode','?')}")
            logger.info("-" * 55)
            return tickers

    # ── Fetch fresh data ─────────────────────────────────────────────
    constituents = fetch_sp500_constituents()

    # Remove duplicate share classes (e.g. GOOG vs GOOGL, BRK-A vs BRK-B)
    constituents = deduplicate_share_classes(constituents)

    if skip_market_cap:
        logger.info("  [UNIVERSE] Skipping market-cap lookup (skip_market_cap=True)")
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
    logger.info(f"\n  [UNIVERSE] Selected {len(tickers)} tickers  |  mode={mode}")
    logger.info("  Sector breakdown:")
    for sec, cnt in breakdown.items():
        bar = "█" * cnt
        logger.info(f"    {sec:<40} {cnt:>3}  {bar}")

    # ── Cache results ────────────────────────────────────────────────
    cache_data = {
        "tickers":   tickers,
        "refreshed": str(datetime.date.today()),
        "mode":      mode,
        "n":         len(tickers),     # store actual count, not requested n
        "sector_breakdown": breakdown.to_dict(),
    }
    # Persist to MySQL (primary) and JSON file (fallback backup)
    if _DB_AVAILABLE:
        try:
            _db_write_universe(
                tickers=tickers,
                mode=mode,
                sector_breakdown=breakdown.to_dict() if hasattr(breakdown, "to_dict") else breakdown,
            )
            logger.info(f"\n  [UNIVERSE] Saved to MySQL universe_cache table")
        except Exception as _e:
            logger.warning(f"  [UNIVERSE] MySQL write failed ({_e}) — falling back to JSON file only")
    _save_cache(cache_data)
    logger.info(f"  [UNIVERSE] Saved to cache file: {CACHE_FILE}")
    logger.info("-" * 55)

    return tickers


def refresh_universe(n: int = DEFAULT_UNIVERSE_N, mode: str = "balanced") -> list[str]:
    """Force a full refresh of the universe cache."""
    return get_universe(n=n, mode=mode, force_refresh=True)


def show_cache_info():
    """Print current cache status using the stored refreshed date."""
    if not os.path.exists(CACHE_FILE):
        logger.info("No universe cache found. Run get_universe() to create one.")
        return
    cached = _load_cache()
    if not cached:
        logger.warning("Cache file exists but could not be read.")
        return
    try:
        refreshed_date = datetime.date.fromisoformat(cached.get("refreshed", ""))
        age = (datetime.date.today() - refreshed_date).days
        is_fresh = age < REFRESH_DAYS
    except ValueError:
        age = "?"
        is_fresh = False
    logger.info(f"\nUniverse Cache Info")
    logger.info(f"  File       : {CACHE_FILE}")
    logger.info(f"  Refreshed  : {cached.get('refreshed')}")
    logger.warning(f"  Age        : {age} day(s)  ({'fresh' if is_fresh else 'STALE — will refresh on next run'})")
    logger.info(f"  Tickers    : {cached.get('n')}  (mode={cached.get('mode')})")
    logger.info(f"  Sectors    :")
    for sec, cnt in (cached.get("sector_breakdown") or {}).items():
        logger.info(f"    {sec:<40} {cnt}")


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
        logger.info(f"\nFirst 20 tickers: {tickers[:20]}")

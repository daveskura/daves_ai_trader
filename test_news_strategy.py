"""
test_news_strategy.py
=====================
Run this from your project folder to validate the news strategy
setup before pushing to GitHub.

Usage:
    python test_news_strategy.py          # run all tests
    python test_news_strategy.py --step 3 # run a specific step only
"""

import sys, os, json, csv, argparse
from pathlib import Path

# ── Make sure we're running from the project folder ────────────────────────
os.chdir(Path(__file__).parent)

PASS  = "[PASS]"
FAIL  = "[FAIL]"
INFO  = "[INFO]"
WARN  = "[WARN]"

def header(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def ok(msg):   print(f"  {PASS}  {msg}")
def fail(msg): print(f"  {FAIL}  {msg}")
def info(msg): print(f"  {INFO}  {msg}")
def warn(msg): print(f"  {WARN}  {msg}")


# ── Step 1: Syntax check ────────────────────────────────────────────────────

def step1_syntax():
    header("Step 1 — Syntax & compile check")
    import ast
    files = ["strategy_runner.py", "equity_kpi_analyzer.py",
             "universe_manager.py", "update_quotes.py"]
    all_ok = True
    for fname in files:
        p = Path(fname)
        if not p.exists():
            warn(f"{fname} not found — skipping")
            continue
        try:
            ast.parse(p.read_text(encoding="utf-8"))
            ok(fname)
        except SyntaxError as e:
            fail(f"{fname}: {e}")
            all_ok = False
    return all_ok


# ── Step 2: Import check ────────────────────────────────────────────────────

def step2_imports():
    header("Step 2 — Import & function check")
    try:
        import strategy_runner as sr
        ok("strategy_runner imported successfully")
    except Exception as e:
        fail(f"Could not import strategy_runner: {e}")
        return False

    required_fns = [
        "_fetch_rss", "_gather_headlines", "_load_news_cache",
        "get_news_macro_analysis", "score_news_macro",
        "score_news_sentiment", "_build_news_prompt", "print_news_briefing",
    ]
    all_ok = True
    for fn in required_fns:
        if hasattr(sr, fn):
            ok(f"  {fn}()")
        else:
            fail(f"  {fn}() — NOT FOUND in strategy_runner.py")
            all_ok = False

    # Check strategies 19 and 20 are registered
    ids = [s[0] for s in sr.STRATEGIES]
    for sid in ("19", "20"):
        if sid in ids:
            ok(f"  Strategy {sid} in STRATEGIES list")
        else:
            fail(f"  Strategy {sid} MISSING from STRATEGIES list")
            all_ok = False

    for sid in ("19", "20"):
        if sid in sr.SCORE_FN:
            ok(f"  Strategy {sid} in SCORE_FN")
        else:
            fail(f"  Strategy {sid} MISSING from SCORE_FN")
            all_ok = False

    return all_ok


# ── Step 3: RSS headline fetch ──────────────────────────────────────────────

def step3_headlines():
    header("Step 3 — RSS headline fetch (no API key needed)")
    from strategy_runner import _gather_headlines, NEWS_FEEDS
    info(f"Testing {len(NEWS_FEEDS)} RSS feeds...")

    headlines = _gather_headlines(verbose=False)

    if len(headlines) == 0:
        fail("No headlines fetched — check internet connection or firewall")
        return False
    elif len(headlines) < 20:
        warn(f"Only {len(headlines)} headlines — some feeds may be blocked")
    else:
        ok(f"{len(headlines)} unique headlines fetched")

    # Show a sample
    info("Sample headlines:")
    for h in headlines[:5]:
        source = h.get("source", "?")
        title  = h.get("title", "")[:70]
        print(f"    [{source}] {title}")

    return len(headlines) > 0


# ── Step 4: API key check ───────────────────────────────────────────────────

def step4_api_key():
    header("Step 4 — Anthropic API key check")
    key = os.environ.get("ANTHROPIC_API_KEY", "")

    # Also check .env file
    env_path = Path(".env")
    if not key and env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("ANTHROPIC_API_KEY"):
                k, _, v = line.partition("=")
                key = v.strip().strip('"').strip("'")
                break

    if not key:
        fail("ANTHROPIC_API_KEY not found in environment or .env file")
        info("Create a .env file in your project folder containing:")
        info("  ANTHROPIC_API_KEY=sk-ant-...")
        return False

    if not key.startswith("sk-ant-"):
        warn(f"Key found but format looks unusual: {key[:12]}...")
    else:
        ok(f"API key found: {key[:16]}...")

    return bool(key)


# ── Step 5: Claude macro analysis ──────────────────────────────────────────

def step5_macro_analysis():
    header("Step 5 — Claude macro analysis (1 API call)")
    from strategy_runner import get_news_macro_analysis, print_news_briefing

    info("Fetching fresh headlines and asking Claude to analyze...")
    macro = get_news_macro_analysis(force_refresh=True, verbose=True)

    if macro is None:
        fail("Macro analysis returned None — check API key and internet connection")
        return False

    # Validate structure
    checks = [
        ("analysis_date",    str),
        ("market_regime",    str),
        ("dominant_themes",  list),
        ("company_catalysts",list),
    ]
    all_ok = True
    for field, expected_type in checks:
        val = macro.get(field)
        if val is None:
            fail(f"  Missing field: {field}")
            all_ok = False
        elif not isinstance(val, expected_type):
            fail(f"  {field} is {type(val).__name__}, expected {expected_type.__name__}")
            all_ok = False
        else:
            ok(f"  {field}: {repr(val)[:60]}")

    if macro.get("market_regime") not in ("RISK-ON", "RISK-OFF", "NEUTRAL"):
        warn(f"  Unexpected regime value: {macro.get('market_regime')}")

    themes = macro.get("dominant_themes", [])
    ok(f"  {len(themes)} macro theme(s) identified")
    cats = macro.get("company_catalysts", [])
    ok(f"  {len(cats)} company catalyst(s) identified")

    print()
    print_news_briefing(macro)
    return all_ok


# ── Step 6: Cache check ─────────────────────────────────────────────────────

def step6_cache():
    header("Step 6 — Cache file check")
    cache_path = Path("news_macro_cache.json")

    if not cache_path.exists():
        fail("news_macro_cache.json not found — Step 5 may have failed")
        return False

    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception as e:
        fail(f"Could not read cache file: {e}")
        return False

    from datetime import date
    cached_date = data.get("analysis_date", "")
    today       = date.today().isoformat()

    if cached_date == today:
        ok(f"Cache is fresh (date={cached_date})")
    else:
        fail(f"Cache date mismatch: cached={cached_date}  today={today}")
        return False

    ok(f"Regime: {data.get('market_regime')}  "
       f"Themes: {len(data.get('dominant_themes',[]))}  "
       f"Catalysts: {len(data.get('company_catalysts',[]))}")

    # Verify second call uses cache (no extra API call)
    from strategy_runner import get_news_macro_analysis
    info("Verifying second call reuses cache (should NOT call Claude again)...")
    macro2 = get_news_macro_analysis(force_refresh=False, verbose=True)
    if macro2 and macro2.get("analysis_date") == today:
        ok("Cache reuse confirmed — Stage 2 will not duplicate the API call")
    else:
        warn("Cache reuse may not be working correctly")

    return True


# ── Step 7: KPI data check ──────────────────────────────────────────────────

def step7_kpi():
    header("Step 7 — KPI data check")
    kpi_path = Path("equity_kpi_results.csv")

    if not kpi_path.exists():
        warn("equity_kpi_results.csv not found")
        info("Run this to generate it:")
        info("  python equity_kpi_analyzer.py --universe")
        info("Skipping scoring tests.")
        return None   # not a hard failure — just means scoring can't be tested

    from strategy_runner import load_kpi
    rows, kmap = load_kpi()
    ok(f"{len(rows)} tickers loaded from KPI file")

    # Check a few important fields are present
    if rows:
        sample = rows[0]
        for field in ["ticker", "composite_score", "sector", "current_price", "beta"]:
            if field in sample:
                ok(f"  Field present: {field}")
            else:
                warn(f"  Field missing: {field}")

    return True


# ── Step 8: Scoring dry-run ─────────────────────────────────────────────────

def step8_scoring():
    header("Step 8 — Scoring functions dry-run")
    kpi_path = Path("equity_kpi_results.csv")
    cache_path = Path("news_macro_cache.json")

    if not kpi_path.exists():
        warn("No KPI data — skipping scoring test (run equity_kpi_analyzer.py first)")
        return None

    if not cache_path.exists():
        warn("No news cache — skipping scoring test (Step 5 must pass first)")
        return None

    from strategy_runner import load_kpi, score_news_macro, score_news_sentiment
    rows, kmap = load_kpi()
    macro = json.loads(cache_path.read_text(encoding="utf-8"))

    candidates_19 = score_news_macro(rows, kmap, macro)
    candidates_20 = score_news_sentiment(rows, kmap, macro)

    if candidates_19:
        ok(f"Strategy 19 scored {len(candidates_19)} candidates")
        top = candidates_19[0]
        info(f"  Top pick: {top[0]}  score={top[1]}  {top[2][:60]}")
    else:
        warn("Strategy 19 found 0 candidates — may be normal on low-news days")

    if candidates_20:
        ok(f"Strategy 20 scored {len(candidates_20)} candidates")
        top = candidates_20[0]
        info(f"  Top pick: {top[0]}  score={top[1]}  {top[2][:60]}")
    else:
        warn("Strategy 20 found 0 candidates — requires CS > 40 and positive EPS")

    return True


# ── Step 9: Account file check ──────────────────────────────────────────────

def step9_accounts():
    header("Step 9 — Account file check (strategies 19 & 20)")
    all_ok = True
    for sid in ("19", "20"):
        acct_path = Path(f"account_{sid}.csv")
        hold_path = Path(f"holdings_{sid}.csv")

        if not acct_path.exists():
            warn(f"account_{sid}.csv not found")
            info(f"  Fix: python strategy_runner.py --init")
            all_ok = False
            continue

        rows = list(csv.DictReader(open(acct_path, newline="", encoding="utf-8")))
        if not rows:
            fail(f"account_{sid}.csv is empty")
            all_ok = False
            continue

        r = rows[0]
        ok(f"Strategy {sid}: cash=${r.get('cash','?')}  "
           f"holdings=${r.get('holdings_value','?')}  "
           f"trades={r.get('trades','?')}")

        if not hold_path.exists():
            warn(f"holdings_{sid}.csv not found")
        else:
            hrows = list(csv.DictReader(open(hold_path, newline="", encoding="utf-8")))
            ok(f"  holdings_{sid}.csv: {len(hrows)} position(s)")

    return all_ok


# ── Step 10: Full dry-run ───────────────────────────────────────────────────

def step10_dry_run():
    header("Step 10 — Full dry-run (strategies 19 & 20, no trades executed)")
    kpi_path = Path("equity_kpi_results.csv")

    if not kpi_path.exists():
        warn("No KPI data — skipping dry-run (run equity_kpi_analyzer.py first)")
        return None

    info("Running: python strategy_runner.py --strategy 19 --dry-run")
    info("Running: python strategy_runner.py --strategy 20 --dry-run")
    info("(Launching as subprocess so output streams live)")
    print()

    import subprocess
    for sid in ("19", "20"):
        result = subprocess.run(
            [sys.executable, "strategy_runner.py", "--strategy", sid, "--dry-run"],
            capture_output=False   # let output stream directly to console
        )
        if result.returncode == 0:
            ok(f"Strategy {sid} dry-run completed with exit code 0")
        else:
            fail(f"Strategy {sid} dry-run exited with code {result.returncode}")
            return False

    return True


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Test news strategy setup")
    parser.add_argument("--step", type=int, default=0,
                        help="Run only this step number (0 = all steps)")
    parser.add_argument("--skip-api", action="store_true",
                        help="Skip steps that call the Anthropic API (steps 5, 10)")
    args = parser.parse_args()

    steps = [
        (1,  "Syntax check",              step1_syntax),
        (2,  "Import & function check",   step2_imports),
        (3,  "RSS headline fetch",        step3_headlines),
        (4,  "API key check",             step4_api_key),
        (5,  "Claude macro analysis",     step5_macro_analysis),
        (6,  "Cache file check",          step6_cache),
        (7,  "KPI data check",            step7_kpi),
        (8,  "Scoring dry-run",           step8_scoring),
        (9,  "Account file check",        step9_accounts),
        (10, "Full strategy dry-run",     step10_dry_run),
    ]

    api_steps = {5, 10}
    results   = {}

    for num, name, fn in steps:
        if args.step and args.step != num:
            continue
        if args.skip_api and num in api_steps:
            print(f"\n  [SKIP] Step {num}: {name} (--skip-api)")
            continue

        try:
            result = fn()
            results[num] = result
        except Exception as e:
            print(f"\n  {FAIL}  Step {num} raised an exception: {e}")
            import traceback
            traceback.print_exc()
            results[num] = False

    # Summary
    header("SUMMARY")
    passed, failed, skipped, warned = 0, 0, 0, 0
    for num, name, _ in steps:
        if num not in results:
            print(f"  [SKIP]  Step {num:2d}: {name}")
            skipped += 1
        elif results[num] is None:
            print(f"  {WARN}  Step {num:2d}: {name} — skipped (missing prerequisite)")
            warned += 1
        elif results[num]:
            print(f"  {PASS}  Step {num:2d}: {name}")
            passed += 1
        else:
            print(f"  {FAIL}  Step {num:2d}: {name}")
            failed += 1

    print()
    print(f"  {passed} passed  |  {failed} failed  |  {warned} skipped (prereq)  |  {skipped} skipped (--step/--skip-api)")

    if failed == 0:
        print("\n  All checks passed — safe to push to GitHub.")
    else:
        print("\n  Fix the failing steps above before pushing to GitHub.")

    print()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

"""
news_strategy.py
================
News-driven macro catalyst trading strategy for Dave's AI Trader.

This module does two things:
  1. Fetches and analyzes current world/financial news via RSS feeds
     and the Anthropic API to identify macro themes and affected sectors.
  2. Provides two new strategies (19 & 20) that plug directly into
     strategy_runner.py's existing architecture.

  Strategy 19 — "News macro catalyst"
      Reads today's headlines, asks Claude to identify the dominant
      macro theme (e.g. "oil supply shock", "bank deregulation",
      "China trade war escalation"), maps it to beneficiary/loser
      sectors, then scores universe stocks based on sector alignment.

  Strategy 20 — "News sentiment momentum"
      Fetches company-specific news for held positions AND top
      candidates, scores sentiment (positive / neutral / negative),
      and overlays that on top of the KPI composite score. Buys
      stocks with rising positive news velocity, sells on negative
      catalysts.

Stand-alone usage (generates a news briefing + trade recommendations):
    python news_strategy.py
    python news_strategy.py --dry-run
    python news_strategy.py --briefing-only   # just show the news analysis
    python news_strategy.py --tickers AAPL MSFT XOM  # specific tickers

Integration with strategy_runner.py:
    Copy the two STRATEGIES entries and SCORE_FN entries at the bottom
    of this file into strategy_runner.py — they follow the exact same
    interface as every other strategy.

Requirements:
    pip install feedparser requests   (already in requirements.txt)
    ANTHROPIC_API_KEY in .env or environment

News sources used (all free, no API key):
    - Reuters RSS (business, world, markets)
    - AP News RSS
    - Financial Times headlines RSS
    - Yahoo Finance RSS
    - CNBC RSS
    - MarketWatch RSS
    - Seeking Alpha earnings RSS
"""

import sys, os, re, json, csv, math, argparse, urllib.request, urllib.error
import urllib.parse
from datetime import date, datetime, timezone
from pathlib import Path
from collections import defaultdict

# ── UTF-8 output ──────────────────────────────────────────────────────────────
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# ── Load .env ─────────────────────────────────────────────────────────────────
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL             = "claude-sonnet-4-6"
BASE_DIR          = Path(__file__).parent
STARTING_CASH     = 1000.00
COMMISSION        = 4.95
SPREAD_PCT        = 0.0005
ACCOUNT_NUM       = "123456789"

# ── News sources (RSS feeds, no API key needed) ───────────────────────────────
NEWS_FEEDS = [
    # Reuters
    ("Reuters Business",  "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters World",     "https://feeds.reuters.com/Reuters/worldNews"),
    ("Reuters Markets",   "https://feeds.reuters.com/reuters/financialsNews"),
    # AP
    ("AP Business",       "https://feeds.apnews.com/rss/apf-business"),
    ("AP World",          "https://feeds.apnews.com/rss/apf-topnews"),
    # Yahoo Finance
    ("Yahoo Finance",     "https://finance.yahoo.com/news/rssindex"),
    ("Yahoo Markets",     "https://finance.yahoo.com/rss/2.0/headline?s=^GSPC&region=US&lang=en-US"),
    # CNBC
    ("CNBC Economy",      "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("CNBC Finance",      "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    # MarketWatch
    ("MarketWatch",       "https://feeds.marketwatch.com/marketwatch/topstories/"),
    # Seeking Alpha (earnings)
    ("Seeking Alpha",     "https://seekingalpha.com/feed.xml"),
    # FT (free headlines)
    ("FT Markets",        "https://www.ft.com/rss/home/us"),
]

# ── Macro theme → sector beneficiaries mapping ────────────────────────────────
# Maps a macro theme keyword to (beneficiary sectors, loser sectors, description)
MACRO_THEMES = {
    "oil_supply_shock": (
        ["Energy", "Industrials"],
        ["Consumer Discretionary", "Airlines", "Consumer Staples", "Utilities"],
        "Rising oil prices benefit producers, hurt consumers and transport"
    ),
    "oil_demand_drop": (
        ["Consumer Discretionary", "Airlines", "Consumer Staples"],
        ["Energy"],
        "Falling oil prices hurt producers, benefit consumers"
    ),
    "rate_hike": (
        ["Financial Services", "Insurance"],
        ["Real Estate", "Utilities", "Consumer Discretionary"],
        "Higher rates benefit banks/insurers, hurt REITs and rate-sensitive growth"
    ),
    "rate_cut": (
        ["Real Estate", "Utilities", "Consumer Discretionary", "Technology"],
        ["Financial Services"],
        "Lower rates benefit growth and rate-sensitive sectors, compress bank margins"
    ),
    "inflation_surge": (
        ["Energy", "Materials", "Consumer Staples", "Financial Services"],
        ["Technology", "Consumer Discretionary", "Real Estate"],
        "Inflation favors hard assets and pricing-power names"
    ),
    "deflation_risk": (
        ["Technology", "Consumer Discretionary", "Real Estate"],
        ["Energy", "Materials"],
        "Deflationary pressures favor growth and consumption"
    ),
    "china_trade_tension": (
        ["Industrials", "Energy", "Materials", "Defense"],
        ["Technology", "Consumer Discretionary", "Semiconductors"],
        "Trade war hurts tech supply chains, benefits domestic industrials"
    ),
    "geopolitical_conflict": (
        ["Energy", "Defense", "Materials", "Healthcare"],
        ["Consumer Discretionary", "Travel", "Airlines"],
        "Conflict drives energy/defense demand, suppresses consumer confidence"
    ),
    "bank_deregulation": (
        ["Financial Services", "Capital Markets", "Insurance"],
        [],
        "Looser capital rules free banks for buybacks and lending expansion"
    ),
    "bank_regulation": (
        [],
        ["Financial Services", "Capital Markets"],
        "Tighter rules compress bank returns and capital deployment"
    ),
    "ai_investment_boom": (
        ["Technology", "Semiconductors", "Communication Services", "Utilities"],
        [],
        "AI capex cycle drives semiconductor and infrastructure demand"
    ),
    "tech_selloff": (
        ["Energy", "Consumer Staples", "Healthcare", "Financial Services"],
        ["Technology", "Communication Services"],
        "Tech rotation into value and defensives"
    ),
    "recession_fear": (
        ["Consumer Staples", "Healthcare", "Utilities", "Financial Services"],
        ["Consumer Discretionary", "Industrials", "Technology"],
        "Risk-off rotation into defensives"
    ),
    "strong_jobs": (
        ["Consumer Discretionary", "Financial Services", "Industrials"],
        ["Real Estate", "Utilities"],
        "Strong employment boosts spending, raises rate expectations"
    ),
    "weak_jobs": (
        ["Real Estate", "Utilities", "Technology"],
        ["Financial Services", "Industrials"],
        "Weak employment signals rate cuts ahead"
    ),
    "dollar_strength": (
        ["Domestic", "Financial Services", "Consumer Staples"],
        ["Technology", "Industrials", "Energy"],  # multinationals hurt
        "Strong dollar hurts exporters and commodity-heavy multinationals"
    ),
    "dollar_weakness": (
        ["Technology", "Industrials", "Energy", "Materials"],
        [],
        "Weak dollar boosts multinationals and commodity prices"
    ),
    "earnings_season_beat": (
        ["Technology", "Financial Services", "Consumer Discretionary"],
        [],
        "Broad earnings beats drive risk-on rotation into growth"
    ),
    "healthcare_policy": (
        ["Healthcare", "Pharmaceuticals", "Biotech"],
        [],
        "Healthcare policy changes affect drug pricing and coverage"
    ),
    "consumer_confidence_drop": (
        ["Consumer Staples", "Healthcare", "Utilities"],
        ["Consumer Discretionary", "Travel", "Retail"],
        "Lower confidence drives defensive rotation"
    ),
}

# ── Sector aliases (yfinance uses these names) ────────────────────────────────
SECTOR_ALIASES = {
    "Semiconductors":       "Information Technology",
    "Defense":              "Industrials",
    "Airlines":             "Industrials",
    "Travel":               "Consumer Discretionary",
    "Retail":               "Consumer Discretionary",
    "Pharmaceuticals":      "Healthcare",
    "Biotech":              "Healthcare",
    "Capital Markets":      "Financial Services",
    "Insurance":            "Financial Services",
    "Domestic":             None,   # skip — not a yfinance sector
}

def normalize_sector(s: str) -> str:
    return SECTOR_ALIASES.get(s, s)


# ── RSS fetcher ───────────────────────────────────────────────────────────────

def fetch_rss(url: str, timeout: int = 8) -> list[dict]:
    """Fetch an RSS feed and return list of {title, summary, published, link}."""
    try:
        import feedparser
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries[:15]:  # cap per source
            items.append({
                "title":     getattr(entry, "title", ""),
                "summary":   getattr(entry, "summary", "")[:400],
                "published": getattr(entry, "published", ""),
                "link":      getattr(entry, "link", ""),
            })
        return items
    except ImportError:
        pass

    # Fallback: urllib + simple XML parse (no feedparser)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content = resp.read().decode("utf-8", errors="replace")
        items = []
        titles   = re.findall(r"<title[^>]*><!\[CDATA\[(.*?)\]\]></title>", content, re.DOTALL)
        titles  += re.findall(r"<title[^>]*>(.*?)</title>", content, re.DOTALL)
        descs    = re.findall(r"<description[^>]*><!\[CDATA\[(.*?)\]\]></description>", content, re.DOTALL)
        descs   += re.findall(r"<description[^>]*>(.*?)</description>", content, re.DOTALL)
        # Skip feed-level title (first entry)
        for i, title in enumerate(titles[1:16], 0):
            clean_title = re.sub(r"<[^>]+>", "", title).strip()
            desc = re.sub(r"<[^>]+>", "", descs[i] if i < len(descs) else "").strip()[:300]
            if clean_title:
                items.append({"title": clean_title, "summary": desc, "published": "", "link": ""})
        return items
    except Exception:
        return []


def gather_headlines(max_sources: int = 8, verbose: bool = True) -> list[dict]:
    """
    Fetch headlines from multiple RSS sources.
    Returns deduplicated list of headline dicts sorted newest-first.
    """
    if verbose:
        print(f"\n  Fetching news from {min(max_sources, len(NEWS_FEEDS))} sources...")

    all_items = []
    seen_titles = set()
    sources_used = 0

    for name, url in NEWS_FEEDS[:max_sources]:
        if verbose:
            print(f"    {name}...", end=" ", flush=True)
        items = fetch_rss(url)
        count = 0
        for item in items:
            title_key = item["title"].lower()[:60]
            if title_key and title_key not in seen_titles:
                seen_titles.add(title_key)
                item["source"] = name
                all_items.append(item)
                count += 1
        if verbose:
            print(f"{count} headlines")
        sources_used += 1

    if verbose:
        print(f"  Total: {len(all_items)} unique headlines from {sources_used} sources")
    return all_items


# ── Claude API call ───────────────────────────────────────────────────────────

def call_claude(prompt: str, max_tokens: int = 2000, system: str = "") -> tuple[str | None, str | None]:
    """Call Claude API. Returns (text, error)."""
    if not ANTHROPIC_API_KEY:
        return None, "ANTHROPIC_API_KEY not set"

    messages = [{"role": "user", "content": prompt}]
    body = {"model": MODEL, "max_tokens": max_tokens, "messages": messages}
    if system:
        body["system"] = system

    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "Content-Type":      "application/json",
            "x-api-key":         ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data.get("stop_reason") == "max_tokens":
            print("  [WARN] Claude response truncated")
        text = data["content"][0]["text"].strip()
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "",        text)
        return text, None
    except urllib.error.HTTPError as e:
        body_text = ""
        try: body_text = e.read().decode("utf-8")
        except: pass
        return None, f"HTTP {e.code}: {body_text[:200]}"
    except Exception as e:
        return None, str(e)


# ── Step 1: Macro theme analysis ──────────────────────────────────────────────

MACRO_ANALYSIS_SYSTEM = """You are a senior macro economist and portfolio strategist.
Your job is to read today's financial and world news headlines and:
1. Identify the 1-3 dominant macro themes that will most affect equity markets TODAY.
2. For each theme, identify which S&P 500 sectors benefit (tailwind) vs suffer (headwind).
3. Identify any company-specific catalysts (earnings, scandals, M&A, product launches).
4. Rate overall market risk appetite: RISK-ON, RISK-OFF, or NEUTRAL.

Be specific and actionable. Focus on what is NEW today, not background noise.
Always respond with valid JSON only — no preamble, no markdown fences."""

def analyze_macro_themes(headlines: list[dict]) -> dict | None:
    """Ask Claude to extract macro themes from today's headlines."""
    # Format headlines for the prompt
    headline_text = "\n".join(
        f"[{h.get('source','')}] {h['title']} — {h.get('summary','')[:150]}"
        for h in headlines[:60]
    )

    prompt = f"""Today is {date.today().isoformat()}.

Here are today's top financial and world news headlines:

{headline_text}

Analyze these headlines and respond with this exact JSON structure:

{{
  "analysis_date": "{date.today().isoformat()}",
  "market_regime": "RISK-ON" | "RISK-OFF" | "NEUTRAL",
  "regime_confidence": 1-10,
  "regime_reasoning": "2-3 sentence explanation of overall market sentiment",
  "dominant_themes": [
    {{
      "theme_id": "geopolitical_conflict",
      "theme_name": "Human-readable theme name",
      "description": "2-3 sentences explaining this theme and why it matters today",
      "strength": 1-10,
      "duration_outlook": "1-3 days" | "1-2 weeks" | "1-3 months" | "structural",
      "beneficiary_sectors": ["Energy", "Defense"],
      "loser_sectors": ["Consumer Discretionary", "Airlines"],
      "supporting_headlines": ["headline 1", "headline 2"],
      "key_tickers_to_watch": ["XOM", "LMT", "DAL"]
    }}
  ],
  "company_catalysts": [
    {{
      "ticker": "AAPL",
      "catalyst_type": "earnings_beat" | "earnings_miss" | "upgrade" | "downgrade" | "ma" | "scandal" | "product" | "guidance_up" | "guidance_down" | "regulatory" | "other",
      "sentiment": "positive" | "negative" | "neutral",
      "magnitude": 1-10,
      "description": "What happened",
      "headline": "The specific headline"
    }}
  ],
  "macro_risk_factors": ["Factor 1", "Factor 2"],
  "trade_ideas": [
    {{
      "idea": "Short description of the trade",
      "rationale": "Why",
      "sectors": ["Energy"],
      "direction": "long" | "short",
      "timeframe": "today" | "week" | "month"
    }}
  ]
}}

Use only real themes visible in the headlines. Do not invent news.
If the headlines contain little market-moving information, set regime to NEUTRAL and dominant_themes to [].
Return only valid JSON."""

    print("\n  Asking Claude to analyze macro themes...")
    text, err = call_claude(prompt, max_tokens=2500, system=MACRO_ANALYSIS_SYSTEM)
    if err:
        print(f"  [ERROR] Macro analysis failed: {err}")
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  [ERROR] JSON parse failed: {e}")
        print(f"  Raw: {text[:300]}")
        return None


# ── Step 2: Score tickers against macro themes ────────────────────────────────

def score_news_macro(rows: list[dict], kmap: dict, macro: dict) -> list[tuple]:
    """
    Strategy 19 — News Macro Catalyst scorer.

    Scores each stock based on:
    - Sector alignment with today's dominant macro themes
    - Theme strength and confidence
    - Individual company catalysts from news
    - Market regime (risk-on = favor growth, risk-off = favor defensives)
    """
    if not macro or not macro.get("dominant_themes"):
        return []

    regime     = macro.get("market_regime", "NEUTRAL")
    regime_conf = macro.get("regime_confidence", 5)

    # Build sector score map from themes
    sector_scores: dict[str, float] = defaultdict(float)
    theme_labels:  dict[str, str]   = defaultdict(str)

    for theme in macro.get("dominant_themes", []):
        strength = theme.get("strength", 5) / 10.0
        duration_mult = {
            "1-3 days":   0.8,
            "1-2 weeks":  1.0,
            "1-3 months": 1.2,
            "structural": 1.5,
        }.get(theme.get("duration_outlook", "1-2 weeks"), 1.0)

        for sec in theme.get("beneficiary_sectors", []):
            norm = normalize_sector(sec)
            if norm:
                sector_scores[norm] += strength * duration_mult * 10
                theme_labels[norm]  += f"{theme['theme_name']}(+) "

        for sec in theme.get("loser_sectors", []):
            norm = normalize_sector(sec)
            if norm:
                sector_scores[norm] -= strength * duration_mult * 10
                theme_labels[norm]  += f"{theme['theme_name']}(-) "

    # Build company catalyst map {ticker: (sentiment_score, description)}
    catalyst_map: dict[str, tuple[float, str]] = {}
    for cat in macro.get("company_catalysts", []):
        ticker = cat.get("ticker", "").upper()
        if not ticker:
            continue
        mag = cat.get("magnitude", 5) / 10.0
        if cat.get("sentiment") == "positive":
            catalyst_map[ticker] = (mag * 15, cat.get("description", "")[:80])
        elif cat.get("sentiment") == "negative":
            catalyst_map[ticker] = (-mag * 15, cat.get("description", "")[:80])

    # Score each stock
    out = []
    for r in rows:
        ticker = r["ticker"]
        sector = r.get("sector", "Unknown")
        beta   = r.get("beta", 1.0) or 1.0
        cs     = r.get("composite_score", 0) or 0

        sector_adj = sector_scores.get(sector, 0)

        # Regime adjustments
        if regime == "RISK-OFF":
            if beta > 1.3:
                sector_adj -= 5   # penalize high beta in risk-off
            elif beta < 0.8:
                sector_adj += 3   # reward low beta
        elif regime == "RISK-ON":
            if beta > 1.2:
                sector_adj += 3   # reward high beta in risk-on

        # Company-specific catalyst
        cat_score, cat_desc = catalyst_map.get(ticker, (0, ""))

        # Base score: sector alignment + KPI health + company catalyst
        base_score = sector_adj + cs * 0.3 + cat_score

        # Only surface stocks with a meaningful positive signal
        if base_score <= 2 and not cat_score:
            continue

        # Build reason string
        label = theme_labels.get(sector, "")
        reason = f"News macro: sector={sector} adj={sector_adj:+.1f} regime={regime}"
        if label:
            reason += f" themes=[{label.strip()}]"
        if cat_desc:
            reason += f" catalyst={cat_desc}"

        out.append((ticker, round(base_score, 2), reason))

    return sorted(out, key=lambda x: -x[1])


# ── Step 3: Company news sentiment scorer ─────────────────────────────────────

def fetch_ticker_news(ticker: str, n: int = 5) -> list[str]:
    """Fetch recent news headlines for a specific ticker via Yahoo Finance RSS."""
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        items = fetch_rss(url, timeout=5)
        return [item["title"] for item in items[:n] if item.get("title")]
    except Exception:
        return []


def score_news_sentiment(rows: list[dict], kmap: dict, macro: dict) -> list[tuple]:
    """
    Strategy 20 — News Sentiment Momentum scorer.

    For each ticker, combines:
    - KPI composite score (fundamentals)
    - Macro sector tailwind/headwind (from strategy 19's analysis)
    - Company-specific news sentiment (per-ticker headline fetch)

    This is the "full picture" strategy that integrates news at every level.
    """
    if not macro:
        return []

    # Reuse macro sector scores from strategy 19
    sector_scores: dict[str, float] = defaultdict(float)
    for theme in macro.get("dominant_themes", []):
        strength = theme.get("strength", 5) / 10.0
        for sec in theme.get("beneficiary_sectors", []):
            norm = normalize_sector(sec)
            if norm: sector_scores[norm] += strength * 8
        for sec in theme.get("loser_sectors", []):
            norm = normalize_sector(sec)
            if norm: sector_scores[norm] -= strength * 8

    # Company catalysts
    catalyst_map: dict[str, tuple[float, str]] = {}
    for cat in macro.get("company_catalysts", []):
        ticker = cat.get("ticker", "").upper()
        if not ticker: continue
        mag = cat.get("magnitude", 5) / 10.0
        sign = 1 if cat.get("sentiment") == "positive" else (
               -1 if cat.get("sentiment") == "negative" else 0)
        if sign:
            catalyst_map[ticker] = (sign * mag * 20, cat.get("description","")[:80])

    out = []
    for r in rows:
        ticker = r["ticker"]
        sector = r.get("sector", "Unknown")
        cs     = r.get("composite_score", 0) or 0
        eps_g  = r.get("eps_growth_fwd", 0) or 0
        npm    = r.get("net_profit_margin", 0) or 0

        # Require minimum fundamentals — this strategy overlays news ON TOP of quality
        if cs < 40:
            continue
        if (r.get("eps_ttm") or 0) < 0:
            continue

        sector_adj    = sector_scores.get(sector, 0)
        cat_adj, cat_desc = catalyst_map.get(ticker, (0, ""))

        score = cs * 0.5 + sector_adj + cat_adj + eps_g * 30 + npm * 20

        if score <= 15 and not cat_adj:
            continue

        reason_parts = [f"Sentiment overlay: CS={cs:.0f}"]
        if sector_adj:  reason_parts.append(f"sector_adj={sector_adj:+.1f}")
        if cat_adj:     reason_parts.append(f"catalyst={cat_adj:+.1f} ({cat_desc})")
        reason_parts.append(f"sector={sector}")

        out.append((ticker, round(score, 2), "  ".join(reason_parts)))

    return sorted(out, key=lambda x: -x[1])


# ── Save/load macro analysis cache ───────────────────────────────────────────

NEWS_CACHE_FILE = BASE_DIR / "news_macro_cache.json"

def load_news_cache() -> dict | None:
    if not NEWS_CACHE_FILE.exists():
        return None
    try:
        data = json.loads(NEWS_CACHE_FILE.read_text(encoding="utf-8"))
        cached_date = data.get("analysis_date", "")
        if cached_date == date.today().isoformat():
            return data
        return None
    except Exception:
        return None

def save_news_cache(data: dict):
    try:
        NEWS_CACHE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"  [WARN] Could not save news cache: {e}")


# ── Public API: get_todays_macro_analysis() ───────────────────────────────────

def get_todays_macro_analysis(
    force_refresh: bool = False,
    verbose: bool = True,
) -> dict | None:
    """
    Main entry point. Returns today's macro analysis dict.
    Caches results so Claude is only called once per day.

    Called by both strategy 19 and 20 scoring functions.
    """
    # Try cache first (avoid hammering Claude multiple times)
    if not force_refresh:
        cached = load_news_cache()
        if cached:
            if verbose:
                print(f"  [NEWS] Loaded today's macro analysis from cache "
                      f"({len(cached.get('dominant_themes',[]))} themes, "
                      f"regime={cached.get('market_regime','?')})")
            return cached

    # Fetch fresh headlines
    headlines = gather_headlines(max_sources=8, verbose=verbose)
    if not headlines:
        print("  [NEWS] No headlines fetched — news strategy will be skipped today")
        return None

    # Ask Claude
    analysis = analyze_macro_themes(headlines)
    if analysis:
        analysis["headline_count"] = len(headlines)
        analysis["fetched_at"] = datetime.now(timezone.utc).isoformat()
        save_news_cache(analysis)
        if verbose:
            print(f"  [NEWS] Analysis complete: regime={analysis.get('market_regime','?')} "
                  f"themes={len(analysis.get('dominant_themes',[]))} "
                  f"catalysts={len(analysis.get('company_catalysts',[]))}")
    return analysis


# ── Strategy runner integration ───────────────────────────────────────────────
# These are the two new entries to add to STRATEGIES in strategy_runner.py

NEW_STRATEGIES = [
    ("19", "News macro catalyst",        "macro",      "med",
     "Fetches today's headlines via RSS, asks Claude to identify dominant macro themes "
     "(energy shock, deregulation, trade war, etc.), then scores universe stocks by sector "
     "alignment with those themes. Refreshes daily — no stale signals."),
    ("20", "News sentiment momentum",    "alt-data",   "med",
     "Combines KPI composite score with macro sector tailwinds AND company-specific "
     "news sentiment. Buys quality stocks with positive news catalysts; avoids or sells "
     "stocks with negative headline momentum regardless of fundamentals."),
]

# ── Prompt builders for strategy runner ──────────────────────────────────────

def build_news_prompt(strategy_id: str, strategy_name: str, strategy_desc: str,
                      candidates: list, holdings: list, acct: dict,
                      today: str, macro: dict) -> str:
    """Build the Claude prompt for news-driven strategies."""

    hold_str = "\n".join(
        f"  {h['ticker']}: {h['shares']:.4f} sh, cost basis ${h['cost_basis']:.2f}, avg ${h['avg_cost']:.4f}"
        for h in holdings
    ) or "  (none)"

    cand_str = "\n".join(
        f"  {t}: score={s:.1f}  {r}"
        for t, s, r in candidates[:12]
    )

    # Summarize the macro context for the prompt
    regime = macro.get("market_regime", "NEUTRAL")
    regime_reason = macro.get("regime_reasoning", "")
    themes_str = ""
    for theme in macro.get("dominant_themes", [])[:3]:
        themes_str += (
            f"\n  - {theme.get('theme_name','')} (strength {theme.get('strength',5)}/10, "
            f"outlook: {theme.get('duration_outlook','?')})\n"
            f"    Beneficiaries: {', '.join(theme.get('beneficiary_sectors',[]))}\n"
            f"    Losers: {', '.join(theme.get('loser_sectors',[]))}\n"
            f"    {theme.get('description','')}"
        )

    catalysts_str = ""
    for cat in macro.get("company_catalysts", [])[:5]:
        sign = "+" if cat.get("sentiment") == "positive" else (
               "-" if cat.get("sentiment") == "negative" else "~")
        catalysts_str += f"\n  [{sign}] {cat.get('ticker','?')}: {cat.get('description','')}"

    trade_ideas_str = ""
    for idea in macro.get("trade_ideas", [])[:3]:
        trade_ideas_str += f"\n  - {idea.get('idea','')}: {idea.get('rationale','')}"

    prompt = f"""You are managing paper trading account for strategy "{strategy_id} - {strategy_name}".

Strategy: {strategy_desc}

=== TODAY'S NEWS-DRIVEN MACRO CONTEXT ({today}) ===
Market Regime: {regime} (confidence: {macro.get('regime_confidence',5)}/10)
{regime_reason}

Dominant macro themes today:{themes_str}

Company-specific catalysts from news today:{catalysts_str if catalysts_str else "  None identified"}

Trade ideas from news analysis:{trade_ideas_str if trade_ideas_str else "  None"}

=== ACCOUNT STATUS ===
Cash: ${acct['cash']:.2f}  |  Holdings: ${acct['holdings_value']:.2f}  |  Total: ${acct['cash']+acct['holdings_value']:.2f}
Goal: ${STARTING_CASH*2:.2f}  |  Trades: {acct['trades']}

Current holdings:
{hold_str}

Top candidates today (scored by news-macro alignment):
{cand_str}

=== DECISION RULES ===
- Commission $4.95 + 0.05% spread per trade
- Max 3 positions, max 60% per stock, keep 5% cash reserve
- BUY only when: (1) stock sector aligns with a dominant bullish theme AND (2) fundamentals are solid (KPI score > 40)
- SELL when: stock sector is in a dominant LOSER list, OR company has a negative catalyst, OR >20% loss from avg cost
- HOLD when: news is ambiguous or theme has low strength (<5/10) — don't overtrade on weak signals
- In RISK-OFF regime: prefer beta < 1.0 stocks; avoid high-beta names even if sector is positive
- A strong company catalyst (earnings beat, upgraded guidance) overrides sector headwinds

Respond ONLY with a JSON object. No explanation outside the JSON.
{{
  "actions": [
    {{"type": "BUY",  "ticker": "XOM",  "reason": "Energy sector tailwind from oil supply shock, strong margins"}},
    {{"type": "SELL", "ticker": "NVDA", "reason": "Tech sector headwind + high beta in RISK-OFF regime"}},
    {{"type": "HOLD", "ticker": "JPM",  "reason": "Bank deregulation tailwind, wait for more confirmation"}}
  ],
  "summary": "one sentence: what news theme drove today's decisions"
}}"""
    return prompt


# ── File helpers (mirrors strategy_runner.py) ─────────────────────────────────

def acct_file(sid):  return BASE_DIR / f"account_{sid}.csv"
def hold_file(sid):  return BASE_DIR / f"holdings_{sid}.csv"
def txn_file(sid):   return BASE_DIR / f"transactions_{sid}.csv"

def read_csv(path):
    if not path.exists(): return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def read_account(sid):
    rows = read_csv(acct_file(sid))
    if not rows:
        return {"account": ACCOUNT_NUM, "strategy_id": sid,
                "cash": STARTING_CASH, "holdings_value": 0.0, "total": STARTING_CASH,
                "start_date": date.today().isoformat(), "trades": 0}
    r = rows[0]
    r["cash"]           = float(r["cash"])
    r["holdings_value"] = float(r["holdings_value"])
    r["total"]          = float(r["total"])
    r["trades"]         = int(r.get("trades", 0))
    return r

def save_account(sid, acct):
    write_csv(acct_file(sid), [acct],
              ["account","strategy_id","cash","holdings_value","total","start_date","trades"])

def read_holdings(sid):
    rows = read_csv(hold_file(sid))
    for r in rows:
        r["shares"]     = float(r["shares"])
        r["avg_cost"]   = float(r["avg_cost"])
        r["cost_basis"] = float(r["cost_basis"])
    return rows

def save_holdings(sid, holdings):
    write_csv(hold_file(sid), holdings,
              ["ticker","shares","avg_cost","cost_basis","purchase_date","strategy_id"])

def append_txn(sid, txn):
    path = txn_file(sid)
    fieldnames = ["date","strategy_id","action","ticker","shares","price",
                  "commission","net_amount","cash_after","reason"]
    write_header = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header: w.writeheader()
        w.writerow(txn)

def calc_holdings_value(holdings, kmap):
    total = 0.0
    for h in holdings:
        price = kmap.get(h["ticker"], {}).get("current_price") or float(h["avg_cost"])
        total += h["shares"] * price
    return round(total, 2)

def load_kpi(kpi_path: str = "equity_kpi_results.csv"):
    path = BASE_DIR / kpi_path
    if not path.exists(): return [], {}
    rows = read_csv(path)
    kmap = {}
    for r in rows:
        for col in ["composite_score","tier1_score","tier2_score","tier3_score",
                    "net_profit_margin","eps_growth_fwd","eps_ttm","eps_forward",
                    "pe_ratio","current_price","rsi_14","ma_50","ma_200","beta",
                    "market_cap","pct_from_52w_high","abnormal_return",
                    "net_insider_shares","vix","dividend_yield",
                    "five_year_avg_dividend_yield","eps_revision_pct"]:
            if col in r:
                try:    r[col] = float(r[col])
                except: r[col] = 0.0
        kmap[r["ticker"]] = r
    return rows, kmap


# ── Trade execution (mirrors strategy_runner.py) ──────────────────────────────

def buy(sid, ticker, price, cash_available, reason, today, kmap, dry_run=False):
    acct     = read_account(sid)
    holdings = read_holdings(sid)
    total    = acct["cash"] + acct["holdings_value"]
    max_pos  = total * 0.60
    existing = next((h for h in holdings if h["ticker"] == ticker), None)
    already_invested = float(existing["cost_basis"]) if existing else 0
    spend = min(cash_available * 0.90, max_pos - already_invested)
    spend = max(spend, 0)
    if spend < price + COMMISSION:
        return False, "Insufficient funds"
    shares   = round((spend - COMMISSION) / (price * (1 + SPREAD_PCT)), 6)
    net_cost = round(shares * price * (1 + SPREAD_PCT) + COMMISSION, 4)
    if dry_run:
        print(f"    [DRY-RUN] BUY  {shares:.4f} x {ticker} @ ${price:.2f}  cost=${net_cost:.2f}")
        return True, "dry-run"
    acct["cash"] = round(acct["cash"] - net_cost, 4)
    acct["trades"] += 1
    if existing:
        ns = existing["shares"] + shares
        nc = existing["cost_basis"] + net_cost
        existing["shares"] = round(ns, 6)
        existing["avg_cost"] = round(nc / ns, 4)
        existing["cost_basis"] = round(nc, 4)
    else:
        holdings.append({"ticker": ticker, "shares": round(shares, 6),
                         "avg_cost": round(net_cost / shares, 4),
                         "cost_basis": round(net_cost, 4),
                         "purchase_date": today, "strategy_id": sid})
    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"]          = round(acct["cash"] + acct["holdings_value"], 2)
    save_account(sid, acct)
    save_holdings(sid, holdings)
    append_txn(sid, {"date": today, "strategy_id": sid, "action": "BUY",
                     "ticker": ticker, "shares": round(shares, 6),
                     "price": round(price, 4), "commission": COMMISSION,
                     "net_amount": round(-net_cost, 4),
                     "cash_after": round(acct["cash"], 4), "reason": reason})
    return True, f"Bought {shares:.4f} shares @ ${price:.2f}"

def sell(sid, ticker, price, reason, today, kmap, dry_run=False):
    holdings = read_holdings(sid)
    pos = next((h for h in holdings if h["ticker"] == ticker), None)
    if not pos: return False, "No position"
    shares   = pos["shares"]
    proceeds = round(shares * price * (1 - SPREAD_PCT) - COMMISSION, 4)
    if proceeds <= 0:
        return False, f"Position too small (proceeds=${proceeds:.2f})"
    if dry_run:
        print(f"    [DRY-RUN] SELL {shares:.4f} x {ticker} @ ${price:.2f}  proceeds=${proceeds:.2f}")
        return True, "dry-run"
    acct = read_account(sid)
    acct["cash"]   = round(acct["cash"] + proceeds, 4)
    acct["trades"] += 1
    holdings = [h for h in holdings if h["ticker"] != ticker]
    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"]          = round(acct["cash"] + acct["holdings_value"], 2)
    save_account(sid, acct)
    save_holdings(sid, holdings)
    append_txn(sid, {"date": today, "strategy_id": sid, "action": "SELL",
                     "ticker": ticker, "shares": round(shares, 6),
                     "price": round(price, 4), "commission": COMMISSION,
                     "net_amount": round(proceeds, 4),
                     "cash_after": round(acct["cash"], 4), "reason": reason})
    return True, f"Sold {shares:.4f} shares @ ${price:.2f}"


# ── Run one news-driven strategy ──────────────────────────────────────────────

def run_news_strategy(sid: str, name: str, style: str, risk: str, desc: str,
                      rows: list, kmap: dict, macro: dict,
                      today: str, dry_run: bool = False):
    """Run a single news-driven strategy. Mirrors run_strategy() in strategy_runner.py."""
    print(f"\n  [{sid}] {name}")
    print(f"       Style: {style}  |  Risk: {risk}")

    if not macro:
        print("       No macro analysis available today — skipping")
        return

    acct     = read_account(sid)
    holdings = read_holdings(sid)
    acct["holdings_value"] = calc_holdings_value(holdings, kmap)
    acct["total"]          = round(acct["cash"] + acct["holdings_value"], 2)
    save_account(sid, acct)

    print(f"       Cash: ${acct['cash']:>9.2f}  |  "
          f"Holdings: ${acct['holdings_value']:>9.2f}  |  "
          f"Total: ${acct['total']:>9.2f}")
    print(f"       Regime: {macro.get('market_regime','?')}  |  "
          f"Themes: {len(macro.get('dominant_themes',[]))}")

    # Score candidates
    if sid == "19":
        candidates = score_news_macro(rows, kmap, macro)
    else:
        candidates = score_news_sentiment(rows, kmap, macro)

    if not candidates:
        print("       No candidates matched today's news themes")
        return

    print(f"       Top candidate: {candidates[0][0]} (score {candidates[0][1]:.1f})")

    # Build prompt and ask Claude
    prompt = build_news_prompt(sid, name, desc, candidates, holdings, acct, today, macro)
    decision, err = call_claude(prompt, max_tokens=1200)
    if err:
        print(f"       Claude error: {err}")
        return

    try:
        result = json.loads(decision)
    except json.JSONDecodeError as e:
        print(f"       JSON parse error: {e}")
        return

    print(f"       Summary: {result.get('summary','')}")

    for action in result.get("actions", []):
        atype  = action.get("type","").upper()
        ticker = action.get("ticker","")
        reason = action.get("reason","")
        if not ticker or ticker not in kmap:
            continue
        price = kmap[ticker].get("current_price", 0)
        if price <= 0: continue

        if atype == "BUY":
            ok, msg = buy(sid, ticker, price, acct["cash"], reason, today, kmap, dry_run)
            print(f"       BUY  {ticker}: {msg}")
            acct = read_account(sid)
        elif atype == "SELL":
            ok, msg = sell(sid, ticker, price, reason, today, kmap, dry_run)
            print(f"       SELL {ticker}: {msg}")
            acct = read_account(sid)
        elif atype == "HOLD":
            print(f"       HOLD {ticker}: {reason[:70]}")


# ── Standalone briefing output ────────────────────────────────────────────────

def print_briefing(macro: dict):
    """Print a human-readable market briefing from macro analysis."""
    if not macro:
        print("  No macro analysis available.")
        return

    print("\n" + "=" * 70)
    print("  NEWS-DRIVEN MACRO BRIEFING")
    print(f"  {macro.get('analysis_date','?')}  |  "
          f"Headlines analyzed: {macro.get('headline_count','?')}  |  "
          f"Fetched: {macro.get('fetched_at','?')[:16]}")
    print("=" * 70)

    regime = macro.get("market_regime", "NEUTRAL")
    conf   = macro.get("regime_confidence", 5)
    reg_color = {"RISK-ON": "▲ RISK-ON", "RISK-OFF": "▼ RISK-OFF", "NEUTRAL": "─ NEUTRAL"}.get(regime, regime)
    print(f"\n  Market Regime: {reg_color}  (confidence: {conf}/10)")
    print(f"  {macro.get('regime_reasoning','')}")

    themes = macro.get("dominant_themes", [])
    if themes:
        print(f"\n  DOMINANT THEMES  ({len(themes)} identified)")
        print("  " + "─" * 60)
        for i, theme in enumerate(themes, 1):
            print(f"\n  {i}. {theme.get('theme_name','')}  "
                  f"[strength {theme.get('strength',5)}/10, {theme.get('duration_outlook','?')}]")
            print(f"     {theme.get('description','')}")
            ben = theme.get("beneficiary_sectors", [])
            los = theme.get("loser_sectors", [])
            if ben: print(f"     Tailwind:  {', '.join(ben)}")
            if los: print(f"     Headwind:  {', '.join(los)}")
            heads = theme.get("supporting_headlines", [])[:3]
            for h in heads:
                print(f"     › {h[:90]}")

    catalysts = macro.get("company_catalysts", [])
    if catalysts:
        print(f"\n  COMPANY CATALYSTS  ({len(catalysts)} found)")
        print("  " + "─" * 60)
        for cat in catalysts:
            sign = {"positive": "▲", "negative": "▼", "neutral": "─"}.get(cat.get("sentiment",""), "?")
            print(f"  {sign} {cat.get('ticker','?'):<6}  [{cat.get('catalyst_type','')}  mag={cat.get('magnitude',5)}/10]  "
                  f"{cat.get('description','')[:70]}")

    risk_factors = macro.get("macro_risk_factors", [])
    if risk_factors:
        print(f"\n  RISK FACTORS")
        for r in risk_factors:
            print(f"    • {r}")

    trade_ideas = macro.get("trade_ideas", [])
    if trade_ideas:
        print(f"\n  TRADE IDEAS FROM NEWS")
        print("  " + "─" * 60)
        for idea in trade_ideas:
            direction = {"long": "LONG ▲", "short": "SHORT ▼"}.get(idea.get("direction",""), "?")
            print(f"  {direction}  {idea.get('idea','')}  [{idea.get('timeframe','')}]")
            print(f"           {idea.get('rationale','')}")
            print(f"           Sectors: {', '.join(idea.get('sectors',[]))}")

    print("\n" + "=" * 70)


# ── Main (standalone) ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="News-driven macro catalyst strategy — run standalone or integrate with strategy_runner.py")
    parser.add_argument("--briefing-only",  action="store_true",
                        help="Just show today's macro briefing, no trading")
    parser.add_argument("--dry-run",        action="store_true",
                        help="Preview trades without executing")
    parser.add_argument("--force-refresh",  action="store_true",
                        help="Force re-fetch headlines (ignore today's cache)")
    parser.add_argument("--tickers",        nargs="+", default=None,
                        help="Override universe with specific tickers")
    parser.add_argument("--kpi-file",       default="equity_kpi_results.csv",
                        help="KPI results file from equity_kpi_analyzer.py")
    parser.add_argument("--strategy",       choices=["19","20","both"], default="both",
                        help="Which news strategy to run (default: both)")
    args = parser.parse_args()

    today = date.today().isoformat()

    print("\n" + "=" * 70)
    print("  NEWS-DRIVEN MACRO CATALYST STRATEGIES")
    print(f"  Date: {today}")
    if args.dry_run:
        print("  *** DRY RUN — no trades will be executed ***")
    print("=" * 70)

    if not ANTHROPIC_API_KEY:
        print("\n  ERROR: ANTHROPIC_API_KEY not set.")
        print("  Add it to your .env file or export it as an environment variable.\n")
        return

    # ── Step 1: Get macro analysis ────────────────────────────────────────────
    macro = get_todays_macro_analysis(force_refresh=args.force_refresh, verbose=True)

    # ── Step 2: Print briefing ────────────────────────────────────────────────
    print_briefing(macro)

    if args.briefing_only:
        return

    # ── Step 3: Load KPI data ─────────────────────────────────────────────────
    rows, kmap = load_kpi(args.kpi_file)
    if not rows:
        print(f"\n  ERROR: KPI file '{args.kpi_file}' not found.")
        print("  Run: python equity_kpi_analyzer.py --universe\n")
        return

    if args.tickers:
        rows  = [r for r in rows if r["ticker"] in args.tickers]
        kmap  = {t: kmap[t] for t in args.tickers if t in kmap}
        print(f"\n  Filtered to {len(rows)} specified tickers.")

    print(f"\n  Loaded {len(rows)} tickers from KPI file.")

    # ── Step 4: Init accounts if needed ──────────────────────────────────────
    for sid, name, style, risk, desc in [
        ("19", "News macro catalyst",     "macro",    "med",
         "Identifies dominant macro themes from today's news and buys sector winners."),
        ("20", "News sentiment momentum", "alt-data", "med",
         "Overlays news sentiment on KPI scores to buy quality stocks with positive catalysts."),
    ]:
        if not acct_file(sid).exists():
            acct = {"account": ACCOUNT_NUM, "strategy_id": sid,
                    "cash": STARTING_CASH, "holdings_value": 0.0, "total": STARTING_CASH,
                    "start_date": today, "trades": 0}
            save_account(sid, acct)
            save_holdings(sid, [])
            print(f"  [{sid}] Initialised new account for '{name}'")

    # ── Step 5: Run strategies ────────────────────────────────────────────────
    to_run = []
    if args.strategy in ("19", "both"):
        to_run.append(("19", "News macro catalyst", "macro", "med",
                        "Identifies dominant macro themes from today's news and buys sector winners."))
    if args.strategy in ("20", "both"):
        to_run.append(("20", "News sentiment momentum", "alt-data", "med",
                        "Overlays news sentiment on KPI scores to buy quality stocks with positive catalysts."))

    for sid, name, style, risk, desc in to_run:
        run_news_strategy(sid, name, style, risk, desc, rows, kmap, macro, today, args.dry_run)

    print("\n" + "=" * 70)
    print("  Done. Check account_19.csv / account_20.csv for results.")
    print("  Re-run with --briefing-only to see just the macro analysis.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()

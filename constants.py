"""
constants.py — Shared constants for the multi-strategy trading system.

Import from here in all scripts to eliminate sync problems:
    from constants import STRATEGIES, HOLD_PERIODS, MACRO_THEME_MAP, SCORE_THRESHOLDS

Scripts that use these:
    strategy_runner.py, test_news_strategy.py, update_quotes.py, equity_kpi_analyzer.py
"""

# ── Engine parameters ────────────────────────────────────────────────────────
STARTING_CASH  = 1000.00
COMMISSION     = 1.00       # flat $ per trade
SPREAD_PCT     = 0.0015     # 15 bps — realistic for mid/small-cap names; large-caps are tighter
ACCOUNT_NUM    = "123456789"
MIN_HOLD_DAYS  = 3          # global fallback; see HOLD_PERIODS for per-strategy overrides
STOP_LOSS_PCT  = 0.20       # 20% hard stop-loss on any position
MODEL          = "claude-sonnet-4-6"

# ── KPI freshness thresholds ─────────────────────────────────────────────────
KPI_WARN_HOURS       = 4    # warn if KPI file is older than this
KPI_WARN_HOURS_PEAD  = 4    # stricter threshold for S07 (PEAD) — same value,
                             # but kept separate so it can be tightened independently

# ── Per-strategy minimum hold periods (trading days) ─────────────────────────
HOLD_PERIODS = {
    "02":  3,   # Mean reversion — short-cycle, RSI bounce thesis
    "03": 10,   # Value investing — weeks to months for re-rating
    "06": 15,   # Low volatility / defensive — hold through noise
    "07":  3,   # PEAD — event-driven, drift resolves quickly
    "08": 15,   # Dividend growth — income, very low turnover
    "09": 10,   # Insider buying — 6-12 month thesis; gate at 10 days
    "10":  5,   # Macro-regime adaptive — regime can shift weekly
    "11": 10,   # S&P 500 value tilt — patient holding required
    "12":  7,   # Momentum — weekly trend check is fine
    "13": 15,   # Quality / profitability — stable compounders
    "14": 999,  # Passive — never sell (buy-and-hold benchmark)
    "18": 10,   # Capex beneficiary / semis — conviction hold
    "19":  5,   # News macro catalyst — themes shift within the week
    "20":  5,   # News sentiment momentum — short-lived sentiment edge
    "21": 15,   # Defense & war economy — multi-year thesis
}

# ── Strategy registry ────────────────────────────────────────────────────────
# Each tuple: (id, name, style, risk_level, description)
STRATEGIES = [
    ("02", "Mean reversion",             "contrarian", "med",
     "Buy oversold stocks (RSI < 38). Sell when RSI recovers above 50."),
    ("03", "Value investing",            "value",      "low",
     "Buy fundamentally cheap stocks: low P/E, low P/B. Hold until fair value."),
    ("06", "Low volatility / defensive", "defensive",  "low",
     "Lowest beta stocks with stable earnings. Hold through high-VIX environments."),
    ("07", "Earnings surprise (PEAD)",   "event",      "high",
     "Buy profitable stocks with strong forward EPS growth AND a positive abnormal return today."),
    ("08", "Dividend growth",            "income",     "low",
     "Companies with strong dividend yield and growing payout."),
    ("09", "Insider buying signal",      "alt-data",   "med",
     "Follow C-suite open-market purchases."),
    ("10", "Macro-regime adaptive",      "macro",      "med",
     "Switch between aggressive and defensive posture based on VIX."),
    ("11", "S&P 500 value tilt",         "academic",   "med",
     "Pure value factor within the S&P 500."),
    ("12", "Momentum (academic / Asness)", "academic", "high",
     "Cliff Asness AQR-style momentum with crash protection."),
    ("13", "Quality / profitability",    "academic",   "low",
     "Novy-Marx gross profitability factor."),
    ("14", "Passive S&P 500 benchmark",  "passive",    "low",
     "Buy and hold the top market-cap stocks. No active trading."),
    ("18", "Capex beneficiary / semis",  "thematic",   "high",
     "Semiconductor and hardware infrastructure stocks."),
    ("19", "News macro catalyst",        "macro",      "med",
     "Fetches headlines, asks Claude to identify macro themes."),
    ("20", "News sentiment momentum",    "alt-data",   "med",
     "Overlays news sentiment on KPI composite scores."),
    ("21", "Defense & war economy",      "thematic",   "med",
     "Defense primes, energy, and cybersecurity stocks."),
]

# Convenience set for O(1) membership checks
STRATEGY_IDS = {s[0] for s in STRATEGIES}

# ── Macro theme map ───────────────────────────────────────────────────────────
# Format: theme_id -> (beneficiary_sectors, loser_sectors)
# Used by score_news_macro (S19) and score_news_sentiment (S20) to augment
# Claude's own sector calls with built-in domain knowledge.
MACRO_THEME_MAP = {
    # ── Monetary policy ───────────────────────────────────────────────────────
    "rate_cut": (
        ["Real Estate", "Utilities", "Financials", "Consumer Discretionary"],
        ["Consumer Staples"],
    ),
    "rate_hike": (
        ["Financials"],
        ["Real Estate", "Utilities", "Consumer Discretionary", "Information Technology"],
    ),
    "quantitative_easing": (
        ["Real Estate", "Financials", "Information Technology"],
        ["Consumer Staples"],
    ),
    "quantitative_tightening": (
        ["Financials"],
        ["Real Estate", "Utilities", "Information Technology"],
    ),
    "yield_curve_inversion": (
        ["Consumer Staples", "Utilities", "Health Care"],
        ["Financials", "Consumer Discretionary", "Industrials"],
    ),

    # ── Inflation & macro conditions ──────────────────────────────────────────
    "inflation": (
        ["Energy", "Materials", "Real Estate", "Consumer Staples"],
        ["Consumer Discretionary", "Information Technology", "Utilities"],
    ),
    "disinflation": (
        ["Consumer Discretionary", "Information Technology", "Utilities"],
        ["Energy", "Materials"],
    ),
    "recession_fear": (
        ["Consumer Staples", "Utilities", "Health Care"],
        ["Consumer Discretionary", "Financials", "Industrials", "Materials"],
    ),
    "soft_landing": (
        ["Consumer Discretionary", "Financials", "Industrials"],
        ["Consumer Staples", "Utilities"],
    ),
    "stagflation": (
        ["Energy", "Materials", "Consumer Staples"],
        ["Consumer Discretionary", "Information Technology", "Financials"],
    ),

    # ── Trade & geopolitics ───────────────────────────────────────────────────
    "tariffs": (
        ["Industrials", "Materials", "Consumer Staples"],
        ["Consumer Discretionary", "Information Technology"],
    ),
    "trade_deal": (
        ["Industrials", "Materials", "Consumer Discretionary"],
        [],
    ),
    "sanctions": (
        ["Energy", "Defense"],
        ["Industrials", "Consumer Discretionary"],
    ),
    "supply_chain": (
        ["Industrials", "Information Technology"],
        ["Consumer Discretionary"],
    ),
    "nearshoring": (
        ["Industrials", "Real Estate", "Materials"],
        [],
    ),

    # ── Energy & commodities ──────────────────────────────────────────────────
    "oil_spike": (
        ["Energy"],
        ["Consumer Discretionary", "Industrials", "Airlines"],
    ),
    "oil_crash": (
        ["Consumer Discretionary", "Industrials"],
        ["Energy"],
    ),
    "commodity_boom": (
        ["Materials", "Energy"],
        ["Consumer Discretionary", "Industrials"],
    ),
    "commodity_bust": (
        ["Consumer Discretionary", "Industrials"],
        ["Materials", "Energy"],
    ),

    # ── Technology & thematic ─────────────────────────────────────────────────
    "ai_capex": (
        ["Information Technology", "Industrials", "Utilities"],
        [],
    ),
    "ai_regulation": (
        ["Financials", "Health Care"],
        ["Information Technology"],
    ),
    "semiconductor_shortage": (
        ["Information Technology"],
        ["Consumer Discretionary", "Industrials"],
    ),
    "cloud_growth": (
        ["Information Technology"],
        [],
    ),
    "crypto_rally": (
        ["Financials", "Information Technology"],
        [],
    ),
    "crypto_crash": (
        [],
        ["Financials", "Information Technology"],
    ),

    # ── Geopolitical / defense ────────────────────────────────────────────────
    "defense_spending": (
        ["Industrials", "Information Technology"],
        [],
    ),
    "geopolitical_tension": (
        ["Energy", "Defense", "Consumer Staples"],
        ["Consumer Discretionary", "Industrials"],
    ),
    "peace_deal": (
        ["Consumer Discretionary", "Industrials"],
        ["Energy", "Defense"],
    ),

    # ── Currency ──────────────────────────────────────────────────────────────
    "usd_strength": (
        ["Financials"],
        ["Materials", "Industrials", "Energy"],
    ),
    "usd_weakness": (
        ["Materials", "Industrials", "Energy"],
        ["Financials"],
    ),

    # ── Housing & credit ──────────────────────────────────────────────────────
    "housing_boom": (
        ["Real Estate", "Financials", "Materials", "Consumer Discretionary"],
        [],
    ),
    "housing_bust": (
        [],
        ["Real Estate", "Financials", "Materials", "Consumer Discretionary"],
    ),
    "credit_tightening": (
        ["Consumer Staples", "Utilities"],
        ["Financials", "Real Estate", "Consumer Discretionary"],
    ),
    "credit_expansion": (
        ["Financials", "Consumer Discretionary", "Real Estate"],
        [],
    ),

    # ── International ─────────────────────────────────────────────────────────
    "china_stimulus": (
        ["Materials", "Consumer Discretionary", "Industrials"],
        [],
    ),
    "china_slowdown": (
        [],
        ["Materials", "Consumer Discretionary", "Energy"],
    ),
    "emerging_market_rally": (
        ["Materials", "Industrials"],
        [],
    ),
    "europe_recession": (
        ["Consumer Staples", "Utilities"],
        ["Industrials", "Materials"],
    ),

    # ── Health & social ───────────────────────────────────────────────────────
    "pandemic_variant": (
        ["Health Care", "Consumer Staples", "Information Technology"],
        ["Consumer Discretionary", "Industrials", "Real Estate"],
    ),
    "healthcare_reform": (
        ["Health Care"],
        ["Consumer Discretionary"],
    ),
}

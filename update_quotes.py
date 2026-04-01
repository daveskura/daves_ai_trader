"""
update_quotes.py — Fetch live quotes for all held positions and refresh
				   every strategy's holdings_value, account total, and
				   the leaderboard WITHOUT triggering any buy/sell logic.

Also recomputes abnormal_return for ALL tickers in equity_kpi_results.csv
using today's live prices vs yesterday's close and SPY as the market proxy.
This fixes the data-freshness gap where strategy_runner.py (running at 4:30 PM)
was using an abnormal_return computed from the morning KPI run — 8+ hours stale
for PEAD (S07) which depends entirely on that signal.

Usage:
	python update_quotes.py			  # update all strategies
	python update_quotes.py --strategy 03   # one strategy only
	python update_quotes.py --show		  # print holdings table after update
	python update_quotes.py --show --strategy 03
	python update_quotes.py --no-kpi-refresh   # skip abnormal_return refresh
	python update_quotes.py --stale-threshold 5   # warn if price > 5% from avg_cost

The script requires only the standard library + yfinance (already in
requirements.txt alongside the rest of the trading engine).
"""

import sys
import csv
import argparse
import time
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from collections import defaultdict
from functools import wraps

# ── Fix Windows console encoding ──────────────────────────────────────────────
if sys.platform == 'win32':
	import io
	sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
	sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ── Configure logging ──────────────────────────────────────────────────────────
logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
	handlers=[
		logging.FileHandler(Path(__file__).parent / "quote_update.log", encoding='utf-8'),
		logging.StreamHandler()
	]
)
logger = logging.getLogger(__name__)

# ── UTF-8 output (Windows) ────────────────────────────────────────────────────
if hasattr(sys.stdout, "reconfigure"):
	sys.stdout.reconfigure(encoding="utf-8")

try:
	import yfinance as yf
except ImportError:
	sys.exit("ERROR: yfinance not installed.  Run:  pip install yfinance")

try:
	import pandas as pd
except ImportError:
	sys.exit("ERROR: pandas not installed.  Run:  pip install pandas")

# ── Constants — must mirror strategy_runner.py ────────────────────────────────
STARTING_CASH = 1_000.00
ACCOUNT_NUM   = "123456789"
BASE_DIR	  = Path(__file__).parent

# FIX: Added stale price threshold (warn if price deviates > 5% from avg_cost)
DEFAULT_STALE_THRESHOLD = 5.0  # percent

STRATEGIES = [
	("02", "Mean reversion",				   "contrarian",  "med"),
	("03", "Value investing",				  "value",	   "low"),
	("06", "Low volatility / defensive",	   "defensive",   "low"),
	("07", "Earnings surprise (PEAD)",		 "event",	   "high"),
	("08", "Dividend growth",				  "income",	  "low"),
	("09", "Insider buying signal",			"alt-data",	"med"),
	("10", "Macro-regime adaptive",			"macro",	   "med"),
	("11", "S&P 500 value tilt",			   "academic",	"med"),
	("12", "Momentum (academic / Asness)",	 "academic",	"high"),
	("13", "Quality / profitability",		  "academic",	"low"),
	("14", "Passive S&P 500 benchmark",		"passive",	 "low"),
	("18", "Capex beneficiary / semis",		"thematic",	"high"),
	("19", "News macro catalyst",			  "macro",	   "med"),
	("20", "News sentiment momentum",		  "alt-data",	"med"),
	("21", "Defense & war economy",			"thematic",	"med"),
]

# ── Retry decorator for yfinance calls ────────────────────────────────────────
def retry(max_attempts: int = 3, backoff: float = 2.0):
	"""Retry decorator with exponential backoff for network operations."""
	def decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			last_exception = None
			for attempt in range(max_attempts):
				try:
					return func(*args, **kwargs)
				except Exception as e:
					last_exception = e
					if attempt < max_attempts - 1:
						wait_time = backoff * (2 ** attempt)
						logger.warning(f"{func.__name__} failed (attempt {attempt + 1}/{max_attempts}): {e}")
						logger.info(f"Retrying in {wait_time:.1f} seconds...")
						time.sleep(wait_time)
					else:
						logger.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
			raise last_exception
		return wrapper
	return decorator

# ── File helpers ──────────────────────────────────────────────────────────────

def acct_file(sid: str) -> Path: return BASE_DIR / f"account_{sid}.csv"
def hold_file(sid: str) -> Path: return BASE_DIR / f"holdings_{sid}.csv"
def leader_file() -> Path: return BASE_DIR / "leaderboard.csv"
KPI_FILE = BASE_DIR / "equity_kpi_results.csv"

def read_csv(path: Path) -> list:
	if not path.exists():
		return []
	with open(path, newline="", encoding="utf-8") as f:
		return list(csv.DictReader(f))

def write_csv(path: Path, rows: list, fieldnames: list):
	with open(path, "w", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=fieldnames)
		w.writeheader()
		w.writerows(rows)

def read_account(sid: str) -> dict | None:
	rows = read_csv(acct_file(sid))
	if not rows:
		return None
	r = rows[0]
	r["cash"] = float(r["cash"])
	r["holdings_value"] = float(r["holdings_value"])
	r["total"] = float(r["total"])
	r["trades"] = int(r.get("trades", 0))
	return r

def save_account(sid: str, acct: dict):
	write_csv(acct_file(sid), [acct],
			  ["account", "strategy_id", "cash", "holdings_value", "total", "start_date", "trades"])

def read_holdings(sid: str) -> list:
	rows = read_csv(hold_file(sid))
	for r in rows:
		r["shares"] = float(r["shares"])
		r["avg_cost"] = float(r["avg_cost"])
		r["cost_basis"] = float(r["cost_basis"])
	return rows

def save_holdings(sid: str, holdings: list):
	write_csv(hold_file(sid), holdings,
			  ["ticker", "shares", "avg_cost", "cost_basis", "purchase_date", "strategy_id"])

# ── Improved abnormal_return refresh with better error handling ──────────
@retry(max_attempts=2, backoff=1.0)
def refresh_abnormal_returns(verbose: bool = True) -> int:
	"""
	Recompute abnormal_return for every ticker in equity_kpi_results.csv
	using today's live intraday move vs yesterday's close, with SPY as the
	market proxy.

	Returns the number of tickers successfully updated.
	"""
	if not KPI_FILE.exists():
		if verbose:
			logger.warning(f"KPI file not found: {KPI_FILE} — skipping abnormal_return refresh")
		return 0

	kpi_rows = read_csv(KPI_FILE)
	if not kpi_rows:
		return 0

	tickers = [r["ticker"] for r in kpi_rows if r.get("ticker")]
	if not tickers:
		return 0

	if verbose:
		logger.info(f"Refreshing abnormal_return for {len(tickers)} tickers ...")

	# Fetch 5-day history for SPY (market benchmark) + all universe tickers
	all_syms = ["SPY"] + tickers
	try:
		raw = yf.download(
			all_syms,
			period="5d",
			auto_adjust=True,
			progress=False,
			threads=True,
		)
		# Handle both single and multi-ticker response shapes
		if "Close" in raw.columns.get_level_values(0) if hasattr(raw.columns, 'get_level_values') else False:
			closes = raw["Close"]
		else:
			closes = raw
	except Exception as e:
		logger.error(f"yfinance download failed: {e}")
		return 0

	# Get SPY's return for the most recent complete trading day
	try:
		spy_closes = closes["SPY"].dropna()
		if len(spy_closes) < 2:
			if verbose:
				logger.warning("Insufficient SPY history — skipping abnormal_return refresh")
			return 0
		spy_ret = (spy_closes.iloc[-1] - spy_closes.iloc[-2]) / spy_closes.iloc[-2]
	except Exception as e:
		logger.error(f"SPY return computation failed: {e}")
		return 0

	# Build beta lookup from KPI rows
	beta_lookup = {}
	for r in kpi_rows:
		try:
			beta_lookup[r["ticker"]] = float(r.get("beta") or 1.0)
		except (ValueError, TypeError):
			beta_lookup[r["ticker"]] = 1.0

	# Compute abnormal returns for each ticker
	ab_map: dict[str, float] = {}
	for sym in tickers:
		try:
			sym_closes = closes[sym].dropna()
			if len(sym_closes) < 2:
				continue
			stock_ret = (sym_closes.iloc[-1] - sym_closes.iloc[-2]) / sym_closes.iloc[-2]
			beta_val = beta_lookup.get(sym, 1.0)
			ab_map[sym] = round(float(stock_ret - beta_val * spy_ret), 4)
		except Exception as e:
			logger.debug(f"Could not compute abnormal_return for {sym}: {e}")
			continue

	if not ab_map:
		if verbose:
			logger.warning("No abnormal returns computed")
		return 0

	# Patch the KPI rows with fresh abnormal_return values
	updated = 0
	fieldnames = list(kpi_rows[0].keys()) if kpi_rows else []
	if "abnormal_return" not in fieldnames:
		fieldnames.append("abnormal_return")

	for r in kpi_rows:
		sym = r.get("ticker", "")
		if sym in ab_map:
			r["abnormal_return"] = ab_map[sym]
			updated += 1

	write_csv(KPI_FILE, kpi_rows, fieldnames)

	if verbose:
		logger.info(f"abnormal_return refreshed for {updated}/{len(tickers)} tickers "
					f"(SPY today: {spy_ret*100:+.2f}%)")
	return updated

def collect_tickers(run_ids: list) -> set:
	tickers = set()
	for sid in run_ids:
		for h in read_holdings(sid):
			tickers.add(h["ticker"])
	return tickers

# ── Improved quote fetching with better fallback handling ─────────────────
@retry(max_attempts=2, backoff=1.0)
def fetch_quotes(tickers: set, stale_threshold: float = DEFAULT_STALE_THRESHOLD) -> tuple[dict, dict]:
	"""
	Returns tuple of (prices, previous_closes) dicts.
	prices: {ticker: current_price}
	previous_closes: {ticker: prev_close} for fallback
	
	FIX: Now also fetches previous close as a fallback when current price is stale.
	"""
	if not tickers:
		return {}, {}

	ticker_list = sorted(tickers)
	logger.info(f"Fetching quotes for {len(ticker_list)} ticker(s) via Yahoo Finance ...")

	prices = {}
	previous_closes = {}
	failed = []
	stale_warnings = []

	try:
		# Batch download with 2 days of history to get previous close
		data = yf.download(
			ticker_list,
			period="2d",
			auto_adjust=True,
			progress=False,
			threads=True,
		)
		
		# Handle the data structure
		if data.empty:
			logger.error("No data returned from yfinance")
			return {}, {}

		if 'pd' not in globals():
			logger.error("Pandas is required for MultiIndex checks.")
			return {}, {}

		# Get the most recent close and previous close for each ticker
		if hasattr(data.columns, 'get_level_values') and isinstance(data.columns, pd.MultiIndex):
			# Multi-ticker case
			for sym in ticker_list:
				try:
					if "Close" in data.columns.get_level_values(0):
						close_data = data["Close"][sym].dropna()
					else:
						close_data = data[sym]["Close"].dropna() if "Close" in data[sym].columns else data[sym].dropna()
					
					if len(close_data) >= 2:
						previous_closes[sym] = round(float(close_data.iloc[-2]), 4)
						current_price = round(float(close_data.iloc[-1]), 4)
						
						if current_price > 0:
							prices[sym] = current_price
						else:
							failed.append(sym)
					elif len(close_data) == 1:
						previous_closes[sym] = round(float(close_data.iloc[-1]), 4)
						failed.append(sym)
					else:
						failed.append(sym)
				except Exception as e:
					logger.debug(f"Error processing {sym}: {e}")
					failed.append(sym)
		else:
			# Single ticker or unexpected format
			for sym in ticker_list:
				try:
					close_data = data["Close"] if "Close" in data.columns else data
					if len(close_data) >= 2:
						previous_closes[sym] = round(float(close_data.iloc[-2]), 4)
						current_price = round(float(close_data.iloc[-1]), 4)
						if current_price > 0:
							prices[sym] = current_price
						else:
							failed.append(sym)
					else:
						failed.append(sym)
				except Exception:
					failed.append(sym)
	
	except Exception as e:
		logger.error(f"Batch download failed: {e}")
		# Fall back to individual ticker fetching
		logger.info("Falling back to individual ticker fetching...")
		for sym in ticker_list:
			try:
				ticker = yf.Ticker(sym)
				hist = ticker.history(period="2d")
				if not hist.empty:
					if len(hist) >= 2:
						previous_closes[sym] = round(float(hist["Close"].iloc[-2]), 4)
						current_price = round(float(hist["Close"].iloc[-1]), 4)
						if current_price > 0:
							prices[sym] = current_price
						else:
							failed.append(sym)
					else:
						failed.append(sym)
				else:
					failed.append(sym)
				time.sleep(0.1)  # Be gentle with rate limits
			except Exception as e:
				logger.debug(f"Could not fetch {sym}: {e}")
				failed.append(sym)

	# Log failures
	if failed:
		logger.warning(f"No quote returned for: {', '.join(failed[:10])}")
		if len(failed) > 10:
			logger.warning(f"... and {len(failed) - 10} more")

	return prices, previous_closes

# ── Update one strategy with stale-price warnings ────────────────────────
def update_strategy(sid: str, prices: dict, previous_closes: dict, today: str, 
					show: bool, stale_threshold: float) -> dict | None:
	"""
	Update a single strategy's holdings with current prices.
	FIX: Now uses previous_close as fallback and warns about stale prices.
	"""
	acct = read_account(sid)
	if acct is None:
		logger.warning(f"[{sid}] No account file — skipped (run --init first)")
		return None

	holdings = read_holdings(sid)
	if not holdings:
		acct["holdings_value"] = 0.0
		acct["total"] = round(acct["cash"], 2)
		save_account(sid, acct)
		return acct

	# Update each holding with the latest price
	hv = 0.0
	stale_positions = []
	
	for h in holdings:
		sym = h["ticker"]
		shares = h["shares"]
		avg_cost = h["avg_cost"]
		
		# Try to get current price, fall back to previous close, then avg_cost
		price = prices.get(sym)
		price_source = "current"
		
		if price is None:
			price = previous_closes.get(sym)
			price_source = "prev_close"
			
		if price is None:
			price = avg_cost
			price_source = "avg_cost"
			logger.warning(f"[{sid}] {sym}: No quote or prev_close available — using avg_cost ${avg_cost:.2f}")

		mv = round(shares * price, 2)
		hv += mv
		
		# Check for stale prices (large deviation from avg_cost when using fallback)
		if price_source != "current" and avg_cost > 0:
			pct_diff = abs(price - avg_cost) / avg_cost * 100
			if pct_diff > stale_threshold:
				stale_positions.append({
					"ticker": sym,
					"price": price,
					"avg_cost": avg_cost,
					"pct_diff": pct_diff,
					"source": price_source
				})
		
		if show:
			pnl = price - avg_cost
			pct = (pnl / avg_cost * 100) if avg_cost else 0.0
			sign = "+" if pnl >= 0 else ""
			source_flag = {
				"current": "",
				"prev_close": " (prev close)",
				"avg_cost": " (using avg_cost - STALE!)"
			}.get(price_source, "")
			
			logger.info(f"	{sym:<6}  {shares:>8.4f} sh  "
						f"cost ${avg_cost:>8.2f}  now ${price:>8.2f}  "
						f"MV ${mv:>9.2f}  P&L {sign}{pct:.1f}%{source_flag}")

	# Log stale price warnings
	if stale_positions:
		logger.warning(f"[{sid}] Stale price warnings ({len(stale_positions)} positions):")
		for sp in stale_positions[:5]:
			logger.warning(f"	{sp['ticker']}: price ${sp['price']:.2f} is {sp['pct_diff']:.1f}% "
						  f"from avg_cost ${sp['avg_cost']:.2f} (using {sp['source']})")
		if len(stale_positions) > 5:
			logger.warning(f"	... and {len(stale_positions) - 5} more")

	acct["holdings_value"] = round(hv, 2)
	acct["total"] = round(acct["cash"] + hv, 2)
	save_account(sid, acct)
	save_holdings(sid, holdings)
	return acct

# ── Rebuild leaderboard ───────────────────────────────────────────────────────
def update_leaderboard(today: str, run_ids: list, all_ids: list) -> list:
	"""Re-reads every strategy account and rewrites leaderboard.csv."""
	rows = []
	for sid, name, style, risk in all_ids:
		acct = read_account(sid)
		if acct is None:
			continue
		total = acct["cash"] + acct["holdings_value"]
		pnl = total - STARTING_CASH
		pct = (total / STARTING_CASH - 1) * 100 if STARTING_CASH else 0.0
		rows.append({
			"rank": 0,
			"date": today,
			"strategy_id": sid,
			"strategy_name": name,
			"style": style,
			"risk": risk,
			"cash": round(acct["cash"], 2),
			"holdings_value": round(acct["holdings_value"], 2),
			"total": round(total, 2),
			"pnl": round(pnl, 2),
			"pct_return": round(pct, 2),
			"trades": acct["trades"],
		})
	rows.sort(key=lambda x: -x["total"])
	for i, r in enumerate(rows, 1):
		r["rank"] = i
	write_csv(leader_file(), rows,
			  ["rank", "date", "strategy_id", "strategy_name", "style", "risk",
			   "cash", "holdings_value", "total", "pnl", "pct_return", "trades"])
	return rows

# ── Pretty leaderboard print (ASCII only) ─────────────────────────────────────
def print_leaderboard(lb: list):
	logger.info(f"\n  {'Rk':<4} {'ID':<4} {'Strategy':<35} {'Cash':>9} {'Holdings':>10} "
				f"{'Total':>9} {'P&L':>9} {'Return':>8}")
	logger.info("  " + "-" * 95)
	for r in lb:
		sign = "+" if r["pnl"] >= 0 else ""
		logger.info(f"  {r['rank']:<4} {r['strategy_id']:<4} {r['strategy_name'][:35]:<35} "
					f"${r['cash']:>8.2f} ${r['holdings_value']:>9.2f} "
					f"${r['total']:>8.2f} {sign}${r['pnl']:>8.2f} "
					f"{sign}{r['pct_return']:>6.2f}%")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
	parser = argparse.ArgumentParser(
		description="Fetch live quotes and update holdings values for all strategies")
	parser.add_argument("--strategy", type=str,
						help="Update only this strategy ID, e.g. 03")
	parser.add_argument("--show", action="store_true",
						help="Print per-position detail after updating")
	parser.add_argument("--no-kpi-refresh", action="store_true",
						help="Skip the abnormal_return refresh in equity_kpi_results.csv")
	parser.add_argument("--stale-threshold", type=float, default=DEFAULT_STALE_THRESHOLD,
						help=f"Warn if price deviates more than X percent from avg_cost (default: {DEFAULT_STALE_THRESHOLD})")
	parser.add_argument("--verbose", action="store_true",
						help="Enable verbose debug output")
	args = parser.parse_args()

	if args.verbose:
		logging.getLogger().setLevel(logging.DEBUG)

	today = date.today().isoformat()
	now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

	logger.info("\n" + "=" * 65)
	logger.info("  PORTFOLIO QUOTE UPDATER")
	logger.info(f"  {now}")
	logger.info("=" * 65)

	all_ids = [(s[0], s[1], s[2], s[3]) for s in STRATEGIES]
	run_ids = [args.strategy] if args.strategy else [s[0] for s in STRATEGIES]

	# 1. Refresh abnormal_return in KPI file
	if not args.no_kpi_refresh:
		refresh_abnormal_returns(verbose=True)

	# 2. Collect all tickers we need prices for
	tickers = collect_tickers(run_ids)
	if not tickers:
		logger.info("\n  No open positions found — nothing to update.")
		logger.info("  (Run strategy_runner.py to open positions first.)\n")
		return

	# 3. Fetch quotes
	prices, previous_closes = fetch_quotes(tickers, args.stale_threshold)
	if not prices and not previous_closes:
		logger.error("\n  ERROR: Could not fetch any quotes. Check internet connection.\n")
		return

	fetched_at = datetime.now().strftime("%H:%M:%S")
	price_count = len(prices)
	fallback_count = len([t for t in tickers if t not in prices and t in previous_closes])
	
	logger.info(f"  Got {price_count} current prices, {fallback_count} fallback (prev close) "
				f"for {len(tickers)} tickers  (as of {fetched_at})")
	
	if fallback_count > 0:
		logger.warning(f"  {fallback_count} ticker(s) using previous close instead of current price")

	# 4. Update each strategy
	logger.info("")
	updated = 0
	for sid in run_ids:
		name = next((s[1] for s in STRATEGIES if s[0] == sid), sid)
		logger.info(f"  [{sid}] {name}")
		acct = update_strategy(sid, prices, previous_closes, today, 
							   show=args.show, stale_threshold=args.stale_threshold)
		if acct:
			pnl = acct["total"] - STARTING_CASH
			sign = "+" if pnl >= 0 else ""
			logger.info(f"	   Cash ${acct['cash']:>9.2f}  "
						f"Holdings ${acct['holdings_value']:>9.2f}  "
						f"Total ${acct['total']:>9.2f}  "
						f"P&L {sign}${pnl:.2f}")
			updated += 1

	# 5. Rebuild leaderboard
	lb = update_leaderboard(today, run_ids, all_ids)

	logger.info("\n" + "-" * 65)
	logger.info("  LEADERBOARD  (all strategies, ranked by total value)")
	logger.info("-" * 65)
	print_leaderboard(lb)
	logger.info("-" * 65)
	logger.info(f"\n  Updated {updated} strategy account(s).")
	logger.info(f"  Leaderboard saved -> {leader_file().name}")  # FIX: replaced arrow with ->
	logger.info(f"  Prices as of: {fetched_at}")
	if fallback_count > 0:
		logger.warning(f"  Note: {fallback_count} ticker(s) using previous close due to no current quote")
	logger.info("=" * 65 + "\n")


if __name__ == "__main__":
	main()
# -*- coding: utf-8 -*-
"""
update_quotes.py -- Fetch live quotes for all held positions and refresh
				   every strategy's holdings_value, account total, and
				   the leaderboard WITHOUT triggering any buy/sell logic.

Also recomputes abnormal_return for ALL tickers in the equity_kpi MySQL table
using today's live prices vs yesterday's close and SPY as the market proxy.
This fixes the data-freshness gap where strategy_runner.py (running at 4:30 PM)
was using an abnormal_return computed from the morning KPI run -- 8+ hours stale
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
import argparse
import time
from datetime import date, datetime
from pathlib import Path
from functools import wraps

# -- Fix Windows console encoding ----------------------------------------------
if sys.platform == 'win32':
	import io
	sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
	sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# -- Configure logging ----------------------------------------------------------
# db_logger writes structured rows to the run_logs MySQL table and echoes to
# stdout. The old quote_update.log FileHandler is removed.
# yfinance and peewee are silenced at WARNING to eliminate the high-volume
# per-request DEBUG traces that bloated quote_update.log to 1.2 MB per run.
import logging
logging.getLogger("yfinance").setLevel(logging.WARNING)
logging.getLogger("peewee").setLevel(logging.WARNING)

from db_logger import Logger as _DbLogger
logger = _DbLogger(run_stage="quotes", echo=True)

# -- UTF-8 output (Windows) ----------------------------------------------------
if hasattr(sys.stdout, "reconfigure"):
	sys.stdout.reconfigure(encoding="utf-8")

try:
	import yfinance as yf
except ImportError:
	sys.exit("ERROR: yfinance not installed.  Run:  pip install yfinance")

try:
	import pandas as pd
	import numpy as np
except ImportError:
	sys.exit("ERROR: pandas/numpy not installed.  Run:  pip install pandas numpy")

# -- Constants -- imported from single source of truth -------------------------
from constants import STARTING_CASH, ACCOUNT_NUM, STRATEGIES
from db import (
    init_schema,
    get_connection as _db_get_connection,
    read_account   as _db_read_account,
    save_account   as _db_save_account,
    read_holdings  as _db_read_holdings,
    save_holdings  as _db_save_holdings,
    write_leaderboard as _db_write_leaderboard,
    update_kpi_abnormal_returns as _db_update_kpi_ab,
    read_kpi_rows  as _db_read_kpi_rows,
    write_kpi_rows as _db_write_kpi_rows,
)

# Warn if fallback price deviates more than this % from avg_cost
DEFAULT_STALE_THRESHOLD = 5.0  # percent

# -- Retry decorator for yfinance calls ----------------------------------------
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

# -- Data-layer helpers (MySQL via db.py) --------------------------------------

def read_account(sid: str) -> dict | None:
	return _db_read_account(sid) or None

def save_account(sid: str, acct: dict):
	_db_save_account(sid, acct)

def read_holdings(sid: str) -> list:
	return _db_read_holdings(sid)

def save_holdings(sid: str, holdings: list):
	_db_save_holdings(sid, holdings)

# -- Improved abnormal_return refresh with better error handling ----------
@retry(max_attempts=2, backoff=1.0)
def refresh_abnormal_returns(verbose: bool = True) -> int:
	"""
	Recompute abnormal_return for every ticker in the equity_kpi table
	using today's live intraday move vs yesterday's close, with SPY as the
	market proxy.

	Returns the number of tickers successfully updated.
	"""
	try:
		kpi_rows = _db_read_kpi_rows()
	except Exception as e:
		if verbose:
			logger.warning(f"Could not read KPI from database: {e}")
		return 0
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
				logger.warning("Insufficient SPY history -- skipping abnormal_return refresh")
			return 0
		spy_ret = (spy_closes.iloc[-1] - spy_closes.iloc[-2]) / spy_closes.iloc[-2]
		if not np.isfinite(spy_ret):
			logger.error(f"SPY return is non-finite ({spy_ret}) — skipping abnormal_return refresh")
			return 0
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
			prev_close = sym_closes.iloc[-2]
			if prev_close <= 0:
				logger.debug(f"Skipping {sym}: previous close is {prev_close}")
				continue
			stock_ret = (sym_closes.iloc[-1] - prev_close) / prev_close
			if not np.isfinite(stock_ret):
				logger.debug(f"Skipping {sym}: non-finite stock return {stock_ret}")
				continue
			beta_val = beta_lookup.get(sym, 1.0)
			ab_map[sym] = round(float(stock_ret - beta_val * spy_ret), 4)
		except Exception as e:
			logger.debug(f"Could not compute abnormal_return for {sym}: {e}")
			continue

	if not ab_map:
		if verbose:
			logger.warning("No abnormal returns computed")
		return 0

	# Patch abnormal_return for updated tickers only (fast targeted UPDATE)
	updated = len(ab_map)
	_db_update_kpi_ab(ab_map)

	if verbose:
		logger.info(f"abnormal_return refreshed for {updated}/{len(tickers)} tickers "
					f"(SPY today: {spy_ret*100:+.2f}%)")
	return updated

def collect_tickers(run_ids: list) -> set:
	"""Return all tickers held by the given strategy IDs. Single DB query."""
	if not run_ids:
		return set()
	conn = _db_get_connection()
	try:
		cur = conn.cursor()
		placeholders = ",".join(["%s"] * len(run_ids))
		cur.execute(
			f"SELECT DISTINCT ticker FROM holdings WHERE strategy_id IN ({placeholders})",
			run_ids,
		)
		return {row[0] for row in cur.fetchall()}
	finally:
		conn.close()

# -- Improved quote fetching with better fallback handling -----------------
@retry(max_attempts=2, backoff=1.0)
def fetch_quotes(tickers: set, stale_threshold: float = DEFAULT_STALE_THRESHOLD) -> tuple[dict, dict]:
	"""
	Returns tuple of (prices, previous_closes) dicts.
	prices: {ticker: current_price}
	previous_closes: {ticker: prev_close} for fallback
	
	"""
	if not tickers:
		return {}, {}

	ticker_list = sorted(tickers)
	logger.info(f"Fetching quotes for {len(ticker_list)} ticker(s) via Yahoo Finance ...")

	prices = {}
	previous_closes = {}
	failed = []

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

# -- Update one strategy with stale-price warnings ------------------------
def update_strategy(sid: str, prices: dict, previous_closes: dict, today: str, 
					show: bool, stale_threshold: float) -> dict | None:
	"""
	Update a single strategy's holdings with current prices.
	"""
	acct = read_account(sid)
	if acct is None:
		logger.warning(f"[{sid}] No account file -- skipped (run --init first)")
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
			logger.warning(f"[{sid}] {sym}: No quote or prev_close available -- using avg_cost ${avg_cost:.2f}")

		if price <= 0:
			logger.error(f"[{sid}] {sym}: price is {price} (zero or negative) -- skipping MV, possible data corruption")
			continue

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
	# Note: holdings themselves (cost basis, shares) are not modified here --
	# only the account's holdings_value total is updated.
	return acct

# -- Rebuild leaderboard -------------------------------------------------------
def update_leaderboard(today: str, run_ids: list, all_ids: list) -> list:
	"""Re-reads every strategy account and upserts leaderboard rows in MySQL.
	Uses a single connection to fetch all accounts at once."""
	conn = _db_get_connection()
	try:
		cur = conn.cursor(dictionary=True)
		cur.execute("SELECT * FROM accounts")
		acct_map = {}
		for r in cur.fetchall():
			acct_map[r["strategy_id"]] = {
				"cash":           float(r["cash"]),
				"holdings_value": float(r["holdings_value"]),
				"total":          float(r["total"]),
				"trades":         int(r["trades"]),
			}
	finally:
		conn.close()

	rows = []
	for sid, name, style, risk, *_ in all_ids:
		acct = acct_map.get(sid)
		if acct is None:
			# Account missing from DB -- show as $0 rather than silently skip
			acct = {"cash": 0.0, "holdings_value": 0.0, "total": 0.0, "trades": 0}
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
	_db_write_leaderboard(rows)
	return rows

# -- Pretty leaderboard print (ASCII only) -------------------------------------
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

# -- Main ----------------------------------------------------------------------
def main():
	parser = argparse.ArgumentParser(
		description="Fetch live quotes and update holdings values for all strategies")
	parser.add_argument("--strategy", type=str,
						help="Update only this strategy ID, e.g. 03")
	parser.add_argument("--show", action="store_true",
						help="Print per-position detail after updating")
	parser.add_argument("--no-kpi-refresh", action="store_true",
						help="Skip the abnormal_return refresh in the equity_kpi table")
	parser.add_argument("--stale-threshold", type=float, default=DEFAULT_STALE_THRESHOLD,
						help=f"Warn if price deviates more than X percent from avg_cost (default: {DEFAULT_STALE_THRESHOLD})")
	parser.add_argument("--verbose", action="store_true",
						help="Enable verbose debug output")
	args = parser.parse_args()

	if args.verbose:
		# Target only this module's logger — raising the root level would
		# re-enable the yfinance/peewee DEBUG noise suppressed at module load.
		logging.getLogger(__name__).setLevel(logging.DEBUG)

	today = date.today().isoformat()
	now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

	logger.info("\n" + "=" * 65)
	logger.info("  PORTFOLIO QUOTE UPDATER")
	logger.info(f"  {now}")
	logger.info("=" * 65)

	init_schema()  # ensure MySQL tables exist
	all_ids = [(s[0], s[1], s[2], s[3]) for s in STRATEGIES]
	if args.strategy:
		valid_ids = {s[0] for s in STRATEGIES}
		if args.strategy not in valid_ids:
			logger.error(f"Unknown strategy ID '{args.strategy}'. Valid IDs: {sorted(valid_ids)}")
			logger.flush()
			sys.exit(1)
		run_ids = [args.strategy]
	else:
		run_ids = [s[0] for s in STRATEGIES]

	# 1. Refresh abnormal_return in KPI file
	if not args.no_kpi_refresh:
		refresh_abnormal_returns(verbose=True)

	# 2. Collect all tickers we need prices for
	tickers = collect_tickers(run_ids)
	if not tickers:
		logger.info("\n  No open positions found -- nothing to update.")
		logger.info("  (Run strategy_runner.py to open positions first.)\n")
		logger.flush()
		return

	# 3. Fetch quotes
	prices, previous_closes = fetch_quotes(tickers, args.stale_threshold)
	if not prices and not previous_closes:
		logger.error("\n  ERROR: Could not fetch any quotes. Check internet connection.\n")
		logger.flush()
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
	logger.info("  Leaderboard saved -> MySQL (leaderboard table)")
	logger.info(f"  Prices as of: {fetched_at}")
	if fallback_count > 0:
		logger.warning(f"  Note: {fallback_count} ticker(s) using previous close due to no current quote")
	logger.info("=" * 65 + "\n")
	logger.flush()


if __name__ == "__main__":
	main()
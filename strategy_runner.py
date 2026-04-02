# -*- coding: utf-8 -*-
"""
strategy_runner.py  --  Multi-strategy paper trading engine
Runs 20 active strategies + 1 passive benchmark concurrently.
Each strategy has its own account, holdings, and transactions in MySQL.
Produces a daily leaderboard stored in MySQL.

Usage:
	python strategy_runner.py				  # run all strategies
	python strategy_runner.py --dry-run		# preview decisions only
	python strategy_runner.py --strategy 02	# run one strategy only
	python strategy_runner.py --init		   # initialise all account files fresh

Active strategies: 02, 03, 06, 07, 08, 09, 10, 11, 12, 13, 14, 18, 19, 20, 21
"""

import sys
import os
import re
import json
import argparse
import urllib.error
import urllib.request
import time
from datetime import date, datetime
from pathlib import Path
from collections import defaultdict
from functools import wraps
from typing import Dict, List, Tuple, Optional, Any

# -- Configure logging ----------------------------------------------------------
# db_logger writes structured rows to the run_logs MySQL table and echoes to
# stdout. The old trading.log FileHandler is removed -- all log output now
# goes to MySQL (with automatic stderr fallback if the DB is unreachable).
from db_logger import Logger as _DbLogger
logger = _DbLogger(run_stage="full", echo=True)

# -- UTF-8 output (Windows fix) ----------------------------------------------
if hasattr(sys.stdout, "reconfigure"):
	sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
	sys.stderr.reconfigure(encoding="utf-8")

# -- Load .env if present ----------------------------------------------------
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
	for line in _env_path.read_text(encoding="utf-8").splitlines():
		line = line.strip()
		if line and not line.startswith("#") and "=" in line:
			k, v = line.split("=", 1)
			v = v.strip().strip('"').strip("'")
			os.environ.setdefault(k.strip(), v)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
BASE_DIR = Path(__file__).parent   # must stay local -- relative to this file

# -- Shared constants (single source of truth) --------------------------------
# All scripts should import from constants.py -- do not define these inline.
from constants import (
	# engine parameters
	STARTING_CASH, COMMISSION, SPREAD_PCT, ACCOUNT_NUM,
	MIN_HOLD_DAYS, STOP_LOSS_PCT, MODEL,
	# freshness thresholds
	KPI_WARN_HOURS, KPI_WARN_HOURS_PEAD,
	# strategy registry & maps
	HOLD_PERIODS, STRATEGIES, MACRO_THEME_MAP,
)
from db import (
	init_schema,
	get_connection as _db_get_connection,
	read_account   as _db_read_account,
	save_account   as _db_save_account,
	read_holdings  as _db_read_holdings,
	save_holdings  as _db_save_holdings,
	append_txn     as _db_append_txn,
	write_leaderboard          as _db_write_leaderboard,
	read_leaderboard_history   as _db_read_lb_history,
	append_leaderboard_history as _db_append_lb_history,
	read_kpi_rows              as _db_read_kpi_rows,
	write_news_macro_cache     as _db_write_news_macro,
	read_news_macro_cache      as _db_read_news_macro,
	read_news_macro_cache_latest as _db_read_news_macro_latest,
)

# -- Retry decorator for API calls --------------------------------------------
def retry(max_attempts: int = 3, backoff: float = 2.0, exceptions: tuple = (Exception,)):
	"""
	Retry decorator with exponential backoff for network operations.
	
	Args:
		max_attempts: Maximum number of retry attempts
		backoff: Initial backoff time in seconds (doubles each attempt)
		exceptions: Tuple of exceptions to catch and retry
	"""
	def decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			last_exception = None
			for attempt in range(max_attempts):
				try:
					return func(*args, **kwargs)
				except exceptions as e:
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

# NEWS CONFIG
NEWS_FEEDS = [
	"https://feeds.content.reuters.com/reuters/businessNews",
	"https://feeds.a.dj.com/rss/WSJBusiness.xml",
	"https://search.cnbc.com/rs/search/view.xml?partnerId=2000&keywords=stock%20market",
]

# -- Data-layer helpers (MySQL via db.py) -------------------------------------

# Module-level cache for KPI data age — computed once at startup by
# check_data_freshness() and reused by the S07 PEAD guard per strategy run.
_kpi_age_hours: float | None = None


def check_data_freshness():
	"""Warn if KPI data is stale. Checks MAX(updated_at) in the equity_kpi table."""
	try:
		conn = _db_get_connection()
		try:
			cur = conn.cursor()
			cur.execute("SELECT MAX(updated_at) FROM equity_kpi")
			row = cur.fetchone()
			if row and row[0]:
				global _kpi_age_hours
				age_hours = (datetime.now() - row[0]).total_seconds() / 3600
				_kpi_age_hours = age_hours
				if age_hours > KPI_WARN_HOURS_PEAD:
					logger.warning(
						f"KPI data is {age_hours:.1f}h old (threshold: {KPI_WARN_HOURS_PEAD}h). "
						f"S07 (PEAD) requires fresh abnormal-return data -- consider re-running "
						f"equity_kpi_analyzer.py before trading."
					)
				elif age_hours > KPI_WARN_HOURS:
					logger.warning(f"KPI data is {age_hours:.1f}h old -- may be slightly stale.")
		finally:
			conn.close()
	except Exception as e:
		logger.debug(f"check_data_freshness: {e}")

def read_account(sid: str) -> Dict:
	acct = _db_read_account(sid)
	if not acct:
		return {"account": ACCOUNT_NUM, "strategy_id": sid,
				"cash": STARTING_CASH, "holdings_value": 0.0, "total": STARTING_CASH,
				"start_date": date.today().isoformat(), "trades": 0}
	return acct

def save_account(sid: str, acct: Dict):
	_db_save_account(sid, acct)

def read_holdings(sid: str) -> List[Dict]:
	return _db_read_holdings(sid)

def save_holdings(sid: str, holdings: List[Dict]):
	_db_save_holdings(sid, holdings)

def append_txn(sid: str, txn: Dict):
	_db_append_txn(sid, txn)

# -- Holdings value helper (FIXED: handle 0.0 price correctly) -----------------
def calc_holdings_value(holdings: List[Dict], kmap: Dict) -> float:
	"""Compute total holdings market value using current prices."""
	total = 0.0
	for h in holdings:
		price_data = kmap.get(h["ticker"], {}).get("current_price")
		# Check for None explicitly, not falsy (0.0 is a valid price)
		if price_data is None:
			price = float(h["avg_cost"])
		else:
			price = price_data
		total += h["shares"] * price
	return round(total, 2)

# -- KPI data loader -----------------------------------------------------------

def load_kpi() -> Tuple[List[Dict], Dict]:
	"""Load KPI data from MySQL."""
	try:
		rows = _db_read_kpi_rows()
	except Exception as e:
		logger.warning(f"Could not load KPI from database: {e}")
		return [], {}
	if not rows:
		logger.warning("equity_kpi table is empty -- run equity_kpi_analyzer.py first.")
		return [], {}
	kmap = {r["ticker"]: r for r in rows}
	return rows, kmap

def enforce_stop_losses(sid: str, holdings: List[Dict], kmap: Dict, today: str,
						dry_run: bool = False) -> List[Dict]:
	"""
	Hard stop-loss executed in code BEFORE Claude is called.
	FIX: Now collects all stop-loss candidates first, then executes them
	to avoid race conditions where the holdings list changes mid-loop.
	"""
	to_sell = []
	for h in holdings:
		ticker	= h["ticker"]
		avg_cost  = float(h["avg_cost"])
		_raw_price = kmap.get(ticker, {}).get("current_price")
		if _raw_price is None:
			logger.warning(f"[STOP-LOSS] {ticker}: no market price in KPI data -- "
						   f"stop-loss check skipped (using avg_cost as fallback)")
			price = avg_cost
		else:
			price = _raw_price
		loss_pct  = (price - avg_cost) / avg_cost if avg_cost else 0.0

		if loss_pct <= -STOP_LOSS_PCT:
			reason = (f"Hard stop-loss: price ${price:.2f} is "
					  f"{loss_pct*100:.1f}% below avg cost ${avg_cost:.2f}")
			to_sell.append((ticker, price, reason))
			logger.info(f"[STOP-LOSS] {ticker}: {reason}")

	# Execute all stop-losses after collecting them
	for ticker, price, reason in to_sell:
		ok, msg = sell(sid, ticker, price, reason, today, kmap, dry_run)
		logger.info(f"SELL {ticker}: {msg}")

	if to_sell:
		holdings = read_holdings(sid)   # reload once after all stops
	return holdings

def buy(sid: str, ticker: str, price: float, cash_available: float, reason: str,
		today: str, kmap: Dict, dry_run: bool = False) -> Tuple[bool, str]:
	"""Buy as many shares as cash allows (up to 60% of total account)."""
	# Re-read account to ensure fresh data
	acct = read_account(sid)
	holdings = read_holdings(sid)
	total = acct["cash"] + acct["holdings_value"]
	max_pos = total * 0.60
	existing = next((h for h in holdings if h["ticker"] == ticker), None)
	
	if existing:
		_raw_price = kmap.get(ticker, {}).get("current_price")
		cur_price = _raw_price if _raw_price is not None else float(existing["avg_cost"])
		already_invested = existing["shares"] * cur_price
	else:
		already_invested = 0
	
	spend = min(cash_available * 0.90, max_pos - already_invested)
	spend = max(spend, 0)
	
	if spend < price + COMMISSION:
		return False, "Insufficient funds"
	
	shares = round((spend - COMMISSION) / (price * (1 + SPREAD_PCT)), 6)
	net_cost = round(shares * price * (1 + SPREAD_PCT) + COMMISSION, 4)

	if dry_run:
		logger.info(f"[DRY-RUN] BUY {shares:.4f} x {ticker} @ ${price:.2f} cost=${net_cost:.2f}")
		return True, "dry-run"

	# Verify cash is still sufficient (single authoritative read at top of function)
	if acct["cash"] < net_cost:
		return False, f"Insufficient funds: ${acct['cash']:.2f} < ${net_cost:.2f}"
	
	acct["cash"] = round(acct["cash"] - net_cost, 4)
	acct["trades"] += 1
	
	if existing:
		new_total_shares = existing["shares"] + shares
		new_cost_basis = existing["cost_basis"] + net_cost
		existing["shares"] = round(new_total_shares, 6)
		existing["avg_cost"] = round(new_cost_basis / new_total_shares, 4)
		existing["cost_basis"] = round(new_cost_basis, 4)
	else:
		holdings.append({"ticker": ticker, "shares": round(shares, 6),
						 "avg_cost": round(net_cost / shares, 4),
						 "cost_basis": round(net_cost, 4),
						 "purchase_date": today, "strategy_id": sid})

	acct["holdings_value"] = calc_holdings_value(holdings, kmap)
	acct["total"] = round(acct["cash"] + acct["holdings_value"], 2)
	save_account(sid, acct)
	save_holdings(sid, holdings)
	append_txn(sid, {"date": today, "strategy_id": sid, "action": "BUY",
					 "ticker": ticker, "shares": round(shares, 6),
					 "price": round(price, 4), "commission": COMMISSION,
					 "net_amount": round(-net_cost, 4),
					 "cash_after": round(acct["cash"], 4), "reason": reason})
	return True, f"Bought {shares:.4f} shares @ ${price:.2f}"

def sell(sid: str, ticker: str, price: float, reason: str, today: str,
		 kmap: Dict, dry_run: bool = False) -> Tuple[bool, str]:
	"""Sell the full position in ticker."""
	holdings = read_holdings(sid)
	pos = next((h for h in holdings if h["ticker"] == ticker), None)
	if not pos:
		return False, "No position"

	# Per-strategy minimum hold period
	is_stop = "stop-loss" in reason.lower() or "stop loss" in reason.lower()
	hold_floor = HOLD_PERIODS.get(sid, MIN_HOLD_DAYS)
	if not is_stop:
		days_held = _trading_days_held(pos.get("purchase_date", today), today)
		if days_held < hold_floor:
			return False, (f"Min-hold period not met ({days_held}/{hold_floor} "
						   f"trading days) -- holding {ticker}")

	shares = pos["shares"]
	proceeds = round(shares * price * (1 - SPREAD_PCT) - COMMISSION, 4)
	if proceeds <= 0:
		return False, f"Position too small to sell after commission (proceeds=${proceeds:.2f})"
	
	if dry_run:
		logger.info(f"[DRY-RUN] SELL {shares:.4f} x {ticker} @ ${price:.2f} proceeds=${proceeds:.2f}")
		return True, "dry-run"
	
	acct = read_account(sid)
	acct["cash"] = round(acct["cash"] + proceeds, 4)
	acct["trades"] += 1
	holdings = [h for h in holdings if h["ticker"] != ticker]
	acct["holdings_value"] = calc_holdings_value(holdings, kmap)
	acct["total"] = round(acct["cash"] + acct["holdings_value"], 2)
	save_account(sid, acct)
	save_holdings(sid, holdings)
	append_txn(sid, {"date": today, "strategy_id": sid, "action": "SELL",
					 "ticker": ticker, "shares": round(shares, 6),
					 "price": round(price, 4), "commission": COMMISSION,
					 "net_amount": round(proceeds, 4),
					 "cash_after": round(acct["cash"], 4), "reason": reason})
	return True, f"Sold {shares:.4f} shares @ ${price:.2f}"

def _trading_days_held(purchase_date_str: str, today_str: str) -> int:
	"""Return number of weekdays between purchase_date and today."""
	try:
		d0 = date.fromisoformat(purchase_date_str)
		d1 = date.fromisoformat(today_str)
		delta = (d1 - d0).days
		if delta <= 0:
			return 0
		# O(1) formula: count full weeks * 5 + remaining weekdays
		full_weeks, remainder = divmod(delta, 7)
		# Count weekdays in the remaining partial week
		start_dow = d0.weekday()  # 0=Mon, 6=Sun
		extra = sum(1 for i in range(remainder) if (start_dow + i) % 7 < 5)
		weekdays = full_weeks * 5 + extra
		return weekdays
	except Exception as _e:
		logger.warning(f"_trading_days_held: could not parse dates ('{purchase_date_str}', '{today_str}'): {_e} -- defaulting to 999 (hold blocked)")
		return 999

# Holds up to PASSIVE_MAX_POSITIONS largest-cap stocks, equally weighted.
# 5 positions is a reasonable S&P 500 proxy without being as variable as 2.
PASSIVE_MAX_POSITIONS = 5

def run_passive(sid: str, rows: List[Dict], kmap: Dict, today: str, dry_run: bool = False):
	"""Passive benchmark: buy-and-hold top N stocks by market cap.
	Targets PASSIVE_MAX_POSITIONS equally-weighted positions. Never sells.
	"""
	acct = read_account(sid)
	holdings = read_holdings(sid)

	acct["holdings_value"] = calc_holdings_value(holdings, kmap)
	acct["total"] = round(acct["cash"] + acct["holdings_value"], 2)
	save_account(sid, acct)

	held       = {h["ticker"] for h in holdings}
	target_n   = PASSIVE_MAX_POSITIONS
	slots_open = target_n - len(holdings)

	if slots_open <= 0:
		logger.info(f"  [14] Passive: {target_n} positions full -- holding.")
		logger.info(f"       Cash: ${acct['cash']:>9.2f} | Holdings: ${acct['holdings_value']:>9.2f} | "
					f"Total: ${acct['total']:>9.2f} | Positions: {sorted(held)}")
		return

	# Spread remaining cash equally across open slots (with a small reserve)
	per_slot = (acct["cash"] * 0.95) / slots_open

	top_n = [t for t, _, _ in score_passive(rows, kmap)[:target_n * 2]
			 if t not in held]  # fetch extras in case some have no price

	bought = 0
	for ticker in top_n:
		if bought >= slots_open:
			break
		price = kmap.get(ticker, {}).get("current_price", 0)
		if price <= 0:
			continue
		if per_slot < price + COMMISSION:
			continue
		ok, msg = buy(sid, ticker, price, per_slot, "Passive: buy top market-cap", today, kmap, dry_run)
		logger.info(f"  [14] BUY {ticker}: {msg}")
		if ok:
			bought += 1
			acct     = read_account(sid)
			holdings = read_holdings(sid)

	logger.info(f"  [14] Cash: ${acct['cash']:>9.2f} | Holdings: ${acct['holdings_value']:>9.2f} | "
				f"Total: ${acct['total']:>9.2f} | Positions: {[h['ticker'] for h in holdings]}")

# -- Leaderboard ---------------------------------------------------------------
def update_leaderboard(today: str, kmap: Optional[Dict] = None) -> List[Dict]:
	"""
	Build and write the daily leaderboard to MySQL.
	Uses a single shared connection to read all accounts + holdings at once,
	rather than one connection per strategy, to minimise DB round-trips.
	"""
	acct_map: Dict[str, Dict] = {}
	holdings_map: Dict[str, List[Dict]] = {}
	conn = _db_get_connection()
	try:
		cur = conn.cursor(dictionary=True)

		# Fetch all accounts in one query
		cur.execute("SELECT * FROM accounts")
		for r in cur.fetchall():
			acct_map[r["strategy_id"]] = {
				"account":        r["account"],
				"strategy_id":    r["strategy_id"],
				"cash":           float(r["cash"]),
				"holdings_value": float(r["holdings_value"]),
				"total":          float(r["total"]),
				"start_date":     str(r["start_date"]),
				"trades":         int(r["trades"]),
			}

		# Fetch all holdings in one query
		cur.execute("SELECT * FROM holdings")
		for r in cur.fetchall():
			sid = r["strategy_id"]
			holdings_map.setdefault(sid, []).append({
				"ticker":        r["ticker"],
				"shares":        float(r["shares"]),
				"avg_cost":      float(r["avg_cost"]),
				"cost_basis":    float(r["cost_basis"]),
				"purchase_date": str(r["purchase_date"]),
				"strategy_id":   sid,
			})

	finally:
		conn.close()   # release connection before doing any heavy calculation

	# Refresh holdings values using kmap AFTER the connection is closed
	if kmap:
		update_params = []
		for sid in acct_map:
			acct = acct_map[sid]
			h = holdings_map.get(sid, [])
			if h:
				hv = calc_holdings_value(h, kmap)
				total = round(acct["cash"] + hv, 2)
				acct["holdings_value"] = hv
				acct["total"] = total
				update_params.append((hv, total, sid))
		if update_params:
			conn2 = _db_get_connection()
			try:
				cur2 = conn2.cursor()
				cur2.executemany(
					"UPDATE accounts SET holdings_value=%s, total=%s WHERE strategy_id=%s",
					update_params,
				)
				conn2.commit()
			except Exception:
				conn2.rollback()
				raise
			finally:
				conn2.close()

	rows = []
	for sid, name, style, risk, desc in STRATEGIES:
		acct = acct_map.get(sid)
		if not acct:
			continue
		total = acct["cash"] + acct["holdings_value"]
		pnl = total - STARTING_CASH
		pct = (total / STARTING_CASH - 1) * 100
		rows.append({
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
	for i, r in enumerate(rows):
		r["rank"] = i + 1

	_db_write_leaderboard(rows)
	return rows

def ask_claude(strategy_id: str, strategy_name: str, strategy_desc: str,
			   candidates: List[Tuple], holdings: List[Dict], acct: Dict,
			   today: str, kmap: Optional[Dict] = None) -> Tuple[Optional[Dict], Optional[str]]:
	"""Ask Claude to decide which of the top candidates to buy/sell."""
	if kmap is None:
		kmap = {}

	# Build holdings string with P&L
	hold_lines = []
	for h in holdings:
		avg_cost = float(h["avg_cost"])
		_raw_price = kmap.get(h["ticker"], {}).get("current_price")
		cur_price = _raw_price if _raw_price is not None else avg_cost
		unreal = (cur_price - avg_cost) * h["shares"]
		pnl_pct = (cur_price - avg_cost) / avg_cost * 100 if avg_cost else 0
		# Include RSI and MA signal so Claude can apply sell rules that depend on them
		# (e.g. S02: sell when RSI > 50; S12: sell when MA turns bearish)
		kd_held   = kmap.get(h['ticker'], {})
		rsi_held  = kd_held.get('rsi_14')
		ma_held   = kd_held.get('ma_signal', '?')
		rsi_s_held = f"{rsi_held:.0f}" if isinstance(rsi_held, float) else '?'
		ma_s_held  = 'Golden(ok)' if 'BULLISH' in str(ma_held) else ('Death(x)' if 'BEARISH' in str(ma_held) else str(ma_held))
		hold_lines.append(
			f"  {h['ticker']}: {h['shares']:.4f} sh  avg_cost ${avg_cost:.2f}  "
			f"now ${cur_price:.2f}  P&L ${unreal:+.2f} ({pnl_pct:+.1f}%)  "
			f"RSI={rsi_s_held}  MA={ma_s_held}  held since {h.get('purchase_date','?')}"
		)
	hold_str = "\n".join(hold_lines) or "  (none)"

	# Build candidates string with enriched KPIs.
	# Reason strings are truncated to keep the prompt compact -- the scorer
	# already ranked these; Claude only needs the signal, not the full detail.
	REASON_MAX = 80
	cand_lines = []
	for t, s, r in candidates[:10]:
		kd    = kmap.get(t, {})
		rsi   = kd.get("rsi_14", "?")
		ma_sig = kd.get("ma_signal", "?")
		pe    = kd.get("pe_ratio", "?")
		rsi_s = f"{rsi:.0f}" if isinstance(rsi, float) else str(rsi)
		pe_s  = f"{pe:.1f}"  if isinstance(pe,  float) else str(pe)
		ma_s  = "Golden(ok)" if "BULLISH" in str(ma_sig) else ("Death(x)" if "BEARISH" in str(ma_sig) else str(ma_sig))
		reason_short = (r[:REASON_MAX] + "...") if len(r) > REASON_MAX else r
		cand_lines.append(f"  {t}: score={s:.1f}  RSI={rsi_s}  MA={ma_s}  P/E={pe_s}  {reason_short}")
	cand_str = "\n".join(cand_lines)

	hold_floor = HOLD_PERIODS.get(strategy_id, MIN_HOLD_DAYS)

	prompt = f"""You are managing a paper trading account for strategy "{strategy_id} - {strategy_name}".

Strategy description: {strategy_desc}

Account:
  Cash available: ${acct['cash']:.2f}
  Holdings value: ${acct['holdings_value']:.2f}
  Total: ${acct['cash']+acct['holdings_value']:.2f}
  Goal: ${STARTING_CASH*2:.2f} (double the money)
  Trades so far: {acct['trades']}

Current holdings (with purchase date):
{hold_str}

Top-ranked candidates today (by strategy scoring):
{cand_str}

Rules:
- Commission is $1.00 per trade + 0.15% spread (larger for less-liquid names). Round-trip ~$2-4 -- a position needs ~0.4-0.8% gain just to break even.
- Max 3 positions at once; max 60% of portfolio in any one stock
- Keep at least 5% cash reserve
- MINIMUM HOLD: do NOT sell a position held fewer than {hold_floor} trading days
  (hard stop-losses are handled automatically before this prompt runs).
- SELL only if: score has genuinely deteriorated, MA has turned bearish (death cross), or a clearly better opportunity exists.
- HOLD is the correct action when signals are mixed or the position is young.

Respond ONLY with a JSON object. No explanation outside the JSON.
{{
  "actions": [
	{{"type": "BUY",  "ticker": "AAPL", "reason": "why"}},
	{{"type": "SELL", "ticker": "XYZ",  "reason": "why"}},
	{{"type": "HOLD", "ticker": "MU",   "reason": "why"}}
  ],
  "summary": "one sentence summary of today's decisions"
}}
"""

	payload = json.dumps({
		"model": MODEL,
		"max_tokens": 1200,
		"messages": [{"role": "user", "content": prompt}]
	}).encode("utf-8")

	req = urllib.request.Request(
		"https://api.anthropic.com/v1/messages",
		data=payload,
		headers={
			"Content-Type": "application/json",
			"x-api-key": ANTHROPIC_API_KEY,
			"anthropic-version": "2023-06-01",
		},
		method="POST",
	)
	
	with urllib.request.urlopen(req, timeout=60) as resp:
		data = json.loads(resp.read().decode("utf-8"))

	content_blocks = data.get("content", [])
	if not content_blocks or content_blocks[0].get("type") != "text":
		logger.error(f"ask_claude [{strategy_id}]: unexpected response structure: {data}")
		return None, "Unexpected API response structure"
	text = content_blocks[0]["text"].strip()

	if data.get("stop_reason") == "max_tokens":
		logger.warning(f"Claude response truncated (hit max_tokens=1200). Partial text: {text[:200]}...")
		return None, "Response truncated"
	
	text = re.sub(r"^```[a-z]*\n?", "", text)
	text = re.sub(r"\n?```$", "", text)
	try:
		return json.loads(text), None
	except json.JSONDecodeError as e:
		logger.error(f"ask_claude [{strategy_id}]: could not parse JSON response: {e}")
		logger.error(f"  Raw text (first 300 chars): {text[:300]}")
		return None, f"JSON parse error: {e}"

# -- Strategy-specific sector/ticker constants (hoisted for performance) -------
CAPEX_SECTORS   = {"Information Technology"}
DEFENSE_SECTORS = {"Industrials", "Energy", "Information Technology"}
DEFENSE_TICKERS = {
	"LMT", "RTX", "NOC", "GD", "LHX", "BA", "HWM", "TDG",   # defense primes
	"CRWD", "PANW", "FTNT",                                    # cybersecurity
	"GE", "GEV", "HON", "CAT", "EMR",                         # industrial/aerospace
	"XOM", "CVX", "COP", "OXY", "SLB", "BKR", "EOG",         # energy
}

# -- Score functions -----------------------------------------------------------

def score_passive(rows: List[Dict], kmap: Dict) -> List[Tuple]:
	"""Passive benchmark: rank by market cap for buy-and-hold selection."""
	out = []
	for r in rows:
		cap = r.get("market_cap", 0)
		out.append((r["ticker"], cap, "Passive hold: largest market cap"))
	return sorted(out, key=lambda x: -x[1])

def score_mean_reversion(rows, kmap):
	"""Buy oversold (RSI < 38)."""
	out = []
	for r in rows:
		rsi = r.get("rsi_14", 50)
		if rsi > 38: continue
		if r.get("ma_signal", "") == "BEARISH (Death Cross)": continue
		score = (40 - rsi) * 2 + r.get("composite_score", 0) * 0.3
		out.append((r["ticker"], round(score, 2), f"Oversold RSI={rsi:.1f}"))
	return sorted(out, key=lambda x: -x[1])

def score_value(rows, kmap):
	"""Low P/E + high profit margin."""
	out = []
	for r in rows:
		pe = r.get("pe_ratio", 999)
		npm = r.get("net_profit_margin", 0)
		if pe <= 0 or pe > 25: continue
		if npm < 0.05: continue
		score = (25 - pe) * 2 + npm * 100
		out.append((r["ticker"], round(score, 2), f"P/E={pe:.1f} margin={npm*100:.1f}%"))
	return sorted(out, key=lambda x: -x[1])

def score_low_volatility(rows, kmap):
	"""Lowest beta + decent composite score."""
	out = []
	for r in rows:
		beta = r.get("beta", 1)
		cs = r.get("composite_score", 0)
		if beta <= 0 or beta > 1.0: continue
		if cs < 40: continue
		score = (1.5 - beta) * 40 + cs * 0.4
		out.append((r["ticker"], round(score, 2), f"Low beta={beta:.2f} CS={cs:.0f}"))
	return sorted(out, key=lambda x: -x[1])

def score_earnings_surprise(rows, kmap):
	"""PEAD proxy: EPS growth + abnormal return."""
	out = []
	for r in rows:
		eps_ttm = r.get("eps_ttm", 0) or 0
		eps_g = r.get("eps_growth_fwd", 0)
		ab = r.get("abnormal_return", 0)
		if eps_ttm <= 0: continue
		if eps_g < 0.20: continue
		if ab <= 0.02: continue
		score = eps_g * 150 + ab * 200 + r.get("composite_score", 0) * 0.2
		out.append((r["ticker"], round(score, 2),
					f"PEAD: fwd_growth={eps_g*100:.1f}% ab_ret={ab*100:.2f}%"))
	
	# Fallback for days with no strong signals
	if not out:
		for r in rows:
			eps_ttm = r.get("eps_ttm", 0) or 0
			eps_g = r.get("eps_growth_fwd", 0)
			ab = r.get("abnormal_return", 0)
			if eps_ttm <= 0: continue
			if eps_g < 0.15: continue
			if ab <= 0.015: continue
			score = eps_g * 150 + ab * 200 + r.get("composite_score", 0) * 0.2
			out.append((r["ticker"], round(score, 2),
						f"PEAD(fallback): fwd_growth={eps_g*100:.1f}% ab_ret={ab*100:.2f}%"))
		out = sorted(out, key=lambda x: -x[1])[:5]
	
	return out  # already sorted by the fallback block above

def score_dividend_growth(rows, kmap):
	"""High dividend yield + growing payout.
	Hard gates: must pay a dividend (dy > 0), yield must clear a minimum
	threshold, and the company must be profitable (npm > 0).
	Non-dividend payers are excluded entirely -- no fallback scoring.
	"""
	MIN_YIELD = 0.005   # 0.5% -- filters out token/special dividends
	out = []
	for r in rows:
		dy = r.get("dividend_yield", 0) or 0
		if dy < MIN_YIELD:          # strict: zero AND sub-threshold excluded
			continue
		npm = r.get("net_profit_margin", 0) or 0
		if npm <= 0:                # must be profitable to sustain the dividend
			continue
		beta = r.get("beta", 1)
		if beta > 1.2:
			continue
		pe   = r.get("pe_ratio", 999)
		dy5  = r.get("five_year_avg_dividend_yield", 0) or 0
		# pe < 0 means data is unreliable (mixed fiscal year) — apply max penalty
		pe_pen = max(0, pe - 35) * 0.3 if pe > 0 else max(0, 999 - 35) * 0.3
		score  = dy * 400 + max(0, dy - dy5) * 200 + npm * 40 - pe_pen
		reason = f"Div yield={dy*100:.2f}% 5yr={dy5*100:.2f}% margin={npm*100:.1f}% beta={beta:.2f}"
		out.append((r["ticker"], round(score, 2), reason))
	return sorted(out, key=lambda x: -x[1])

def score_insider_buying(rows, kmap):
	"""Positive net insider shares = confidence signal."""
	out = []
	for r in rows:
		ins = r.get("net_insider_shares", 0)
		if ins <= 0: continue
		if ins > 100_000:
			ins_score = 40
		elif ins > 10_000:
			ins_score = 25
		elif ins > 1_000:
			ins_score = 12
		else:
			ins_score = 5
		score = ins_score + r.get("composite_score", 0) * 0.4
		out.append((r["ticker"], round(score, 2), f"Net insider buy={int(ins):,} shares"))
	return sorted(out, key=lambda x: -x[1])

def score_macro_adaptive(rows, kmap):
	"""Switch between aggressive and defensive based on VIX."""
	# Use first non-None VIX across all rows — rows[0] may have missing/stale data
	vix = next((r.get("vix") for r in rows if r.get("vix") is not None), 20)
	out = []
	for r in rows:
		beta = r.get("beta", 1)
		cs = r.get("composite_score", 0)
		if vix > 22:
			if beta > 1.0: continue
			score = cs * 0.5 + (1.5 - beta) * 30
			label = f"Defensive (VIX={vix:.1f})"
		else:
			if cs < 55: continue
			score = cs * 0.8 + r.get("abnormal_return", 0) * 100
			label = f"Aggressive (VIX={vix:.1f})"
		out.append((r["ticker"], round(score, 2), label))
	return sorted(out, key=lambda x: -x[1])

def score_large_cap_value(rows, kmap):
	"""Pure S&P 500 value tilt."""
	out = []
	for r in rows:
		pe = r.get("pe_ratio", 999)
		npm = r.get("net_profit_margin", 0)
		beta = r.get("beta", 1.0) or 1.0
		cs = r.get("composite_score", 0)
		cap = r.get("market_cap", 0)
		
		if pe <= 0 or pe > 22: continue
		if npm < 0.05: continue
		if beta > 1.2: continue
		if cs < 35: continue
		
		score = (22 - pe) * 3 + npm * 80 + (1.3 - beta) * 15 + cs * 0.1
		out.append((r["ticker"], round(score, 2),
					f"Value: P/E={pe:.1f} margin={npm*100:.1f}% beta={beta:.2f} "
					f"cap=${cap/1e9:.0f}B CS={cs:.0f}"))
	return sorted(out, key=lambda x: -x[1])

def score_academic_momentum(rows, kmap):
	"""Asness-style momentum with crash protection."""
	out = []
	for r in rows:
		cs = r.get("composite_score", 0)
		rsi = r.get("rsi_14", 50)
		beta = r.get("beta", 1)
		pct_from_high = r.get("pct_from_52w_high", 0)
		
		if r.get("ma_signal", "") != "BULLISH (Golden Cross)": continue
		if rsi > 75: continue
		if beta > 2.2: continue
		if pct_from_high > -0.05: continue
		
		# RSI rewards stronger trend — use rsi directly (not 100-rsi which penalises momentum)
		score = cs * 0.6 + rsi * 0.2 + r.get("abnormal_return", 0) * 100
		out.append((r["ticker"], round(score, 2),
					f"Asness: RSI={rsi:.1f} beta={beta:.2f} from52wh={pct_from_high*100:.1f}%"))
	return sorted(out, key=lambda x: -x[1])

def score_quality_profitability(rows, kmap):
	"""Novy-Marx: high gross margin + low beta."""
	out = []
	for r in rows:
		npm = r.get("net_profit_margin", 0)
		beta = r.get("beta", 1)
		eps_g = r.get("eps_growth_fwd", 0)
		if npm < 0.15: continue
		if beta > 1.3: continue
		score = npm * 100 + (1.5 - beta) * 20 + eps_g * 30
		out.append((r["ticker"], round(score, 2),
					f"Quality: margin={npm*100:.1f}% beta={beta:.2f}"))
	return sorted(out, key=lambda x: -x[1])

def score_capex_beneficiary(rows, kmap):
	"""Capex Beneficiary / Semiconductor Infrastructure."""
	out = []
	for r in rows:
		sector = r.get("sector", "") or ""
		eps_ttm = r.get("eps_ttm", 0) or 0
		eps_fwd = r.get("eps_growth_fwd", 0) or 0
		npm = r.get("net_profit_margin", 0) or 0
		pe = r.get("pe_ratio", 999) or 999
		ma_sig = r.get("ma_signal", "")
		ab = r.get("abnormal_return", 0) or 0
		cs = r.get("composite_score", 0)
		beta = r.get("beta", 1.0) or 1.0
		rsi = r.get("rsi_14", 50) or 50
		
		if sector not in CAPEX_SECTORS: continue
		if eps_ttm <= 0: continue
		if eps_fwd < 0.08: continue
		if npm < 0.12: continue
		if pe > 80 or pe <= 0: continue
		if "BULLISH" not in ma_sig: continue
		if rsi > 82: continue
		
		score = (npm * 120 + eps_fwd * 100 + ab * 150 + cs * 0.25 + min(beta, 2.0) * 8)
		reason = (f"Capex beneficiary: sector={sector} margin={npm*100:.1f}% "
				  f"fwd_growth={eps_fwd*100:.1f}% P/E={pe:.1f} beta={beta:.2f}")
		out.append((r["ticker"], round(score, 2), reason))
	return sorted(out, key=lambda x: -x[1])

def score_defense_war_economy(rows, kmap):
	"""Strategy 21 -- Defense & War Economy."""
	out = []
	for r in rows:
		ticker = r["ticker"]
		sector = r.get("sector", "") or ""
		eps_ttm = r.get("eps_ttm", 0) or 0
		eps_fwd = r.get("eps_growth_fwd", 0) or 0
		npm = r.get("net_profit_margin", 0) or 0
		ma_sig = r.get("ma_signal", "")
		ab = r.get("abnormal_return", 0) or 0
		cs = r.get("composite_score", 0)
		rsi = r.get("rsi_14", 50) or 50
		beta = r.get("beta", 1.0) or 1.0
		
		in_whitelist = ticker in DEFENSE_TICKERS
		in_sector = sector in DEFENSE_SECTORS
		
		if not in_whitelist and not in_sector: continue
		if eps_ttm <= 0: continue
		if npm < 0.08: continue
		if "BULLISH" not in ma_sig: continue
		if rsi > 78: continue
		
		whitelist_bonus = 15 if in_whitelist else 0
		cyber_bonus = 10 if ticker in {"CRWD", "PANW", "FTNT"} else 0
		
		score = (npm * 100 + eps_fwd * 80 + ab * 120 + cs * 0.25 + whitelist_bonus + cyber_bonus)
		reason = (f"Defense/war economy: {ticker} margin={npm*100:.1f}% "
				  f"fwd_growth={eps_fwd*100:.1f}% RSI={rsi:.1f} beta={beta:.2f}")
		out.append((ticker, round(score, 2), reason))
	return sorted(out, key=lambda x: -x[1])

# -- News-related functions ----------------------------------------------------

def _fetch_rss(url: str, verbose: bool = False) -> list:
	"""Fetch and parse a single RSS feed URL. Returns list of headline dicts."""
	try:
		import feedparser
	except ImportError:
		logger.error("feedparser not installed. Run: pip install feedparser")
		return []
	try:
		feed = feedparser.parse(url)
		return [
			{"title": e.title, "source": url.split('/')[2]}
			for e in feed.entries
			if hasattr(e, "title")
		]
	except Exception as e:
		if verbose:
			logger.warning(f"Failed to fetch {url}: {e}")
		return []


def _gather_headlines(verbose=False):
	"""Fetches and deduplicates headlines from all configured RSS feeds."""
	seen, headlines = set(), []
	for url in NEWS_FEEDS:
		for h in _fetch_rss(url, verbose):
			if h["title"] not in seen:
				headlines.append(h)
				seen.add(h["title"])
	return headlines


def _build_news_prompt(headlines):
	"""Builds the prompt for Claude to analyze macro trends."""
	headlines_text = "\n".join([f"- {h['title']} ({h['source']})" for h in headlines[:50]])
	
	return f"""You are a macro analyst. Analyze these financial headlines and identify:

1. The dominant macro themes (3-5)
2. Market regime (RISK-ON, RISK-OFF, or NEUTRAL)
3. Sector beneficiaries and losers for each theme
4. Specific company catalysts mentioned

Headlines:
{headlines_text}

Respond with ONLY a JSON object in this exact format:
{{
    "analysis_date": "{date.today().isoformat()}",
    "market_regime": "RISK-ON",
    "dominant_themes": [
        {{
            "theme_id": "ai_capex",
            "theme_name": "AI Infrastructure Boom",
            "strength": 8,
            "duration_outlook": "1-3 months",
            "beneficiary_sectors": ["Information Technology"],
            "loser_sectors": []
        }}
    ],
    "company_catalysts": [
        {{
            "ticker": "NVDA",
            "sentiment": "positive",
            "magnitude": 8,
            "description": "Strong AI chip demand"
        }}
    ]
}}
"""


def _load_news_cache(cache_path):
	"""Loads news cache with validation."""
	if not cache_path.exists():
		return None
	try:
		data = json.loads(cache_path.read_text(encoding="utf-8"))
		# Basic validation
		if "dominant_themes" in data and "market_regime" in data:
			return data
		else:
			logger.warning(f"Cache file missing required fields: {cache_path}")
	except (json.JSONDecodeError, OSError) as e:
		logger.warning(f"Could not read news cache: {e}")
	return None


@retry(max_attempts=3, backoff=2.0, exceptions=(urllib.error.URLError, urllib.error.HTTPError))
def get_news_macro_analysis(force_refresh=False, verbose=False):
	"""Calls Claude API to analyze macro trends from headlines."""
	cache_path = BASE_DIR / "news_macro_cache.json"  # kept as fallback only

	if not force_refresh:
		# Try MySQL first, fall back to JSON file
		cached = None
		try:
			cached = _db_read_news_macro(date.today().isoformat())
		except Exception:
			pass
		if not cached:
			cached = _load_news_cache(cache_path)
		if cached:
			if verbose:
				logger.info("Using cached news analysis (MySQL)")
			return cached
	
	if not ANTHROPIC_API_KEY:
		logger.error("ANTHROPIC_API_KEY not set, cannot perform news analysis")
		return {"analysis_date": date.today().isoformat(), "dominant_themes": [], "company_catalysts": []}
	
	headlines = _gather_headlines(verbose)
	if not headlines:
		logger.warning("No headlines fetched, returning empty analysis")
		return {"analysis_date": date.today().isoformat(), "dominant_themes": [], "company_catalysts": []}
	
	if verbose:
		logger.info(f"Fetched {len(headlines)} headlines, calling Claude for analysis...")
	
	prompt = _build_news_prompt(headlines)
	
	payload = json.dumps({
		"model": MODEL,
		"max_tokens": 1500,
		"messages": [{"role": "user", "content": prompt}]
	}).encode("utf-8")
	
	req = urllib.request.Request(
		"https://api.anthropic.com/v1/messages",
		data=payload,
		headers={
			"Content-Type": "application/json",
			"x-api-key": ANTHROPIC_API_KEY,
			"anthropic-version": "2023-06-01",
		},
		method="POST",
	)
	
	with urllib.request.urlopen(req, timeout=60) as resp:
		data = json.loads(resp.read().decode("utf-8"))

	content_blocks = data.get("content", [])
	if not content_blocks or content_blocks[0].get("type") != "text":
		logger.error(f"get_news_macro_analysis: unexpected response structure: {data}")
		return {"analysis_date": date.today().isoformat(), "dominant_themes": [], "company_catalysts": []}
	text = content_blocks[0]["text"].strip()

	if data.get("stop_reason") == "max_tokens":
		logger.warning(f"Claude response truncated (hit max_tokens=1500). Partial text: {text[:200]}...")
	
	# Clean markdown code blocks
	text = re.sub(r"^```[a-z]*\n?", "", text)
	text = re.sub(r"\n?```$", "", text)
	
	try:
		result = json.loads(text)
	except json.JSONDecodeError as e:
		logger.error(f"Failed to parse Claude response as JSON: {e}")
		logger.error(f"Response text: {text[:500]}")
		return {"analysis_date": date.today().isoformat(), "dominant_themes": [], "company_catalysts": []}
	
	# Add analysis date if missing
	if "analysis_date" not in result:
		result["analysis_date"] = date.today().isoformat()
	
	# Save to MySQL (primary) and JSON file (fallback)
	try:
		_db_write_news_macro(result)
		if verbose:
			logger.info("News macro analysis saved to MySQL news_macro_cache table")
	except Exception as _e:
		logger.warning(f"MySQL write failed for news cache ({_e}) — saving JSON only")
	cache_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

	return result


def print_news_briefing(macro_data: dict = None):
	"""Prints a formatted news briefing for review.

	Args:
	    macro_data: Pre-fetched macro analysis dict. If None, fetches from cache
	                (or calls the API if no cache exists). Passing data avoids a
	                redundant API call when the caller already has the result.
	"""
	if macro_data is None:
		macro_data = get_news_macro_analysis(force_refresh=False)
	
	logger.info("\n" + "="*70)
	logger.info("  NEWS MACRO BRIEFING")
	logger.info(f"  Analysis Date: {macro_data.get('analysis_date', 'N/A')}")
	logger.info("="*70)
	
	regime = macro_data.get("market_regime", "UNKNOWN")
	regime_emoji = "(up)" if regime == "RISK-ON" else "(dn)" if regime == "RISK-OFF" else "(=)?"
	logger.info(f"\nMarket Regime: {regime_emoji} {regime}")
	
	logger.info("\nDominant Themes:")
	for theme in macro_data.get("dominant_themes", []):
		logger.info(f"  * {theme.get('theme_name', theme.get('theme_id', 'Unknown'))} "
			  f"(strength: {theme.get('strength', '?')}/10, "
			  f"duration: {theme.get('duration_outlook', '?')})")
		if theme.get("beneficiary_sectors"):
			logger.info(f"    -> Beneficiaries: {', '.join(theme['beneficiary_sectors'])}")
		if theme.get("loser_sectors"):
			logger.info(f"    -> Losers: {', '.join(theme['loser_sectors'])}")
	
	logger.info("\nCompany Catalysts:")
	for cat in macro_data.get("company_catalysts", []):
		sentiment = "[+]" if cat.get("sentiment") == "positive" else "[-]" if cat.get("sentiment") == "negative" else "[?]"
		logger.info(f"  {sentiment} {cat.get('ticker', 'Unknown')}: {cat.get('description', '')} "
			  f"(magnitude: {cat.get('magnitude', '?')}/10)")
	
	logger.info("\n" + "="*70)


def score_news_macro(rows: list, kmap: dict, macro: dict = None) -> list:
	"""
	Strategy 19 -- News Macro Catalyst scorer.
	Scores each stock by sector alignment with today's dominant macro themes.
	Theme strength, duration, and market regime all adjust the final score.
	"""
	if not macro or not macro.get("dominant_themes"):
		return []

	regime = macro.get("market_regime", "NEUTRAL")
	sector_scores: dict[str, float] = defaultdict(float)
	sector_labels: dict[str, list]  = defaultdict(list)

	for theme in macro.get("dominant_themes", []):
		strength = theme.get("strength", 5) / 10.0
		dur_mult = {"1-3 days": 0.8, "1-2 weeks": 1.0,
					"1-3 months": 1.2, "structural": 1.5}.get(
						theme.get("duration_outlook", "1-2 weeks"), 1.0)

		# Merge Claude's sectors with our built-in theme map for extra coverage
		builtin  = MACRO_THEME_MAP.get(theme.get("theme_id", ""), ([], []))
		ben_secs = list(set(theme.get("beneficiary_sectors", []) + builtin[0]))
		los_secs = list(set(theme.get("loser_sectors", []) + builtin[1]))

		for sec in ben_secs:
			sector_scores[sec] += strength * dur_mult * 10
			sector_labels[sec].append(f"{theme['theme_name']}(+)")
		for sec in los_secs:
			sector_scores[sec] -= strength * dur_mult * 10
			sector_labels[sec].append(f"{theme['theme_name']}(-)")

	# Per-company catalysts from news
	catalyst_map: dict[str, tuple] = {}
	for cat in macro.get("company_catalysts", []):
		ticker = (cat.get("ticker") or "").upper()
		if not ticker: continue
		mag  = cat.get("magnitude", 5) / 10.0
		sign = (1  if cat.get("sentiment") == "positive" else
			   -1  if cat.get("sentiment") == "negative" else 0)
		if sign:
			catalyst_map[ticker] = (sign * mag * 15, cat.get("description", "")[:80])

	out = []
	for r in rows:
		ticker = r["ticker"]
		sector = r.get("sector", "Unknown")
		beta   = r.get("beta", 1.0) or 1.0
		cs	 = r.get("composite_score", 0) or 0

		sec_adj = sector_scores.get(sector, 0.0)

		# Regime modifier
		if regime == "RISK-OFF":
			if beta > 1.3:  sec_adj -= 5
			elif beta < 0.8: sec_adj += 3
		elif regime == "RISK-ON":
			if beta > 1.2:  sec_adj += 3

		cat_score, cat_desc = catalyst_map.get(ticker, (0, ""))
		score = sec_adj + cs * 0.3 + cat_score

		if score <= 2 and not cat_score:
			continue

		labels = " ".join(sector_labels.get(sector, []))
		reason = f"News macro: {sector} adj={sec_adj:+.1f} regime={regime}"
		if labels:   reason += f" [{labels}]"
		if cat_desc: reason += f" | catalyst: {cat_desc}"

		out.append((ticker, round(score, 2), reason))

	return sorted(out, key=lambda x: -x[1])


def score_news_sentiment(rows: list, kmap: dict, macro: dict = None) -> list:
	"""
	Strategy 20 -- News Sentiment Momentum scorer.
	Quality gate (CS > 40, positive EPS) then amplified by macro sector tailwinds
	and company-specific catalysts. Fundamentals + news together.
	"""
	if not macro:
		return []

	sector_scores: dict[str, float] = defaultdict(float)
	for theme in macro.get("dominant_themes", []):
		strength = theme.get("strength", 5) / 10.0
		builtin  = MACRO_THEME_MAP.get(theme.get("theme_id", ""), ([], []))
		for sec in set(theme.get("beneficiary_sectors", []) + builtin[0]):
			sector_scores[sec] += strength * 8
		for sec in set(theme.get("loser_sectors", []) + builtin[1]):
			sector_scores[sec] -= strength * 8

	catalyst_map: dict[str, tuple] = {}
	for cat in macro.get("company_catalysts", []):
		ticker = (cat.get("ticker") or "").upper()
		if not ticker: continue
		mag  = cat.get("magnitude", 5) / 10.0
		sign = (1  if cat.get("sentiment") == "positive" else
			   -1  if cat.get("sentiment") == "negative" else 0)
		if sign:
			catalyst_map[ticker] = (sign * mag * 20, cat.get("description", "")[:80])

	out = []
	for r in rows:
		ticker = r["ticker"]
		cs	 = r.get("composite_score", 0) or 0
		eps_g  = r.get("eps_growth_fwd", 0) or 0
		npm	= r.get("net_profit_margin", 0) or 0
		sector = r.get("sector", "Unknown")

		if cs < 40: continue						 # quality gate
		if (r.get("eps_ttm") or 0) < 0: continue	# must be profitable

		sec_adj		   = sector_scores.get(sector, 0.0)
		cat_adj, cat_desc = catalyst_map.get(ticker, (0, ""))
		score = cs * 0.5 + sec_adj + cat_adj + eps_g * 30 + npm * 20

		if score <= 15 and not cat_adj:
			continue

		parts = [f"Sentiment: CS={cs:.0f}"]
		if sec_adj:  parts.append(f"sector_adj={sec_adj:+.1f}")
		if cat_adj:  parts.append(f"catalyst={cat_adj:+.1f} ({cat_desc})")
		parts.append(f"[{sector}]")

		out.append((ticker, round(score, 2), "  ".join(parts)))

	return sorted(out, key=lambda x: -x[1])


# -- Scoring function dispatch ------------------------------------------------
SCORE_FN = {
	"02": score_mean_reversion,
	"03": score_value,
	"06": score_low_volatility,
	"07": score_earnings_surprise,
	"08": score_dividend_growth,
	"09": score_insider_buying,
	"10": score_macro_adaptive,
	"11": score_large_cap_value,
	"12": score_academic_momentum,
	"13": score_quality_profitability,
	"14": score_passive,
	"18": score_capex_beneficiary,
	"19": score_news_macro,
	"20": score_news_sentiment,
	"21": score_defense_war_economy,
}


# -- Run one strategy ----------------------------------------------------------
def run_strategy(sid: str, name: str, style: str, risk: str, desc: str,
				 rows: List[Dict], kmap: Dict, today: str, dry_run: bool = False):
	"""Run a single strategy with all safety checks."""
	# Tag the shared logger with this strategy's ID so every log line written
	# during this call is queryable by strategy_id in the run_logs table.
	# try/finally guarantees the tag is cleared even on early returns.
	logger.strategy_id = sid
	try:
		_run_strategy_inner(sid, name, style, risk, desc, rows, kmap, today, dry_run)
	finally:
		logger.strategy_id = ""
		logger.flush()


def _run_strategy_inner(sid: str, name: str, style: str, risk: str, desc: str,
						 rows: List[Dict], kmap: Dict, today: str, dry_run: bool = False):
	"""Inner implementation — called by run_strategy() which handles logger cleanup."""
	logger.info(f"\n  [{sid}] {name}")
	logger.info(f"	   Style: {style}  |  Risk: {risk}")

	if sid == "14":
		run_passive(sid, rows, kmap, today, dry_run)
		return

	acct = read_account(sid)
	holdings = read_holdings(sid)

	acct["holdings_value"] = calc_holdings_value(holdings, kmap)
	acct["total"] = round(acct["cash"] + acct["holdings_value"], 2)
	save_account(sid, acct)

	logger.info(f"	   Cash: ${acct['cash']:>9.2f}  |  "
				f"Holdings: ${acct['holdings_value']:>9.2f}  |  "
				f"Total: ${acct['total']:>9.2f}")

	holdings = enforce_stop_losses(sid, holdings, kmap, today, dry_run)
	acct = read_account(sid)

	score_fn = SCORE_FN.get(sid)
	if not score_fn or not rows:
		logger.warning("No KPI data or scoring function -- skipping")
		return

	# -- S07 (PEAD) staleness enforcement -------------------------------------
	# Uses the KPI age cached at startup by check_data_freshness() to avoid
	# opening a second DB connection per strategy run.
	if sid == "07":
		age_h = _kpi_age_hours
		if age_h is None:
			# KPI age unknown (DB was unreachable at startup or table empty).
			# Safer to skip PEAD than trade on data of unknown freshness.
			logger.warning(
				"  [07] PEAD SKIPPED: KPI data freshness unknown "
				"(check_data_freshness failed at startup). "
				"Run equity_kpi_analyzer.py first."
			)
			return
		# age_h is guaranteed non-None here (None case returned above)
		if age_h >= KPI_WARN_HOURS_PEAD * 2:
			logger.warning(
				f"  [07] PEAD SKIPPED: KPI data is {age_h:.1f}h old "
				f"(limit: {KPI_WARN_HOURS_PEAD * 2}h). "
				f"Run update_quotes.py + equity_kpi_analyzer.py first."
			)
			return
		elif age_h >= KPI_WARN_HOURS_PEAD:
			logger.warning(
				f"  [07] PEAD WARNING: KPI data is {age_h:.1f}h old -- "
				f"abnormal_return signal may be stale. "
				f"Results will be used but treated with lower confidence."
			)

	# Strategies 19 & 20: load news macro analysis from MySQL (JSON file as fallback)
	if sid in ("19", "20"):
		# Read news macro from MySQL (today or yesterday allowed)
		macro = None
		try:
			macro = _db_read_news_macro(today)
			if macro is None:
				# Try yesterday's cache
				macro = _db_read_news_macro_latest()
				if macro:
					cache_date = macro.get("analysis_date", "")
					try:
						cache_age_days = (date.fromisoformat(today) - date.fromisoformat(cache_date)).days
					except Exception:
						cache_age_days = 99
					if cache_age_days > 1:
						logger.warning(
							f"	   News cache is {cache_age_days} days old ({cache_date}) -- "
							f"too stale to trade on. Skipping S{sid}."
						)
						macro = None
					else:
						logger.info(f"	   News cache is from yesterday ({cache_date}), using it")
		except Exception as _e:
			# MySQL unavailable — fall back to JSON file
			macro_cache = BASE_DIR / "news_macro_cache.json"
			if macro_cache.exists():
				try:
					macro = json.loads(macro_cache.read_text(encoding="utf-8"))
					logger.warning(f"	   MySQL unavailable ({_e}) — using JSON file cache")
				except Exception:
					pass
		if macro is None:
			logger.info("	   No news macro cache found — run news strategies (Stage 1) first")
		candidates = score_fn(rows, kmap, macro)
	else:
		candidates = score_fn(rows, kmap)
	if not candidates:
		logger.info("No candidates matched this strategy's filters today")
		return

	logger.info(f"	   Top candidate: {candidates[0][0]} (score {candidates[0][1]:.1f})")

	# Hard-enforce the 3-position cap in code, not just in the Claude prompt.
	# Re-read fresh holdings in case stop-losses fired above.
	current_holdings = read_holdings(sid)
	at_capacity = len(current_holdings) >= 3
	if at_capacity:
		logger.info(f"	   At capacity ({len(current_holdings)}/3 positions) — filtering to SELL/HOLD only")
		candidates = [(t, s, r) for t, s, r in candidates if t in {h['ticker'] for h in current_holdings}]
		if not candidates:
			logger.info("	   No held positions in today's candidate list — nothing to do")
			return

	decision, err = ask_claude(sid, name, desc, candidates, current_holdings, acct, today, kmap=kmap)
	if err:
		logger.error(f"Claude error: {err}")
		return

	logger.info(f"	   Summary: {decision.get('summary', '')}")

	for action in decision.get("actions", []):
		atype = action.get("type", "").upper()
		ticker = action.get("ticker", "")
		reason = action.get("reason", "")
		if not ticker or ticker not in kmap:
			continue
		price = kmap[ticker].get("current_price", 0)
		if price <= 0:
			continue

		if atype == "BUY":
			# Re-check capacity before each BUY — a prior BUY in this loop
			# may have filled the last slot.
			if len(read_holdings(sid)) >= 3:
				logger.info(f"	   BUY {ticker} skipped — already at 3-position capacity")
				continue
			# Re-read acct so cash_available reflects any prior buys.
			acct = read_account(sid)
			ok, msg = buy(sid, ticker, price, acct["cash"], reason, today, kmap, dry_run)
			logger.info(f"	   BUY  {ticker}: {msg}")
			acct = read_account(sid)
		elif atype == "SELL":
			ok, msg = sell(sid, ticker, price, reason, today, kmap, dry_run)
			logger.info(f"	   SELL {ticker}: {msg}")
			acct = read_account(sid)
		elif atype == "HOLD":
			logger.info(f"	   HOLD {ticker}: {reason[:60]}")


# -- Initialise account files --------------------------------------------------
def init_accounts(force: bool = False):
	"""Initialize all strategy accounts to starting cash."""
	logger.info("\n" + "-"*55)
	logger.info("  INITIALISING STRATEGY ACCOUNTS")
	logger.info("-"*55)
	today = date.today().isoformat()
	for sid, name, style, risk, desc in STRATEGIES:
		existing = _db_read_account(sid)
		if existing and not force:
			logger.info(f"  [{sid}] {name[:40]} -- already exists, skipping")
			continue
		acct = {"account": ACCOUNT_NUM, "strategy_id": sid,
				"cash": STARTING_CASH, "holdings_value": 0.0, "total": STARTING_CASH,
				"start_date": today, "trades": 0}
		save_account(sid, acct)
		save_holdings(sid, [])
		logger.info(f"  [{sid}] {name[:40]} -- created  ${STARTING_CASH:.2f}")
	logger.info("-"*55)
	logger.info("  Done. Run without --init to start trading.")
	logger.info("-"*55 + "\n")



# -- Validation suite (--validate) --------------------------------------------
def run_validation() -> bool:
	"""
	Inline self-test suite -- equivalent to test_news_strategy.py steps.
	Run with:  python strategy_runner.py --validate

	Returns True if all checks pass, False otherwise.
	Exit code mirrors the return value (0 = pass, 1 = fail).
	"""
	import traceback

	PASS  = "  (ok)"
	FAIL  = "  (x)"
	results = []

	def check(name, fn):
		try:
			ok, detail = fn()
			tag = PASS if ok else FAIL
			results.append((ok, name, detail))
			logger.info(f"{tag}  {name}" + (f": {detail}" if detail else ""))
		except Exception as e:
			results.append((False, name, str(e)))
			logger.error(f"{FAIL}  {name}: EXCEPTION -- {e}")
			if "--verbose" in sys.argv:
				traceback.print_exc()

	logger.info("\n" + "="*65)
	logger.info("  VALIDATION SUITE")
	logger.info("="*65)

	# -- 1. constants.py is importable and has required keys ------------------
	def t_constants():
		from constants import STRATEGIES, HOLD_PERIODS, MACRO_THEME_MAP
		missing_holds = [s[0] for s in STRATEGIES if s[0] not in HOLD_PERIODS]
		if missing_holds:
			return False, f"HOLD_PERIODS missing keys: {missing_holds}"
		return True, f"{len(STRATEGIES)} strategies, {len(HOLD_PERIODS)} hold periods, {len(MACRO_THEME_MAP)} macro themes"
	check("constants.py importable and complete", t_constants)

	# -- 2. MACRO_THEME_MAP structure -----------------------------------------
	def t_macro_theme_map():
		bad = []
		for k, v in MACRO_THEME_MAP.items():
			if not (isinstance(v, tuple) and len(v) == 2
					and isinstance(v[0], list) and isinstance(v[1], list)):
				bad.append(k)
		if bad:
			return False, f"Malformed entries: {bad}"
		return True, f"{len(MACRO_THEME_MAP)} themes, all well-formed"
	check("MACRO_THEME_MAP structure", t_macro_theme_map)

	# -- 3. _fetch_rss is defined and callable --------------------------------
	def t_fetch_rss():
		if not callable(_fetch_rss):
			return False, "_fetch_rss not callable"
		# Dry call with a dummy URL -- should return [] without crashing
		result = _fetch_rss("https://example.invalid/rss", verbose=False)
		if not isinstance(result, list):
			return False, f"Expected list, got {type(result)}"
		return True, "callable, returns list on failure"
	check("_fetch_rss defined and callable", t_fetch_rss)

	# -- 4. _gather_headlines is defined and callable --------------------------
	def t_gather_headlines():
		if not callable(_gather_headlines):
			return False, "_gather_headlines not callable"
		return True, "callable"
	check("_gather_headlines defined and callable", t_gather_headlines)

	# -- 5. _build_news_prompt generates valid prompt --------------------------
	def t_build_news_prompt():
		headlines = [
			{"title": "Fed raises rates", "source": "reuters.com"},
			{"title": "AI spending surges", "source": "wsj.com"},
		]
		prompt = _build_news_prompt(headlines)
		if not isinstance(prompt, str) or len(prompt) < 50:
			return False, "Prompt too short or wrong type"
		if "JSON" not in prompt:
			return False, "Prompt does not request JSON output"
		return True, f"prompt is {len(prompt)} chars"
	check("_build_news_prompt output", t_build_news_prompt)

	# -- 6. _load_news_cache handles missing file gracefully -------------------
	def t_load_news_cache():
		result = _load_news_cache(Path("/nonexistent/path/cache.json"))
		if result is not None:
			return False, f"Expected None for missing file, got {type(result)}"
		return True, "returns None for missing file"
	check("_load_news_cache missing file", t_load_news_cache)

	# -- 7. score_news_macro returns [] with no macro data --------------------
	def t_score_news_macro_empty():
		rows = [{"ticker": "AAPL", "sector": "Information Technology",
				 "composite_score": 75, "beta": 1.1}]
		kmap = {"AAPL": {"current_price": 180.0}}
		result = score_news_macro(rows, kmap, macro=None)
		if result != []:
			return False, f"Expected [], got {result}"
		result2 = score_news_macro(rows, kmap, macro={"dominant_themes": []})
		if result2 != []:
			return False, f"Expected [] with empty themes, got {result2}"
		return True, "returns [] with no/empty macro"
	check("score_news_macro empty macro guard", t_score_news_macro_empty)

	# -- 8. score_news_macro scores correctly with real macro data -------------
	def t_score_news_macro_scoring():
		rows = [
			{"ticker": "NVDA", "sector": "Information Technology",
			 "composite_score": 80, "beta": 1.4, "eps_ttm": 5.0},
			{"ticker": "XOM",  "sector": "Energy",
			 "composite_score": 60, "beta": 0.9, "eps_ttm": 8.0},
		]
		kmap = {r["ticker"]: {"current_price": 100.0} for r in rows}
		macro = {
			"market_regime": "RISK-ON",
			"dominant_themes": [{
				"theme_id": "ai_capex",
				"theme_name": "AI Capex Boom",
				"strength": 8,
				"duration_outlook": "1-3 months",
				"beneficiary_sectors": ["Information Technology"],
				"loser_sectors": [],
			}],
			"company_catalysts": [],
		}
		result = score_news_macro(rows, kmap, macro=macro)
		if not result:
			return False, "Expected candidates, got empty list"
		tickers = [r[0] for r in result]
		if "NVDA" not in tickers:
			return False, f"NVDA (IT sector) should appear in results; got {tickers}"
		# NVDA (IT, high beta, RISK-ON) should outscore XOM (Energy, no theme)
		nvda_score = next(r[1] for r in result if r[0] == "NVDA")
		xom_entry  = next((r for r in result if r[0] == "XOM"), None)
		if xom_entry and nvda_score <= xom_entry[1]:
			return False, f"NVDA score {nvda_score} should beat XOM {xom_entry[1]}"
		return True, f"NVDA scored {nvda_score:.1f}, results={[r[0] for r in result]}"
	check("score_news_macro scoring logic", t_score_news_macro_scoring)

	# -- 9. score_news_sentiment quality gate ---------------------------------
	def t_score_news_sentiment_gate():
		rows = [
			{"ticker": "AAPL", "sector": "Information Technology",
			 "composite_score": 75, "eps_ttm": 6.0, "eps_growth_fwd": 0.12, "net_profit_margin": 0.25},
			{"ticker": "JUNK", "sector": "Information Technology",
			 "composite_score": 30, "eps_ttm": 1.0, "eps_growth_fwd": 0.05, "net_profit_margin": 0.02},
		]
		kmap = {r["ticker"]: {"current_price": 100.0} for r in rows}
		macro = {
			"market_regime": "NEUTRAL",
			"dominant_themes": [{
				"theme_id": "ai_capex", "theme_name": "AI", "strength": 7,
				"duration_outlook": "1-2 weeks",
				"beneficiary_sectors": ["Information Technology"], "loser_sectors": [],
			}],
			"company_catalysts": [],
		}
		result = score_news_sentiment(rows, kmap, macro=macro)
		tickers = [r[0] for r in result]
		if "JUNK" in tickers:
			return False, "JUNK (CS=30) should be filtered by quality gate (CS < 40)"
		if "AAPL" not in tickers:
			return False, "AAPL (CS=75) should pass quality gate"
		return True, f"quality gate working; passed: {tickers}"
	check("score_news_sentiment quality gate", t_score_news_sentiment_gate)

	# -- 10. print_news_briefing is callable -----------------------------------
	def t_print_news_briefing():
		if not callable(print_news_briefing):
			return False, "not callable"
		return True, "callable"
	check("print_news_briefing defined and callable", t_print_news_briefing)

	# -- 11. SCORE_FN dispatch covers all strategies except passive ------------
	def t_score_fn_dispatch():
		missing = [s[0] for s in STRATEGIES
				   if s[0] != "14" and s[0] not in SCORE_FN]
		if missing:
			return False, f"SCORE_FN missing strategies: {missing}"
		return True, f"{len(SCORE_FN)} entries cover all non-passive strategies"
	check("SCORE_FN dispatch complete", t_score_fn_dispatch)

	# -- 12. calc_holdings_value uses is-None not falsy or --------------------
	def t_holdings_zero_price():
		holdings = [{"ticker": "TEST", "shares": 10, "avg_cost": "50.00"}]
		kmap_zero = {"TEST": {"current_price": 0.0}}
		val = calc_holdings_value(holdings, kmap_zero)
		if val != 0.0:
			return False, f"0.0 price should give value=0.0, got {val}"
		kmap_none = {"TEST": {"current_price": None}}
		val2 = calc_holdings_value(holdings, kmap_none)
		if val2 != 500.0:
			return False, f"None price should fall back to avg_cost*shares=500, got {val2}"
		return True, "0.0 price handled correctly; None falls back to avg_cost"
	check("calc_holdings_value zero-price handling", t_holdings_zero_price)

	# -- 13. Passive strategy targets PASSIVE_MAX_POSITIONS (not 2) ------------
	def t_passive_position_count():
		if PASSIVE_MAX_POSITIONS < 3:
			return False, f"PASSIVE_MAX_POSITIONS={PASSIVE_MAX_POSITIONS} is too low for a valid benchmark"
		return True, f"PASSIVE_MAX_POSITIONS={PASSIVE_MAX_POSITIONS}"
	check("passive strategy position count", t_passive_position_count)

	# -- 14. score_dividend_growth excludes non-dividend payers ---------------
	def t_dividend_gate():
		rows = [
			{"ticker": "DIV",  "dividend_yield": 0.04, "beta": 0.9, "pe_ratio": 18,
			 "net_profit_margin": 0.15, "five_year_avg_dividend_yield": 0.035},
			{"ticker": "NODIV","dividend_yield": 0.0,  "beta": 0.8, "pe_ratio": 12,
			 "net_profit_margin": 0.20, "five_year_avg_dividend_yield": 0.0},
			{"ticker": "TINY", "dividend_yield": 0.001,"beta": 0.9, "pe_ratio": 15,
			 "net_profit_margin": 0.10, "five_year_avg_dividend_yield": 0.001},
			{"ticker": "LOSS", "dividend_yield": 0.05, "beta": 0.9, "pe_ratio": 20,
			 "net_profit_margin": -0.02,"five_year_avg_dividend_yield": 0.04},
		]
		result  = score_dividend_growth(rows, {})
		tickers = [r[0] for r in result]
		if "NODIV" in tickers:
			return False, "Non-dividend payer (dy=0) should be excluded"
		if "TINY"  in tickers:
			return False, "Sub-threshold yield (0.1%) should be excluded"
		if "LOSS"  in tickers:
			return False, "Unprofitable company should be excluded"
		if "DIV" not in tickers:
			return False, "Valid dividend payer should be included"
		return True, f"gates working -- included: {tickers}"
	check("score_dividend_growth exclusion gates", t_dividend_gate)

	# -- 15. Candidate reason strings are truncated in ask_claude -------------
	def t_reason_truncation():
		# Verify the constant is set and is a reasonable value
		import inspect
		src = inspect.getsource(ask_claude)
		if "REASON_MAX" not in src:
			return False, "REASON_MAX not found in ask_claude source"
		if "..." not in src and "..." not in src:
			return False, "No truncation ellipsis found in ask_claude"
		return True, "REASON_MAX truncation present"
	check("ask_claude reason string truncation", t_reason_truncation)

	# -- 16. PEAD staleness guard is in run_strategy for S07 ------------------
	def t_pead_staleness_guard():
		import inspect
		# The PEAD guard lives in _run_strategy_inner; run_strategy is the
		# 4-line try/finally wrapper that only resets logger.strategy_id.
		src = inspect.getsource(_run_strategy_inner)
		has_sid_check  = 'sid == "07"' in src
		has_age_check  = "age_h" in src
		has_skip       = "PEAD SKIPPED" in src
		if not (has_sid_check and has_age_check and has_skip):
			return False, f"Missing guard: sid_check={has_sid_check} age_check={has_age_check} skip_msg={has_skip}"
		return True, "PEAD staleness guard present in _run_strategy_inner"
	check("PEAD staleness enforcement in _run_strategy_inner", t_pead_staleness_guard)

	# -- Summary ---------------------------------------------------------------
	passed = sum(1 for ok, _, _ in results if ok)
	total  = len(results)
	failed = total - passed
	logger.info("-"*65)
	summary_msg = f"  {passed}/{total} checks passed" + (f"  ({failed} FAILED)" if failed else "  -- all good")
	if failed:
		logger.error(summary_msg)
	else:
		logger.info(summary_msg)
	logger.info("="*65 + "\n")
	return failed == 0


# -- Main ----------------------------------------------------------------------
def main():
	parser = argparse.ArgumentParser(description="Multi-strategy paper trading runner")
	parser.add_argument("--dry-run",	action="store_true", help="Preview trades, don't execute")
	parser.add_argument("--init",	   action="store_true", help="Initialise all account files")
	parser.add_argument("--force-init", action="store_true", help="Re-initialise (resets all accounts!)")
	parser.add_argument("--strategy",   type=str,			help="Run only this strategy ID, e.g. 01")

	parser.add_argument("--news-brief", action="store_true", help="Print news briefing and exit")
	parser.add_argument("--validate",   action="store_true", help="Run self-test suite and exit")
	args = parser.parse_args()

	today = date.today().isoformat()
	init_schema()  # ensure MySQL tables exist

	if args.validate:
		ok = run_validation()
		logger.flush()
		sys.exit(0 if ok else 1)

	if args.news_brief:
		print_news_briefing()
		logger.flush()
		return

	if args.init or args.force_init:
		init_accounts(force=args.force_init)
		logger.flush()
		return

	if not ANTHROPIC_API_KEY:
		logger.error("\n  ERROR: ANTHROPIC_API_KEY not set.")
		logger.error("  Add it to your .env file or export it as an environment variable.\n")
		logger.flush()
		sys.exit(1)

	logger.info("\n" + "="*65)
	logger.info("  MULTI-STRATEGY PAPER TRADING ENGINE")
	logger.info(f"  Date: {today}  |  Strategies: {len(STRATEGIES)}  |  Goal: $2,000 each")
	logger.info(f"  Model: {MODEL}")
	if args.dry_run:
		logger.info("  *** DRY RUN -- no trades will be executed ***")
	logger.info("="*65)

	rows, kmap = load_kpi()
	if not rows:
		logger.error("\n  ERROR: No KPI data found. Run equity_kpi_analyzer.py first.\n")
		logger.flush()
		sys.exit(1)

	logger.info(f"\n  Loaded {len(rows)} tickers from KPI file.")

	if args.strategy:
		if args.strategy not in {s[0] for s in STRATEGIES}:
			logger.error(f"Unknown strategy ID '{args.strategy}'. Valid IDs: {[s[0] for s in STRATEGIES]}")
			logger.flush()
			sys.exit(1)
		run_ids = [args.strategy]
	else:
		run_ids = [s[0] for s in STRATEGIES]

	for sid, name, style, risk, desc in STRATEGIES:
		if sid not in run_ids:
			continue
		run_strategy(sid, name, style, risk, desc, rows, kmap, today, dry_run=args.dry_run)

	logger.strategy_id = ""  # reset — leaderboard logs belong to the pipeline, not a strategy
	logger.info("\n" + "-"*65)
	logger.info("  LEADERBOARD")
	logger.info("-"*65)
	lb = update_leaderboard(today, kmap=kmap)
	logger.info(f"  {'Rank':<5} {'ID':<4} {'Strategy':<35} {'Total':>9} {'P&L':>9} {'Return':>8}")
	logger.info(f"  {'-'*4} {'-'*4} {'-'*35} {'-'*9} {'-'*9} {'-'*8}")
	for r in lb:
		sign = "+" if r["pnl"] >= 0 else ""
		logger.info(f"  {r['rank']:<5} {r['strategy_id']:<4} {r['strategy_name'][:35]:<35} "
					f"${r['total']:>8.2f} {sign}${r['pnl']:>8.2f} {sign}{r['pct_return']:>7.2f}%")
	logger.info("-"*65)
	logger.info("  Leaderboard saved to MySQL (leaderboard table)")
	logger.info("="*65 + "\n")
	logger.flush()


if __name__ == "__main__":
	check_data_freshness()
	main()
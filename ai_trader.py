"""
ai_trader.py
============
AI-powered paper trading agent.
Reads daily KPI scores from equity_kpi_analyzer.py output,
then uses Claude AI to decide what to buy and sell.

Usage (run this AFTER equity_kpi_analyzer.py each day):
    python ai_trader.py
    python ai_trader.py --kpi-file equity_kpi_results.csv
    python ai_trader.py --dry-run        # preview decisions without executing

Files managed:
    trading_account.csv   — cash + holdings value snapshot
    holdings.csv          — current stock positions
    transactions.csv      — full ledger of every trade

Brokerage fees:
    Commission : $4.95 per trade (flat)
    Spread     : 0.05% of trade value (simulated bid/ask)
"""

import argparse
import csv
import datetime
import os
import json
import sys
import requests

# Force UTF-8 output on Windows to avoid Unicode errors in logs/console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ── LOAD .env FILE ──────────────────────────────────────────────────────────
def load_env(path=".env"):
    """Read key=value pairs from a .env file into os.environ."""
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

load_env()

# ── CONFIG ─────────────────────────────────────────────────────────────────
ACCOUNT_FILE     = "trading_account.csv"
HOLDINGS_FILE    = "holdings.csv"
TRANSACTIONS_FILE= "transactions.csv"
DEFAULT_KPI_FILE = "equity_kpi_results.csv"

COMMISSION       = 4.95        # flat fee per trade
SPREAD_PCT       = 0.0005      # 0.05% simulated spread
MAX_POSITIONS    = 3           # max simultaneous holdings
MAX_POSITION_PCT = 0.60        # max 60% of portfolio in one stock
MIN_TRADE_VALUE  = 20.00       # don't trade below this value
CASH_RESERVE_PCT = 0.05        # keep 5% cash in reserve
TARGET_VALUE     = 2000.00     # goal: double the money

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
MODEL             = "claude-sonnet-4-5-20250929"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ── FILE I/O ────────────────────────────────────────────────────────────────

def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline='') as f:
        return list(csv.DictReader(f))

def write_csv(path, rows, fieldnames):
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def read_account():
    rows = read_csv(ACCOUNT_FILE)
    if not rows:
        return {"account_number":"123456789","owner":"AI Stock Broker",
                "cash_balance":1000.0,"holdings_value":0.0,"total_value":1000.0,
                "last_updated":str(datetime.date.today())}
    r = rows[0]
    r["cash_balance"]   = float(r["cash_balance"])
    r["holdings_value"] = float(r["holdings_value"])
    r["total_value"]    = float(r["total_value"])
    return r

def write_account(acct):
    acct["last_updated"] = str(datetime.date.today())
    write_csv(ACCOUNT_FILE,
              [acct],
              ["account_number","owner","cash_balance","holdings_value","total_value","last_updated"])

def read_holdings():
    rows = read_csv(HOLDINGS_FILE)
    for r in rows:
        for f in ["shares","avg_cost_per_share","total_cost",
                  "current_price","current_value","unrealised_pnl","unrealised_pnl_pct"]:
            try: r[f] = float(r[f])
            except: r[f] = 0.0
    return rows

def write_holdings(holdings):
    fields = ["ticker","shares","avg_cost_per_share","total_cost",
              "current_price","current_value","unrealised_pnl",
              "unrealised_pnl_pct","date_first_bought","last_updated"]
    write_csv(HOLDINGS_FILE, holdings, fields)

def next_txn_id():
    rows = read_csv(TRANSACTIONS_FILE)
    return f"TXN-{len(rows)+1:04d}"

def append_transaction(txn):
    rows = read_csv(TRANSACTIONS_FILE)
    rows.append(txn)
    fields = ["txn_id","date","type","ticker","shares","price_per_share",
              "gross_value","commission","net_value","cash_before","cash_after",
              "holdings_value","total_value","notes"]
    write_csv(TRANSACTIONS_FILE, rows, fields)

# ── KPI DATA ────────────────────────────────────────────────────────────────

def load_kpi(path):
    rows = read_csv(path)
    numeric = ["composite_score","tier1_score","tier2_score","tier3_score",
               "current_price","net_profit_margin","eps_growth_fwd",
               "current_ratio","rsi_14","beta","pe_ratio","market_cap",
               "abnormal_return","pct_from_52w_high"]
    for r in rows:
        for f in numeric:
            try: r[f] = float(r[f])
            except: r[f] = None
    return rows

# ── PORTFOLIO SUMMARY ────────────────────────────────────────────────────────

def portfolio_summary(acct, holdings, kpi_map):
    """Refresh holding prices from KPI data and return updated summary."""
    total_holdings_value = 0.0
    updated = []
    for h in holdings:
        ticker = h["ticker"]
        price  = kpi_map.get(ticker, {}).get("current_price") or h["current_price"]
        value  = round(h["shares"] * price, 2)
        cost   = h["total_cost"]
        pnl    = round(value - cost, 2)
        pnl_pct= round((pnl / cost * 100) if cost else 0, 2)
        h.update({
            "current_price":     round(price, 4),
            "current_value":     value,
            "unrealised_pnl":    pnl,
            "unrealised_pnl_pct":pnl_pct,
            "last_updated":      str(datetime.date.today()),
        })
        total_holdings_value += value
        updated.append(h)

    acct["holdings_value"] = round(total_holdings_value, 2)
    acct["total_value"]    = round(acct["cash_balance"] + total_holdings_value, 2)
    return acct, updated

# ── AI DECISION ENGINE ───────────────────────────────────────────────────────

def ask_claude(prompt, api_key):
    """Call Claude via Anthropic API and return the text response."""
    resp = requests.post(
        ANTHROPIC_API_URL,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": MODEL,
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": prompt}]
        },
        timeout=60
    )
    resp.raise_for_status()
    data = resp.json()
    return data["content"][0]["text"]


def build_prompt(acct, holdings, kpi_rows, today, kpi_map):
    """Build the prompt for Claude to make trading decisions."""

    # Top 25 candidates by composite score
    held_tickers = {h["ticker"] for h in holdings}
    candidates = sorted(
        [r for r in kpi_rows if r.get("composite_score") and r.get("current_price")],
        key=lambda x: x["composite_score"], reverse=True
    )[:25]

    # Holdings summary
    holdings_txt = ""
    if holdings:
        for h in holdings:
            pnl_sign = "+" if h["unrealised_pnl"] >= 0 else ""
            today_score = kpi_map.get(h["ticker"], {}).get("composite_score")
            score_str = f"today_score={today_score:.1f}" if today_score else "today_score=n/a"
            holdings_txt += (
                f"  {h['ticker']:<6} {h['shares']:.4f} shares @ ${h['avg_cost_per_share']:.2f} avg cost | "
                f"current ${h['current_price']:.2f} | value ${h['current_value']:.2f} | "
                f"P&L {pnl_sign}{h['unrealised_pnl']:.2f} ({pnl_sign}{h['unrealised_pnl_pct']:.1f}%) | {score_str}\n"
            )
    else:
        holdings_txt = "  None — fully in cash\n"

    # ── Risk flag logic ───────────────────────────────────────────────
    import datetime as dt

    def get_risk_flags(r):
        flags = []
        def _f(v, d=0.0):
            try: return float(v)
            except: return d
        beta = _f(r.get("beta"), 1.0)
        rsi  = _f(r.get("rsi_14"), None) if r.get("rsi_14") not in (None, "", "nan") else None
        vix  = _f(r.get("vix"), 20.0)
        pct_from_high = _f(r.get("pct_from_52w_high"), 0.0)
        price = _f(r.get("current_price"), 0.0)
        ar_raw = r.get("abnormal_return")
        sector = (r.get("sector") or "").lower()

        # High beta in fearful market
        if beta > 1.3 and vix > 20:
            flags.append(f"HIGH-BETA({beta:.1f}) in elevated VIX({vix:.0f})")

        # Overbought RSI
        if rsi and rsi > 70:
            flags.append(f"OVERBOUGHT RSI({rsi:.0f})")

        # Near 52-week high — limited upside
        if pct_from_high > -0.03:
            flags.append(f"NEAR 52W-HIGH({pct_from_high*100:.1f}%)")

        # Elevated VIX — general market fear
        if vix > 25:
            flags.append(f"HIGH-VIX({vix:.0f}) market fear")

        # Geopolitically sensitive sectors
        sensitive = ["technology", "semiconductor", "energy", "materials"]
        if any(s in sector for s in sensitive):
            flags.append(f"GEO-SENSITIVE sector({r.get('sector','?')})")

        # Earnings within 7 days (approximate: flag if abnormal_return is extreme)
        # Since we don't have earnings dates, flag very high recent abnormal returns
        ar = _f(ar_raw, 0.0)
        if abs(ar) > 0.04:
            flags.append(f"HIGH-VOLATILITY abnormal_return({ar*100:.1f}%)")

        return flags

    # Top candidates table with risk flags
    cand_txt = ""
    for r in candidates:
        held  = "HELD" if r["ticker"] in held_tickers else "    "
        flags = get_risk_flags(r)
        flag_str = " [RISK: " + ", ".join(flags) + "]" if flags else ""
        cand_txt += (
            f"  {held} {r['ticker']:<6} score={r['composite_score']:.1f} "
            f"t1={r.get('tier1_score') or 0:.0f} t2={r.get('tier2_score') or 0:.0f} "
            f"price=${r.get('current_price') or 0:.2f} "
            f"beta={r.get('beta') or 1.0:.2f} "
            f"rsi={r.get('rsi_14') or 'n/a'} "
            f"ma={('BULL' if 'BULL' in str(r.get('ma_signal','')) else 'BEAR') if r.get('ma_signal') else '?  '} "
            f"margin={((r.get('net_profit_margin') or 0)*100):.1f}% "
            f"from_52wh={((r.get('pct_from_52w_high') or 0)*100):.1f}% "
            f"sector={r.get('sector','?')}"
            f"{flag_str}\n"
        )

    prompt = f"""You are an AI paper trading agent managing a simulated brokerage account.
Your goal is to double the starting balance of $1,000 (reach ${TARGET_VALUE:.0f}) as efficiently as possible.
Today is {today}.

=== ACCOUNT STATUS ===
Account:        #{acct['account_number']}
Cash available: ${acct['cash_balance']:.2f}
Holdings value: ${acct['holdings_value']:.2f}
Total value:    ${acct['total_value']:.2f}
Goal:           ${TARGET_VALUE:.0f}  ({((acct['total_value']/TARGET_VALUE)*100):.1f}% of the way there)
Cash reserve:   Keep at least {CASH_RESERVE_PCT*100:.0f}% of total value in cash

=== CURRENT HOLDINGS ===
{holdings_txt}
=== TOP STOCK CANDIDATES (by composite KPI score) ===
Columns: score, tier1, tier2, price, beta, rsi, MA signal, net margin, % from 52w high, sector, [RISK flags]
{cand_txt}
=== RISK FLAG GUIDE ===
- HIGH-BETA + elevated VIX: stock will amplify market swings — avoid in fearful markets
- OVERBOUGHT RSI: momentum may be exhausted, pullback likely
- NEAR 52W-HIGH: limited upside headroom, asymmetric downside risk
- HIGH-VIX: broad market fear — prefer low-beta defensive stocks
- GEO-SENSITIVE: exposed to geopolitical shocks (trade wars, export controls, oil price)
- HIGH-VOLATILITY: large recent price swings — higher uncertainty

=== TRADING RULES ===
- Commission: ${COMMISSION:.2f} flat per trade
- Spread cost: {SPREAD_PCT*100:.2f}% of trade value
- Max positions: {MAX_POSITIONS} stocks simultaneously
- Max single position: {MAX_POSITION_PCT*100:.0f}% of total portfolio value
- Minimum trade value: ${MIN_TRADE_VALUE:.2f}
- You may BUY, SELL, or HOLD on each position
- You can buy fractional shares

BUY rules:
- Only buy stocks with composite_score >= 65
- Prefer stocks with LOW or NO risk flags — a slightly lower score with no flags beats a higher score with multiple flags
- Prefer beta < 1.2 when VIX > 22 (defensive posture in fearful markets)
- Prefer stocks at least 10% below their 52-week high (room to recover)
- Prefer RSI between 30-60 (not overbought, has momentum room)
- Concentrate capital: top 1-3 picks only, aim for positions > $150 each
- Favour the highest conviction pick (high score + low risk flags) with the most capital

SELL rules — only sell if ONE of these clearly applies:
- Score drops below 50 AND stays there for multiple days
- Stock has lost more than 20% from your average cost (hard stop-loss)
- A replacement scores 10+ points higher AND has fewer risk flags AND you have a free position slot
- DO NOT sell on a single bad day, normal dips (< 10%), or just because ranking shifted

HOLD bias:
- Each round-trip trade costs ~$10 in commissions — hold unless there is strong conviction to change
- Volatility is normal — only react to fundamental deterioration, not daily price noise

=== YOUR TASK ===
Analyse the situation, pay close attention to RISK flags before buying anything, and return ONLY a valid JSON object (no other text, no markdown) in this exact format:

{{
  "reasoning": "2-3 sentence summary of your overall strategy today",
  "trades": [
    {{
      "action": "BUY" or "SELL",
      "ticker": "XXXX",
      "amount_usd": 123.45,
      "reasoning": "one sentence why"
    }}
  ]
}}

If no trades are warranted today, return an empty trades array.
Amount_usd for SELL means the dollar value to sell (use current value for full exit).
Amount_usd for BUY means dollars to spend (before commission).
"""
    return prompt


# ── EXECUTE TRADES ──────────────────────────────────────────────────────────

def execute_buy(acct, holdings, ticker, amount_usd, price, kpi_row, dry_run):
    spread_cost  = round(amount_usd * SPREAD_PCT, 4)
    total_cost   = round(amount_usd + COMMISSION + spread_cost, 2)

    if total_cost > acct["cash_balance"]:
        print(f"    ✗ BUY {ticker}: insufficient cash (need ${total_cost:.2f}, have ${acct['cash_balance']:.2f})")
        return acct, holdings, None

    if amount_usd < MIN_TRADE_VALUE:
        print(f"    ✗ BUY {ticker}: trade value ${amount_usd:.2f} below minimum ${MIN_TRADE_VALUE:.2f}")
        return acct, holdings, None

    shares = round(amount_usd / price, 6)
    cash_before = acct["cash_balance"]

    if not dry_run:
        acct["cash_balance"] = round(cash_before - total_cost, 2)

        # Update holdings
        existing = next((h for h in holdings if h["ticker"] == ticker), None)
        if existing:
            new_shares    = existing["shares"] + shares
            new_total_cost= existing["total_cost"] + amount_usd
            existing.update({
                "shares":            round(new_shares, 6),
                "avg_cost_per_share":round(new_total_cost / new_shares, 4),
                "total_cost":        round(new_total_cost, 2),
                "current_price":     price,
                "current_value":     round(new_shares * price, 2),
                "last_updated":      str(datetime.date.today()),
            })
        else:
            holdings.append({
                "ticker":            ticker,
                "shares":            shares,
                "avg_cost_per_share":round(price, 4),
                "total_cost":        round(amount_usd, 2),
                "current_price":     price,
                "current_value":     round(shares * price, 2),
                "unrealised_pnl":    0.0,
                "unrealised_pnl_pct":0.0,
                "date_first_bought": str(datetime.date.today()),
                "last_updated":      str(datetime.date.today()),
            })

        holdings_val = sum(h["current_value"] for h in holdings)
        acct["holdings_value"] = round(holdings_val, 2)
        acct["total_value"]    = round(acct["cash_balance"] + holdings_val, 2)

        txn = {
            "txn_id":          next_txn_id(),
            "date":            str(datetime.date.today()),
            "type":            "BUY",
            "ticker":          ticker,
            "shares":          round(shares, 6),
            "price_per_share": price,
            "gross_value":     round(amount_usd, 2),
            "commission":      round(COMMISSION + spread_cost, 4),
            "net_value":       round(total_cost, 2),
            "cash_before":     cash_before,
            "cash_after":      acct["cash_balance"],
            "holdings_value":  acct["holdings_value"],
            "total_value":     acct["total_value"],
            "notes":           f"Score={kpi_row.get('composite_score','?')} signal={kpi_row.get('signal','?')}",
        }
        append_transaction(txn)

    print(f"    ✓ BUY  {ticker:6} {shares:.4f} shares @ ${price:.2f} | "
          f"cost ${total_cost:.2f} (incl. ${COMMISSION:.2f} comm + ${spread_cost:.4f} spread)")
    return acct, holdings, shares


def execute_sell(acct, holdings, ticker, amount_usd, price, dry_run):
    existing = next((h for h in holdings if h["ticker"] == ticker), None)
    if not existing:
        print(f"    ✗ SELL {ticker}: not in holdings")
        return acct, holdings, None

    # Cap sell amount at full position value
    amount_usd   = min(amount_usd, existing["current_value"])
    shares_to_sell = round(min(amount_usd / price, existing["shares"]), 6)
    gross_value  = round(shares_to_sell * price, 2)
    spread_cost  = round(gross_value * SPREAD_PCT, 4)
    net_proceeds = round(gross_value - COMMISSION - spread_cost, 2)
    cash_before  = acct["cash_balance"]

    if not dry_run:
        acct["cash_balance"] = round(cash_before + net_proceeds, 2)

        if shares_to_sell >= existing["shares"] - 0.0001:
            holdings = [h for h in holdings if h["ticker"] != ticker]
        else:
            frac = shares_to_sell / existing["shares"]
            existing["shares"]     = round(existing["shares"] - shares_to_sell, 6)
            existing["total_cost"] = round(existing["total_cost"] * (1 - frac), 2)
            existing["current_value"] = round(existing["shares"] * price, 2)
            existing["last_updated"]  = str(datetime.date.today())

        holdings_val = sum(h["current_value"] for h in holdings)
        acct["holdings_value"] = round(holdings_val, 2)
        acct["total_value"]    = round(acct["cash_balance"] + holdings_val, 2)

        txn = {
            "txn_id":          next_txn_id(),
            "date":            str(datetime.date.today()),
            "type":            "SELL",
            "ticker":          ticker,
            "shares":          shares_to_sell,
            "price_per_share": price,
            "gross_value":     gross_value,
            "commission":      round(COMMISSION + spread_cost, 4),
            "net_value":       net_proceeds,
            "cash_before":     cash_before,
            "cash_after":      acct["cash_balance"],
            "holdings_value":  acct["holdings_value"],
            "total_value":     acct["total_value"],
            "notes":           f"Sold {shares_to_sell:.4f} of {existing.get('shares',0)+shares_to_sell:.4f} shares",
        }
        append_transaction(txn)

    pnl = round(net_proceeds - (existing.get("total_cost",0) * (shares_to_sell / (existing.get("shares",shares_to_sell)+shares_to_sell))), 2)
    print(f"    ✓ SELL {ticker:6} {shares_to_sell:.4f} shares @ ${price:.2f} | "
          f"proceeds ${net_proceeds:.2f} (incl. ${COMMISSION:.2f} comm + ${spread_cost:.4f} spread)")
    return acct, holdings, net_proceeds


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="AI Paper Trading Agent")
    parser.add_argument("--kpi-file", default=DEFAULT_KPI_FILE,
                        help="Path to equity_kpi_results.csv")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Show decisions without executing trades")
    parser.add_argument("--api-key",  default=None,
                        help="Anthropic API key (or set ANTHROPIC_API_KEY env variable)")
    args = parser.parse_args()

    # Resolve API key: argument > environment variable
    api_key = args.api_key or ANTHROPIC_API_KEY
    if not api_key:
        print("\n✗ No Anthropic API key found.")
        print("  Get one free at: https://console.anthropic.com/")
        print("  Then either:")
        print("    python ai_trader.py --api-key sk-ant-...")
        print("    set ANTHROPIC_API_KEY=sk-ant-...   (Windows)")
        print("    export ANTHROPIC_API_KEY=sk-ant-...  (Mac/Linux)")
        sys.exit(1)

    today = str(datetime.date.today())
    print("\n" + "="*65)
    print("  AI PAPER TRADING AGENT")
    print(f"  Date    : {today}")
    print(f"  KPI file: {args.kpi_file}")
    if args.dry_run:
        print("  MODE    : DRY RUN — no trades will be executed")
    print("="*65)

    # ── Load data ────────────────────────────────────────────────────
    if not os.path.exists(args.kpi_file):
        print(f"\n✗ KPI file not found: {args.kpi_file}")
        print("  Run equity_kpi_analyzer.py first to generate it.")
        sys.exit(1)

    acct     = read_account()
    holdings = read_holdings()
    kpi_rows = load_kpi(args.kpi_file)
    kpi_map  = {r["ticker"]: r for r in kpi_rows}

    # Refresh holdings prices
    acct, holdings = portfolio_summary(acct, holdings, kpi_map)
    write_holdings(holdings)
    write_account(acct)

    print(f"\n  Cash:         ${acct['cash_balance']:>10.2f}")
    print(f"  Holdings:     ${acct['holdings_value']:>10.2f}")
    print(f"  Total:        ${acct['total_value']:>10.2f}  (goal: ${TARGET_VALUE:.0f})")
    print(f"  Progress:     {(acct['total_value']/TARGET_VALUE*100):.1f}%")

    if holdings:
        print(f"\n  Current positions ({len(holdings)}):")
        for h in sorted(holdings, key=lambda x: -x["current_value"]):
            sign = "+" if h["unrealised_pnl"] >= 0 else ""
            print(f"    {h['ticker']:<6}  {h['shares']:.4f} shares  "
                  f"${h['current_value']:.2f}  P&L: {sign}{h['unrealised_pnl']:.2f} ({sign}{h['unrealised_pnl_pct']:.1f}%)")

    # Check if goal already reached
    if acct["total_value"] >= TARGET_VALUE:
        print(f"\n🎉 GOAL REACHED! Portfolio value ${acct['total_value']:.2f} >= ${TARGET_VALUE:.0f}")
        print("   Consider withdrawing profits or raising the target.")
        return

    # ── Ask Claude for trading decisions ────────────────────────────
    print(f"\n  Asking Claude for today's trading decisions...")
    prompt   = build_prompt(acct, holdings, kpi_rows, today, kpi_map)

    try:
        response = ask_claude(prompt, api_key)
    except requests.exceptions.HTTPError as e:
        print(f"\n✗ Claude API error: {e}")
        try:
            print(f"  Detail: {e.response.json()}")
        except Exception:
            print(f"  Detail: {e.response.text[:300]}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Claude API error: {e}")
        sys.exit(1)

    # Parse JSON response
    try:
        # Strip any accidental markdown fences
        clean = response.strip().replace("```json","").replace("```","").strip()
        decision = json.loads(clean)
    except json.JSONDecodeError:
        print(f"\n✗ Could not parse Claude's response as JSON:")
        print(response[:500])
        sys.exit(1)

    print(f"\n  📋 Claude's reasoning:")
    print(f"     {decision.get('reasoning','')}")

    trades = decision.get("trades", [])
    if not trades:
        print("\n  No trades today — holding current positions.")
    else:
        print(f"\n  Executing {len(trades)} trade(s)...\n")

    # ── Execute trades ───────────────────────────────────────────────
    for trade in trades:
        action  = trade.get("action","").upper()
        ticker  = trade.get("ticker","")
        amount  = float(trade.get("amount_usd", 0))
        reason  = trade.get("reasoning","")
        kpi_row = kpi_map.get(ticker, {})
        price   = kpi_row.get("current_price")

        if not price:
            print(f"    ✗ {action} {ticker}: no price data available")
            continue

        print(f"  → {action} {ticker}  ${amount:.2f}  |  {reason}")

        if action == "BUY":
            # Enforce cash reserve
            reserve     = acct["total_value"] * CASH_RESERVE_PCT
            spendable   = acct["cash_balance"] - reserve
            amount      = min(amount, spendable)
            # Enforce max position size
            max_position= acct["total_value"] * MAX_POSITION_PCT
            existing_val= next((h["current_value"] for h in holdings if h["ticker"]==ticker), 0)
            amount      = min(amount, max_position - existing_val)

            if amount < MIN_TRADE_VALUE:
                print(f"    ✗ BUY {ticker}: adjusted amount ${amount:.2f} too small after constraints")
                continue
            acct, holdings, _ = execute_buy(acct, holdings, ticker, amount, price, kpi_row, args.dry_run)

        elif action == "SELL":
            acct, holdings, _ = execute_sell(acct, holdings, ticker, amount, price, args.dry_run)

    # ── Save state ───────────────────────────────────────────────────
    if not args.dry_run:
        acct, holdings = portfolio_summary(acct, holdings, kpi_map)
        write_account(acct)
        write_holdings(holdings)

    # ── Final summary ────────────────────────────────────────────────
    print(f"\n{'─'*65}")
    print(f"  END OF DAY SUMMARY")
    print(f"{'─'*65}")
    print(f"  Cash:         ${acct['cash_balance']:>10.2f}")
    print(f"  Holdings:     ${acct['holdings_value']:>10.2f}")
    print(f"  Total:        ${acct['total_value']:>10.2f}")
    print(f"  Goal:         ${TARGET_VALUE:>10.2f}  ({(acct['total_value']/TARGET_VALUE*100):.1f}% complete)")

    if holdings:
        print(f"\n  Positions ({len(holdings)}):")
        for h in sorted(holdings, key=lambda x: -x["current_value"]):
            sign = "+" if h["unrealised_pnl"] >= 0 else ""
            print(f"    {h['ticker']:<6}  {h['shares']:.4f} shares  "
                  f"${h['current_value']:.2f}  P&L: {sign}{h['unrealised_pnl']:.2f} ({sign}{h['unrealised_pnl_pct']:.1f}%)")

    print(f"\n  Transactions log: {TRANSACTIONS_FILE}")
    print("="*65 + "\n")


if __name__ == "__main__":
    main()

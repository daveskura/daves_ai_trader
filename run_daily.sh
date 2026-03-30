#!/usr/bin/env bash
# run_daily.sh — Linux equivalent of run_daily.bat

# ── Change to script directory ────────────────────────────────────────────
cd "$(dirname "$0")" || exit 1

# ── Weekend guard ─────────────────────────────────────────────────────────
DOW=$(date +%u)   # 1=Mon … 6=Sat, 7=Sun
if [ "$DOW" -ge 6 ]; then
    echo "Weekend - skipping."
    exit 0
fi

# ── Python availability check ─────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found on PATH. Install Python 3 and ensure it is in your PATH."
    exit 1
fi

# ── Detect current hour (local time) to choose run stage ──────────────────
# Stage 1 (pre-market news):  run before 9:30 AM  → hour 0-8
# Stage 2 (post-market full): run after  4:00 PM  → hour 16-23
# Between 9 AM - 4 PM:        market is open, skip automatic run
HOUR_NUM=$(date +%-H)   # %-H strips leading zero (GNU date)

echo
echo "============================================"
echo " DAILY RUN: $(date)  [hour=${HOUR_NUM}]"
echo "============================================"
echo

# ── Allow manual override: run_daily.sh --all | --news | --full ───────────
RUN_MODE="auto"
case "$1" in
    --all)  RUN_MODE="all"  ;;
    --news) RUN_MODE="news" ;;
    --full) RUN_MODE="full" ;;
esac

if [ "$RUN_MODE" = "auto" ]; then
    if [ "$HOUR_NUM" -lt 9 ]; then
        RUN_MODE="news"
        echo "[AUTO] Pre-market detected - running news strategies only"
    elif [ "$HOUR_NUM" -ge 16 ]; then
        RUN_MODE="full"
        echo "[AUTO] Post-market detected - running full pipeline"
    else
        echo "[AUTO] Market hours (9 AM - 4 PM) - skipping automatic run."
        echo "       Use run_daily.sh --news or --full to run manually."
        exit 0
    fi
fi

# ── Log rotation (keep last 90 lines) ────────────────────────────────────
log_rotate() {
    local log="daily_run.log"
    if [ -f "$log" ]; then
        local count
        count=$(wc -l < "$log")
        if [ "$count" -gt 90 ]; then
            tail -90 "$log" > "${log}.tmp" && mv "${log}.tmp" "$log"
        fi
    fi
}

# ═══════════════════════════════════════════════════════════════════
# STAGE 1 — PRE-MARKET NEWS RUN  (strategies 19 & 20 only)
# Best run 6:30–9:00 AM ET to catch overnight headlines before the
# market opens and prices them in.
# ═══════════════════════════════════════════════════════════════════
run_news() {
    echo "============================================"
    echo " STAGE 1 - PRE-MARKET NEWS RUN"
    echo " Fetching headlines and running strategies"
    echo " 19 (News macro) and 20 (News sentiment)"
    echo "============================================"
    echo

    echo "[NEWS 1/2] Running strategy 19 - News macro catalyst..."
    if ! python3 strategy_runner.py --strategy 19; then
        echo "ERROR: Strategy 19 failed."
        echo "$(date) Strategy 19 FAILED" >> daily_run.log
        exit 1
    fi
    echo "[NEWS 1/2] Done."
    echo

    echo "[NEWS 2/2] Running strategy 20 - News sentiment momentum..."
    if ! python3 strategy_runner.py --strategy 20; then
        echo "ERROR: Strategy 20 failed."
        echo "$(date) Strategy 20 FAILED" >> daily_run.log
        exit 1
    fi
    echo "[NEWS 2/2] Done."
    echo
}

# ═══════════════════════════════════════════════════════════════════
# STAGE 2 — POST-MARKET FULL RUN  (all active strategies)
# Best run 4:30–6:00 PM ET after market close when prices are settled.
# Strategies 19 & 20 reuse the morning news cache — no duplicate
# Claude API call.
# ═══════════════════════════════════════════════════════════════════
run_full() {
    echo "============================================"
    echo " STAGE 2 - POST-MARKET FULL RUN"
    echo "============================================"
    echo

    echo "[STEP 1] Running KPI analyser..."
    if ! python3 equity_kpi_analyzer.py --universe; then
        echo "ERROR: KPI analyser failed - aborting."
        echo "$(date) KPI analyser FAILED" >> daily_run.log
        exit 1
    fi
    echo "[STEP 1] Done."
    echo

    echo "[STEP 2] Running all strategies..."
    if ! python3 strategy_runner.py; then
        echo "ERROR: Strategy runner failed."
        echo "$(date) Strategy runner FAILED" >> daily_run.log
        exit 1
    fi
    echo "[STEP 2] Done."
    echo

    echo "============================================"
    echo " All done. Open leaderboard.html and drop"
    echo " leaderboard.csv onto it to see results."
    echo "============================================"
    echo

    echo "$(date) Full run complete" >> daily_run.log
    log_rotate
}

# ── Dispatch ──────────────────────────────────────────────────────────────
case "$RUN_MODE" in
    news)
        run_news
        echo "============================================"
        echo " Stage 1 complete. News macro cache saved."
        echo " Run again after 4:30 PM for full pipeline."
        echo "============================================"
        echo
        echo "$(date) News-only run complete" >> daily_run.log
        log_rotate
        ;;
    full)
        run_full
        ;;
    all)
        run_news
        run_full
        ;;
esac

exit 0

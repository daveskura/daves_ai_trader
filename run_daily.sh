#!/usr/bin/env bash
# run_daily.sh — Linux equivalent of run_daily.bat
# Logs pipeline events to MySQL via db_logger.py

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

# ── Logging helper ────────────────────────────────────────────────────────
# Writes one structured row to the run_logs MySQL table.
# Usage: db_log <stage> <level> <message>
# Falls back silently if the DB is down (db_logger.py handles that internally).
db_log() {
    local stage="$1"
    local level="$2"
    local message="$3"
    python3 db_logger.py \
        --stage    "$stage" \
        --level    "$level" \
        --message  "$message" \
        --no-echo 2>/dev/null || true
}

# ── Detect current hour (local time) to choose run stage ──────────────────
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
        db_log "auto" "INFO" "Pre-market run started (hour=${HOUR_NUM})"
    elif [ "$HOUR_NUM" -ge 16 ]; then
        RUN_MODE="full"
        echo "[AUTO] Post-market detected - running full pipeline"
        db_log "auto" "INFO" "Post-market run started (hour=${HOUR_NUM})"
    else
        echo "[AUTO] Market hours (9 AM - 4 PM) - skipping automatic run."
        echo "       Use run_daily.sh --news or --full to run manually."
        db_log "auto" "INFO" "Skipped — market hours (hour=${HOUR_NUM})"
        exit 0
    fi
fi

# ═══════════════════════════════════════════════════════════════════
# STAGE 1 — PRE-MARKET NEWS RUN  (strategies 19 & 20 only)
# ═══════════════════════════════════════════════════════════════════
run_news() {
    echo "============================================"
    echo " STAGE 1 - PRE-MARKET NEWS RUN"
    echo " Fetching headlines and running strategies"
    echo " 19 (News macro) and 20 (News sentiment)"
    echo "============================================"
    echo

    db_log "news" "INFO" "Stage 1 started — strategies 19 and 20"

    echo "[NEWS 1/2] Running strategy 19 - News macro catalyst..."
    db_log "news" "INFO" "Running strategy 19 (News macro catalyst)"
    if ! python3 strategy_runner.py --strategy 19; then
        echo "ERROR: Strategy 19 failed."
        db_log "news" "ERROR" "Strategy 19 FAILED — pipeline aborted"
        exit 1
    fi
    db_log "news" "INFO" "Strategy 19 completed OK"
    echo "[NEWS 1/2] Done."
    echo

    echo "[NEWS 2/2] Running strategy 20 - News sentiment momentum..."
    db_log "news" "INFO" "Running strategy 20 (News sentiment momentum)"
    if ! python3 strategy_runner.py --strategy 20; then
        echo "ERROR: Strategy 20 failed."
        db_log "news" "ERROR" "Strategy 20 FAILED — pipeline aborted"
        exit 1
    fi
    db_log "news" "INFO" "Strategy 20 completed OK"
    echo "[NEWS 2/2] Done."
    echo
}

# ═══════════════════════════════════════════════════════════════════
# STAGE 2 — POST-MARKET FULL RUN  (all active strategies)
# ═══════════════════════════════════════════════════════════════════
run_full() {
    echo "============================================"
    echo " STAGE 2 - POST-MARKET FULL RUN"
    echo "============================================"
    echo

    db_log "full" "INFO" "Stage 2 started — full pipeline"

    echo "[STEP 1] Running KPI analyser..."
    db_log "full" "INFO" "Step 1: KPI analyser started"
    if ! python3 equity_kpi_analyzer.py --universe; then
        echo "ERROR: KPI analyser failed - aborting."
        db_log "full" "ERROR" "Step 1: KPI analyser FAILED — pipeline aborted"
        exit 1
    fi
    db_log "full" "INFO" "Step 1: KPI analyser completed OK"
    echo "[STEP 1] Done."
    echo

    echo "[STEP 2] Refreshing live quotes and abnormal_return..."
    db_log "full" "INFO" "Step 2: Quote updater started"
    if ! python3 update_quotes.py --no-kpi-refresh; then
        echo "WARNING: Quote updater failed — strategy runner will use KPI file prices."
        db_log "full" "WARNING" "Step 2: Quote updater FAILED (non-fatal — using KPI prices)"
        # Non-fatal: strategy_runner can still run with KPI file prices
    else
        db_log "full" "INFO" "Step 2: Quote updater completed OK"
    fi
    echo "[STEP 2] Done."
    echo

    echo "[STEP 3] Running all strategies..."
    db_log "full" "INFO" "Step 3: Strategy runner started"
    if ! python3 strategy_runner.py; then
        echo "ERROR: Strategy runner failed."
        db_log "full" "ERROR" "Step 3: Strategy runner FAILED"
        exit 1
    fi
    db_log "full" "INFO" "Step 3: Strategy runner completed OK"
    echo "[STEP 3] Done."
    echo

    echo "============================================"
    echo " All done. Steps: KPI --> quotes --> strategies."
    echo " Run: python3 show_results.py"
    echo " to view the leaderboard from MySQL."
    echo "============================================"
    echo

    db_log "full" "INFO" "Stage 2 complete — full pipeline finished OK"
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
        db_log "news" "INFO" "Stage 1 complete — news-only run finished OK"
        ;;
    full)
        run_full
        ;;
    all)
        db_log "auto" "INFO" "Running both stages (--all flag)"
        run_news
        run_full
        ;;
esac

exit 0

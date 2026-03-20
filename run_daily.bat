@echo off
chcp 65001 > nul
set PYTHONIOENCODING=utf-8

cd /d "%~dp0"

:: ── Weekend guard ─────────────────────────────────────────────────────────
powershell -nologo -noprofile -command "if ((Get-Date).DayOfWeek -in 'Saturday','Sunday') { exit 1 } else { exit 0 }"
if errorlevel 1 (
    echo Weekend - skipping.
    exit /b 0
)

:: ── Python availability check ─────────────────────────────────────────────
where python >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found on PATH. Install Python and ensure it is in your PATH.
    call :pause_if_interactive
    exit /b 1
)

:: ── Detect current hour (local time) to choose run stage ──────────────────
:: Stage 1 (pre-market news):  run before 9:30 AM  → hour 0-8
:: Stage 2 (post-market full): run after  4:00 PM  → hour 16-23
:: Between 9:30 AM - 4 PM:     market is open, skip automatic run
for /f "tokens=1 delims=:" %%h in ("%TIME: =0%") do set HOUR=%%h
:: Strip leading zero so numeric comparison works
set /a HOUR_NUM=%HOUR%

echo.
echo ============================================
echo  DAILY RUN: %DATE% %TIME%  [hour=%HOUR_NUM%]
echo ============================================
echo.

:: Allow manual override: run_daily.bat --all | --news | --full
set RUN_MODE=auto
if /i "%~1"=="--all"   set RUN_MODE=all
if /i "%~1"=="--news"  set RUN_MODE=news
if /i "%~1"=="--full"  set RUN_MODE=full

if "%RUN_MODE%"=="auto" (
    if %HOUR_NUM% LSS 9 (
        set RUN_MODE=news
        echo [AUTO] Pre-market detected - running news strategies only
    ) else if %HOUR_NUM% GEQ 16 (
        set RUN_MODE=full
        echo [AUTO] Post-market detected - running full pipeline
    ) else (
        echo [AUTO] Market hours ^(9 AM - 4 PM^) - skipping automatic run.
        echo        Use run_daily.bat --news or --full to run manually.
        call :pause_if_interactive
        exit /b 0
    )
)

:: ═══════════════════════════════════════════════════════════════════
:: STAGE 1 — PRE-MARKET NEWS RUN  (strategies 19 & 20 only)
:: Best run 6:30–9:00 AM ET to catch overnight headlines before the
:: market opens and prices them in.
:: ═══════════════════════════════════════════════════════════════════
if "%RUN_MODE%"=="news" goto :run_news
if "%RUN_MODE%"=="all"  goto :run_news
goto :run_full

:run_news
echo ============================================
echo  STAGE 1 - PRE-MARKET NEWS RUN
echo  Fetching headlines and running strategies
echo  19 ^(News macro^) and 20 ^(News sentiment^)
echo ============================================
echo.

echo [NEWS 1/2] Running strategy 19 - News macro catalyst...
python strategy_runner.py --strategy 19
if errorlevel 1 (
    echo ERROR: Strategy 19 failed.
    echo %DATE% %TIME% Strategy 19 FAILED >> daily_run.log
    call :pause_if_interactive
    exit /b 1
)
echo [NEWS 1/2] Done.
echo.

echo [NEWS 2/2] Running strategy 20 - News sentiment momentum...
python strategy_runner.py --strategy 20
if errorlevel 1 (
    echo ERROR: Strategy 20 failed.
    echo %DATE% %TIME% Strategy 20 FAILED >> daily_run.log
    call :pause_if_interactive
    exit /b 1
)
echo [NEWS 2/2] Done.
echo.

if "%RUN_MODE%"=="news" (
    echo ============================================
    echo  Stage 1 complete. News macro cache saved.
    echo  Run again after 4:30 PM for full pipeline.
    echo ============================================
    echo.
    echo %DATE% %TIME% News-only run complete >> daily_run.log
    call :log_rotate
    call :pause_if_interactive
    exit /b 0
)

:: Fall through to full run if mode=all
goto :run_full

:: ═══════════════════════════════════════════════════════════════════
:: STAGE 2 — POST-MARKET FULL RUN  (all 20 strategies)
:: Best run 4:30–6:00 PM ET after market close when prices are settled.
:: Strategies 19 & 20 reuse the morning news cache — no duplicate
:: Claude API call.
:: ═══════════════════════════════════════════════════════════════════
:run_full
echo ============================================
echo  STAGE 2 - POST-MARKET FULL RUN
echo ============================================
echo.

echo [STEP 1] Running KPI analyser...
python equity_kpi_analyzer.py --universe
if errorlevel 1 (
    echo ERROR: KPI analyser failed - aborting.
    echo %DATE% %TIME% KPI analyser FAILED >> daily_run.log
    call :pause_if_interactive
    exit /b 1
)
echo [STEP 1] Done.
echo.

echo [STEP 2] Running all strategies ^(1-20^)...
python strategy_runner.py
if errorlevel 1 (
    echo ERROR: Strategy runner failed.
    echo %DATE% %TIME% Strategy runner FAILED >> daily_run.log
    call :pause_if_interactive
    exit /b 1
)
echo [STEP 2] Done.
echo.

echo ============================================
echo  All done. Open leaderboard.html and drop
echo  leaderboard.csv onto it to see results.
echo ============================================
echo.

echo %DATE% %TIME% Full run complete >> daily_run.log
call :log_rotate
call :pause_if_interactive
exit /b 0

:: ── Log rotation (keep last 90 lines) ────────────────────────────────────
:log_rotate
powershell -nologo -noprofile -command ^
  "try { $lines = Get-Content 'daily_run.log' -ErrorAction Stop; if ($lines.Count -gt 90) { $lines | Select-Object -Last 90 | Set-Content 'daily_run.log' } } catch {}"
exit /b 0

:: ── Pause only when a console is attached (not Task Scheduler) ───────────
:pause_if_interactive
if defined SESSIONNAME (
    pause
)
exit /b 0

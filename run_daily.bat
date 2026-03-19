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

echo.
echo ============================================
echo  DAILY RUN: %DATE% %TIME%
echo ============================================
echo.

:: ── Step 1: KPI analyser ──────────────────────────────────────────────────
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

:: ── Step 2: Strategy runner ───────────────────────────────────────────────
echo [STEP 2] Running strategy runner...
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

:: ── Log with rotation (keep last 90 days / ~90 lines) ────────────────────
echo %DATE% %TIME% Daily run complete >> daily_run.log
powershell -nologo -noprofile -command ^
  "try { $lines = Get-Content 'daily_run.log' -ErrorAction Stop; if ($lines.Count -gt 90) { $lines | Select-Object -Last 90 | Set-Content 'daily_run.log' } } catch {}"

:: Only pause when run interactively (not from Task Scheduler)
call :pause_if_interactive
exit /b 0

:: ── Subroutine: pause only when a console is attached ────────────────────
:pause_if_interactive
:: If launched by Task Scheduler, %SESSIONNAME% is empty; skip pause.
if defined SESSIONNAME (
    pause
)
exit /b 0

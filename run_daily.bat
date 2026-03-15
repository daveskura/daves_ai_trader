@echo off
chcp 65001 > nul
set PYTHONIOENCODING=utf-8

cd /d "%~dp0"

powershell -nologo -noprofile -command "if ((Get-Date).DayOfWeek -in 'Saturday','Sunday') { exit 1 } else { exit 0 }"
if errorlevel 1 (
    echo Weekend - skipping.
    exit /b 0
)

echo.
echo ============================================
echo  DAILY RUN: %DATE% %TIME%
echo ============================================
echo.

echo [STEP 1] Running KPI analyser...
python equity_kpi_analyzer.py --universe
if errorlevel 1 (
    echo KPI analyser failed - aborting
    pause
    exit /b 1
)
echo [STEP 1] Done.
echo.

echo [STEP 2] Running strategy runner...
python strategy_runner.py
if errorlevel 1 (
    echo Strategy runner failed
    pause
    exit /b 1
)
echo [STEP 2] Done.
echo.

echo ============================================
echo  All done. Open leaderboard.html and drop
echo  leaderboard.csv onto it to see results.
echo ============================================
echo.

echo %DATE% %TIME% Daily run complete >> daily_run.log
pause

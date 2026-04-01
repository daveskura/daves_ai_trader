@echo off
chcp 65001 > nul
set PYTHONIOENCODING=utf-8

cd /d "%~dp0"

echo.
echo ============================================
echo  PAPER TRADING -- TEST RUN
echo  %DATE% %TIME%
echo  Bypasses weekend/hour guards.
echo  Tests DB, KPI, strategies, quotes, leaderboard.
echo ============================================
echo.

:: -- Python availability check ---------------------------------------------
where python >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found on PATH.
    call :pause_exit 1
)

:: -- MySQL / db.py connectivity check -------------------------------------
echo [TEST 0/6] Checking MySQL connection via db.py...
python test_db_connect.py
if errorlevel 1 (
    echo ERROR: Cannot connect to MySQL.
    echo        Check DB_HOST / DB_USER / DB_PASSWORD in your .env file.
    echo        Run setup_mysql.sql first if you have not already.
    call :pause_exit 1
)
echo [TEST 0/6] Done.
echo.

:: -- Hard-reset accounts to a clean slate ----------------------------------
echo [TEST 1/6] skipping 

:: -- KPI analyser ----------------------------------------------------------
echo [TEST 2/6] Running KPI analyser (--universe)...
echo            Fetches live data from Yahoo Finance -- may take 1-2 min.
python equity_kpi_analyzer.py --universe
if errorlevel 1 (
    echo ERROR: KPI analyser failed.
    call :pause_exit 1
)
echo [TEST 2/6] Done.
echo.

:: -- News strategies (calls Claude API) ------------------------------------
echo [TEST 3/6] Running news strategies 19 and 20...
python strategy_runner.py --strategy 19
if errorlevel 1 (
    echo ERROR: Strategy 19 failed.
    call :pause_exit 1
)
python strategy_runner.py --strategy 20
if errorlevel 1 (
    echo ERROR: Strategy 20 failed.
    call :pause_exit 1
)
echo [TEST 3/6] Done.
echo.

:: -- Full strategy run (calls Claude API for each strategy) ----------------
echo [TEST 4/6] Running all strategies...
echo            This will take several minutes.
python strategy_runner.py
if errorlevel 1 (
    echo ERROR: strategy_runner.py failed.
    call :pause_exit 1
)
echo [TEST 4/6] Done.
echo.

:: -- Quote updater ---------------------------------------------------------
echo [TEST 5/6] Running quote updater (--show)...
python update_quotes.py --show
if errorlevel 1 (
    echo WARNING: update_quotes.py returned an error.
    echo          Non-fatal if no positions were opened yet.
)
echo [TEST 5/6] Done.
echo.

:: -- Verify leaderboard rows in MySQL -------------------------------------
echo [TEST 6/6] Verifying leaderboard rows in MySQL...
python test_db_leaderboard.py
if errorlevel 1 (
    echo ERROR: No leaderboard rows found in MySQL after full run.
    call :pause_exit 1
)
echo [TEST 6/6] Done.
echo.

:: -- Summary ---------------------------------------------------------------
echo ============================================
echo  ALL TESTS PASSED  --  %DATE% %TIME%
echo ============================================
echo.
echo  To inspect results, run:  python show_results.py
echo.

echo %DATE% %TIME% Test run PASSED >> daily_run.log
call :pause_exit 0

:pause_exit
exit /b %1

@echo off
REM ============================================
REM FreshIn Daily Merge - Setup Script
REM ============================================
REM This script:
REM 1. Imports the scheduled task for daily 9AM execution
REM 2. Runs the merge immediately
REM
REM Email notifications will be sent to:
REM   - warren@framecore.ai
REM   - roy@framecore.ai
REM ============================================

echo ============================================
echo FreshIn Daily Merge Setup
echo ============================================
echo.

cd /d "C:\Users\RoyT6\Downloads\FreshIn Engine"

REM Check if running as admin (required for Task Scheduler)
net session >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Not running as administrator.
    echo [INFO] Will attempt to import task anyway...
    echo [INFO] If this fails, right-click this script and "Run as administrator"
    echo.
)

REM Delete existing task if present
echo [1/3] Removing any existing scheduled task...
schtasks /delete /tn "FreshIn_Daily_Merge_9AM" /f >nul 2>&1

REM Import the new scheduled task
echo [2/3] Creating scheduled task for daily 9AM execution...
schtasks /create /xml "C:\Users\RoyT6\Downloads\FreshIn Engine\FreshIn_Daily_Merge_9AM.xml" /tn "FreshIn_Daily_Merge_9AM"

if %ERRORLEVEL% EQU 0 (
    echo [SUCCESS] Scheduled task created!
    echo           Task will run daily at 9:00 AM
) else (
    echo [WARN] Could not create scheduled task.
    echo        You may need to run this as Administrator.
    echo        Or manually import the XML file via Task Scheduler.
)

echo.
echo [3/3] Running initial merge NOW...
echo ============================================
echo.

REM Run the merge immediately
call "C:\Users\RoyT6\Downloads\FreshIn Engine\run_freshin_merge_daily.bat"

echo.
echo ============================================
echo Setup complete!
echo.
echo The merge will run automatically every day at 9:00 AM.
echo Email notifications will be sent to:
echo   - warren@framecore.ai
echo   - roy@framecore.ai
echo.
echo To run manually: run_freshin_merge_daily.bat
echo To view logs: Check merger_log.txt and Audit Reports folder
echo ============================================

pause

@echo off
REM ============================================================================
REM FreshIn Daily Merge Setup Script
REM Installs scheduled task for 9 AM daily metadata merge
REM ============================================================================

echo.
echo ============================================================
echo   FreshIn Daily Metadata Merger - Setup
echo   Sends notifications to warren@framecore.ai
echo   and roy@framecore.ai on completion
echo ============================================================
echo.

REM Check for admin privileges
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: This script requires administrator privileges.
    echo Please right-click and select "Run as administrator"
    pause
    exit /b 1
)

REM Create the scheduled task
echo Creating scheduled task "FreshIn_Daily_9AM"...
schtasks /create /tn "FreshIn_Daily_9AM" /xml "C:\Users\RoyT6\Downloads\FreshIn\FreshIn_Daily_9AM.xml" /f

if %errorLevel% equ 0 (
    echo.
    echo SUCCESS: Scheduled task created!
    echo.
    echo Task Details:
    echo   - Name: FreshIn_Daily_9AM
    echo   - Schedule: Daily at 9:00 AM
    echo   - Action: Runs freshin_meta_merger_v27.72.py
    echo   - Notifications: warren@framecore.ai, roy@framecore.ai
    echo.
    echo IMPORTANT: Configure email credentials:
    echo   1. Open System Properties ^> Environment Variables
    echo   2. Add these system variables:
    echo      FRESHIN_SMTP_SERVER = smtp.gmail.com (or your SMTP server)
    echo      FRESHIN_SMTP_PORT = 587
    echo      FRESHIN_SENDER_EMAIL = your-sender@email.com
    echo      FRESHIN_SENDER_PASSWORD = your-app-password
    echo.
    echo   For Gmail, use an App Password (not your regular password).
    echo   See: https://support.google.com/accounts/answer/185833
    echo.
) else (
    echo.
    echo ERROR: Failed to create scheduled task.
    echo Please check the XML file and try again.
)

echo.
echo To run the merge NOW, use:
echo   python "C:\Users\RoyT6\Downloads\FreshIn\freshin_meta_merger_v27.72.py"
echo.
echo To view/modify the task:
echo   - Open Task Scheduler (taskschd.msc)
echo   - Look for "FreshIn_Daily_9AM" in the root folder
echo.

pause

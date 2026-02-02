@echo off
REM FreshIn Metadata Merger - Daily 9AM Task
REM Merges new metadata from FreshIn sources into BFD_VIEWS master database
REM Sends email notifications to warren@framecore.ai and roy@framecore.ai

cd /d "C:\Users\RoyT6\Downloads\FreshIn Engine"

REM Run via WSL (Ubuntu/Debian)
wsl python3 "/mnt/c/Users/RoyT6/Downloads/FreshIn Engine/freshin_meta_merger_v27.72.py"

REM Capture exit code
set EXIT_CODE=%ERRORLEVEL%

if %EXIT_CODE% EQU 0 (
    echo [SUCCESS] FreshIn merge completed successfully at %DATE% %TIME%
) else (
    echo [ERROR] FreshIn merge failed with exit code %EXIT_CODE% at %DATE% %TIME%
)

exit /b %EXIT_CODE%

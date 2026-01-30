@echo off
REM ============================================================================
REM SCHIG - Sacred Child Gatherer - Daily Collection Runner
REM ============================================================================
REM Runs the nightly data collection from all configured APIs
REM Schedule via Windows Task Scheduler at 02:00 AM
REM ============================================================================

cd /d "C:\Users\RoyT6\Downloads\FreshIn"

echo ============================================================================
echo SCHIG - Sacred Child Gatherer - Starting Collection
echo Date: %date% Time: %time%
echo ============================================================================

"C:\Python314\python.exe" "C:\Users\RoyT6\Downloads\FreshIn\sacred_child_gatherer.py" --scheduled

echo ============================================================================
echo SCHIG Collection completed at %date% %time%
echo ============================================================================

echo SCHIG Collection completed at %date% %time% >> "C:\Users\RoyT6\Downloads\FreshIn\collection_log.txt"

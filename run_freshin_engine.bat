@echo off
REM ============================================================================
REM VDB FreshIn! Engine Daily Runner
REM Scheduled: 11:00 AM Daily
REM Task: VDB_FreshIn_Daily_11AM
REM ============================================================================

echo.
echo ============================================================================
echo     VDB FRESHIN! ENGINE - DAILY RUN
echo     %DATE% %TIME%
echo ============================================================================
echo.

REM Log file location
set LOGFILE=C:\Users\RoyT6\Downloads\Fresh In!\freshin_daily.log

REM Log start
echo [%DATE% %TIME%] Starting VDB FreshIn! Engine >> "%LOGFILE%"

REM Run via WSL with GPU support
wsl bash -c "cd '/mnt/c/Users/RoyT6/Downloads/GPU Enablement' && source ~/miniforge3/etc/profile.d/conda.sh && conda activate rapids-24.12 && export LD_LIBRARY_PATH=/usr/lib/wsl/lib:$LD_LIBRARY_PATH && export NUMBA_CUDA_USE_NVIDIA_BINDING=1 && python '/mnt/c/Users/RoyT6/Downloads/Fresh In!/vdb_freshin_engine.py'" >> "%LOGFILE%" 2>&1

REM Log completion
echo [%DATE% %TIME%] VDB FreshIn! Engine completed >> "%LOGFILE%"
echo.

echo ============================================================================
echo     COMPLETE - Check Audit Reports folder for details
echo ============================================================================

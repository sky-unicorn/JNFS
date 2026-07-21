@echo off
setlocal

set "APP_HOME=%~dp0.."
set "PID_DIR=%APP_HOME%\pids"

set SERVICE=%1

if "%SERVICE%"=="" (
    echo Stopping all services...
    call "%~f0" datanode
    call "%~f0" namenode
    call "%~f0" registry
    goto :eof
)

if not "%SERVICE%"=="registry" if not "%SERVICE%"=="namenode" if not "%SERVICE%"=="datanode" (
    echo Unknown service: %SERVICE%
    echo Usage: stop.bat [registry|namenode|datanode]
    goto :eof
)

set "PID_FILE=%PID_DIR%\%SERVICE%.pid"

if not exist "%PID_FILE%" (
    echo %SERVICE% is not running ^(no PID file at %PID_FILE%^).
    goto :eof
)

set /p PID=<"%PID_FILE%"

rem Check if process is still running
tasklist /fi "PID eq %PID%" /fo csv /nh 2>nul | findstr /i "%PID%" >nul
if errorlevel 1 (
    echo %SERVICE% is not running ^(stale PID %PID%, cleaning up^).
    del "%PID_FILE%"
    goto :eof
)

echo Stopping %SERVICE% ^(PID %PID%^)...
rem Send graceful termination (no /f flag) to trigger JVM shutdown hook
taskkill /pid %PID% >nul 2>&1

rem Wait up to 30 seconds for graceful shutdown
set WAITED=0
:wait_loop
if %WAITED% geq 30 goto force_kill
tasklist /fi "PID eq %PID%" /fo csv /nh 2>nul | findstr /i "%PID%" >nul
if errorlevel 1 (
    echo %SERVICE% stopped gracefully.
    del "%PID_FILE%"
    goto :eof
)
timeout /t 1 /nobreak >nul
set /a WAITED+=1
goto wait_loop

:force_kill
echo Warning: %SERVICE% did not stop within 30s, force killing...
taskkill /f /pid %PID% >nul 2>&1
del "%PID_FILE%"
echo %SERVICE% killed.

endlocal
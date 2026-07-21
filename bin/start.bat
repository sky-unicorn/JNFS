@echo off
setlocal

set "APP_HOME=%~dp0.."
rem Normalize APP_HOME to an absolute path (resolves the trailing .. so logs are clean)
for %%I in ("%APP_HOME%") do set "APP_HOME=%%~fI"
set "CONF_DIR=%APP_HOME%\conf"
set "LIB_DIR=%APP_HOME%\lib"
set "PID_DIR=%APP_HOME%\pids"

rem Service selection: registry, namenode, datanode
set SERVICE=%1

if "%SERVICE%"=="" (
    echo Starting all services...
    call "%~f0" registry
    call "%~f0" namenode
    call "%~f0" datanode
    goto :eof
)

if "%SERVICE%"=="registry" (
    set MAIN_CLASS=org.jnfs.registry.RegistryServer
) else if "%SERVICE%"=="namenode" (
    set MAIN_CLASS=org.jnfs.namenode.NameNodeServer
) else if "%SERVICE%"=="datanode" (
    set MAIN_CLASS=org.jnfs.datanode.DataNodeServer
) else (
    echo Unknown service: %SERVICE%
    echo Usage: start.bat [registry|namenode|datanode]
    goto :eof
)

echo Starting %SERVICE%...

rem Create pids directory
if not exist "%PID_DIR%" mkdir "%PID_DIR%"

set "PID_FILE=%PID_DIR%\%SERVICE%.pid"

rem Check if already running
if exist "%PID_FILE%" (
    set /p EXISTING_PID=<"%PID_FILE%"
    tasklist /fi "PID eq %EXISTING_PID%" /fo csv /nh 2>nul | findstr /i "%EXISTING_PID%" >nul
    if not errorlevel 1 (
        echo Error: %SERVICE% is already running with PID %EXISTING_PID%
        goto :eof
    ) else (
        echo Warning: stale PID file detected, removing %PID_FILE%
        del "%PID_FILE%"
    )
)

rem Launch java in a new console window and capture its PID via PowerShell
rem Using -ArgumentList array avoids embedded quote issues with paths containing spaces
rem -WorkingDirectory pins the JVM cwd to APP_HOME so relative paths resolve there
powershell -NoProfile -Command "$jvmArgs = @('-DAPP_HOME=%APP_HOME%','-Dlogback.configurationFile=%CONF_DIR%\logback-%SERVICE%.xml','-cp','%CONF_DIR%;%LIB_DIR%\*','%MAIN_CLASS%'); $p = Start-Process -FilePath java -ArgumentList $jvmArgs -WorkingDirectory '%APP_HOME%' -PassThru; $p.Id | Out-File -FilePath '%PID_FILE%' -Encoding ascii -NoNewline"

rem Verify PID file was written
if not exist "%PID_FILE%" (
    echo Error: failed to start %SERVICE% ^(PID file not created^)
    goto :eof
)

set /p STARTED_PID=<"%PID_FILE%"
echo %SERVICE% started with PID %STARTED_PID%

endlocal
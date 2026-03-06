@echo off
setlocal

set "APP_HOME=%~dp0.."
set "CONF_DIR=%APP_HOME%\conf"
set "LIB_DIR=%APP_HOME%\lib"

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
start "JNFS %SERVICE%" cmd /k java -DAPP_HOME="%APP_HOME%" -Dlogback.configurationFile="%CONF_DIR%\logback-%SERVICE%.xml" -cp "%CONF_DIR%;%LIB_DIR%\*" %MAIN_CLASS%

endlocal

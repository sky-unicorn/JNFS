@echo off
setlocal

set APP_HOME=%~dp0..
set CONF_DIR=%APP_HOME%\conf
set LIB_DIR=%APP_HOME%\lib

rem 选择要启动的服务: registry, namenode, datanode
set SERVICE=%1

if "%SERVICE%"=="" (
    echo Usage: start.bat [registry|namenode|datanode|example]
    goto :eof
)

if "%SERVICE%"=="registry" (
    set MAIN_CLASS=org.jnfs.registry.RegistryServer
    set JAR_NAME=jnfs-registry-*.jar
) else if "%SERVICE%"=="namenode" (
    set MAIN_CLASS=org.jnfs.namenode.NameNodeServer
    set JAR_NAME=jnfs-namenode-*.jar
) else if "%SERVICE%"=="datanode" (
    set MAIN_CLASS=org.jnfs.datanode.DataNodeServer
    set JAR_NAME=jnfs-datanode-*.jar
) else if "%SERVICE%"=="example" (
    set MAIN_CLASS=org.jnfs.example.ExampleApp
    set JAR_NAME=jnfs-example-*.jar
) else (
    echo Unknown service: %SERVICE%
    goto :eof
)

echo Starting %SERVICE%...
java -DAPP_HOME="%APP_HOME%" -Dlogback.configurationFile="%CONF_DIR%\logback-%SERVICE%.xml" -cp "%CONF_DIR%;%LIB_DIR%\*" %MAIN_CLASS%

endlocal

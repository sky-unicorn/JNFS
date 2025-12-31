@echo off
setlocal

cd /d %~dp0..

set JAR_PATH=jnfs-registry\target\jnfs-registry-0.0.1-SNAPSHOT.jar

if not exist "%JAR_PATH%" (
    echo Error: Cannot find %JAR_PATH%
    echo Please run 'mvn package' first.
    pause
    exit /b 1
)

echo Starting JNFS Registry...
title JNFS Registry
java -jar "%JAR_PATH%"
pause

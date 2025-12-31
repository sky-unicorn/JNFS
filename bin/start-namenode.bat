@echo off
setlocal

:: Switch to project root directory
cd /d %~dp0..

set JAR_NAME=JNFS-0.0.1-SNAPSHOT.jar
set TARGET_DIR=target

if exist "%TARGET_DIR%\%JAR_NAME%" (
    set JAR_PATH=%TARGET_DIR%\%JAR_NAME%
) else (
    if exist "%JAR_NAME%" (
        set JAR_PATH=%JAR_NAME%
    ) else (
        echo Error: Cannot find %JAR_NAME% in target directory or current directory.
        echo Please run 'mvn package' first.
        pause
        exit /b 1
    )
)

echo Starting JNFS NameNode Server...
title JNFS NameNode
java -cp "%JAR_PATH%" org.jnfs.namenode.NameNodeServer
pause

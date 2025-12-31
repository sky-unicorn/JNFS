@echo off
setlocal

cd /d %~dp0..

set JAR_PATH=jnfs-namenode\target\jnfs-namenode-0.0.1-SNAPSHOT.jar

if not exist "%JAR_PATH%" (
    echo Error: Cannot find %JAR_PATH%
    echo Please run 'mvn package' first.
    pause
    exit /b 1
)

echo Starting JNFS NameNode...
title JNFS NameNode
java -jar "%JAR_PATH%"
pause

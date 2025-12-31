@echo off
setlocal

:: Switch to project root directory
cd /d %~dp0..

echo Starting all JNFS components...

start "JNFS Registry" cmd /c "bin\start-registry.bat"
timeout /t 5

start "JNFS NameNode" cmd /c "bin\start-namenode.bat"
timeout /t 5

start "JNFS DataNode" cmd /c "bin\start-datanode.bat"

echo All components started.
pause

@echo off
rem Automatic setup for Windows: double-click this file, or run it in a terminal.
rem It starts install.ps1 (same folder) with the options you pass, e.g.:  install.cmd -Mode test
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install.ps1" %*
set RC=%ERRORLEVEL%
echo.
if "%RC%"=="0" (echo Setup finished.) else (echo Setup stopped with exit code %RC%. Read the messages above.)
pause
exit /b %RC%

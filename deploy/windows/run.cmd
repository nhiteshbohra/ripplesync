@echo off
rem run.cmd - run xrpl_ingest from the repo's .venv with the service environment.
rem NOT TESTED ON A REAL HOST as a scheduled task (written and reviewed only).
rem
rem   deploy\windows\run.cmd tail | backfill | status | doctor | ...   (all args go to xrpl_ingest)
rem
rem Used by install-task.ps1 (Task Scheduler). Sets PYTHONUTF8=1, PYTHONUNBUFFERED=1,
rem XRPL_INGEST_HOME=<repo> and LOG_FILE=logs\xrpl_ingest_<command>.log (each only if not already set
rem in the environment). Exit code = xrpl_ingest's.
rem Graceful stop = the stop file, e.g. a line tail=0 in process_status.txt (Stop-ScheduledTask
rem kills the process at once).
setlocal EnableExtensions DisableDelayedExpansion

for %%I in ("%~dp0..\..") do set "REPO=%%~fI"
cd /d "%REPO%" || exit /b 2

set "PYTHONUTF8=1"
set "PYTHONUNBUFFERED=1"
if not defined XRPL_INGEST_HOME set "XRPL_INGEST_HOME=%REPO%"

set "PY=%REPO%\.venv\Scripts\python.exe"
if not exist "%PY%" goto :novenv

if defined LOG_FILE goto :run
rem one file per command: tail and backfill tasks must not rotate the same file (Windows locks it)
set "LOG_FILE=logs\xrpl_ingest_%~1.log"
if not exist "%XRPL_INGEST_HOME%\logs\" mkdir "%XRPL_INGEST_HOME%\logs"

:run
"%PY%" -m xrpl_ingest %*
exit /b %ERRORLEVEL%

:novenv
echo run.cmd: "%PY%" not found - run install.cmd in the repo folder first. 1>&2
exit /b 2

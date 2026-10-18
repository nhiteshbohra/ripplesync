#Requires -Version 5.1
<#
.SYNOPSIS
  Register (or remove) a Windows Scheduled Task that runs `xrpl_ingest tail` or `backfill`
  at system startup.

.DESCRIPTION
  NOT TESTED ON A REAL HOST: written against the ScheduledTasks module documentation and only
  syntax-checked. Every Task Scheduler behaviour below is UNVERIFIED.

  The task "xrpl-ingest-<Command>" runs deploy\windows\run.cmd <Command> (which sets
  PYTHONUTF8, PYTHONUNBUFFERED, XRPL_INGEST_HOME and a default LOG_FILE under logs\) as the
  user who registers it, at startup, whether or not that user is logged on (S4U logon: no
  password stored; no access to network shares - local MongoDB / xrpld over TCP are fine).
  Settings: no execution time limit, restart on failure (tail: 999 times, backfill: 3 times,
  every minute), a second start while running is ignored (MultipleInstances IgnoreNew).

  Graceful stop on Windows = the stop file in the repo (base dir):
      Set-Content -Encoding ascii process_status.txt 'tail=0'      # or backfill=0 / current_process=0
  Stop-ScheduledTask (or "End" in the Task Scheduler UI) kills the process immediately; data
  stays consistent (unfinished chunks are redone) but in-flight work is lost.
  Remove the stop line again before the next start.

  Must be run from an elevated PowerShell (an at-startup task needs administrator rights).

.PARAMETER Command
  tail (default) or backfill.

.PARAMETER Uninstall
  Remove the task instead of registering it.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1 -Command tail
  Start-ScheduledTask -TaskName xrpl-ingest-tail          # start now instead of at next boot
  Get-ScheduledTaskInfo -TaskName xrpl-ingest-tail        # LastTaskResult = xrpl_ingest exit code

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1 -Command backfill -Uninstall
#>
[CmdletBinding()]
param(
    [ValidateSet('tail', 'backfill')]
    [string]$Command = 'tail',
    [switch]$Uninstall
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repo = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$runCmd = Join-Path $PSScriptRoot 'run.cmd'
$taskName = "xrpl-ingest-$Command"

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$isAdmin = ([Security.Principal.WindowsPrincipal]$identity).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host 'ERROR: run this from an elevated PowerShell ("Run as administrator").' -ForegroundColor Red
    exit 2
}

if ($Uninstall) {
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if ($null -eq $task) {
        Write-Host "Task $taskName is not registered; nothing to do."
        exit 0
    }
    if ($task.State -eq 'Running') {
        Write-Warning "$taskName is running. Stop it gracefully first (process_status.txt: $Command=0) and wait for it to exit; removing the task does not stop the process."
    }
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "Removed task $taskName."
    exit 0
}

if (-not (Test-Path -LiteralPath (Join-Path $repo '.venv\Scripts\python.exe'))) {
    Write-Host "ERROR: $repo\.venv not found; run deploy\windows\setup-windows.ps1 first." -ForegroundColor Red
    exit 2
}
if (-not (Test-Path -LiteralPath $runCmd)) {
    Write-Host "ERROR: $runCmd not found." -ForegroundColor Red
    exit 2
}
if ($repo.Contains('%') -or $repo.Contains('"')) {
    Write-Host "ERROR: the repo path '$repo' contains % or a double quote, which cmd.exe would mangle; move the repo." -ForegroundColor Red
    exit 2
}

# cmd /d /c ""<run.cmd>" <command>": cmd strips the outer quote pair, keeping the quoted path
# (the usual idiom for paths with spaces). /d skips AutoRun commands from the registry.
$cmdExe = Join-Path $env:SystemRoot 'System32\cmd.exe'
$action = New-ScheduledTaskAction -Execute $cmdExe -Argument ('/d /c ""{0}" {1}"' -f $runCmd, $Command) -WorkingDirectory $repo

$trigger = New-ScheduledTaskTrigger -AtStartup

$userId = "$env:USERDOMAIN\$env:USERNAME"
$principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType S4U -RunLevel Limited

$restartCount = 3
if ($Command -eq 'tail') { $restartCount = 999 }
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount $restartCount `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

$description = "xrpl_ingest $Command (repo: $repo). Graceful stop: line '$Command=0' in process_status.txt. Installed by deploy\windows\install-task.ps1."
Register-ScheduledTask -TaskName $taskName -Description $description -Action $action -Trigger $trigger `
    -Principal $principal -Settings $settings -Force | Out-Null

Write-Host "Registered task $taskName (runs at startup as $userId; not started now)."
Write-Host "  Start now : Start-ScheduledTask -TaskName $taskName"
Write-Host "  Status    : Get-ScheduledTaskInfo -TaskName $taskName   (LastTaskResult = exit code)"
Write-Host "  Logs      : $repo\logs\xrpl_ingest.log (run.cmd default LOG_FILE)"
Write-Host "  Stop      : Set-Content -Encoding ascii '$repo\process_status.txt' '$Command=0'"
Write-Host "  Remove    : install-task.ps1 -Command $Command -Uninstall"
if ($Command -eq 'backfill') {
    Write-Host '  Note: the backfill task runs again at every boot (it resumes; finished chunks are skipped).'
    Write-Host '        Disable it when the backfill is done: Disable-ScheduledTask -TaskName xrpl-ingest-backfill'
}
exit 0

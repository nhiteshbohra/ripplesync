#Requires -Version 5.1
<#
.SYNOPSIS
  Set up the XRPL ingest on Windows: .venv with Python 3.12, requirements, .env, doctor.

.DESCRIPTION
  NOT TESTED ON A REAL HOST end to end (written and syntax-checked only).

  Does:
    1. checks for Python 3.12 via the py launcher (py -3.12)
    2. creates .venv in the repo (kept if it exists) and runs pip install -r requirements.txt
    3. copies .env.example to .env if .env is missing (an existing .env is never touched)
    4. runs: .venv\Scripts\python.exe -m xrpl_ingest doctor
  Does NOT install:
    - xrpld: there is no Windows package (xrpl.org: Windows is for development only).
      Run the node on Linux, or in WSL2 with setup/setup_ubuntu.sh node, and point SERVERS
      in .env at it.
    - MongoDB: install MongoDB Community 8.0 (.msi) yourself; the link is printed below.
      (MongoDB is not supported inside WSL, so on a Windows machine it runs on Windows.)

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File deploy\windows\setup-windows.ps1
#>
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repo = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
Set-Location -LiteralPath $repo
$mongoMsiUrl = 'https://www.mongodb.com/try/download/community'

function Write-Step([string]$Text) { Write-Host "==> $Text" -ForegroundColor Cyan }

# Run a native program with output to the console; return its exit code. Native stderr must
# not become a terminating error under $ErrorActionPreference = 'Stop' (Windows PowerShell 5.1).
function Invoke-Native([string]$Exe, [string[]]$Arguments) {
    $old = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Exe @Arguments | Out-Host
        return $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $old
    }
}

# --- 1. Python 3.12 ----------------------------------------------------------------------
Write-Step 'Checking for Python 3.12 (py -3.12)'
$pyVer = $null
$old = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try {
    # No double quotes in the code: Windows PowerShell 5.1 does not escape them for native programs.
    $out = & py -3.12 -c 'import sys; print(sys.version_info[0], sys.version_info[1], sep=chr(46))' 2>$null
    if ($LASTEXITCODE -eq 0 -and $null -ne $out) { $pyVer = ([string]($out | Select-Object -First 1)).Trim() }
} catch {
    $pyVer = $null
} finally {
    $ErrorActionPreference = $old
}
if ($pyVer -ne '3.12') {
    Write-Host 'ERROR: Python 3.12 not found (py -3.12 failed).' -ForegroundColor Red
    Write-Host '  Install Python 3.12 from https://www.python.org/downloads/windows/ (keep "py launcher" ticked) and re-run.'
    exit 2
}
Write-Host "Python: py -3.12 ($pyVer)"

# --- 2. venv + requirements ----------------------------------------------------------------
$venvPy = Join-Path $repo '.venv\Scripts\python.exe'
if (Test-Path -LiteralPath $venvPy) {
    Write-Step 'Using existing .venv'
} else {
    Write-Step 'Creating .venv'
    $rc = Invoke-Native 'py' @('-3.12', '-m', 'venv', (Join-Path $repo '.venv'))
    if ($rc -ne 0) { Write-Host "ERROR: venv creation failed (exit $rc)." -ForegroundColor Red; exit 2 }
}

Write-Step 'Installing requirements.txt into .venv'
$rc = Invoke-Native $venvPy @('-m', 'pip', 'install', '--upgrade', 'pip')
if ($rc -ne 0) { Write-Warning "pip self-upgrade failed (exit $rc); continuing with the current pip." }
$rc = Invoke-Native $venvPy @('-m', 'pip', 'install', '-r', (Join-Path $repo 'requirements.txt'))
if ($rc -ne 0) { Write-Host "ERROR: pip install -r requirements.txt failed (exit $rc)." -ForegroundColor Red; exit 2 }

# --- 3. .env -----------------------------------------------------------------------------
$envFile = Join-Path $repo '.env'
if (Test-Path -LiteralPath $envFile) {
    Write-Step '.env already exists; leaving it unchanged'
} else {
    Copy-Item -LiteralPath (Join-Path $repo '.env.example') -Destination $envFile
    Write-Step '.env created from .env.example: edit MONGO_URI and SERVERS before running anything'
}

# --- 4. doctor ---------------------------------------------------------------------------
Write-Step 'Running: python -m xrpl_ingest doctor'
$env:PYTHONUTF8 = '1'
if (-not $env:XRPL_INGEST_HOME) { $env:XRPL_INGEST_HOME = $repo }
$rc = Invoke-Native $venvPy @('-m', 'xrpl_ingest', 'doctor')
switch ($rc) {
    0 { Write-Host 'doctor: OK' -ForegroundColor Green }
    1 { Write-Warning 'doctor reported warnings (see above).' }
    2 { Write-Warning 'doctor: configuration problem (exit 2) - fix .env and run it again.' }
    4 { Write-Warning 'doctor: MongoDB or the xrpld node is not reachable yet (exit 4).' }
    default { Write-Warning "doctor exited with $rc." }
}

Write-Host ''
Write-Host 'Not installed by this script:' -ForegroundColor Yellow
Write-Host "  MongoDB : install MongoDB Community 8.0 (.msi, runs as a Windows service) from $mongoMsiUrl"
Write-Host '            (7.0 also works; 9.0 is not supported yet by the pinned pymongo).'
Write-Host '  xrpld   : no Windows package. Run it on a Linux host, or in WSL2 (bash setup/setup_ubuntu.sh node),'
Write-Host '            then set SERVERS in .env. For WSL2 with mirrored networking (Windows 11 22H2+;'
Write-Host '            %UserProfile%\.wslconfig: [wsl2] networkingMode=mirrored) ws://127.0.0.1:6006 is expected'
Write-Host '            to work (UNVERIFIED). Full history (~39 TB, XFS/ZFS) needs a real Linux host.'
Write-Host ''
Write-Host 'Next steps (in this folder):'
Write-Host '  .venv\Scripts\python.exe -m xrpl_ingest doctor --node              # node synced? (server_state full)'
Write-Host '  copy backfill_range.txt.example backfill_range.txt                 # then edit start / end'
Write-Host '  .venv\Scripts\python.exe -m xrpl_ingest backfill --start 32570 --end 33570   # small trial'
Write-Host '  # run the live tail from day 1, alongside the full backfill (elevated PowerShell):'
Write-Host '  powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1 -Command tail'
Write-Host '  powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1 -Command backfill'
Write-Host '  .venv\Scripts\python.exe -m xrpl_ingest status                     # progress, failed ledgers, ETA'
Write-Host "  Set-Content -Encoding ascii process_status.txt 'backfill=0'        # graceful stop (tail=0, current_process=0)"
exit 0

#Requires -Version 5.1
<#
.SYNOPSIS
  Automatic setup for Windows 10 / 11. Easiest: double-click install.cmd in the repo folder.

.DESCRIPTION
  Looks at this computer (Python, MongoDB, RAM, disk), installs what is missing with winget
  (Windows' own package manager; you are asked first), creates .venv and .env, runs the tests
  and the doctor check.

  Setups (Windows cannot run the XRPL node itself; xrpl.org: Windows is for development only):
    test     Try it on this computer: reads a little real data from public XRPL servers.
    connect  Use your own node that runs elsewhere (a Linux server, or WSL2); you give its address.

  The manual steps are in README.md ("Manual setup"); this script runs the same steps.

.PARAMETER Mode
  test or connect. Without it you are asked.
.PARAMETER NodeUrl
  connect only: the node's websocket, e.g. ws://192.168.1.20:6006 (default ws://127.0.0.1:6006).
.PARAMETER Yes
  Accept every default without questions.
.PARAMETER Check
  Only look and report; change nothing.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File install.ps1
  powershell -ExecutionPolicy Bypass -File install.ps1 -Mode test -Yes
#>
[CmdletBinding()]
param(
    [ValidateSet('test', 'connect')][string]$Mode = '',
    [string]$NodeUrl = '',
    [switch]$Yes,
    [switch]$Check
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repo = $PSScriptRoot
Set-Location -LiteralPath $repo
$publicServers = 'wss://s2.ripple.com,wss://xrplcluster.com'
$localMongo = 'mongodb://127.0.0.1:27017'
$venvPy = Join-Path $repo '.venv\Scripts\python.exe'

function Say([string]$t)  { Write-Host "==> $t" -ForegroundColor Cyan }
function Ok([string]$t)   { Write-Host "OK   $t" -ForegroundColor Green }
function Warn([string]$t) { Write-Host "WARN $t" -ForegroundColor Yellow }
function Fail([string]$t) { Write-Host "ERROR $t" -ForegroundColor Red; exit 1 }

function Ask([string]$q, [bool]$def = $true) {
    if ($Yes) { return $def }
    $hint = if ($def) { '[Y/n]' } else { '[y/N]' }
    $a = Read-Host "$q $hint"
    if ([string]::IsNullOrWhiteSpace($a)) { return $def }
    return $a.Trim().ToLower().StartsWith('y')
}

# Run a native program, output to the console, return its exit code. Native stderr must not become
# a terminating error under ErrorActionPreference=Stop (Windows PowerShell 5.1).
function Invoke-Native([string]$Exe, [string[]]$Arguments) {
    $old = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & $Exe @Arguments | Out-Host; return $LASTEXITCODE } finally { $ErrorActionPreference = $old }
}

function Get-NativeOutput([string]$Exe, [string[]]$Arguments) {
    $old = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $out = & $Exe @Arguments 2>$null
        if ($LASTEXITCODE -ne 0) { return $null }
        return ($out | Out-String).Trim()
    } catch { return $null } finally { $ErrorActionPreference = $old }
}

function Test-Port([string]$h, [int]$p) {
    $c = New-Object System.Net.Sockets.TcpClient
    try { return $c.ConnectAsync($h, $p).Wait(1500) -and $c.Connected } catch { return $false } finally { $c.Close() }
}

function Update-PathFromRegistry {
    $m = [Environment]::GetEnvironmentVariable('Path', 'Machine')
    $u = [Environment]::GetEnvironmentVariable('Path', 'User')
    $env:Path = "$m;$u"
}

# Tested Python 3.12 through the py launcher. Returns @(exe, args, version) or $null.
function Find-Python {
    if (-not (Get-Command py -ErrorAction SilentlyContinue)) { return $null }
    foreach ($tag in @('-3.12')) {
        $v = Get-NativeOutput 'py' @($tag, '-c', 'import sys; print(sys.version_info[0], sys.version_info[1], sep=chr(46))')
        if ($v) {
            $parts = $v.Split('.')
            if ([int]$parts[0] -eq 3 -and [int]$parts[1] -eq 12) { return @('py', $tag, $v) }
        }
    }
    return $null
}

# ---------------------------------------------------------------------------------- 1. look
Say 'Looking at this computer'
$os = Get-CimInstance Win32_OperatingSystem
$ramGb = [math]::Round($os.TotalVisibleMemorySize / 1MB)
$cores = [Environment]::ProcessorCount
$drive = (Get-Item -LiteralPath $repo).PSDrive
$freeGb = [math]::Floor($drive.Free / 1GB)
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$winget = [bool](Get-Command winget -ErrorAction SilentlyContinue)
$py = Find-Python
$mongoSvc = Get-Service -Name 'MongoDB' -ErrorAction SilentlyContinue
$mongoUp = Test-Port '127.0.0.1' 27017
$mongoState = if ($mongoUp) { 'running on 127.0.0.1:27017' } elseif ($mongoSvc) { "installed (service $($mongoSvc.Status))" } else { 'not installed' }
$pyState = if ($py) { "Python $($py[2]) (py $($py[1]))" } else { 'Python 3.12 not found' }
$envState = if (Test-Path -LiteralPath (Join-Path $repo '.env')) { 'exists' } else { 'none' }
$venvState = if (Test-Path -LiteralPath $venvPy) { 'exists' } else { 'none' }

Write-Host ''
Write-Host 'This computer'
'{0,-12} {1}' -f 'System', $os.Caption | Write-Host
'{0,-12} {1}' -f 'RAM', "$ramGb GB" | Write-Host
'{0,-12} {1}' -f 'CPU cores', $cores | Write-Host
'{0,-12} {1}' -f 'Free disk', "$freeGb GB (drive $($drive.Name):)" | Write-Host
'{0,-12} {1}' -f 'Python', $pyState | Write-Host
'{0,-12} {1}' -f 'MongoDB', $mongoState | Write-Host
'{0,-12} {1}' -f 'winget', $(if ($winget) { 'available' } else { 'not available (install "App Installer" from the Microsoft Store)' }) | Write-Host
'{0,-12} {1}' -f '.env', $envState | Write-Host
'{0,-12} {1}' -f '.venv', $venvState | Write-Host
Write-Host ''
Write-Host 'Setups'
Write-Host '  test     Try it on this computer: reads a little real data from public XRPL servers.'
Write-Host '           Needs Python + MongoDB (installed for you). Any computer.'
Write-Host '  connect  Use your own node running elsewhere (Linux server or WSL2). You give its address.'
Write-Host '  (The node itself runs only on Linux: use install.sh there.)'
Write-Host ''

if ($Check) { Write-Host '(-Check: nothing was changed)'; exit 0 }

if (-not $Mode) {
    if ($Yes) { $Mode = 'test' }
    else {
        $a = Read-Host 'Which setup? [test/connect, Enter = test]'
        $Mode = if ([string]::IsNullOrWhiteSpace($a)) { 'test' } else { $a.Trim().ToLower() }
        if ($Mode -eq '1') { $Mode = 'test' } elseif ($Mode -eq '2') { $Mode = 'connect' }
        if ($Mode -notin @('test', 'connect')) { Fail 'please answer test or connect' }
    }
}
if ($Mode -eq 'connect' -and -not $NodeUrl) {
    $NodeUrl = 'ws://127.0.0.1:6006'
    if (-not $Yes) {
        $a = Read-Host "Node websocket address [Enter = $NodeUrl]"
        if (-not [string]::IsNullOrWhiteSpace($a)) { $NodeUrl = $a.Trim() }
    }
}
if ($Mode -eq 'connect' -and -not ($NodeUrl.StartsWith('ws://') -or $NodeUrl.StartsWith('wss://'))) { Fail 'the node address must start with ws:// or wss://' }

Write-Host ''
Write-Host "Plan for the '$Mode' setup:"
if (-not $py) { Write-Host '  - install Python 3.12 (winget: Python.Python.3.12)' }
if (-not $mongoUp) { Write-Host "  - MongoDB: $mongoState -> install / start MongoDB 8.0 (winget: MongoDB.Server; Windows asks for administrator rights)" }
Write-Host '  - .venv with the Python packages from requirements.txt'
Write-Host '  - .env settings for this setup (existing .env: backup kept, MONGO_URI kept)'
Write-Host '  - run the tests and the doctor check'
if ($Mode -eq 'test') { Write-Host '  - store one real ledger as a final check' }
if (-not (Ask 'Continue?' $true)) { Write-Host 'Nothing changed.'; exit 0 }

# ---------------------------------------------------------------------------------- 2. Python
if (-not $py) {
    if (-not $winget) { Fail 'Python 3.12 is missing and winget is not available. Install Python 3.12 from https://www.python.org/downloads/windows/ (keep "py launcher" ticked), then run install.cmd again.' }
    Say 'Installing Python 3.12 with winget'
    $rc = Invoke-Native 'winget' @('install', '--id', 'Python.Python.3.12', '--exact', '--accept-package-agreements', '--accept-source-agreements')
    Update-PathFromRegistry
    $py = Find-Python
    if (-not $py) { Fail "Python was not found after the install (winget exit $rc). Close this window, open a new one and run install.cmd again." }
}
Ok "Python $($py[2])"

# ---------------------------------------------------------------------------------- 3. MongoDB
if (-not (Test-Port '127.0.0.1' 27017)) {
    if ($mongoSvc) {
        Say 'Starting the MongoDB service'
        try { Start-Service -Name 'MongoDB' } catch { Warn "could not start the MongoDB service ($($_.Exception.Message)); start it from an administrator window: Start-Service MongoDB" }
    } elseif ($winget) {
        if (Ask 'MongoDB is not installed. Install MongoDB 8.0 with winget now?' $true) {
            $versions = Get-NativeOutput 'winget' @('show', '--id', 'MongoDB.Server', '--exact', '--versions', '--accept-source-agreements')
            $ver = $null
            if ($versions) { $ver = ($versions -split "`r?`n" | Where-Object { $_.Trim() -match '^8\.0\.\d+$' } | Select-Object -First 1) }
            $wgArgs = @('install', '--id', 'MongoDB.Server', '--exact', '--accept-package-agreements', '--accept-source-agreements')
            if ($ver) { $wgArgs += @('--version', $ver.Trim()); Say "Installing MongoDB $($ver.Trim()) (a Windows administrator prompt may appear)" }
            else { Warn 'could not list MongoDB versions; installing the newest one (9.0 is NOT supported yet: choose 8.0 if asked)' }
            $rc = Invoke-Native 'winget' $wgArgs
            if ($rc -ne 0) { Warn "winget exited with $rc" }
        }
    } else {
        Warn 'MongoDB is missing and winget is not available: install MongoDB Community 8.0 (.msi) from https://www.mongodb.com/try/download/community'
    }
    $waited = 0
    while (-not (Test-Port '127.0.0.1' 27017) -and $waited -lt 60) { Start-Sleep -Seconds 3; $waited += 3 }
}
$mongoUp = Test-Port '127.0.0.1' 27017
if ($mongoUp) { Ok 'MongoDB is running on 127.0.0.1:27017' } else { Warn 'MongoDB is not reachable on 127.0.0.1:27017 yet; you can set MONGO_URI in .env to another MongoDB' }

# ---------------------------------------------------------------------------------- 4. .venv
if (Test-Path -LiteralPath (Join-Path $repo '.venv')) {
    $venvVersion = Get-NativeOutput $venvPy @('-c', 'import sys; print(sys.version_info[0], sys.version_info[1], sep=chr(46))')
    if ($venvVersion -ne '3.12') {
        Warn "Existing .venv uses Python '$venvVersion'; Python 3.12 is the tested version."
        if (-not (Ask 'Back up .venv and recreate it with Python 3.12?' $true)) { Fail 'Existing .venv left unchanged; Python 3.12 is required.' }
        $backup = Join-Path $repo ('.venv-backup.' + [guid]::NewGuid().ToString('N'))
        $null = New-Item -ItemType Directory -Path $backup
        $backupVenv = Join-Path $backup '.venv'
        Move-Item -LiteralPath (Join-Path $repo '.venv') -Destination $backupVenv
        Say "Previous .venv saved in $backupVenv (move it back before using it)"
    } else { Say 'Using the existing .venv (Python 3.12)' }
}
if (-not (Test-Path -LiteralPath $venvPy)) {
    Say 'Creating .venv'
    $rc = Invoke-Native $py[0] @($py[1], '-m', 'venv', (Join-Path $repo '.venv'))
    if ($rc -ne 0) { Fail "creating .venv failed (exit $rc)" }
}
if ((Get-NativeOutput $venvPy @('-c', 'import sys; print(sys.version_info[0], sys.version_info[1], sep=chr(46))')) -ne '3.12') { Fail '.venv is not using Python 3.12' }
Say 'Installing the Python packages (requirements.txt)'
$null = Invoke-Native $venvPy @('-m', 'pip', 'install', '-q', '--upgrade', 'pip')
$rc = Invoke-Native $venvPy @('-m', 'pip', 'install', '-q', '-r', (Join-Path $repo 'requirements.txt'))
if ($rc -ne 0) { Fail "pip install -r requirements.txt failed (exit $rc)" }
Ok 'Python environment ready: .venv'

# ---------------------------------------------------------------------------------- 5. .env
$envPath = Join-Path $repo '.env'
$envArgs = @((Join-Path $repo 'setup\envfile.py'), $envPath, '--example', (Join-Path $repo '.env.example'))
$hasUri = (Invoke-Native $venvPy @((Join-Path $repo 'setup\envfile.py'), $envPath, '--has', 'MONGO_URI')) -eq 0
if (-not $hasUri) { $envArgs += "MONGO_URI=$localMongo" }
if ($Mode -eq 'test') {
    $envArgs += @("SERVERS=$publicServers", 'DB_NAME=xrpl_test', 'NUM_PROCESSES=1', 'CONCURRENCY=2', 'CHUNK_SIZE=100')
} else {
    $envArgs += @("SERVERS=$NodeUrl", 'CHUNK_SIZE=1000', "NUM_PROCESSES=$([math]::Min($cores, 4))", 'CONCURRENCY=16')
    $db = Get-NativeOutput $venvPy @('-c', "from dotenv import dotenv_values as d; print(d(r'$envPath').get('DB_NAME') or '')")
    if ($db -eq 'xrpl_test' -or $db -eq 'xrpl_trial') { $envArgs += 'DB_NAME=xrpl_data_collection' }
}
Say "Writing settings into .env ($Mode setup)"
$rc = Invoke-Native $venvPy $envArgs
if ($rc -ne 0) { Fail "writing .env failed (exit $rc)" }

# ---------------------------------------------------------------------------------- 6. checks
$env:PYTHONUTF8 = '1'
$env:XRPL_INGEST_HOME = $repo
Say 'Running the test suite'
$uri = Get-NativeOutput $venvPy @('-c', "from dotenv import dotenv_values as d; print(d(r'$envPath').get('MONGO_URI') or '')")
if ($mongoUp -and ($uri -like 'mongodb://127.0.0.1:27017*' -or $uri -like 'mongodb://localhost:27017*')) {
    Remove-Item Env:XRPL_INGEST_NO_INTEGRATION -ErrorAction SilentlyContinue
} else {
    $env:XRPL_INGEST_NO_INTEGRATION = '1'   # end-to-end tests need a local MongoDB without password
}
$rc = Invoke-Native $venvPy @('-m', 'pytest', '-q')
Remove-Item Env:XRPL_INGEST_NO_INTEGRATION -ErrorAction SilentlyContinue
if ($rc -ne 0) { Fail 'tests failed (output above). Nothing on your system is broken; please report the output.' }
Ok 'tests passed'

Say 'Checking the setup (python -m xrpl_ingest doctor)'
$rc = Invoke-Native $venvPy @('-m', 'xrpl_ingest', 'doctor')
switch ($rc) {
    0 { Ok 'doctor: everything OK' }
    1 { Warn 'doctor: warnings (see above). With public servers "node state ?" is normal.' }
    2 { Warn 'doctor: configuration problem (exit 2): fix .env, then run: .venv\Scripts\python.exe -m xrpl_ingest doctor' }
    4 { Warn 'doctor: MongoDB or the node is not reachable (exit 4).' }
    default { Warn "doctor exited with $rc" }
}

if ($Mode -eq 'test') {
    Say 'Storing one real ledger to prove it works (fetch --ledger validated)'
    $rc = Invoke-Native $venvPy @('-m', 'xrpl_ingest', 'fetch', '--ledger', 'validated')
    if ($rc -eq 0) { Ok 'a real XRPL ledger is now in MongoDB (database xrpl_test)' }
    else { Warn 'fetch did not work (see above); check internet access and MongoDB.' }
}

# ---------------------------------------------------------------------------------- 7. done
Write-Host ''
Say "Done: $Mode setup"
Write-Host 'Start every session in this folder with:   .\.venv\Scripts\Activate.ps1'
Write-Host '  (if activation is blocked: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass)'
if ($Mode -eq 'test') {
    Write-Host 'Try:'
    Write-Host '  python -m xrpl_ingest backfill --start 32570 --end 32769   # 200 old ledgers (about a minute)'
    Write-Host '  python -m xrpl_ingest verify   --start 32570 --end 32769   # prints OK'
    Write-Host '  python -m xrpl_ingest tail                                 # live ledgers; Ctrl+C stops'
    Write-Host '  python -m xrpl_ingest status                               # what is stored'
    Write-Host 'This is a test setup (public servers, database xrpl_test). For real use, set up a node on'
    Write-Host 'Linux (install.sh) and run this script again with -Mode connect.'
} else {
    Write-Host 'Next:'
    Write-Host '  python -m xrpl_ingest doctor --node                        # node reachable and synced?'
    Write-Host '  copy backfill_range.txt.example backfill_range.txt         # then edit start / end'
    Write-Host '  python -m xrpl_ingest backfill --start 32570 --end 33569   # small trial'
    Write-Host '  Run tail / backfill in the background (administrator PowerShell):'
    Write-Host '    powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1 -Command tail'
    Write-Host '    powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1 -Command backfill'
}
exit 0

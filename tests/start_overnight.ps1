param([switch]$Worker)
$ErrorActionPreference = 'Stop'
$reviews = Join-Path $PSScriptRoot '..\reviews'
if (-not $Worker) {
    $process = Start-Process powershell.exe -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass',
        '-File', ('"' + $PSCommandPath + '"'), '-Worker') -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $reviews 'overnight_launcher.stdout.log') `
        -RedirectStandardError (Join-Path $reviews 'overnight_launcher.stderr.log')
    @{pid=$process.Id; started_utc=(Get-Date).ToUniversalTime().ToString('o')} |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $reviews 'overnight_launcher.json')
    $process | Select-Object Id,ProcessName
    exit
}
Add-Type @'
using System.Runtime.InteropServices;
public static class OvernightAwake {
    [DllImport("kernel32.dll")]
    public static extern uint SetThreadExecutionState(uint flags);
}
'@
if ([OvernightAwake]::SetThreadExecutionState([uint32]2147483649) -eq 0) {
    throw 'Could not request Windows stay awake; overnight run not started.'
}
try {
    & wsl.exe -d Ubuntu-24.04 -u root -- /home/ubuntu/ripplesync/.venv/bin/python `
        '/mnt/f/projects/ripple blockchain/tests/overnight_remaining.py' --hours 8
    if ($LASTEXITCODE -ne 0) { throw "Overnight runner exited $LASTEXITCODE; inspect overnight_remaining_status.json" }
} finally {
    [void][OvernightAwake]::SetThreadExecutionState([uint32]2147483648)
}

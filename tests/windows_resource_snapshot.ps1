$ErrorActionPreference = 'Stop'
$system = Get-CimInstance Win32_OperatingSystem
$available = Get-Counter -Counter '\Memory\Available MBytes' -ErrorAction SilentlyContinue
$distribution = Get-ChildItem 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss' | ForEach-Object {
    Get-ItemProperty -LiteralPath $_.PSPath
} | Where-Object DistributionName -eq 'Ubuntu-24.04' | Select-Object -First 1
$backingDrive = [regex]::Match([string]$distribution.BasePath, '(?i)([a-z]):[\\/]').Groups[1].Value.ToUpperInvariant()
$snapshot = [ordered]@{
    utc = (Get-Date).ToUniversalTime().ToString('o')
    total_ram_mb = [math]::Round($system.TotalVisibleMemorySize / 1024)
    free_ram_mb = [math]::Round($system.FreePhysicalMemory / 1024)
    available_ram_counter_mb = if ($available) { [math]::Round($available.CounterSamples[0].CookedValue) } else { $null }
    wsl_distribution = $distribution.DistributionName
    wsl_base_path = $distribution.BasePath
    wsl_backing_drive = $backingDrive
    drives = @(Get-PSDrive -PSProvider FileSystem | Select-Object Name,@{Name='free_gb';Expression={[math]::Round($_.Free / 1GB,1)}})
    cpu = @(Get-CimInstance Win32_Processor | Select-Object Name,CurrentClockSpeed,MaxClockSpeed,NumberOfCores,NumberOfLogicalProcessors)
    battery = @(Get-CimInstance Win32_Battery | Select-Object BatteryStatus,EstimatedChargeRemaining)
    processes = @(Get-Process | Sort-Object WorkingSet64 -Descending | Select-Object -First 18 Id,ProcessName,
        @{Name='working_mb';Expression={[math]::Round($_.WorkingSet64 / 1MB)}})
}
$json = $snapshot | ConvertTo-Json -Depth 5
$json | Set-Content -LiteralPath (Join-Path $PSScriptRoot '..\reviews\windows_resource_latest.json')
$json

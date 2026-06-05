<#
.SYNOPSIS
	Launch GoCluster with pprof enabled and collect recurring runtime profiles.

.DESCRIPTION
	Starts gocluster.exe in a separate PowerShell window with profiling-related
	environment variables, waits for the pprof endpoint, and periodically captures
	CPU, heap, allocs, block, mutex, goroutine, trace, retained-state, and OS
	process samples until the target process exits.

.NOTES
	Prerequisites: gocluster.exe, active config directory, PowerShell, and a
	local pprof port that is free.
	Side effects: starts a cluster process, writes launcher/controller logs, and
	creates profile/sample files under logs/.
	Safety: intended for local/profiling runs; verify config paths and profiling
	overhead before using results for production claims.
#>

$repoRoot        = "C:\src\gocluster"
$exePath         = Join-Path $repoRoot "gocluster.exe"
$configDir       = Join-Path $repoRoot "data\config"
$pprofAddr       = "localhost:6061"
$profileSeconds  = 60      # duration of each CPU profile
$traceSeconds    = 30      # duration of each Go execution trace
$intervalSeconds = 900     # time between captures
$osSampleSeconds = 15      # OS process sample cadence
$logsDir         = Join-Path $repoRoot "logs"
$blockProfileRate = "10ms" # block profile sampling threshold
$mutexProfileFraction = "10" # 1/N mutex events sampled
$mapLogInterval  = "60s"   # retained-state cardinality log cadence

# Ensure logs directory exists
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null

$launchTs = Get-Date -Format "yyyyMMdd-HHmmss"
$controllerLogPath = Join-Path $logsDir ("profiling-run-$launchTs.log")

function Write-ProfilerInfo {
    param([string]$Message)
    $line = "{0:o} {1}" -f (Get-Date).ToUniversalTime(), $Message
    Add-Content -Path $controllerLogPath -Value $line
    Write-Host $Message
}

function Write-ProfilerWarning {
    param([string]$Message)
    $line = "{0:o} WARNING: {1}" -f (Get-Date).ToUniversalTime(), $Message
    Add-Content -Path $controllerLogPath -Value $line
    Write-Warning $Message
}

function Quote-PowerShellLiteral {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Read-GoRuntimeSettings {
    param([string]$RuntimeYamlPath)

    if (-not (Test-Path -LiteralPath $RuntimeYamlPath -PathType Leaf)) {
        throw "runtime.yaml not found at $RuntimeYamlPath"
    }

    $values = @{}
    $inGoRuntime = $false
    $sectionIndent = 0

    foreach ($rawLine in Get-Content -LiteralPath $RuntimeYamlPath) {
        if (-not $inGoRuntime) {
            if ($rawLine -match '^(\s*)go_runtime\s*:\s*(?:#.*)?$') {
                $inGoRuntime = $true
                $sectionIndent = $matches[1].Length
            }
            continue
        }

        if ($rawLine -match '^\s*(?:#.*)?$') {
            continue
        }

        if ($rawLine -match '^(\s*)[A-Za-z_][A-Za-z0-9_]*\s*:') {
            $indent = $matches[1].Length
            if ($indent -le $sectionIndent) {
                break
            }
        }

        if ($rawLine -match '^\s+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([^#]*?)\s*(?:#.*)?$') {
            $key = $matches[1]
            if ($key -notin @("memory_limit_mib", "gc_percent", "max_procs")) {
                continue
            }

            $rawValue = $matches[2].Trim().Trim("'").Trim('"')
            $parsedValue = 0
            if ($rawValue -eq "" -or -not [int]::TryParse($rawValue, [ref]$parsedValue)) {
                throw "invalid go_runtime.$key value in ${RuntimeYamlPath}: $rawValue"
            }
            $values[$key] = $parsedValue
        }
    }

    foreach ($requiredKey in @("memory_limit_mib", "gc_percent", "max_procs")) {
        if (-not $values.ContainsKey($requiredKey)) {
            throw "missing go_runtime.$requiredKey in $RuntimeYamlPath"
        }
    }
    if ($values["memory_limit_mib"] -lt 0 -or ($values["memory_limit_mib"] -gt 0 -and $values["memory_limit_mib"] -lt 64)) {
        throw "invalid go_runtime.memory_limit_mib $($values["memory_limit_mib"]) in $RuntimeYamlPath (must be 0 or >= 64)"
    }
    if ($values["gc_percent"] -lt 0) {
        throw "invalid go_runtime.gc_percent $($values["gc_percent"]) in $RuntimeYamlPath (must be >= 0)"
    }
    if ($values["max_procs"] -lt 0) {
        throw "invalid go_runtime.max_procs $($values["max_procs"]) in $RuntimeYamlPath (must be >= 0)"
    }

    return [pscustomobject]@{
        MemoryLimitMiB = $values["memory_limit_mib"]
        GCPercent = $values["gc_percent"]
        MaxProcs = $values["max_procs"]
    }
}

function Format-GoRuntimeEnvValue {
    param(
        [int]$Value,
        [string]$Suffix = ""
    )
    if ($Value -le 0) {
        return "unchanged"
    }
    return "$Value$Suffix"
}

function Get-ClusterProcessByParent {
    param(
        [int]$ParentPid,
        [string]$TargetExePath
    )

    $exeName = Split-Path -Path $TargetExePath -Leaf
    $candidates = @(Get-CimInstance -ClassName Win32_Process -Filter "ParentProcessId=$ParentPid" |
        Where-Object { $_.Name -ieq $exeName })

    foreach ($candidate in $candidates) {
        if ($candidate.ExecutablePath -and [string]::Equals($candidate.ExecutablePath, $TargetExePath, [StringComparison]::OrdinalIgnoreCase)) {
            return $candidate
        }
    }
    if ($candidates.Count -eq 1) {
        return $candidates[0]
    }
    return $null
}

$runtimeYamlPath = Join-Path $configDir "runtime.yaml"
try {
    $goRuntime = Read-GoRuntimeSettings -RuntimeYamlPath $runtimeYamlPath
} catch {
    Write-ProfilerWarning "Failed to read Go runtime settings from ${runtimeYamlPath}: $_"
    exit 1
}
$goMemLimit = Format-GoRuntimeEnvValue -Value $goRuntime.MemoryLimitMiB -Suffix "MiB"
$goGC = Format-GoRuntimeEnvValue -Value $goRuntime.GCPercent
$goMaxProcs = Format-GoRuntimeEnvValue -Value $goRuntime.MaxProcs

# Env vars for the cluster
$envVars = @{
    DXC_CONFIG_PATH = $configDir
    DXC_PPROF_ADDR  = $pprofAddr
    # Enable periodic heap logging to match the CPU profiling cadence:
    DXC_HEAP_LOG_INTERVAL = "60s"
    DXC_MAP_LOG_INTERVAL = $mapLogInterval
    DXC_BLOCK_PROFILE_RATE = $blockProfileRate
    DXC_MUTEX_PROFILE_FRACTION = $mutexProfileFraction
    DXC_PSKR_MQTT_DEBUG = "false"
}
if ($goRuntime.MemoryLimitMiB -gt 0) {
    $envVars["GOMEMLIMIT"] = $goMemLimit
}
if ($goRuntime.GCPercent -gt 0) {
    $envVars["GOGC"] = $goGC
}
if ($goRuntime.MaxProcs -gt 0) {
    $envVars["GOMAXPROCS"] = $goMaxProcs
}

# Start the cluster through a wrapper in a new terminal so the UI owns its console.
$launcherPath = Join-Path $logsDir ("gocluster-profile-launch-$launchTs.ps1")
$launcherLines = @(
    '$ErrorActionPreference = ''Stop'''
    "Set-Location -LiteralPath $(Quote-PowerShellLiteral $repoRoot)"
)
foreach ($k in ($envVars.Keys | Sort-Object)) {
    $launcherLines += ('$env:{0} = {1}' -f $k, (Quote-PowerShellLiteral $envVars[$k]))
}
$launcherLines += "& $(Quote-PowerShellLiteral $exePath)"
$launcherLines += 'exit $LASTEXITCODE'
Set-Content -Path $launcherPath -Value $launcherLines -Encoding ASCII

try {
    $launcherProc = Start-Process -FilePath "powershell.exe" `
        -ArgumentList @("-NoLogo", "-ExecutionPolicy", "Bypass", "-File", $launcherPath) `
        -WorkingDirectory $repoRoot `
        -WindowStyle Normal `
        -PassThru
} catch {
    Write-ProfilerWarning "Failed to start gocluster launcher terminal: $_"
    exit 1
}
if (-not $launcherProc) {
    Write-ProfilerWarning "Failed to start gocluster launcher terminal"
    exit 1
}

$clusterProcessInfo = $null
for ($i = 0; $i -lt 30; $i++) {
    $clusterProcessInfo = Get-ClusterProcessByParent -ParentPid $launcherProc.Id -TargetExePath $exePath
    if ($clusterProcessInfo) {
        break
    }
    if ($launcherProc.HasExited) {
        break
    }
    Start-Sleep -Seconds 1
}
if (-not $clusterProcessInfo) {
    Write-ProfilerWarning "Unable to discover child gocluster.exe process for launcher PID $($launcherProc.Id); see $launcherPath"
    exit 1
}

$targetPid = [int]$clusterProcessInfo.ProcessId
$proc = Get-Process -Id $targetPid -ErrorAction SilentlyContinue
if (-not $proc) {
    Write-ProfilerWarning "Discovered gocluster.exe PID $targetPid, but the process is no longer running"
    exit 1
}

Write-ProfilerInfo "gocluster started in separate terminal (launcher PID=$($launcherProc.Id), gocluster PID=$targetPid); pprof at http://$pprofAddr; runtime.yaml=$runtimeYamlPath; GOMEMLIMIT=$goMemLimit; GOGC=$goGC; GOMAXPROCS=$goMaxProcs"
Write-ProfilerInfo "Profiler controller log -> $controllerLogPath"

$osSamplePath = Join-Path $logsDir ("os-process-$launchTs-pid$targetPid.csv")
$osSampleWarningPath = Join-Path $logsDir ("os-process-$launchTs-pid$targetPid.warnings.log")
$processorCount = [Environment]::ProcessorCount
$osSamplerJob = Start-Job -ArgumentList $targetPid, $osSampleSeconds, $osSamplePath, $osSampleWarningPath, $processorCount -ScriptBlock {
    param($targetPid, $sampleSeconds, $csvPath, $warningPath, $processorCount)

    $previous = $null
    $previousTime = $null

    while ($true) {
        $now = Get-Date
        try {
            $p = Get-CimInstance -ClassName Win32_Process -Filter "ProcessId=$targetPid" -ErrorAction Stop
        } catch {
            Add-Content -Path $warningPath -Value ("{0:o} OS process sample failed: {1}" -f $now, $_)
            Start-Sleep -Seconds $sampleSeconds
            continue
        }

        if (-not $p) { break }

        $elapsedSeconds = $null
        if ($previousTime) {
            $elapsedSeconds = ($now - $previousTime).TotalSeconds
        }

        $cpuPercent = ""
        $readBytesPerSec = ""
        $writeBytesPerSec = ""
        $readOpsPerSec = ""
        $writeOpsPerSec = ""

        if ($previous -and $elapsedSeconds -and $elapsedSeconds -gt 0) {
            $cpuDeltaSeconds = (($p.KernelModeTime + $p.UserModeTime) - ($previous.KernelModeTime + $previous.UserModeTime)) / 10000000.0
            $cpuPercent = [Math]::Round(($cpuDeltaSeconds / $elapsedSeconds / $processorCount) * 100, 3)
            $readBytesPerSec = [Math]::Round(($p.ReadTransferCount - $previous.ReadTransferCount) / $elapsedSeconds, 2)
            $writeBytesPerSec = [Math]::Round(($p.WriteTransferCount - $previous.WriteTransferCount) / $elapsedSeconds, 2)
            $readOpsPerSec = [Math]::Round(($p.ReadOperationCount - $previous.ReadOperationCount) / $elapsedSeconds, 2)
            $writeOpsPerSec = [Math]::Round(($p.WriteOperationCount - $previous.WriteOperationCount) / $elapsedSeconds, 2)
        }

        [pscustomobject]@{
            timestamp_utc = $now.ToUniversalTime().ToString("o")
            pid = $targetPid
            working_set_bytes = $p.WorkingSetSize
            private_page_count_bytes = $p.PrivatePageCount
            page_file_usage_kb = $p.PageFileUsage
            virtual_size_bytes = $p.VirtualSize
            handle_count = $p.HandleCount
            thread_count = $p.ThreadCount
            kernel_time_100ns = $p.KernelModeTime
            user_time_100ns = $p.UserModeTime
            cpu_percent = $cpuPercent
            read_operation_count = $p.ReadOperationCount
            write_operation_count = $p.WriteOperationCount
            read_transfer_count_bytes = $p.ReadTransferCount
            write_transfer_count_bytes = $p.WriteTransferCount
            read_bytes_per_sec = $readBytesPerSec
            write_bytes_per_sec = $writeBytesPerSec
            read_ops_per_sec = $readOpsPerSec
            write_ops_per_sec = $writeOpsPerSec
        } | Export-Csv -Path $csvPath -NoTypeInformation -Append

        $previous = $p
        $previousTime = $now
        Start-Sleep -Seconds $sampleSeconds
    }
}

Write-ProfilerInfo "OS process sampling every ${osSampleSeconds}s -> $osSamplePath"

# Wait for pprof to come up
$pprofUrl = "http://$pprofAddr/debug/pprof/"
$ready = $false
for ($i=0; $i -lt 15; $i++) {
    try {
        Invoke-WebRequest -Uri $pprofUrl -TimeoutSec 2 -UseBasicParsing | Out-Null
        $ready = $true
        break
    } catch { Start-Sleep -Seconds 2 }
}
if (-not $ready) { Write-ProfilerWarning "pprof endpoint not reachable yet; proceeding anyway" }

function Get-CPUProfile {
    param($seconds, $destPath, $addr)
    $url = "http://$addr/debug/pprof/profile?seconds=$seconds"
    Invoke-WebRequest -Uri $url -OutFile $destPath -TimeoutSec ($seconds + 10) -UseBasicParsing
}

function Get-HeapProfile {
    param($destPath, $addr)
    $url = "http://$addr/debug/pprof/heap"
    Invoke-WebRequest -Uri $url -OutFile $destPath -TimeoutSec 30 -UseBasicParsing
}

function Get-AllocsProfile {
    param($destPath, $addr)
    $url = "http://$addr/debug/pprof/allocs"
    Invoke-WebRequest -Uri $url -OutFile $destPath -TimeoutSec 30 -UseBasicParsing
}

function Get-BlockProfile {
    param($destPath, $addr)
    $url = "http://$addr/debug/pprof/block"
    Invoke-WebRequest -Uri $url -OutFile $destPath -TimeoutSec 30 -UseBasicParsing
}

function Get-MutexProfile {
    param($destPath, $addr)
    $url = "http://$addr/debug/pprof/mutex"
    Invoke-WebRequest -Uri $url -OutFile $destPath -TimeoutSec 30 -UseBasicParsing
}

function Get-GoroutineProfile {
    param($destPath, $addr, $debugLevel)
    $url = "http://$addr/debug/pprof/goroutine?debug=$debugLevel"
    Invoke-WebRequest -Uri $url -OutFile $destPath -TimeoutSec 30 -UseBasicParsing
}

function Get-TraceProfile {
    param($seconds, $destPath, $addr)
    $url = "http://$addr/debug/pprof/trace?seconds=$seconds"
    Invoke-WebRequest -Uri $url -OutFile $destPath -TimeoutSec ($seconds + 10) -UseBasicParsing
}

# Periodic capture loop (stops when the process exits)
while (-not $proc.HasExited) {
    $ts = Get-Date -Format "yyyyMMdd-HHmmss"
    $dest = Join-Path $logsDir ("cpu-$ts.pprof")
    try {
        Get-CPUProfile -seconds $profileSeconds -destPath $dest -addr $pprofAddr
        Write-ProfilerInfo "Captured CPU profile -> $dest"
    } catch {
        Write-ProfilerWarning "CPU profile capture failed at ${ts}: $($_)"
    }

    $heapDest = Join-Path $logsDir ("heap-$ts.pprof")
    try {
        Get-HeapProfile -destPath $heapDest -addr $pprofAddr
        Write-ProfilerInfo "Captured heap profile -> $heapDest"
    } catch {
        Write-ProfilerWarning "Heap profile capture failed at ${ts}: $($_)"
    }

    $allocsDest = Join-Path $logsDir ("allocs-$ts.pprof")
    try {
        Get-AllocsProfile -destPath $allocsDest -addr $pprofAddr
        Write-ProfilerInfo "Captured allocs profile -> $allocsDest"
    } catch {
        Write-ProfilerWarning "Allocs profile capture failed at ${ts}: $($_)"
    }

    $blockDest = Join-Path $logsDir ("block-$ts.pprof")
    try {
        Get-BlockProfile -destPath $blockDest -addr $pprofAddr
        Write-ProfilerInfo "Captured block profile -> $blockDest"
    } catch {
        Write-ProfilerWarning "Block profile capture failed at ${ts}: $($_)"
    }

    $mutexDest = Join-Path $logsDir ("mutex-$ts.pprof")
    try {
        Get-MutexProfile -destPath $mutexDest -addr $pprofAddr
        Write-ProfilerInfo "Captured mutex profile -> $mutexDest"
    } catch {
        Write-ProfilerWarning "Mutex profile capture failed at ${ts}: $($_)"
    }

    $gorDest = Join-Path $logsDir ("goroutine-$ts.txt")
    try {
        Get-GoroutineProfile -destPath $gorDest -addr $pprofAddr -debugLevel 1
        $firstLine = Get-Content -Path $gorDest -TotalCount 1
        if ($firstLine -match "total\\s+(\\d+)") {
            Write-ProfilerInfo "Captured goroutine dump -> $gorDest (total=$($matches[1]))"
        } else {
            Write-ProfilerInfo "Captured goroutine dump -> $gorDest"
        }
    } catch {
        Write-ProfilerWarning "Goroutine capture failed at ${ts}: $($_)"
    }

    $traceDest = Join-Path $logsDir ("trace-$ts.out")
    try {
        Get-TraceProfile -seconds $traceSeconds -destPath $traceDest -addr $pprofAddr
        Write-ProfilerInfo "Captured trace profile -> $traceDest"
    } catch {
        Write-ProfilerWarning "Trace capture failed at ${ts}: $($_)"
    }

    # Sleep, but break early if the process exits
    for ($i=0; $i -lt $intervalSeconds -and -not $proc.HasExited; $i++) {
        Start-Sleep -Seconds 1
    }
}

if ($osSamplerJob) {
    Wait-Job -Job $osSamplerJob -Timeout 5 | Out-Null
    Receive-Job -Job $osSamplerJob | Out-Host
    Remove-Job -Job $osSamplerJob -Force
}

Write-ProfilerInfo "gocluster exited; stopping capture loop."

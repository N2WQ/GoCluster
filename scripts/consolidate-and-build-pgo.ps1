# Consolidate CPU pprof profiles and build gocluster with PGO.
# - Scans logs/ for cpu-*.pprof
# - Merges them into logs/pgo-merged.pprof
# - Builds gocluster_pgo.exe with -pgo=logs/pgo-merged.pprof
# Usage: run from repo root or let the script set the working directory.

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = "C:\src\gocluster"
$logsDir = Join-Path $repoRoot "logs"
$mergedProfile = Join-Path $logsDir "pgo-merged.pprof"
$outputExe = Join-Path $repoRoot "gocluster_pgo.exe"
$exePath = Join-Path $repoRoot "gocluster.exe"

Set-Location $repoRoot

if (-not (Test-Path $logsDir)) {
    Write-Error "Logs directory not found: $logsDir"
    exit 1
}

$profiles = Get-ChildItem -Path $logsDir -Filter "cpu-*.pprof" | Sort-Object LastWriteTime
if ($profiles.Count -eq 0) {
    Write-Error "No cpu-*.pprof files found in $logsDir"
    exit 1
}

# Merge profiles into a single proto pprof
Write-Host "Merging $($profiles.Count) profiles into $mergedProfile ..."
$profilePaths = $profiles | ForEach-Object { $_.FullName }
if (-not (Test-Path $exePath)) {
    Write-Error "Source binary for profiles not found: $exePath (expected same binary used to generate cpu-*.pprof)"
    exit 1
}
& go tool pprof -proto "-output=$mergedProfile" $exePath @profilePaths
if ($LASTEXITCODE -ne 0) {
    Write-Error "pprof merge failed"
    exit $LASTEXITCODE
}

function Get-GitValue {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Probe,
        [Parameter(Mandatory = $true)][string]$Default
    )
    try {
        $output = & $Probe 2>$null
        if ($LASTEXITCODE -eq 0 -and $output) {
            $trimmed = $output.ToString().Trim()
            if ($trimmed.Length -gt 0) {
                return $trimmed
            }
        }
    } catch {
        # Default is used when git metadata is unavailable.
    }
    return $Default
}

$version = Get-GitValue -Probe { git describe --tags --always --dirty } -Default "dev"
$commit = Get-GitValue -Probe { git rev-parse --short=12 HEAD } -Default "unknown"
$buildTime = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$ldflags = "-X main.Version=$version -X main.Commit=$commit -X main.BuildTime=$buildTime"

# Build with PGO
Write-Host "Building PGO binary -> $outputExe ..."
Write-Host "Stamping build metadata: version=$version commit=$commit built=$buildTime"
& go build "-pgo=$mergedProfile" "-ldflags=$ldflags" "-o=$outputExe" .
if ($LASTEXITCODE -ne 0) {
    Write-Error "go build failed"
    exit $LASTEXITCODE
}

Write-Host "Done. PGO profile: $mergedProfile"
Write-Host "Binary: $outputExe"

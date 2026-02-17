<#
Purpose: Run the consolidate-and-build-pgo pipeline from anywhere, mirroring the manual steps:
  1) cd C:\src\gocluster
  2) cd .\scripts; .\consolidate-and-build-pgo.ps1
  3) cd C:\src\gocluster; .\consolidate-and-build-pgo.ps1
  4) launch the freshly built cluster binary (prefers gocluster_pgo.exe, falls back to gocluster.exe)
Usage:   pwsh -File .\launch-cluster.ps1
#>

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = 'C:\src\gocluster'
if (-not (Test-Path $repoRoot)) {
    throw "Repository root not found at $repoRoot"
}
$buildScript = Join-Path $repoRoot 'scripts\consolidate-and-build-pgo.ps1'
if (-not (Test-Path $buildScript)) {
    throw "Build script not found at $buildScript"
}

# Remember the caller's location so we can restore it.
$originalLocation = Get-Location

try {
    Write-Host "Switching to $repoRoot" -ForegroundColor Cyan
    Set-Location $repoRoot

    # Step 1: run from scripts directory (matches manual workflow).
    Push-Location (Join-Path $repoRoot 'scripts')
    try {
        Write-Host "Running from scripts dir: $buildScript" -ForegroundColor Cyan
        & $buildScript
    }
    finally {
        Pop-Location
    }

    # Step 2: run from repo root (matches the second manual invocation).
    Write-Host "Running from repo root: $buildScript" -ForegroundColor Cyan
    & $buildScript

    # Step 3: launch the cluster using the freshest binary.
    $pgoExe = Join-Path $repoRoot 'gocluster_pgo.exe'
    $fallbackExe = Join-Path $repoRoot 'gocluster.exe'
    $exeToRun = if (Test-Path $pgoExe) { $pgoExe } elseif (Test-Path $fallbackExe) { $fallbackExe } else { $null }
    if (-not $exeToRun) {
        throw "No cluster binary found (expected $pgoExe or $fallbackExe)"
    }

    $env:DXC_CONFIG_PATH = Join-Path $repoRoot 'data\config'
    Write-Host "Launching cluster: $exeToRun (DXC_CONFIG_PATH=$env:DXC_CONFIG_PATH)" -ForegroundColor Green
    Write-Host "Binary version:" -ForegroundColor Green
    & $exeToRun --version
    & $exeToRun
}
finally {
    Set-Location $originalLocation
}

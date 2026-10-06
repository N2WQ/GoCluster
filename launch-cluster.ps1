<#
.SYNOPSIS
    Build once and launch the exact fresh Windows amd64 executable pair.
.DESCRIPTION
    With CPU profiles, uses the strict PGO pipeline. Without profiles, builds a
    fresh ordinary pair. Failed builds never fall back to an existing binary.
#>
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = $PSScriptRoot
$originalLocation = Get-Location
$originalConfigPath = $env:DXC_CONFIG_PATH
$hadConfigPath = Test-Path Env:DXC_CONFIG_PATH
try {
    Set-Location $repoRoot
    $logsDir = Join-Path $repoRoot 'logs'
    $profiles = if (Test-Path -LiteralPath $logsDir -PathType Container) {
        @(Get-ChildItem -LiteralPath $logsDir -Filter 'cpu-*.pprof' -File)
    } else { @() }
    $buildScript = if (@($profiles).Count) { 'consolidate-and-build-pgo.ps1' } else { 'build-executable-pair.ps1' }
    $pair = & (Join-Path $repoRoot ('scripts/' + $buildScript))
    if (-not $pair -or @($pair).Count -ne 1 -or
        -not (Test-Path -LiteralPath $pair.ClusterPath -PathType Leaf) -or
        -not (Test-Path -LiteralPath $pair.PeerDiagnosticPath -PathType Leaf)) { throw 'Builder did not return a complete executable pair.' }
    $env:DXC_CONFIG_PATH = Join-Path $repoRoot 'data/config'
    Write-Host "Launching cluster: $($pair.ClusterPath) (DXC_CONFIG_PATH=$env:DXC_CONFIG_PATH)"
    & $pair.ClusterPath --version
    if ($LASTEXITCODE -ne 0) { throw 'Fresh cluster version probe failed.' }
    & $pair.ClusterPath
    if ($LASTEXITCODE -ne 0) { throw "Fresh cluster exited with code $LASTEXITCODE." }
} finally {
    if ($hadConfigPath) { $env:DXC_CONFIG_PATH = $originalConfigPath } else { Remove-Item Env:DXC_CONFIG_PATH -ErrorAction SilentlyContinue }
    Set-Location $originalLocation
}

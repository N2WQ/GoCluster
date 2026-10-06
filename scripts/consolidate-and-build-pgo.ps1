<#
.SYNOPSIS
    Merge CPU profiles and publish an isolated Windows amd64 PGO executable pair.
.DESCRIPTION
    Requires logs/cpu-*.pprof and the matching root gocluster.exe. Merges profiles
    with go tool pprof, then publishes both executables and binaries.json under
    a unique .tmp/pgo directory. Returns the successful pair paths as an object.
    Existing executable pairs stay intact; caller location is restored.
#>
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$originalLocation = Get-Location
try {
    Set-Location $repoRoot
    $logsDir = Join-Path $repoRoot 'logs'
    if (-not (Test-Path -LiteralPath $logsDir -PathType Container)) { throw "Logs directory not found: $logsDir" }
    $profiles = @(Get-ChildItem -LiteralPath $logsDir -Filter 'cpu-*.pprof' -File | Sort-Object LastWriteTime)
    if ($profiles.Count -eq 0) { throw "No cpu-*.pprof files found in $logsDir" }
    $exePath = Join-Path $repoRoot 'gocluster.exe'
    if (-not (Test-Path -LiteralPath $exePath -PathType Leaf)) { throw "Source binary for profiles not found: $exePath (expected same binary used to generate cpu-*.pprof)" }
    $mergedProfile = Join-Path $logsDir 'pgo-merged.pprof'
    $profilePaths = @($profiles | ForEach-Object FullName)
    & go tool pprof -proto "-output=$mergedProfile" $exePath @profilePaths
    if ($LASTEXITCODE -ne 0) { throw 'pprof merge failed' }
    & (Join-Path $PSScriptRoot 'build-executable-pair.ps1') -ProfilePath $mergedProfile
} finally { Set-Location $originalLocation }

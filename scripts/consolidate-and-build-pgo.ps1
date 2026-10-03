<#
.SYNOPSIS
	Merge CPU profiles and publish an isolated PGO executable pair under .tmp/pgo.

.DESCRIPTION
	Scans the repo logs directory for cpu-*.pprof files, merges them into
	logs/pgo-merged.pprof with go tool pprof, and builds gocluster_pgo.exe with
	Go PGO enabled, plus its peerdiag.exe companion. Both are published together
	under a new .tmp/pgo directory only after both builds succeed. The script
	sets its working directory to the configured repo root before reading profiles.

.NOTES
	Prerequisites: Go toolchain, git, logs/cpu-*.pprof, and the matching
	gocluster.exe used to capture those profiles.
	Side effects: writes logs/pgo-merged.pprof and a unique .tmp/pgo output with
	two executables and binaries.json. Existing executables and outputs stay intact.
	Safety: verify profiles came from the same source binary before using the
	PGO build for performance conclusions.
#>

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = "C:\src\gocluster"
$logsDir = Join-Path $repoRoot "logs"
$mergedProfile = Join-Path $logsDir "pgo-merged.pprof"
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

$commit = Get-GitValue -Probe { git rev-parse --short=12 HEAD } -Default "unknown"
$buildUtc = (Get-Date).ToUniversalTime()
$version = $buildUtc.ToString("yyMMdd")
$buildTime = $buildUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")
$ldflags = "-X main.Version=$version -X main.Commit=$commit -X main.BuildTime=$buildTime"

# A fixed sibling name means the PGO pair must not share the original binary's
# directory. Only this invocation's fresh stage may be moved or removed. Use
# the existing ignored build area so the pair manifest does not dirty sources.
$buildRoot = [IO.Path]::GetFullPath((Join-Path $repoRoot '.tmp/pgo'))
$buildPrefix = $buildRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
$buildID = [guid]::NewGuid().ToString('N')
$stageRoot = Join-Path $buildRoot ('.stage-' + $buildID)
$publishedRoot = Join-Path $buildRoot ('pgo-' + $buildUtc.ToString('yyyyMMddTHHmmssfffZ') + '-' + $buildID)

function Assert-OwnedPGODirectory([string]$Path) {
    $absolute = [IO.Path]::GetFullPath($Path)
    if (-not $absolute.StartsWith($buildPrefix, [StringComparison]::OrdinalIgnoreCase) -or
        [IO.Path]::GetDirectoryName($absolute) -ne $buildRoot) {
        throw "PGO directory escapes the owned build directory: $Path"
    }
    foreach ($directory in @([IO.Path]::GetDirectoryName($buildRoot), $buildRoot)) {
        if ((Test-Path -LiteralPath $directory) -and
            ((Get-Item -LiteralPath $directory -Force).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw "PGO build directory must not use a reparse point: $directory"
        }
    }
    if (Test-Path -LiteralPath $absolute) {
        $item = Get-Item -LiteralPath $absolute -Force
        if (-not $item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw "PGO directory is not an owned ordinary directory: $Path"
        }
    }
}

Assert-OwnedPGODirectory $stageRoot
Assert-OwnedPGODirectory $publishedRoot
$null = New-Item -ItemType Directory -Path $buildRoot -Force
$null = New-Item -ItemType Directory -Path $stageRoot
try {
    $outputExe = Join-Path $stageRoot 'gocluster_pgo.exe'
    $peerDiagnosticExe = Join-Path $stageRoot 'peerdiag.exe'
    Write-Host "Building isolated PGO pair -> $publishedRoot ..."
    Write-Host "Stamping build metadata: version=$version commit=$commit built=$buildTime"
    & go build "-pgo=$mergedProfile" "-ldflags=$ldflags" "-o=$outputExe" .
    if ($LASTEXITCODE -ne 0) { throw 'go build failed' }

    # The helper has a separate workload and does not consume the PGO profile.
    & go build -trimpath -o $peerDiagnosticExe ./cmd/peerdiag
    if ($LASTEXITCODE -ne 0) { throw 'peer diagnostic companion build failed' }

    # These hashes identify the pair, not its source or qualification status.
    @($outputExe, $peerDiagnosticExe) | ForEach-Object {
        [ordered]@{ file = [IO.Path]::GetFileName($_); sha256 = (Get-FileHash -LiteralPath $_ -Algorithm SHA256).Hash }
    } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $stageRoot 'binaries.json')

    Assert-OwnedPGODirectory $stageRoot
    Assert-OwnedPGODirectory $publishedRoot
    if (Test-Path -LiteralPath $publishedRoot) { throw 'PGO output already exists; refusing to replace it.' }
    [IO.Directory]::Move($stageRoot, $publishedRoot)
}
finally {
    Assert-OwnedPGODirectory $stageRoot
    if (Test-Path -LiteralPath $stageRoot) {
        Remove-Item -LiteralPath $stageRoot -Recurse -Force
    }
}

Write-Host "Done. PGO profile: $mergedProfile"
Write-Host "Output directory: $publishedRoot"
Write-Host "Binary: $(Join-Path $publishedRoot 'gocluster_pgo.exe')"
Write-Host "Peer diagnostic companion: $(Join-Path $publishedRoot 'peerdiag.exe')"

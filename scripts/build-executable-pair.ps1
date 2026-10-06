<#
.SYNOPSIS
    Publish a fresh Windows amd64 cluster and peerdiag executable pair.
.DESCRIPTION
    Without ProfilePath, explicitly disables PGO. With ProfilePath, applies the
    supplied profile to the cluster only. Builds in an owned staging directory
    and publishes both executables and their hash manifest together. Existing
    root and previously published binaries remain untouched. Restores caller
    location and GOOS/GOARCH. Returns one object
    containing Mode, OutputDirectory, ClusterPath, PeerDiagnosticPath and
    ManifestPath; build errors throw and return no pair.
#>
param([string]$ProfilePath)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$originalLocation = Get-Location
try {
    Set-Location $repoRoot
    $isPGO = -not [string]::IsNullOrEmpty($ProfilePath)
    if ($isPGO -and -not (Test-Path -LiteralPath $ProfilePath -PathType Leaf)) { throw 'PGO profile not found.' }
    $mode = if ($isPGO) { 'pgo' } else { 'ordinary' }
    $clusterName = if ($isPGO) { 'gocluster_pgo.exe' } else { 'gocluster.exe' }
    $buildRoot = [IO.Path]::GetFullPath((Join-Path $repoRoot ('.tmp/' + $mode)))
    $buildPrefix = $buildRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    $buildUtc = (Get-Date).ToUniversalTime()
    $buildID = [guid]::NewGuid().ToString('N')
    $stageRoot = Join-Path $buildRoot ('.stage-' + $buildID)
    $publishedRoot = Join-Path $buildRoot ($mode + '-' + $buildUtc.ToString('yyyyMMddTHHmmssfffZ') + '-' + $buildID)
    function Assert-OwnedPairDirectory([string]$Path) {
        $absolute = [IO.Path]::GetFullPath($Path)
        if (-not $absolute.StartsWith($buildPrefix, [StringComparison]::OrdinalIgnoreCase) -or
            [IO.Path]::GetDirectoryName($absolute) -ne $buildRoot) { throw "Build directory escapes the owned build directory: $Path" }
        foreach ($directory in @([IO.Path]::GetDirectoryName($buildRoot), $buildRoot, $absolute)) {
            if (Test-Path -LiteralPath $directory) {
                $item = Get-Item -LiteralPath $directory -Force
                if (-not $item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
                    throw "Build directory is not an owned ordinary directory: $directory"
                }
            }
        }
    }
    Assert-OwnedPairDirectory $stageRoot
    Assert-OwnedPairDirectory $publishedRoot
    $null = New-Item -ItemType Directory -Path $buildRoot -Force
    $null = New-Item -ItemType Directory -Path $stageRoot
    try {
        $commit = 'unknown'
        try {
            $gitOutput = & git rev-parse --short=12 HEAD 2>$null
            if ($LASTEXITCODE -eq 0 -and $gitOutput) { $commit = $gitOutput.ToString().Trim() }
        } catch { # Metadata is optional outside a Git checkout.
        }
        $version = $buildUtc.ToString('yyMMdd')
        $buildTime = $buildUtc.ToString('yyyy-MM-ddTHH:mm:ssZ')
        $ldflags = "-X main.Version=$version -X main.Commit=$commit -X main.BuildTime=$buildTime"
        $outputExe = Join-Path $stageRoot $clusterName
        $peerDiagnosticExe = Join-Path $stageRoot 'peerdiag.exe'
        $pgoArgument = if ($isPGO) { "-pgo=$ProfilePath" } else { '-pgo=off' }
        Write-Host "Building isolated $mode pair -> $publishedRoot"
        # The operational pair targets the existing Windows host.
        $savedGOOS = $env:GOOS
        $savedGOARCH = $env:GOARCH
        $hadGOOS = Test-Path Env:GOOS
        $hadGOARCH = Test-Path Env:GOARCH
        try {
            $env:GOOS = 'windows'
            $env:GOARCH = 'amd64'
            & go build $pgoArgument "-ldflags=$ldflags" "-o=$outputExe" .
            if ($LASTEXITCODE -ne 0) { throw 'go build failed' }
            & go build -trimpath -pgo=off -o $peerDiagnosticExe ./cmd/peerdiag
            if ($LASTEXITCODE -ne 0) { throw 'peer diagnostic companion build failed' }
        } finally {
            if ($hadGOOS) { $env:GOOS = $savedGOOS } else { Remove-Item Env:GOOS -ErrorAction SilentlyContinue }
            if ($hadGOARCH) { $env:GOARCH = $savedGOARCH } else { Remove-Item Env:GOARCH -ErrorAction SilentlyContinue }
        }
        # Publication is one directory move after both binaries and their hashes exist.
        @($outputExe, $peerDiagnosticExe) | ForEach-Object {
            [ordered]@{ file = [IO.Path]::GetFileName($_); sha256 = (Get-FileHash -LiteralPath $_ -Algorithm SHA256).Hash }
        } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $stageRoot 'binaries.json')
        Assert-OwnedPairDirectory $stageRoot
        Assert-OwnedPairDirectory $publishedRoot
        if (Test-Path -LiteralPath $publishedRoot) { throw 'Build output already exists; refusing to replace it.' }
        [IO.Directory]::Move($stageRoot, $publishedRoot)
    } finally {
        Assert-OwnedPairDirectory $stageRoot
        if (Test-Path -LiteralPath $stageRoot) { Remove-Item -LiteralPath $stageRoot -Recurse -Force }
    }
    [pscustomobject]@{
        Mode = $mode
        OutputDirectory = $publishedRoot
        ClusterPath = Join-Path $publishedRoot $clusterName
        PeerDiagnosticPath = Join-Path $publishedRoot 'peerdiag.exe'
        ManifestPath = Join-Path $publishedRoot 'binaries.json'
    }
} finally { Set-Location $originalLocation }

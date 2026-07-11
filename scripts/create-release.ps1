<#
.SYNOPSIS
	Build a Windows amd64 ready-to-run GoCluster release package.

.DESCRIPTION
	Creates the ready_to_run payload, renders release README content, builds the
	Windows binary with stamped metadata, writes the release zip, and optionally
	publishes a GitHub release when PackageOnly is not set.

.PARAMETER PackageOnly
	Create the local package without publishing a GitHub release.

.PARAMETER AllowDirty
	Allow local package creation from a dirty worktree. Publishing still requires
	clean, intentional source.

.PARAMETER OutputDir
	Directory where the package output is written. Defaults to the current directory.

.PARAMETER PackageName
	Base name for the generated zip package.

.PARAMETER PackageDirectoryName
	Root directory name inside the generated package.

.PARAMETER Remote
	Git remote used to resolve the release repository. Defaults to origin.

.PARAMETER SkipCodeMapCheck
	Skip the generated code-map freshness check. Only allowed with -PackageOnly
	for local package testing.

.NOTES
	Prerequisites: Go toolchain, git, and GitHub CLI authentication for publishing.
	Side effects: builds a binary, creates package directories/zips, and can
	publish a GitHub release when PackageOnly is omitted.
	Safety: do not publish from a dirty worktree; real secrets and private
	operational state must not enter the release payload.
#>

param(
    [switch]$PackageOnly,
    [switch]$AllowDirty,
    [string]$OutputDir = ".",
    [string]$PackageName = "gocluster-windows-amd64",
    [string]$PackageDirectoryName = "ready_to_run",
    [string]$Remote = "origin",
    [switch]$SkipCodeMapCheck
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-RepoRoot {
    $root = & git rev-parse --show-toplevel
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($root)) {
        throw "Unable to resolve repository root with git."
    }
    return $root.Trim()
}

function Invoke-CheckedCommand {
    param(
        [string]$CommandName,
        [string[]]$Arguments,
        [string]$FailureMessage
    )

    & $CommandName @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
}

function Assert-CleanWorktree {
    param(
        [string]$RepoRoot,
        [switch]$AllowDirty
    )

    $status = @(& git -C $RepoRoot status --porcelain)
    if ($LASTEXITCODE -ne 0) {
        throw "git status --porcelain failed."
    }

    if ($status.Count -gt 0 -and -not $AllowDirty) {
        throw @"
Refusing to create a release from a dirty worktree.
Commit or stash local changes before release, or rerun with -PackageOnly -AllowDirty for a local test package.
"@
    }

    return $status
}

function Assert-GoModulesTidy {
    Invoke-GoRunHost -Arguments @("mod", "tidy", "-diff")
}

function Assert-CodeMapsFresh {
    Invoke-GoRunHost -Arguments @("run", "./cmd/codemap", "check", "-all")
}

function Assert-GitHubCliReady {
    if ($null -eq (Get-Command gh -ErrorAction SilentlyContinue)) {
        throw "GitHub CLI 'gh' is required to publish a release. Install gh and run 'gh auth login'."
    }

    & gh auth status
    if ($LASTEXITCODE -ne 0) {
        throw "GitHub CLI is not authenticated. Run 'gh auth login' before creating a release."
    }
}

function Assert-ReleaseTargetsAvailable {
    param(
        [string]$Remote,
        [string]$Version
    )

    & git rev-parse -q --verify "refs/tags/$Version" | Out-Null
    if ($LASTEXITCODE -eq 0) {
        throw "Local tag $Version already exists."
    }

    & git ls-remote --exit-code --tags $Remote "refs/tags/$Version" | Out-Null
    if ($LASTEXITCODE -eq 0) {
        throw "Remote tag $Version already exists on $Remote."
    }
    if ($LASTEXITCODE -ne 2) {
        throw "Unable to check remote tag $Version on $Remote."
    }

    & gh release view $Version | Out-Null
    if ($LASTEXITCODE -eq 0) {
        throw "GitHub Release $Version already exists."
    }
}

function Copy-TrackedPayload {
    param(
        [string]$RepoRoot,
        [string]$StageRoot,
        [string[]]$AllowlistPrefixes
    )

    $tracked = & git -C $RepoRoot ls-files data
    if ($LASTEXITCODE -ne 0) {
        throw "git ls-files data failed."
    }

    foreach ($relativePath in $tracked) {
        $normalized = $relativePath -replace "\\", "/"
        $allowed = $false
        foreach ($prefix in $AllowlistPrefixes) {
            if ($normalized -eq $prefix -or $normalized.StartsWith($prefix + "/")) {
                $allowed = $true
                break
            }
        }
        if (-not $allowed) {
            continue
        }

        if ($normalized -eq "data/config/openai.yaml") {
            throw "Refusing to package secret-bearing data/config/openai.yaml."
        }

        $source = Join-Path $RepoRoot ($normalized -replace "/", [IO.Path]::DirectorySeparatorChar)
        $destination = Join-Path $StageRoot ($normalized -replace "/", [IO.Path]::DirectorySeparatorChar)
        $destinationDir = Split-Path -Parent $destination
        New-Item -ItemType Directory -Path $destinationDir -Force | Out-Null
        Copy-Item -LiteralPath $source -Destination $destination -Force
    }
}

function Assert-ForbiddenPayloadAbsent {
    param([string]$StageRoot)

    $forbiddenPatterns = @(
        "data/config/openai.yaml",
        "data/archive",
        "data/grids",
        "data/ipinfo",
        "data/scp",
        "data/logs",
        "logs",
        "data/users",
        "data/reputation",
        "data/fcc",
        "data/rbn"
    )

    foreach ($pattern in $forbiddenPatterns) {
        $path = Join-Path $StageRoot ($pattern -replace "/", [IO.Path]::DirectorySeparatorChar)
        if (Test-Path -LiteralPath $path) {
            throw "Forbidden release payload path found: $pattern"
        }
    }
}

function Copy-ReleaseDocument {
    param(
        [string]$RepoRoot,
        [string]$StageRoot,
        [string]$SourceRelativePath,
        [string]$DestinationRelativePath
    )

    $source = Join-Path $RepoRoot ($SourceRelativePath -replace "/", [IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $source)) {
        throw "Required release document is missing: $SourceRelativePath"
    }

    $destination = Join-Path $StageRoot ($DestinationRelativePath -replace "/", [IO.Path]::DirectorySeparatorChar)
    $destinationDir = Split-Path -Parent $destination
    if (-not [string]::IsNullOrWhiteSpace($destinationDir)) {
        New-Item -ItemType Directory -Path $destinationDir -Force | Out-Null
    }
    Copy-Item -LiteralPath $source -Destination $destination -Force
}

function Invoke-GoRunHost {
    param([string[]]$Arguments)

    $oldGOOS = $env:GOOS
    $oldGOARCH = $env:GOARCH
    try {
        Remove-Item Env:GOOS -ErrorAction SilentlyContinue
        Remove-Item Env:GOARCH -ErrorAction SilentlyContinue
        & go @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "go $($Arguments -join ' ') failed."
        }
    }
    finally {
        if ($null -eq $oldGOOS) {
            Remove-Item Env:GOOS -ErrorAction SilentlyContinue
        }
        else {
            $env:GOOS = $oldGOOS
        }
        if ($null -eq $oldGOARCH) {
            Remove-Item Env:GOARCH -ErrorAction SilentlyContinue
        }
        else {
            $env:GOARCH = $oldGOARCH
        }
    }
}

function Render-ReleaseReadme {
    param(
        [string]$RepoRoot,
        [string]$StageRoot
    )

    $templatePath = Join-Path $RepoRoot "docs/release/README.md.template"
    $configDir = Join-Path $StageRoot "data/config"
    $outputPath = Join-Path $StageRoot "README.md"
    Invoke-GoRunHost -Arguments @(
        "run",
        "./cmd/release_readme",
        "-template",
        $templatePath,
        "-config-dir",
        $configDir,
        "-out",
        $outputPath
    )
}

function Read-StagedPayloadText {
    param(
        [string]$StageRoot,
        [string]$RelativePath
    )

    $path = Join-Path $StageRoot ($RelativePath -replace "/", [IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $path)) {
        throw "Required release payload file is missing: $RelativePath"
    }
    return Get-Content -LiteralPath $path -Raw
}

function Assert-MatchingLinesAllowed {
    param(
        [string]$Text,
        [string]$LinePattern,
        [string]$AllowedPattern,
        [string]$Description
    )

    foreach ($line in ($Text -split "\r?\n")) {
        if ($line.TrimStart().StartsWith("#")) {
            continue
        }
        if ($line -match $LinePattern -and $line -notmatch $AllowedPattern) {
            throw "$Description contains a non-public release value."
        }
    }
}

function Assert-RequiredLinePresent {
    param(
        [string]$Text,
        [string]$AllowedPattern,
        [string]$Description
    )

    if ($Text -notmatch $AllowedPattern) {
        throw "$Description is missing the required public release value."
    }
}

function Assert-PublicReleaseConfig {
    param([string]$StageRoot)

    $app = Read-StagedPayloadText -StageRoot $StageRoot -RelativePath "data/config/app.yaml"
    $ingest = Read-StagedPayloadText -StageRoot $StageRoot -RelativePath "data/config/ingest.yaml"
    $peering = Read-StagedPayloadText -StageRoot $StageRoot -RelativePath "data/config/peering.yaml"
    $reputation = Read-StagedPayloadText -StageRoot $StageRoot -RelativePath "data/config/reputation.yaml"

    Assert-RequiredLinePresent -Text $app `
        -AllowedPattern '(?m)^\s*node_id:\s*"N0CALL-\d+"\s*(#.*)?$' `
        -Description "data/config/app.yaml server.node_id"

    Assert-MatchingLinesAllowed -Text $ingest `
        -LinePattern '^\s*callsign:\s*' `
        -AllowedPattern '^\s*callsign:\s*"N0CALL-\d+"\s*(#.*)?$' `
        -Description "data/config/ingest.yaml callsign"
    Assert-MatchingLinesAllowed -Text $ingest `
        -LinePattern '^\s*host:\s*' `
        -AllowedPattern '^\s*host:\s*"(telnet\.reversebeacon\.net|upstream\d+\.example\.invalid)"\s*(#.*)?$' `
        -Description "data/config/ingest.yaml host"

    Assert-RequiredLinePresent -Text $peering `
        -AllowedPattern '(?m)^\s*local_callsign:\s*"N0CALL-\d+"\s*(#.*)?$' `
        -Description "data/config/peering.yaml local_callsign"
    Assert-MatchingLinesAllowed -Text $peering `
        -LinePattern '^\s*enabled:\s*' `
        -AllowedPattern '^\s*enabled:\s*false\s*(#.*)?$' `
        -Description "data/config/peering.yaml enabled"
    Assert-MatchingLinesAllowed -Text $peering `
        -LinePattern '^\s*host:\s*' `
        -AllowedPattern '^\s*host:\s*"peer\d+\.example\.invalid"\s*(#.*)?$' `
        -Description "data/config/peering.yaml host"
    Assert-MatchingLinesAllowed -Text $peering `
        -LinePattern '^\s*password:\s*' `
        -AllowedPattern '^\s*password:\s*""\s*(#.*)?$' `
        -Description "data/config/peering.yaml password"
    Assert-MatchingLinesAllowed -Text $peering `
        -LinePattern '^\s*login_callsign:\s*' `
        -AllowedPattern '^\s*login_callsign:\s*"N0CALL-\d+"\s*(#.*)?$' `
        -Description "data/config/peering.yaml login_callsign"
    Assert-MatchingLinesAllowed -Text $peering `
        -LinePattern '^\s*remote_callsign:\s*' `
        -AllowedPattern '^\s*remote_callsign:\s*"N0PEER-\d+"\s*(#.*)?$' `
        -Description "data/config/peering.yaml remote_callsign"

    Assert-RequiredLinePresent -Text $reputation `
        -AllowedPattern '(?m)^\s*ipinfo_download_enabled:\s*false\s*(#.*)?$' `
        -Description "data/config/reputation.yaml ipinfo_download_enabled"
    Assert-RequiredLinePresent -Text $reputation `
        -AllowedPattern '(?m)^\s*ipinfo_download_token:\s*"REPLACE_WITH_IPINFO_TOKEN"\s*(#.*)?$' `
        -Description "data/config/reputation.yaml ipinfo_download_token"
    Assert-RequiredLinePresent -Text $reputation `
        -AllowedPattern '(?m)^\s*ipinfo_api_enabled:\s*false\s*(#.*)?$' `
        -Description "data/config/reputation.yaml ipinfo_api_enabled"
    Assert-RequiredLinePresent -Text $reputation `
        -AllowedPattern '(?m)^\s*ipinfo_api_token:\s*""\s*(#.*)?$' `
        -Description "data/config/reputation.yaml ipinfo_api_token"
}

function New-ReleaseNotes {
    param(
        [string]$Version,
        [string]$Commit,
        [string]$BuildTime
    )

    return @"
GoCluster $Version

IMPORTANT DOWNLOAD NOTE

Download $PackageName.zip.

Do not use GitHub's automatic "Source code (zip)" or "Source code (tar.gz)"
downloads unless you want the developer source tree.

- Commit: $Commit
- Built: $BuildTime
- Asset: $PackageName.zip

Extract the asset and open the $PackageDirectoryName directory.
"@
}

function Publish-GitHubRelease {
    param(
        [string]$Version,
        [string]$Commit,
        [string]$ZipPath,
        [string]$Remote,
        [string]$BuildTime
    )

    Invoke-CheckedCommand -CommandName "git" `
        -Arguments @("tag", "-a", $Version, "-m", "Release $Version") `
        -FailureMessage "Failed to create tag $Version."
    try {
        Invoke-CheckedCommand -CommandName "git" `
            -Arguments @("push", $Remote, $Version) `
            -FailureMessage "Failed to push tag $Version to $Remote."

        $notes = New-ReleaseNotes -Version $Version -Commit $Commit -BuildTime $BuildTime
        Invoke-CheckedCommand -CommandName "gh" `
            -Arguments @(
                "release",
                "create",
                $Version,
                $ZipPath,
                "--title",
                $Version,
                "--notes",
                $notes
            ) `
            -FailureMessage "Failed to create GitHub Release $Version."
    }
    catch {
        Write-Warning "Release publishing failed after creating local tag $Version. Inspect local/remote tag state before retrying."
        throw
    }
}

if ($AllowDirty -and -not $PackageOnly) {
    throw "-AllowDirty is only permitted with -PackageOnly."
}
if ($SkipCodeMapCheck -and -not $PackageOnly) {
    throw "-SkipCodeMapCheck is only permitted with -PackageOnly."
}

$repoRoot = Resolve-RepoRoot
Push-Location $repoRoot
try {
    $gitStatus = @(Assert-CleanWorktree -RepoRoot $repoRoot -AllowDirty:$AllowDirty)
    Assert-GoModulesTidy
    if ($SkipCodeMapCheck) {
        Write-Warning "Skipping generated code-map freshness check for package-only testing."
    } else {
        Assert-CodeMapsFresh
    }

    $commit = (& git rev-parse --short=12 HEAD).Trim()
    $dirtySuffix = ""
    if ($gitStatus.Count -gt 0) {
        $dirtySuffix = "+dirty"
    }
    $buildUtc = (Get-Date).ToUniversalTime()
    $version = "v$($buildUtc.ToString("yy.dd.MM"))-$commit$dirtySuffix"
    $buildTime = $buildUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")

    if (-not $PackageOnly) {
        Assert-GitHubCliReady
        Assert-ReleaseTargetsAvailable -Remote $Remote -Version $version
    }

    $outputRoot = Join-Path $repoRoot $OutputDir
    $stageRoot = Join-Path $repoRoot $PackageDirectoryName
    $zipPath = Join-Path $outputRoot "$PackageName.zip"
    $legacyNestedStageRoot = Join-Path $outputRoot $PackageDirectoryName

    if (Test-Path -LiteralPath $stageRoot) {
        Remove-Item -LiteralPath $stageRoot -Recurse -Force
    }
    if ([IO.Path]::GetFullPath($legacyNestedStageRoot) -ne [IO.Path]::GetFullPath($stageRoot) -and
        (Test-Path -LiteralPath $legacyNestedStageRoot)) {
        Remove-Item -LiteralPath $legacyNestedStageRoot -Recurse -Force
    }
    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }
    New-Item -ItemType Directory -Path $outputRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $stageRoot -Force | Out-Null

    Copy-TrackedPayload -RepoRoot $repoRoot -StageRoot $stageRoot -AllowlistPrefixes @(
        "data/config",
        "data/cty",
        "data/h3",
        "data/peers/topology.db",
        "data/skm_correction/rbnskew.json"
    )
    Render-ReleaseReadme -RepoRoot $repoRoot -StageRoot $stageRoot
    Copy-ReleaseDocument -RepoRoot $repoRoot -StageRoot $stageRoot `
        -SourceRelativePath "docs/OPERATOR_GUIDE.md" `
        -DestinationRelativePath "docs/OPERATOR_GUIDE.md"
    Assert-PublicReleaseConfig -StageRoot $stageRoot
    Assert-ForbiddenPayloadAbsent -StageRoot $stageRoot

    $exePath = Join-Path $stageRoot "gocluster.exe"
    $ldflags = "-X main.Version=$version -X main.Commit=$commit -X main.BuildTime=$buildTime"

    $env:GOOS = "windows"
    $env:GOARCH = "amd64"
    & go build -trimpath -ldflags $ldflags -o $exePath .
    if ($LASTEXITCODE -ne 0) {
        throw "go build failed."
    }

    Push-Location $repoRoot
    try {
        Compress-Archive -Path $PackageDirectoryName -DestinationPath $zipPath -Force
    }
    finally {
        Pop-Location
    }

    Write-Host "Release package: $zipPath"
    Write-Host "Release version: $version"

    if ($PackageOnly) {
        Write-Host "Package-only mode: no tag, push, or GitHub Release was created."
    }
    else {
        Publish-GitHubRelease -Version $version -Commit $commit -ZipPath $zipPath -Remote $Remote -BuildTime $buildTime
        Write-Host "Published GitHub Release: $version"
    }
}
finally {
    Pop-Location
}

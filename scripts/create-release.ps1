<#
.SYNOPSIS
	Build a Windows amd64 ready-to-run GoCluster release package.

.DESCRIPTION
	Creates the ready_to_run payload, renders release README content, builds the
	Windows binary with stamped metadata, writes the release zip, and optionally
	publishes a GitHub release when PackageOnly is not set.

.PARAMETER PackageOnly
	Create the local package without publishing a GitHub release.

.PARAMETER ReleaseNumber
	Required positive release number. The UTC date and this number form the tag,
	for example 261003r2. Package-only builds stamp the intended tag as metadata.

.PARAMETER AllowDirty
	Allow local package creation from a dirty worktree. Publishing still requires
	clean, intentional source.

.PARAMETER OutputDir
	ZIP output directory, absolute or relative to the repository. Defaults to the repository root.

.PARAMETER PackageName
	Safe single-component base name for the generated zip package.

.PARAMETER PackageDirectoryName
	Safe single-component directory name at the repository root and inside the ZIP.

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
	Markerless legacy outputs are backed up automatically after preparation.
	Marked outputs require an unchanged ownership manifest. Source and output
	directories must have no concurrent writers during packaging.
#>

param(
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 2147483647)]
    [int]$ReleaseNumber,
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
    $result = Invoke-NativeResult -CommandName 'git' -Arguments @('-C', (Join-Path $PSScriptRoot '..'), 'rev-parse', '--show-toplevel')
    if ($result.ExitCode -ne 0 -or $result.Lines.Count -ne 1 -or [string]::IsNullOrWhiteSpace($result.Output)) {
        throw "Unable to resolve repository root with git."
    }
    return [IO.Path]::GetFullPath($result.Output.Trim())
}

function Invoke-NativeResult {
    param([string]$CommandName, [string[]]$Arguments)

    Get-Command $CommandName -ErrorAction Stop | Out-Null
    # Native stderr is diagnostic output on both supported engines. Only the
    # exit status decides success, including when the caller enables PS7's
    # native error-action preference. PS5 wraps redirected stderr in records.
    $ErrorActionPreference = 'Continue'
    $PSNativeCommandUseErrorActionPreference = $false
    $errorPath = [IO.Path]::GetTempFileName()
    try {
        $lines = @(& $CommandName @Arguments 2> $errorPath | ForEach-Object { $_.ToString() })
        $exitCode = $LASTEXITCODE
        return [pscustomobject]@{ Lines = $lines; Output = ($lines -join "`n")
            Diagnostic = [IO.File]::ReadAllText($errorPath); ExitCode = $exitCode }
    } finally {
        Remove-Item -LiteralPath $errorPath -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-CheckedCommand {
    param(
        [string]$CommandName,
        [string[]]$Arguments,
        [string]$FailureMessage
    )

    $result = Invoke-NativeResult -CommandName $CommandName -Arguments $Arguments
    if ($result.ExitCode -ne 0) {
        throw "$FailureMessage`n$($result.Output)`n$($result.Diagnostic)"
    }
    if ($result.Output) { Write-Host $result.Output }
    if ($result.Diagnostic) { Write-Host $result.Diagnostic.TrimEnd() }
}

function Assert-CleanWorktree {
    param(
        [string]$RepoRoot,
        [switch]$AllowDirty,
        [string[]]$GeneratedPaths = @()
    )

    $result = Invoke-NativeResult -CommandName 'git' -Arguments @('-C', $RepoRoot, 'status', '--porcelain=v1', '-z', '--untracked-files=all')
    if ($result.ExitCode -ne 0) {
        throw "git status --porcelain failed for $RepoRoot.`n$($result.Output)`n$($result.Diagnostic)"
    }
    $status = @($result.Output.Split([char]0) | Where-Object { $_ -ne '' })
    # The initial gate excludes nothing. After promotion, only untracked paths
    # in this invocation's verified outputs can be exempted; never source edits.
    $status = @($status | Where-Object {
        $entry = $_
        $owned = $false
        if ($entry.StartsWith('?? ')) {
            foreach ($path in $GeneratedPaths) {
                $relative = $entry.Substring(3)
                if ($relative -ceq $path -or $relative.StartsWith($path + '/', [StringComparison]::Ordinal)) { $owned = $true }
            }
        }
        -not $owned
    })

    if ($status.Count -gt 0 -and -not $AllowDirty) {
        throw @"
Refusing to create a release from a dirty worktree.
Repository: $RepoRoot
Git status:
$($status -join "`n")
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
    param([string]$HostName)
    if ($null -eq (Get-Command gh -ErrorAction SilentlyContinue)) {
        throw "GitHub CLI 'gh' is required to publish a release. Install gh and run 'gh auth login'."
    }

    Invoke-CheckedCommand -CommandName 'gh' -Arguments @('auth', 'status', '--hostname', $HostName) `
        -FailureMessage "GitHub CLI is not authenticated for $HostName. Run 'gh auth login' before creating a release."
}

function Assert-ReleaseTargetsAvailable {
    param(
        [object]$Target,
        [string]$Version
    )

    $local = Invoke-NativeResult -CommandName 'git' -Arguments @('show-ref', '--exists', "refs/tags/$Version")
    if ($local.ExitCode -eq 0) { throw "Local tag $Version already exists." }
    if ($local.ExitCode -ne 2) { throw "Unable to check local tag $Version.`n$($local.Output)`n$($local.Diagnostic)" }
    $remoteResult = Invoke-NativeResult -CommandName 'git' -Arguments @('ls-remote', '--exit-code', '--tags', $Target.PushUrl, "refs/tags/$Version")
    if ($remoteResult.ExitCode -eq 0) { throw "Remote tag $Version already exists on $($Target.Repository)." }
    if ($remoteResult.ExitCode -ne 2) { throw "Unable to check remote tag $Version.`n$($remoteResult.Output)`n$($remoteResult.Diagnostic)" }

    # A successful, complete listing proves absence without interpreting CLI
    # error prose or confusing repository/authentication 404s with missing tags.
    # Require push access so drafts are visible. Process one API page at a time.
    $repository = Invoke-NativeResult -CommandName 'gh' -Arguments @('repo', 'view', $Target.Repository, '--json', 'nameWithOwner,viewerPermission')
    if ($repository.ExitCode -ne 0) { throw "Unable to verify release repository.`n$($repository.Output)`n$($repository.Diagnostic)" }
    $info = $repository.Output | ConvertFrom-Json
    if ($info -isnot [pscustomobject] -or $info.nameWithOwner -isnot [string] -or $info.viewerPermission -isnot [string] -or
        $info.nameWithOwner -ine $Target.NameWithOwner -or $info.viewerPermission -notin @('ADMIN', 'MAINTAIN', 'WRITE')) {
        throw 'Release repository identity or draft visibility could not be verified.'
    }
    $page = 1
    do {
        $response = Invoke-NativeResult -CommandName 'gh' -Arguments @('api', '--hostname', $Target.HostName, '--method', 'GET',
            "repos/$($Target.NameWithOwner)/releases?per_page=100&page=$page")
        if ($response.ExitCode -ne 0) { throw "Unable to check GitHub releases.`n$($response.Output)`n$($response.Diagnostic)" }
        if (-not $response.Output.Trim().StartsWith('[')) { throw 'Invalid GitHub release listing.' }
        # PS5 emits an empty JSON array as one pipeline object; PS7 enumerates
        # it away. A property preserves the array shape in both engines.
        $listing = ConvertFrom-Json -InputObject ('{"items":' + $response.Output + '}')
        if (@($listing.PSObject.Properties).Count -ne 1) { throw 'Invalid GitHub release listing.' }
        $releases = @($listing.items)
        if ($releases.Count -gt 100) { throw 'Invalid GitHub release page size.' }
        foreach ($release in $releases) {
            if ($release.tag_name -isnot [string] -or [string]::IsNullOrWhiteSpace($release.tag_name) -or $release.draft -isnot [bool]) {
                throw 'Invalid GitHub release entry.'
            }
            if ($release.tag_name -ceq $Version) { throw "GitHub Release $Version already exists." }
        }
        $page++
    } while ($releases.Count -eq 100)
}

function Resolve-PublicationTarget {
    param([string]$Remote)

    $result = Invoke-NativeResult -CommandName 'git' -Arguments @('remote', 'get-url', '--push', '--all', $Remote)
    if ($result.ExitCode -ne 0 -or $result.Lines.Count -ne 1) { throw "Remote $Remote must have one unambiguous push URL." }
    $pushUrl = $result.Output.Trim()
    if ($pushUrl -match '^git@(?<server>[^/:\s]+):(?<owner>[^/\s]+)/(?<repo>[^/\s]+?)(?:\.git)?$') {
        $hostName = $Matches.server; $owner = $Matches.owner; $repo = $Matches.repo
    } else {
        $uri = $null
        if (-not [Uri]::TryCreate($pushUrl, [UriKind]::Absolute, [ref]$uri) -or $uri.Scheme -notin @('https', 'ssh') -or
            -not $uri.IsDefaultPort -or $uri.Query -or $uri.Fragment -or $uri.AbsolutePath -notmatch '^/(?<owner>[^/]+)/(?<repo>[^/]+?)(?:\.git)?$') {
            throw "Remote $Remote is not an unambiguous GitHub repository URL."
        }
        $hostName = $uri.DnsSafeHost; $owner = $Matches.owner; $repo = $Matches.repo
    }
    if ($owner -notmatch '^[A-Za-z0-9_.-]+$' -or $repo -notmatch '^[A-Za-z0-9_.-]+$' -or $owner -in @('.', '..') -or $repo -in @('.', '..')) {
        throw "Remote $Remote contains an invalid repository identity."
    }
    return [pscustomobject]@{ PushUrl = $pushUrl; HostName = $hostName; NameWithOwner = "$owner/$repo"; Repository = "$hostName/$owner/$repo" }
}

function Copy-TrackedPayload {
    param(
        [string]$RepoRoot,
        [string]$StageRoot,
        [string[]]$AllowlistPrefixes
    )

    $result = Invoke-NativeResult -CommandName 'git' -Arguments @('-C', $RepoRoot, 'ls-files', '-z', '--', 'data')
    if ($result.ExitCode -ne 0) { throw "git ls-files data failed.`n$($result.Output)`n$($result.Diagnostic)" }
    $tracked = @($result.Output.Split([char]0) | Where-Object { $_ -ne '' })

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
        Invoke-CheckedCommand -CommandName 'go' -Arguments $Arguments -FailureMessage "go $($Arguments -join ' ') failed."
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
        [string]$ReleaseTag,
        [string]$Commit,
        [string]$BuildTime
    )

    return @"
GoCluster $Version

IMPORTANT DOWNLOAD NOTE

Download $PackageName.zip.

Do not use GitHub's automatic "Source code (zip)" or "Source code (tar.gz)"
downloads unless you want the developer source tree.

- Product version: $Version
- Release tag: $ReleaseTag
- Commit: $Commit
- Built: $BuildTime
- Asset: $PackageName.zip

Extract the asset and open the $PackageDirectoryName directory.
"@
}

function Assert-SafePackageName {
    param([string]$Name, [string]$Description)

    if ([string]::IsNullOrWhiteSpace($Name) -or $Name -in @('.', '..') -or $Name.EndsWith('.') -or $Name.EndsWith(' ') -or
        $Name.IndexOfAny([IO.Path]::GetInvalidFileNameChars()) -ge 0 -or
        $Name -match '[\\/:*?"<>|\x00-\x1f]' -or $Name -match '^(?i:CON|PRN|AUX|NUL|COM[1-9\u00b9\u00b2\u00b3]|LPT[1-9\u00b9\u00b2\u00b3])(?:\.|$)') {
        throw "$Description must be a safe single Windows path component."
    }
}

function Test-PathWithin {
    param([string]$Path, [string]$Root)

    return $Path.Equals($Root, [StringComparison]::OrdinalIgnoreCase) -or
        $Path.StartsWith($Root.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)
}

function Assert-NoReparsePath {
    param([string]$Path)

    $current = [IO.Path]::GetFullPath($Path)
    while ($current) {
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw "Reparse-point path is unsafe: $current" }
        }
        $current = [IO.Path]::GetDirectoryName($current)
    }
}

function Get-ReleasePaths {
    param([string]$RepoRoot, [string]$OutputDir, [string]$PackageName, [string]$PackageDirectoryName)

    Assert-SafePackageName $PackageName 'PackageName'
    Assert-SafePackageName $PackageDirectoryName 'PackageDirectoryName'
    if ($PackageDirectoryName -ieq '.tmp') { throw 'PackageDirectoryName collides with the private build directory.' }
    if ([string]::IsNullOrWhiteSpace($OutputDir)) { throw 'OutputDir must not be empty.' }
    if ($OutputDir -match '^[A-Za-z]:(?![\\/])|^[\\/](?![\\/])') { throw 'OutputDir must be repository-relative or a fully qualified absolute directory.' }
    if ($OutputDir -match '(?:^|[\\/])\.\.(?:[\\/]|$)') { throw 'OutputDir must not contain parent-directory traversal; use an explicit absolute directory.' }
    # Windows canonicalization can erase trailing dots/spaces in intermediate
    # components. Validate the spelling before resolving it to a destination.
    $rawComponents = @($OutputDir -split '[\\/]')
    for ($index = 0; $index -lt $rawComponents.Count; $index++) {
        $component = $rawComponents[$index]
        if (-not $component -or $component -ceq '.' -or ($index -eq 0 -and $component -match '^[A-Za-z]:$')) { continue }
        Assert-SafePackageName $component 'OutputDir component'
    }
    $repoPath = [IO.Path]::GetFullPath($RepoRoot).TrimEnd('\', '/')
    $outputPath = if ([IO.Path]::IsPathRooted($OutputDir)) { [IO.Path]::GetFullPath($OutputDir) } else { [IO.Path]::GetFullPath((Join-Path $repoPath $OutputDir)) }
    $outputRootPath = [IO.Path]::GetPathRoot($outputPath)
    if ($outputPath.TrimEnd('\', '/') -ieq $outputRootPath.TrimEnd('\', '/')) {
        $outputPath = $outputRootPath
    } else {
        $outputPath = $outputPath.TrimEnd('\', '/')
    }
    foreach ($component in ($outputPath.Substring([IO.Path]::GetPathRoot($outputPath).Length) -split '[\\/]')) {
        if ($component) { Assert-SafePackageName $component 'OutputDir component' }
    }
    $stagePath = [IO.Path]::GetFullPath((Join-Path $repoPath $PackageDirectoryName))
    $zipPath = [IO.Path]::GetFullPath((Join-Path $outputPath "$PackageName.zip"))
    if ($outputPath -match '(?i)(?:^|[\\/])\.git(?:[\\/]|$)' -or $PackageDirectoryName -ieq '.git' -or
        (Test-PathWithin $outputPath $stagePath) -or
        (Test-PathWithin $zipPath $stagePath) -or
        ((Test-PathWithin $repoPath $outputPath) -and -not $outputPath.Equals($repoPath, [StringComparison]::OrdinalIgnoreCase))) {
        throw 'Output paths overlap staging, repository ancestors, or Git metadata.'
    }
    foreach ($path in @($repoPath, $outputPath, $stagePath, $zipPath)) { Assert-NoReparsePath $path }
    if ((Test-Path -LiteralPath $outputPath) -and -not (Test-Path -LiteralPath $outputPath -PathType Container)) { throw 'OutputDir is not a directory.' }
    # Git's tracked list protects source even when a tracked destination is
    # missing locally. Directory ownership protects unrelated untracked data.
    $tracked = Invoke-NativeResult -CommandName 'git' -Arguments @('-C', $repoPath, 'ls-files', '-z')
    if ($tracked.ExitCode -ne 0) { throw "Unable to verify output/source collisions.`n$($tracked.Output)" }
    foreach ($relative in $tracked.Output.Split([char]0)) {
        if (-not $relative) { continue }
        $source = [IO.Path]::GetFullPath((Join-Path $repoPath $relative))
        if ((Test-PathWithin $source $stagePath) -or $source.Equals($zipPath, [StringComparison]::OrdinalIgnoreCase)) { throw "Output collides with tracked source: $relative" }
    }
    return [pscustomobject]@{ RepoRoot = $repoPath; OutputRoot = $outputPath; StageRoot = $stagePath; ZipPath = $zipPath }
}

function Get-StageInventory {
    param([string]$StageRoot)

    Assert-NoReparsePath $StageRoot
    $pending = [Collections.Generic.Stack[string]]::new()
    $pending.Push($StageRoot)
    $entries = [Collections.Generic.List[object]]::new()
    while ($pending.Count) {
        foreach ($item in (Get-ChildItem -LiteralPath $pending.Pop() -Force)) {
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw "Reparse-point payload is unsafe: $($item.FullName)" }
            $relative = $item.FullName.Substring($StageRoot.Length + 1).Replace('\', '/')
            if ($relative -ieq '.gocluster-release-owner.json') { continue }
            $kind = if ($item.PSIsContainer) { 'directory' } else { 'file' }
            $hash = if ($item.PSIsContainer) { ''; $pending.Push($item.FullName) } else { (Get-FileHash -LiteralPath $item.FullName -Algorithm SHA256).Hash }
            $entries.Add([pscustomobject]@{ Path = $relative; Kind = $kind; Sha256 = $hash })
        }
    }
    return $entries.ToArray() | Sort-Object Path
}

function Assert-StageInventory {
    param([string]$StageRoot, [object[]]$Expected)

    $actual = @(Get-StageInventory $StageRoot)
    if ($actual.Count -ne $Expected.Count) { throw "Generated staging contents changed: $StageRoot" }
    $byPath = @{}
    foreach ($entry in $Expected) {
        if ($entry.Path -isnot [string] -or $byPath.ContainsKey($entry.Path)) { throw 'Invalid generated ownership inventory.' }
        $byPath[$entry.Path] = $entry
    }
    foreach ($entry in $actual) {
        $expectedEntry = $byPath[$entry.Path]
        if ($null -eq $expectedEntry -or $entry.Kind -cne $expectedEntry.Kind -or $entry.Sha256 -cne $expectedEntry.Sha256) {
            throw "Generated staging contents changed: $($entry.Path)"
        }
    }
}

function Assert-OwnedReleaseOutputs {
    param([object]$Paths, [switch]$AllowLegacy)

    $stageExists = Test-Path -LiteralPath $Paths.StageRoot
    $zipExists = Test-Path -LiteralPath $Paths.ZipPath
    if (-not $stageExists -and -not $zipExists) { return $null }
    $markerPath = Join-Path $Paths.StageRoot '.gocluster-release-owner.json'
    if ($AllowLegacy -and -not (Test-Path -LiteralPath $markerPath)) {
        if (($stageExists -and -not (Test-Path -LiteralPath $Paths.StageRoot -PathType Container)) -or
            ($zipExists -and -not (Test-Path -LiteralPath $Paths.ZipPath -PathType Leaf))) {
            throw 'Existing release destinations have incompatible types; staging must be a directory and the ZIP must be a file.'
        }
        foreach ($path in @($Paths.StageRoot, $Paths.ZipPath)) { Assert-NoReparsePath $path }
        if ($stageExists) { Get-StageInventory $Paths.StageRoot | Out-Null }
        return $null
    }
    if (-not (Test-Path -LiteralPath $Paths.StageRoot -PathType Container) -or
        -not (Test-Path -LiteralPath $Paths.ZipPath -PathType Leaf) -or -not (Test-Path -LiteralPath $markerPath -PathType Leaf)) {
        throw 'Existing release outputs have no verifiable ownership. Move legacy or unrelated outputs aside before retrying.'
    }
    foreach ($path in @($Paths.StageRoot, $Paths.ZipPath, $markerPath)) { Assert-NoReparsePath $path }
    $owner = [IO.File]::ReadAllText($markerPath) | ConvertFrom-Json
    if ($owner.Schema -ne 1 -or $owner.RepoRoot -ine $Paths.RepoRoot -or $owner.StageRoot -ine $Paths.StageRoot -or $owner.ZipPath -ine $Paths.ZipPath -or
        $owner.ZipSha256 -cne (Get-FileHash -LiteralPath $Paths.ZipPath -Algorithm SHA256).Hash) {
        throw 'Existing release output ownership or ZIP contents changed. Move outputs aside before retrying.'
    }
    Assert-StageInventory -StageRoot $Paths.StageRoot -Expected @($owner.Entries)
    return $owner
}

function Write-ReleaseOwnership {
    param([object]$Paths, [string]$PreparedStage, [string]$PreparedZip)

    $owner = [ordered]@{ Schema = 1; RepoRoot = $Paths.RepoRoot; StageRoot = $Paths.StageRoot; ZipPath = $Paths.ZipPath
        ZipSha256 = (Get-FileHash -LiteralPath $PreparedZip -Algorithm SHA256).Hash; Entries = @(Get-StageInventory $PreparedStage) }
    # Written after archiving: private replacement bookkeeping is never shipped.
    $owner | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $PreparedStage '.gocluster-release-owner.json') -Encoding UTF8
    return [pscustomobject]$owner
}

function Remove-RunDirectory {
    param([string]$Path, [string]$RunRoot)

    $absolute = [IO.Path]::GetFullPath($Path)
    if (-not (Test-PathWithin $absolute $RunRoot)) { throw "Cleanup escaped owned run directory: $absolute" }
    if (Test-Path -LiteralPath $absolute) {
        Assert-NoReparsePath $absolute
        Get-StageInventory $absolute | Out-Null
        Remove-Item -LiteralPath $absolute -Recurse -Force
    }
}

function Complete-ReleaseOutputs {
    param([object]$Paths, [string]$PreparedStage, [string]$PreparedZip, [string]$RunRoot, [object]$NewOwner)

    $verifiedPaths = Get-ReleasePaths -RepoRoot $Paths.RepoRoot -OutputDir $Paths.OutputRoot -PackageName $PackageName -PackageDirectoryName $PackageDirectoryName
    $oldOwner = Assert-OwnedReleaseOutputs $verifiedPaths -AllowLegacy
    $stageBackup = Join-Path $RunRoot 'previous-stage'
    $zipBackup = $Paths.ZipPath + '.rollback-' + [guid]::NewGuid().ToString('N')
    $stageSaved = $false; $zipSaved = $false; $stagePromoted = $false; $zipPromoted = $false
    New-Item -ItemType Directory -Path $Paths.OutputRoot -Force | Out-Null
    try {
        if (Test-Path -LiteralPath $Paths.StageRoot) { [IO.Directory]::Move($Paths.StageRoot, $stageBackup); $stageSaved = $true }
        if (Test-Path -LiteralPath $Paths.ZipPath) { [IO.File]::Move($Paths.ZipPath, $zipBackup); $zipSaved = $true }
        [IO.Directory]::Move($PreparedStage, $Paths.StageRoot); $stagePromoted = $true
        [IO.File]::Move($PreparedZip, $Paths.ZipPath); $zipPromoted = $true
        Assert-OwnedReleaseOutputs $Paths | Out-Null
    } catch {
        $promotionError = $_
        try {
            if ($stagePromoted) {
                Assert-StageInventory -StageRoot $Paths.StageRoot -Expected @($NewOwner.Entries)
                Assert-NoReparsePath $Paths.StageRoot
                # The freshly promoted stage has the run's independently checked
                # inventory. Move it back into the owned run before cleanup.
                [IO.Directory]::Move($Paths.StageRoot, $PreparedStage)
            }
            if ($zipPromoted) {
                Assert-NoReparsePath $Paths.ZipPath
                if ((Get-FileHash -LiteralPath $Paths.ZipPath -Algorithm SHA256).Hash -cne $NewOwner.ZipSha256) { throw 'Promoted ZIP changed during recovery.' }
                [IO.File]::Move($Paths.ZipPath, $PreparedZip)
            }
            if ($stageSaved) { [IO.Directory]::Move($stageBackup, $Paths.StageRoot) }
            if ($zipSaved) { [IO.File]::Move($zipBackup, $Paths.ZipPath) }
        } catch {
            $script:retainRunRoot = $true
            Write-Warning "Output recovery failed: $($_.Exception.Message). Preserve $RunRoot and $zipBackup; inspect both destinations before retrying."
        }
        throw $promotionError
    }
    try {
        if ($null -eq $oldOwner -and ($stageSaved -or $zipSaved)) {
            # Legacy contents are unknown user data: preserve them permanently,
            # and exempt this run from the outer temporary-directory cleanup.
            $script:retainRunRoot = $true
            if ($zipSaved) { [IO.File]::Move($zipBackup, (Join-Path $RunRoot 'previous-package.zip')) }
            Write-Host "Legacy release outputs preserved at: $RunRoot"
            return
        }
        if ($stageSaved) {
            Assert-StageInventory -StageRoot $stageBackup -Expected @($oldOwner.Entries)
            Remove-RunDirectory -Path $stageBackup -RunRoot $RunRoot
        }
        if ($zipSaved) {
            Assert-NoReparsePath $zipBackup
            if ((Get-FileHash -LiteralPath $zipBackup -Algorithm SHA256).Hash -cne $oldOwner.ZipSha256) { throw "Previous ZIP changed; preserved at $zipBackup" }
            Remove-Item -LiteralPath $zipBackup -Force
        }
    } catch {
        $script:retainRunRoot = $true
        Write-Warning "Previous output disposal failed: $($_.Exception.Message). Preserve $stageBackup and $zipBackup; inspect both destinations before retrying."
        throw
    }
}

function Get-HeadCommit {
    $result = Invoke-NativeResult -CommandName 'git' -Arguments @('rev-parse', '--verify', 'HEAD^{commit}')
    if ($result.ExitCode -ne 0 -or $result.Output.Trim() -notmatch '^(?:[0-9a-f]{40}|[0-9a-f]{64})$') { throw 'Unable to capture a valid HEAD commit.' }
    return $result.Output.Trim()
}

function Assert-ReleaseSourceUnchanged {
    param([string]$RepoRoot, [string]$CommitId, [string[]]$GeneratedPaths = @(), [switch]$AllowDirty)

    if ((Get-HeadCommit) -cne $CommitId) { throw 'HEAD changed during release preparation. No release refs will be created.' }
    Assert-CleanWorktree -RepoRoot $RepoRoot -GeneratedPaths $GeneratedPaths -AllowDirty:$AllowDirty | Out-Null
}

function Publish-GitHubRelease {
    param(
        [string]$Version,
        [string]$ReleaseTag,
        [string]$Commit,
        [string]$CommitId,
        [string]$ZipPath,
        [object]$Target,
        [string]$BuildTime
    )

    Invoke-CheckedCommand -CommandName "git" `
        -Arguments @("tag", "-a", $ReleaseTag, $CommitId, "-m", "Release $ReleaseTag") `
        -FailureMessage "Failed to create tag $ReleaseTag."
    try {
        Invoke-CheckedCommand -CommandName "git" `
            -Arguments @("push", $Target.PushUrl, "refs/tags/${ReleaseTag}:refs/tags/$ReleaseTag") `
            -FailureMessage "Failed to push tag $ReleaseTag to $($Target.Repository)."

        $notes = New-ReleaseNotes -Version $Version -ReleaseTag $ReleaseTag -Commit $Commit -BuildTime $BuildTime
        $notesPath = [IO.Path]::GetTempFileName()
        try {
            Set-Content -LiteralPath $notesPath -Value $notes -Encoding UTF8
            Invoke-CheckedCommand -CommandName "gh" `
                -Arguments @(
                    "release",
                    "create",
                    $ReleaseTag,
                    $ZipPath,
                    "--repo",
                    $Target.Repository,
                    "--verify-tag",
                    "--title",
                    $ReleaseTag,
                    "--notes-file",
                    $notesPath
                ) `
                -FailureMessage "Failed to create GitHub Release $ReleaseTag."
        }
        finally {
            Remove-Item -LiteralPath $notesPath -Force -ErrorAction SilentlyContinue
        }
    }
    catch {
        Write-Warning "Release publishing failed after creating local tag $ReleaseTag. Inspect local/remote tag state before retrying."
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
$oldGOOS = $env:GOOS
$oldGOARCH = $env:GOARCH
$runRoot = $null
$script:retainRunRoot = $false
Push-Location $repoRoot
try {
    Assert-CleanWorktree -RepoRoot $repoRoot -AllowDirty:$AllowDirty | Out-Null
    Assert-GoModulesTidy
    if ($SkipCodeMapCheck) {
        Write-Warning "Skipping generated code-map freshness check for package-only testing."
    } else {
        Assert-CodeMapsFresh
    }

    $commitId = Get-HeadCommit
    $shortCommit = Invoke-NativeResult -CommandName 'git' -Arguments @('rev-parse', '--short=12', $commitId)
    if ($shortCommit.ExitCode -ne 0 -or $shortCommit.Output.Trim() -notmatch '^[0-9a-f]{12,64}$') { throw 'Unable to abbreviate captured commit.' }
    $commit = $shortCommit.Output.Trim()
    $buildUtc = (Get-Date).ToUniversalTime()
    $version = $buildUtc.ToString("yyMMdd")
    $releaseTag = "${version}r${ReleaseNumber}"
    $buildTime = $buildUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")

    $paths = Get-ReleasePaths -RepoRoot $repoRoot -OutputDir $OutputDir -PackageName $PackageName -PackageDirectoryName $PackageDirectoryName
    Assert-OwnedReleaseOutputs $paths -AllowLegacy | Out-Null
    $target = $null
    if (-not $PackageOnly) {
        $target = Resolve-PublicationTarget $Remote
        Assert-GitHubCliReady -HostName $target.HostName
        Assert-ReleaseTargetsAvailable -Target $target -Version $releaseTag
    }

    # One run directory on the repository's volume permits directory promotion
    # without cross-volume moves. .tmp is an existing ignored build boundary.
    $runRoot = [IO.Path]::GetFullPath((Join-Path $repoRoot ('.tmp/release-' + [guid]::NewGuid().ToString('N'))))
    Assert-NoReparsePath $runRoot
    if (Test-Path -LiteralPath $runRoot) { throw 'Release run directory already exists.' }
    $stageRoot = Join-Path $runRoot $PackageDirectoryName
    $preparedZip = Join-Path $runRoot "$PackageName.zip"
    $zipPath = $paths.ZipPath
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
    Copy-ReleaseDocument -RepoRoot $repoRoot -StageRoot $stageRoot `
        -SourceRelativePath "third_party/go-sqlite3/LICENSE" `
        -DestinationRelativePath "licenses/go-sqlite3-LICENSE.txt"
    Copy-ReleaseDocument -RepoRoot $repoRoot -StageRoot $stageRoot `
        -SourceRelativePath "third_party/go-sqlite3/engine/LICENSE" `
        -DestinationRelativePath "licenses/go-sqlite3-engine-LICENSE.txt"
    Copy-ReleaseDocument -RepoRoot $repoRoot -StageRoot $stageRoot `
        -SourceRelativePath "third_party/go-sqlite3/provenance/GO-LICENSE.txt" `
        -DestinationRelativePath "licenses/go-LICENSE.txt"
    Assert-PublicReleaseConfig -StageRoot $stageRoot
    Assert-ForbiddenPayloadAbsent -StageRoot $stageRoot

    $exePath = Join-Path $stageRoot "gocluster.exe"
    $ldflags = "-X main.Version=$version -X main.ReleaseTag=$releaseTag -X main.Commit=$commit -X main.BuildTime=$buildTime"

    $env:GOOS = "windows"
    $env:GOARCH = "amd64"
    Invoke-CheckedCommand -CommandName 'go' -Arguments @('build', '-trimpath', '-ldflags', $ldflags, '-o', $exePath, '.') -FailureMessage 'go build failed.'
    # The companion owns peer diagnostic file I/O. It must come from the same
    # package source and sit beside the cluster; runtime never searches PATH.
    $peerDiagnosticExe = Join-Path $stageRoot "peerdiag.exe"
    Invoke-CheckedCommand -CommandName 'go' -Arguments @('build', '-trimpath', '-o', $peerDiagnosticExe, './cmd/peerdiag') -FailureMessage 'peer diagnostic companion build failed.'
    @($exePath, $peerDiagnosticExe) | ForEach-Object {
        [ordered]@{ file = [IO.Path]::GetFileName($_); sha256 = (Get-FileHash -LiteralPath $_ -Algorithm SHA256).Hash }
    } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $stageRoot 'binaries.json')

    Compress-Archive -LiteralPath $stageRoot -DestinationPath $preparedZip
    $owner = Write-ReleaseOwnership -Paths $paths -PreparedStage $stageRoot -PreparedZip $preparedZip
    Assert-ReleaseSourceUnchanged -RepoRoot $repoRoot -CommitId $commitId -AllowDirty:$AllowDirty
    Complete-ReleaseOutputs -Paths $paths -PreparedStage $stageRoot -PreparedZip $preparedZip -RunRoot $runRoot -NewOwner $owner

    Write-Host "Release package: $zipPath"
    Write-Host "Release version: $version"
    Write-Host "Release tag: $releaseTag"

    if ($PackageOnly) {
        Write-Host "Package-only mode: no tag, push, or GitHub Release was created."
    }
    else {
        Assert-OwnedReleaseOutputs $paths | Out-Null
        $generatedPaths = @($PackageDirectoryName)
        if (Test-PathWithin $zipPath $repoRoot) { $generatedPaths += $zipPath.Substring($repoRoot.Length + 1).Replace('\', '/') }
        Assert-ReleaseSourceUnchanged -RepoRoot $repoRoot -CommitId $commitId -GeneratedPaths $generatedPaths
        Publish-GitHubRelease -Version $version -ReleaseTag $releaseTag -Commit $commit -CommitId $commitId -ZipPath $zipPath -Target $target -BuildTime $buildTime
        Write-Host "Published GitHub Release: $releaseTag"
    }
}
finally {
    try {
        if ($null -ne $runRoot -and -not $script:retainRunRoot) { Remove-RunDirectory -Path $runRoot -RunRoot $runRoot }
    } finally {
        if ($null -eq $oldGOOS) { Remove-Item Env:GOOS -ErrorAction SilentlyContinue } else { $env:GOOS = $oldGOOS }
        if ($null -eq $oldGOARCH) { Remove-Item Env:GOARCH -ErrorAction SilentlyContinue } else { $env:GOARCH = $oldGOARCH }
        Pop-Location
    }
}

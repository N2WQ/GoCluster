# Release-only artifacts. Git snapshots and scratch files belong to the run
# directory. Only the generated artifacts enter the public package.

function Resolve-UpgradeBaseline {
    param([string]$Tag, [string]$CommitId)

    if ($Tag -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$' -or $Tag.Contains('..')) {
        throw 'Supply an exact baseline release tag with -BaseReleaseTag.'
    }
    $result = Invoke-NativeResult 'git' @('rev-parse', '--verify', "refs/tags/$Tag^{commit}")
    $baseCommit = $result.Output.Trim()
    if ($result.ExitCode -ne 0 -or $baseCommit -cnotmatch '^(?:[0-9a-f]{40}|[0-9a-f]{64})$') {
        throw "Cannot resolve baseline tag $Tag to a local commit. Fetch the exact release tag and retry."
    }
    $ancestor = Invoke-NativeResult 'git' @('merge-base', '--is-ancestor', $baseCommit, $CommitId)
    if ($ancestor.ExitCode -ne 0 -or $baseCommit -ceq $CommitId) {
        throw "Baseline tag $Tag must be a strict ancestor of the candidate commit."
    }
    return $baseCommit
}

function Get-LatestPublishedBaseline {
    param([object]$Target)

    $result = Invoke-NativeResult 'gh' @('api', '--hostname', $Target.HostName, '--method', 'GET',
        "repos/$($Target.NameWithOwner)/releases/latest")
    if ($result.ExitCode -ne 0) { throw 'Cannot determine the latest published stable release.' }
    try { $release = $result.Output | ConvertFrom-Json } catch { throw 'Invalid latest release response.' }
    if ($release -isnot [pscustomobject] -or
        -not $release.PSObject.Properties['tag_name'] -or -not $release.PSObject.Properties['draft'] -or
        -not $release.PSObject.Properties['prerelease'] -or -not $release.PSObject.Properties['published_at']) {
        throw 'Invalid latest published stable release response.'
    }
    $published = [DateTimeOffset]::MinValue
    if ($release.tag_name -isnot [string] -or
        $release.tag_name -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$' -or $release.tag_name.Contains('..') -or
        $release.draft -isnot [bool] -or $release.prerelease -isnot [bool] -or $release.draft -or $release.prerelease -or
        ($release.published_at -isnot [string] -and $release.published_at -isnot [datetime] -and $release.published_at -isnot [datetimeoffset]) -or
        -not [DateTimeOffset]::TryParse([string]$release.published_at, [ref]$published)) {
        throw 'Invalid latest published stable release response.'
    }
    return $release.tag_name
}

function Get-RemoteUpgradeBaseline {
    param([object]$Target, [string]$Tag)

    $ref = "refs/tags/$Tag"
    $peeled = "$ref^{}"
    $result = Invoke-NativeResult 'git' @('ls-remote', '--exit-code', '--tags', $Target.PushUrl, $ref, $peeled)
    if ($result.ExitCode -ne 0) { throw "Cannot verify baseline tag $Tag on $($Target.Repository)." }
    $refs = @{}
    foreach ($line in $result.Lines) {
        $fields = $line.Split([char]9)
        if ($fields.Count -ne 2 -or $fields[0] -cnotmatch '^(?:[0-9a-f]{40}|[0-9a-f]{64})$' -or
            $fields[1] -cnotin @($ref, $peeled) -or $refs.ContainsKey($fields[1])) {
            throw "Invalid remote tag response for baseline $Tag."
        }
        $refs[$fields[1]] = $fields[0]
    }
    if (-not $refs.ContainsKey($ref)) { throw "Missing remote baseline tag $Tag." }
    return $(if ($refs.ContainsKey($peeled)) { $refs[$peeled] } else { $refs[$ref] })
}

function Assert-RemoteUpgradeBaseline {
    param([object]$Target, [string]$Tag, [string]$BaseCommit)

    $remoteCommit = Get-RemoteUpgradeBaseline $Target $Tag
    if ($remoteCommit -cne $BaseCommit) {
        throw "Remote baseline tag $Tag does not match local commit $BaseCommit."
    }
}

function Resolve-AutomaticUpgradeBaseline {
    param([object]$Target, [string]$Tag, [string]$CommitId)

    $base = Get-RemoteUpgradeBaseline $Target $Tag
    $object = Invoke-NativeResult 'git' @('cat-file', '-e', "$base^{commit}")
    if ($object.ExitCode -ne 0) {
        # Fetch objects only: leave local tags, branches and FETCH_HEAD intact.
        $fetch = Invoke-NativeResult 'git' @('fetch', '--no-tags', '--no-write-fetch-head', '--no-recurse-submodules',
            $Target.PushUrl, "refs/tags/$Tag")
        if ($fetch.ExitCode -ne 0) { throw "Cannot fetch baseline objects for $Tag." }
    }
    $resolved = Invoke-NativeResult 'git' @('rev-parse', '--verify', "$base^{commit}")
    if ($resolved.ExitCode -ne 0 -or $resolved.Output.Trim() -cne $base) {
        throw "Cannot verify baseline commit for $Tag."
    }
    $shallow = Invoke-NativeResult 'git' @('rev-parse', '--is-shallow-repository')
    if ($shallow.ExitCode -ne 0 -or $shallow.Output.Trim() -cnotin @('true', 'false')) {
        throw 'Cannot determine whether release history is complete.'
    }
    if ($shallow.Output.Trim() -ceq 'true') {
        $history = Invoke-NativeResult 'git' @('fetch', '--unshallow', '--no-tags', '--no-write-fetch-head',
            '--no-recurse-submodules', $Target.PushUrl, 'HEAD', "refs/tags/$Tag")
        if ($history.ExitCode -ne 0) { throw 'Cannot obtain complete release history.' }
    }
    $ancestor = Invoke-NativeResult 'git' @('merge-base', '--is-ancestor', $base, $CommitId)
    if ($ancestor.ExitCode -ne 0 -or $base -ceq $CommitId) {
        throw "Baseline tag $Tag must be a strict ancestor of the candidate commit."
    }
    Assert-RemoteUpgradeBaseline $Target $Tag $base
    return $base
}

function Get-StagedReleaseYaml {
    param([string]$StageRoot)

    $yaml = [Collections.Generic.SortedDictionary[string,string]]::new([StringComparer]::Ordinal)
    foreach ($file in Get-ChildItem -LiteralPath (Join-Path $StageRoot 'data/config') -File -Recurse) {
        if ($file.Extension -notin @('.yaml', '.yml')) { continue }
        $relative = $file.FullName.Substring($StageRoot.Length + 1).Replace('\', '/')
        $yaml.Add($relative, $file.FullName)
    }
    return ,$yaml
}

function Write-ReleaseConfigBundle {
    param([string]$StageRoot, [object]$Yaml, [string]$ReleaseTag, [string]$CommitId, [bool]$Dirty)

    $files = @(
        foreach ($item in $Yaml.GetEnumerator()) {
            [ordered]@{ path = $item.Key; sha256 = (Get-FileHash -LiteralPath $item.Value -Algorithm SHA256).Hash.ToLowerInvariant() }
        }
    )
    $manifest = [ordered]@{
        format_version = 1
        release_tag = $ReleaseTag
        source_commit = $CommitId
        dirty_preview = $Dirty
        files = $files
    }
    $json = ConvertTo-Json -InputObject $manifest -Depth 4
    [IO.File]::WriteAllText((Join-Path $StageRoot 'data/config-bundle.json'), "$json`n")
}

function Open-ReleaseSnapshot {
    param([string]$CommitId, [string]$Path, [string]$ZipPath)

    $result = Invoke-NativeResult 'git' @('archive', '--format=zip', "--output=$ZipPath", $CommitId, '--', $Path)
    if ($result.ExitCode -ne 0) { throw "Cannot read $Path from release commit $CommitId." }
    return [IO.Compression.ZipFile]::OpenRead($ZipPath)
}

function Compare-ReleaseYaml {
    param([string]$Path, [object]$Old, [object]$Yaml, [string]$Scratch)

    if (-not $Old.ContainsKey($Path)) { return @{ Description = 'Added'; Lines = @() } }
    if (-not $Yaml.ContainsKey($Path)) { return @{ Description = 'Removed'; Lines = @() } }

    # Ignore ordinary checkout line-ending conversion for the text
    # classification. The ZIP hashes still cover the original staged bytes.
    [IO.Compression.ZipFileExtensions]::ExtractToFile($Old[$Path], $Scratch, $true)
    $encoding = [Text.UTF8Encoding]::new($false, $true)
    $before = [IO.File]::ReadAllText($Scratch, $encoding).Replace("`r`n", "`n")
    $after = [IO.File]::ReadAllText($Yaml[$Path], $encoding).Replace("`r`n", "`n")
    if ($before -ceq $after) { return $null }

    $previousEncoding = [Console]::OutputEncoding
    try {
        [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
        $result = Invoke-NativeResult 'dyff' @('between', '--omit-header', '--color', 'off', '--set-exit-code',
            '--format-strings=false', '--detect-kubernetes=false', '--detect-renames=false', '--no-cert-inspection',
            $Scratch, $Yaml[$Path])
    } finally { [Console]::OutputEncoding = $previousEncoding }
    if ($result.ExitCode -eq 0) { return @{ Description = 'Text changed; no structural difference reported'; Lines = @() } }
    if ($result.ExitCode -ne 1 -or [string]::IsNullOrWhiteSpace($result.Output)) {
        # Parser diagnostics can include YAML values. Report the file and status.
        throw "dyff could not compare $Path (exit $($result.ExitCode))."
    }
    return @{ Description = 'Structural changes'; Lines = $result.Lines }
}

function Write-ReleaseYamlDiff {
    param([string]$StageRoot, [string]$RunRoot, [object]$Yaml, [string]$BaseCommit, [string]$CommitId)

    $version = Invoke-NativeResult 'dyff' @('version')
    if ($version.ExitCode -ne 0 -or $version.Output.Trim() -cnotmatch ' version 1\.12\.0$') {
        throw 'Upgrade reports require dyff 1.12.0.'
    }
    $baseline = Open-ReleaseSnapshot $BaseCommit 'data/config' (Join-Path $RunRoot 'baseline-config.zip')
    try {
        $paths = [Collections.Generic.SortedSet[string]]::new([StringComparer]::Ordinal)
        $old = [Collections.Generic.Dictionary[string,IO.Compression.ZipArchiveEntry]]::new([StringComparer]::Ordinal)
        foreach ($entry in $baseline.Entries) {
            if ($entry.FullName -notmatch '\.ya?ml$') { continue }
            if ($entry.FullName -notmatch '^data/config/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\.ya?ml$' -or
                $entry.FullName -match '(^|/)\.\.?(/|$)' -or $entry.Name -ieq 'openai.yaml') {
                throw 'Unexpected YAML path in the public baseline snapshot.'
            }
            $old.Add($entry.FullName, $entry)
            [void]$paths.Add($entry.FullName)
        }
        foreach ($path in $Yaml.Keys) { [void]$paths.Add($path) }
        $appendix = [Collections.Generic.List[string]]::new()
        $summary = [Collections.Generic.List[string]]::new()
        $appendix.Add("# Public YAML changes`n")
        $appendix.Add("Baseline: $BaseCommit`nCandidate: $CommitId`n")
        $scratch = Join-Path $RunRoot 'baseline.yaml'
        foreach ($path in $paths) {
            $section = Compare-ReleaseYaml $path $old $Yaml $scratch
            if ($null -eq $section) { continue }
            $summary.Add(('- {0} - {1}' -f $path, $section.Description))
            $appendix.Add(("## {0}`n`n{1}`n" -f $path, $section.Description))
            foreach ($line in $section.Lines) { $appendix.Add("    $line") }
            $appendix.Add('')
        }
        if ($summary.Count -eq 0) {
            $summary.Add('- No changed public YAML files.')
            $appendix.Add('No changed public YAML files.')
        }
        [IO.File]::WriteAllText((Join-Path $StageRoot 'CONFIG-DIFF.md'), ($appendix -join "`n") + "`n")
        return ($summary -join "`n")
    } finally { $baseline.Dispose() }
}

# Hashes contain no file contents. Missing optional assets are represented so
# adding one during execution cannot silently change the runtime input set.
function Get-PC92QualificationAssets([string]$RepositoryRoot, [string]$Family) {
    if ($Family -notin @('runtime', 'q4')) { return @() }
    $paths = @((Join-Path $RepositoryRoot 'data/cty'), (Join-Path $RepositoryRoot 'data/h3'))
    $pipeline = Join-Path $RepositoryRoot 'data/config/pipeline.yaml'
    foreach ($line in Get-Content -LiteralPath $pipeline) {
        if ($line -match '^\s*(confusion_model_file|spotter_reliability_file(?:_cw|_rtty)?):\s*(.*?)\s*$') {
            $value = $Matches[2]
            if ($value -match '^"([^"\\]*)"\s*(?:#.*)?$' -or $value -match "^'([^']*)'\s*(?:#.*)?$") { $value = $Matches[1] }
            elseif ($value -match '^([^#\s]+)(?:\s+#.*)?$') { $value = $Matches[1] }
            else { throw 'manifest_asset_syntax: unsupported model path syntax; cannot attest inputs' }
            if ($value) { $paths += if ([IO.Path]::IsPathRooted($value)) { $value } else { Join-Path $RepositoryRoot $value } }
        }
    }
    return $paths
}

function Get-PC92InputManifest([string]$RepositoryRoot, [string]$Family, [string[]]$ExternalPaths = @()) {
    $relative = @(& git -C $RepositoryRoot ls-files -co --exclude-standard)
    if ($LASTEXITCODE -ne 0) { throw 'manifest_git_failed' }
    $paths = @($relative | ForEach-Object { Join-Path $RepositoryRoot $_ })
    $paths += @(Get-PC92QualificationAssets $RepositoryRoot $Family)
    $paths += $ExternalPaths
    $leaves = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $missing = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($path in $paths) {
        if (-not $path) { continue }
        $absolute = [IO.Path]::GetFullPath($path)
        if (Test-Path -LiteralPath $absolute -PathType Container) {
            foreach ($file in Get-ChildItem -LiteralPath $absolute -File -Recurse -Force) { $null = $leaves.Add($file.FullName) }
        } elseif (Test-Path -LiteralPath $absolute -PathType Leaf) { $null = $leaves.Add($absolute) }
        else { $null = $missing.Add($absolute) }
    }
    $rows = @(foreach ($path in @($leaves | Sort-Object -CaseSensitive)) {
        [ordered]@{ path = $path; sha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash }
    })
    $rows += @(foreach ($path in @($missing | Sort-Object -CaseSensitive)) { [ordered]@{ path = $path; sha256 = 'MISSING' } })
    return ConvertTo-Json -InputObject @($rows) -Depth 4 -Compress
}

function Assert-PC92PinnedReference([string]$Root) {
    $pin = '3e9b3621d94dd45c68702e4a0f896aac33f2a91d'
    $head = & git -C $Root rev-parse HEAD
    if ($LASTEXITCODE -ne 0 -or $head -cne $pin) { throw 'reference_pin_mismatch' }
    & git -C $Root diff --quiet $pin -- perl data/prefix_data.pl
    if ($LASTEXITCODE -ne 0) { throw 'reference_source_modified' }
}

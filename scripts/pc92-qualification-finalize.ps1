# Final reconciliation is deliberately separate from a single workload verdict.
# Review records attest source-level findings; they are not automatic proofs.
# Every attestation and test artifact is retained by hash, on the same source.
. (Join-Path $PSScriptRoot 'pc92-qualification-run.ps1')

function Get-PC92FinalRequirements {
    return [ordered]@{
        profiles = @('runtime/q1', 'runtime/q2', 'runtime/q3', 'runtime/shipped-q1', 'q4/a', 'q4/b',
            'q5/qualification', 'q6/qualification', 'cache/cache-memory', 'cache/cache-sustained', 'retry/qualification')
        checks = @('normal', 'vet', 'staticcheck', 'lint', 'race', 'tagged', 'fuzz', 'benchmarks', 'profiles',
            'windows-native', 'windows-fallback', 'linux-native', 'sqlite-cycles', 'sqlite-sustained', 'packaging', 'documentation')
        # Metadata's three subdivisions sum to 32 MiB. Projection's two sum to 48 MiB.
        partitions = [ordered]@{ spot_dedupe = 96; graph = 96; other_dedupe = 32; queues = 160;
            staging = 16; publication = 12; projection = 36; sqlite = 16; diagnostics = 3; metadata = 13 }
        corrections = @((1..14 | ForEach-Object { 'V15-{0:00}' -f $_ }); (1..6 | ForEach-Object { 'V16-{0:00}' -f $_ }))
    }
}

function Read-PC92FinalSnapshot([string]$Path) {
    try {
        $bytes = [IO.File]::ReadAllBytes($Path)
        $stream = [IO.MemoryStream]::new($bytes, $false)
        $reader = [IO.StreamReader]::new($stream)
        try { $value = $reader.ReadToEnd() | ConvertFrom-Json -ErrorAction Stop }
        finally { $reader.Dispose() }
    }
    catch { throw "invalid_evidence_object: $Path" }
    if ($value -isnot [pscustomobject]) { throw "invalid_evidence_object: $Path" }
    return [pscustomobject]@{ value = $value; artifact = [pscustomobject]@{
        path = $Path; sha256 = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)) } }
}

function Read-PC92FinalObject([string]$Path) {
    return (Read-PC92FinalSnapshot $Path).value
}

function Assert-PC92FinalArtifact($Artifact) {
    if ($Artifact.path -isnot [string] -or -not [IO.Path]::IsPathFullyQualified($Artifact.path) -or
        $Artifact.sha256 -isnot [string] -or $Artifact.sha256 -cnotmatch '^[A-Fa-f0-9]{64}$' -or
        -not (Test-Path -LiteralPath $Artifact.path -PathType Leaf)) { throw 'invalid_artifact' }
    if ((Get-FileHash -LiteralPath $Artifact.path -Algorithm SHA256).Hash -ine $Artifact.sha256) { throw "artifact_changed: $($Artifact.path)" }
}

function Add-PC92FinalArtifact($Artifact, [Collections.Generic.List[object]]$Closure) {
    Assert-PC92FinalArtifact $Artifact
    $Closure.Add([pscustomobject]@{ path = $Artifact.path; sha256 = $Artifact.sha256 })
}

# The source identity includes every tracked/non-ignored file, including tests,
# scripts and documentation. It is path-independent for native OS evidence,
# but it never ignores changed source files or normalizes their bytes.
function Get-PC92FinalSourceDigest([string]$Root) {
    $paths = @(& git -C $Root ls-files -co --exclude-standard | Sort-Object -Unique -CaseSensitive)
    if ($LASTEXITCODE -ne 0 -or $paths.Count -eq 0) { throw 'source_identity_failed' }
    $rows = @(foreach ($path in $paths) {
        $file = Join-Path $Root $path
        $hash = if (Test-Path -LiteralPath $file -PathType Leaf) { (Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash } else { 'MISSING' }
        [ordered]@{ path = $path; sha256 = $hash }
    })
    $bytes = [Text.Encoding]::UTF8.GetBytes((ConvertTo-Json -InputObject $rows -Depth 3 -Compress))
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes))
}

function Assert-PC92FinalIDSet($Rows, [string[]]$Expected, [string]$Kind) {
    if ($Rows -isnot [array]) { throw "invalid_${Kind}_set" }
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($row in $Rows) {
        if ($row.id -isnot [string] -or $row.id -cnotin $Expected -or -not $seen.Add($row.id)) { throw "invalid_${Kind}_set" }
    }
    foreach ($id in $Expected) { if (-not $seen.Contains($id)) { throw "missing_${Kind}: $id" } }
}

function Assert-PC92FinalReviewRow($Row, [string]$Digest, [Collections.Generic.List[object]]$Closure) {
    if ($Row.status -cne 'passed' -or $Row.source_digest -cne $Digest -or
        $Row.summary -isnot [string] -or [string]::IsNullOrWhiteSpace($Row.summary) -or
        $Row.artifacts -isnot [array] -or $Row.artifacts.Count -eq 0 -or
        $Row.open_evidence -isnot [array] -or $Row.open_evidence.Count -ne 0) { throw "review_incomplete: $($Row.id)" }
    foreach ($artifact in $Row.artifacts) { Add-PC92FinalArtifact $artifact $Closure }
}

function Assert-PC92FinalExternalInputs($Run) {
    if ($Run.external_inputs -isnot [array]) { throw 'run_external_inputs_missing' }
    $expected = @()
    if ($Run.family -ceq 'q6') {
        foreach ($field in @('reference_root', 'perl_executable')) {
            if ($Run.$field -isnot [string] -or -not [IO.Path]::IsPathFullyQualified($Run.$field)) { throw "q6_identity_missing: $field" }
        }
        if (-not (Test-Path -LiteralPath $Run.reference_root -PathType Container) -or
            -not (Test-Path -LiteralPath $Run.perl_executable -PathType Leaf)) { throw 'q6_identity_missing' }
        Assert-PC92PinnedReference $Run.reference_root
        $expected = @((Join-Path $Run.reference_root 'perl'), (Join-Path $Run.reference_root 'data/prefix_data.pl'), $Run.perl_executable)
        if (-not (Test-Path -LiteralPath $expected[0] -PathType Container) -or
            -not (Test-Path -LiteralPath $expected[1] -PathType Leaf)) { throw 'q6_reference_inputs_missing' }
        foreach ($field in @('perl_library', 'reference_dll_directory')) {
            if ($Run.$field -isnot [string]) { throw "q6_identity_missing: $field" }
            if ($Run.$field) {
                if (-not [IO.Path]::IsPathFullyQualified($Run.$field) -or -not (Test-Path -LiteralPath $Run.$field -PathType Container)) { throw "q6_identity_missing: $field" }
                $expected += $Run.$field
            }
        }
    }
    if ((ConvertTo-Json -InputObject @($Run.external_inputs) -Compress) -cne
        (ConvertTo-Json -InputObject @($expected) -Compress)) { throw 'run_external_closure_mismatch' }
}

function Assert-PC92FinalRun([string]$Directory, [string]$ID, [string]$Root, [string]$Digest, [Collections.Generic.List[object]]$Closure) {
    $snapshot = Read-PC92FinalSnapshot (Join-Path $Directory 'verdict.json')
    $run = $snapshot.value
    Add-PC92FinalArtifact $snapshot.artifact $Closure
    $parts = $ID.Split('/')
    $plan = Get-PC92QualificationPlan $parts[0] $parts[1]
    if ($run.family -cne $parts[0] -or $run.profile -cne $parts[1] -or $run.status -cne 'measured' -or
        $run.diagnostic -isnot [bool] -or $run.diagnostic -or $run.measurement_passed -isnot [bool] -or -not $run.measurement_passed -or
        $run.provenance_passed -isnot [bool] -or -not $run.provenance_passed -or
        $run.profile_accepted -isnot [bool] -or -not $run.profile_accepted -or
        ($run.exit_code -isnot [int] -and $run.exit_code -isnot [long]) -or $run.exit_code -ne 0 -or
        $run.failure_reasons -isnot [array] -or $run.failure_reasons.Count -ne 0) { throw "run_not_accepted: $ID" }
    if ($run.repository_root -cne $Root -or $run.runtime_settings -cne 'GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1536MiB' -or
        $run.go_version -cne 'go version go1.26.4 windows/amd64') { throw "run_environment_mismatch: $ID" }
    Assert-PC92FinalExternalInputs $run
    $current = Get-PC92InputManifest $Root $parts[0] $run.external_inputs
    foreach ($name in @('source-before.json', 'source-after-build.json', 'source-after-run.json')) {
        $manifestPath = Join-Path $Directory $name
        Add-PC92FinalArtifact ([pscustomobject]@{ path = $manifestPath; sha256 = (Get-FileHash -LiteralPath $manifestPath).Hash }) $Closure
        if ((Get-Content -LiteralPath $manifestPath -Raw).Trim() -cne $current) { throw "run_source_changed: $ID/$name" }
    }
    # The test and helper must still be the retained sibling pair. Paths to
    # another run's binary are not accepted even when hashes happen to match.
    $binary = Join-Path $Directory ($parts[0] + '.test.exe')
    $helper = Join-Path $Directory 'peerdiag.exe'
    if ($run.executable -cne $binary -or $run.helper_executable -cne $helper) { throw "run_binary_path_mismatch: $ID" }
    Add-PC92FinalArtifact ([pscustomobject]@{ path = $binary; sha256 = $run.executable_sha256 }) $Closure
    Add-PC92FinalArtifact ([pscustomobject]@{ path = $helper; sha256 = $run.helper_executable_sha256 }) $Closure
    $log = Join-Path $Directory 'test.log'
    Add-PC92FinalArtifact ([pscustomobject]@{ path = $log; sha256 = $run.test_log_sha256 }) $Closure
    $observation = Join-Path $Directory 'observations.json'
    if ($plan.observation) { Add-PC92FinalArtifact ([pscustomobject]@{ path = $observation; sha256 = $run.observations_sha256 }) $Closure }
    if ($parts[0] -eq 'q6' -and $run.reference_pin -cne '3e9b3621d94dd45c68702e4a0f896aac33f2a91d') { throw 'reference_pin_mismatch' }
    if ($run.run_id -cnotmatch '^[a-f0-9]{32}$' -or $run.elapsed_seconds -isnot [ValueType] -or
        $run.elapsed_seconds -is [bool] -or -not [double]::IsFinite([double]$run.elapsed_seconds)) { throw "invalid_run_identity: $ID" }
    Test-PC92QualificationEvidence $plan $run.run_id (Get-Content -LiteralPath $log -Raw) $observation $run.elapsed_seconds
    return [ordered]@{ id = $ID; run_id = $run.run_id; source_digest = $Digest;
        verdict = $snapshot.artifact }
}

function Invoke-PC92FinalQualification([string]$BundlePath, [string]$OutputDirectory) {
    $ErrorActionPreference = 'Stop'
    $output = [IO.Path]::GetFullPath($OutputDirectory)
    if (Test-Path -LiteralPath $output) { throw 'final_output_not_fresh' }
    $bundleSnapshot = Read-PC92FinalSnapshot ([IO.Path]::GetFullPath($BundlePath))
    $bundle = $bundleSnapshot.value
    $root = [IO.Path]::GetFullPath($bundle.repository_root)
    if ($output -eq $root -or $output.StartsWith($root.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw 'output_inside_source' }
    $null = New-Item -ItemType Directory -Path $output
    $path = Join-Path $output 'final-verdict.json'
    $result = [ordered]@{ schema_version = 1; status = 'incomplete'; audit_corrections_complete = $false;
        overall_accepted = $false; started_utc = [DateTime]::UtcNow.ToString('o'); finished_utc = '';
        failure_reasons = @(); runs = @(); review = $null; source_digest = ''; repository_root = $root; artifacts = @() }
    $result | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $path
    $failure = $null
    try {
        $closure = [Collections.Generic.List[object]]::new()
        Add-PC92FinalArtifact $bundleSnapshot.artifact $closure
        $requirements = Get-PC92FinalRequirements
        $digest = Get-PC92FinalSourceDigest $root
        $result.source_digest = $digest
        if (($bundle.schema_version -isnot [int] -and $bundle.schema_version -isnot [long]) -or
            $bundle.schema_version -ne 1 -or $bundle.source_digest -cne $digest) { throw 'bundle_source_mismatch' }
        $reviewSnapshot = Read-PC92FinalSnapshot $bundle.review.path
        if ($reviewSnapshot.artifact.sha256 -ine $bundle.review.sha256) { throw 'review_changed_before_read' }
        Add-PC92FinalArtifact $bundle.review $closure
        $review = $reviewSnapshot.value
        $result.review = $bundle.review
        if ($review.source_digest -cne $digest -or $review.reviewer -isnot [string] -or [string]::IsNullOrWhiteSpace($review.reviewer) -or
            $review.open_evidence -isnot [array] -or $review.open_evidence.Count -ne 0) { throw 'review_open_evidence' }
        Assert-PC92FinalIDSet $review.corrections $requirements.corrections 'correction'
        foreach ($row in $review.corrections) { Assert-PC92FinalReviewRow $row $digest $closure }
        $result.audit_corrections_complete = $true
        Assert-PC92FinalIDSet $review.partitions @($requirements.partitions.Keys) 'partition'
        [long]$total = 0
        foreach ($row in $review.partitions) {
            Assert-PC92FinalReviewRow $row $digest $closure
            if (($row.bound_bytes -isnot [long] -and $row.bound_bytes -isnot [int]) -or $row.bound_bytes -le 0 -or
                $row.bound_bytes -gt ($requirements.partitions[$row.id] * 1MB)) { throw "partition_limit: $($row.id)" }
            $total += $row.bound_bytes
        }
        if ($total -gt 480MB) { throw 'aggregate_limit' }
        $result['owned_bound_bytes'] = $total
        Assert-PC92FinalIDSet $review.checks $requirements.checks 'check'
        foreach ($row in $review.checks) {
            Assert-PC92FinalReviewRow $row $digest $closure
            if (($row.exit_code -isnot [int] -and $row.exit_code -isnot [long]) -or $row.exit_code -ne 0 -or
                $row.command -isnot [string] -or [string]::IsNullOrWhiteSpace($row.command)) { throw "check_incomplete: $($row.id)" }
        }
        Assert-PC92FinalIDSet $bundle.runs $requirements.profiles 'profile'
        $seenRuns = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        foreach ($entry in $bundle.runs) {
            $run = Assert-PC92FinalRun ([IO.Path]::GetFullPath($entry.directory)) $entry.id $root $digest $closure
            if (-not $seenRuns.Add($run.run_id)) { throw 'duplicate_run_identity' }
            $result.runs += $run
        }
        # Recheck the validated closure before publication. Verdict hashes come
        # from the same bytes that were parsed, not a later reread. The source
        # and external input manifests also cover files referenced indirectly.
        if ((Get-PC92FinalSourceDigest $root) -cne $digest) { throw 'source_changed_during_finalization' }
        foreach ($entry in $bundle.runs) {
            $run = Read-PC92FinalObject (Join-Path $entry.directory 'verdict.json')
            Assert-PC92FinalExternalInputs $run
            $current = Get-PC92InputManifest $root $run.family $run.external_inputs
            if ((Get-Content -LiteralPath (Join-Path $entry.directory 'source-before.json') -Raw).Trim() -cne $current) { throw 'inputs_changed_during_finalization' }
        }
        foreach ($artifact in $closure) { Assert-PC92FinalArtifact $artifact }
        $result.artifacts = @($closure | Sort-Object path, sha256 -Unique)
        $result.status = 'accepted'; $result.overall_accepted = $true
    } catch {
        $failure = $_; $result.status = 'failed'; $result.failure_reasons = @($_.Exception.Message)
    } finally {
        # A missing workload may leave correction closeout valid. Changed
        # source/review/correction evidence cannot leave that narrower flag true.
        if ($result.audit_corrections_complete) {
            try {
                Assert-PC92FinalArtifact $bundle.review
                if ((Get-PC92FinalSourceDigest $root) -cne $digest) { throw 'correction_source_changed' }
                foreach ($row in $review.corrections) {
                    foreach ($artifact in $row.artifacts) { Assert-PC92FinalArtifact $artifact }
                }
            } catch {
                $result.audit_corrections_complete = $false
                $result.overall_accepted = $false
                $result.status = 'failed'
                $result.failure_reasons += $_.Exception.Message
                if (-not $failure) { $failure = $_ }
            }
        }
        $result.finished_utc = [DateTime]::UtcNow.ToString('o')
        $temporary = Join-Path $output 'final-verdict.pending.json'
        $result | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $temporary
        [IO.File]::Move($temporary, $path, $true)
    }
    if ($failure) { throw $failure }
    Write-Output "Final evidence: $path"
}

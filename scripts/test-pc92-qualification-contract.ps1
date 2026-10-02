<#!
Behavioral fixtures execute all five real wrappers against isolated fake build
and test processes. No fixture output is qualification evidence. Every negative
case asserts its specific failure reason and the authoritative final verdict.
!#>
$ErrorActionPreference = 'Stop'
$repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$base = Join-Path ([IO.Path]::GetTempPath()) ('pc92-wrapper-fixtures-' + [guid]::NewGuid().ToString('N'))
$fixture = Join-Path $base 'source'
$bin = Join-Path $base 'bin'
$reference = Join-Path $base 'reference'
$engine = (Get-Process -Id $PID).Path
$realGit = (Get-Command git).Source
$savedPath = $env:PATH
$names = @('PC92_FIXTURE_REAL_GIT', 'PC92_FIXTURE_REFERENCE', 'PC92_FIXTURE_REPO', 'PC92_FIXTURE_SCENARIO', 'PC92_FIXTURE_LOG', 'PC92_FIXTURE_FAMILY', 'PC92_FIXTURE_DLL')
$saved = @{}
foreach ($name in $names) { $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
$passed = 0

function Restore-FixtureInputs {
    Set-Content -LiteralPath (Join-Path $fixture 'subject.go') -Value 'package fixture'
    Set-Content -LiteralPath (Join-Path $fixture 'data/cty/cty.plist') -Value 'asset'
    Set-Content -LiteralPath (Join-Path $reference 'perl/Fixture.pm') -Value 'reference'
    $added = Join-Path $fixture 'new-input.pl'
    if (Test-Path -LiteralPath $added) { Remove-Item -LiteralPath $added }
}

function Invoke-WrapperFixture([string]$Family, [string]$Case, [string]$Reason = '') {
    Restore-FixtureInputs
    $env:PC92_FIXTURE_FAMILY = $Family
    $env:PC92_FIXTURE_SCENARIO = $Case
    $label = "$Family-$Case"
    $output = Join-Path $base $label
    $env:PC92_FIXTURE_LOG = Join-Path $base ($label + '-processes.txt')
    $script = if ($Family -eq 'cache') { 'pc92-qualification.ps1' } else { "pc92-$Family-qualification.ps1" }
    $profile = if ($Family -eq 'cache') { 'cache-memory' } elseif ($Family -eq 'q4') { 'preflight-a' } else { 'preflight' }
    if ($Case -eq 'short_duration') {
        $profile = switch ($Family) { runtime { 'q1' }; q4 { 'a' }; cache { 'cache-sustained' }; default { 'qualification' } }
    }
    $arguments = @('-NoProfile', '-File', (Join-Path $fixture "scripts/$script"), '-Profile', $profile, '-OutputDirectory', $output)
    if ($Family -eq 'q6') { $arguments += @('-DXSpiderRoot', $reference, '-PerlPath', (Join-Path $bin 'go.exe'), '-PerlDLLDirectory', (Join-Path $reference 'dll')) }
    $text = (& $engine @arguments 2>&1 | Out-String)
    $exit = $LASTEXITCODE
    $verdictFile = Join-Path $output 'verdict.json'
    if (-not (Test-Path -LiteralPath $verdictFile)) { throw "${label}: missing final verdict`n$text" }
    $verdict = Get-Content -LiteralPath $verdictFile -Raw | ConvertFrom-Json
    if ($verdict.qualified -or $verdict.overall_accepted) { throw "$label falsely claims complete qualification" }
    if ($Reason) {
        if ($exit -eq 0 -or $verdict.status -ne 'failed' -or $verdict.measurement_passed -or $verdict.profile_accepted) { throw "$label falsely passed`n$text" }
        if (-not (($verdict.failure_reasons -join ' ').Contains($Reason))) { throw "$label wrong failure reason: $($verdict.failure_reasons)`n$text" }
    } else {
        if ($exit -ne 0 -or $verdict.status -ne 'measured' -or -not $verdict.measurement_passed -or -not $verdict.provenance_passed) { throw "$label positive failed`n$text" }
        $processes = @(Get-Content -LiteralPath $env:PC92_FIXTURE_LOG)
        if (@($processes | Where-Object { $_.StartsWith('build:') }).Count -ne 1 -or @($processes | Where-Object { $_.StartsWith('execute:') }).Count -ne 1) { throw "$label did not build once and execute once" }
        $built = ($processes | Where-Object { $_.StartsWith('build:') }).Substring(6)
        $executed = ($processes | Where-Object { $_.StartsWith('execute:') }).Substring(8)
        if ($built -cne $executed -or (Get-FileHash -LiteralPath $built).Hash -cne $verdict.executable_sha256) { throw "$label did not execute the retained binary" }
        $old = Get-Content -LiteralPath $verdictFile -Raw
        $retry = (& $engine @arguments 2>&1 | Out-String)
        if ($LASTEXITCODE -eq 0 -or -not $retry.Contains('output_not_fresh') -or (Get-Content -LiteralPath $verdictFile -Raw) -cne $old) { throw "$label reused/overwrote prior evidence" }
    }
    $script:passed++
    Write-Host "PASS $label"
}

try {
    foreach ($path in @($bin, (Join-Path $fixture 'scripts'), (Join-Path $fixture 'internal/cluster'), (Join-Path $fixture 'peer'), (Join-Path $fixture 'data/config'), (Join-Path $fixture 'data/cty'), (Join-Path $reference 'perl'), (Join-Path $reference 'data'), (Join-Path $reference 'dll'))) {
        $null = New-Item -ItemType Directory -Path $path -Force
    }
    Get-ChildItem -LiteralPath $PSScriptRoot -Filter 'pc92-*qualification*.ps1' | Copy-Item -Destination (Join-Path $fixture 'scripts')
    Set-Content -LiteralPath (Join-Path $fixture 'data/config/pipeline.yaml') -Value 'confusion_model_file: ""'
    Set-Content -LiteralPath (Join-Path $fixture '.gitignore') -Value 'data/cty/'
    Set-Content -LiteralPath (Join-Path $reference 'data/prefix_data.pl') -Value 'reference-prefix'
    Set-Content -LiteralPath (Join-Path $reference 'dll/compiler-marker.txt') -Value 'runtime-only DLL path'
    Restore-FixtureInputs
    & $realGit -C $fixture init -q
    & $realGit -C $fixture add .
    & $realGit -C $fixture -c user.name=Fixture -c user.email=fixture@example.invalid commit -q -m fixture
    if ($LASTEXITCODE -ne 0) { throw 'Fixture repository initialization failed' }
    & go -C $repo build -o (Join-Path $bin 'go.exe') ./scripts/testdata/pc92qualificationtool
    if ($LASTEXITCODE -ne 0) { throw 'Fixture executable build failed' }
    Copy-Item -LiteralPath (Join-Path $bin 'go.exe') -Destination (Join-Path $bin 'git.exe')
    $env:PC92_FIXTURE_REAL_GIT = $realGit
    $env:PC92_FIXTURE_REFERENCE = $reference
    $env:PC92_FIXTURE_REPO = $fixture
    $env:PC92_FIXTURE_DLL = Join-Path $reference 'dll'
    $env:PATH = $bin + [IO.Path]::PathSeparator + $savedPath
    foreach ($family in @('runtime', 'q4', 'q5', 'q6', 'cache')) {
        Invoke-WrapperFixture $family 'positive'
        foreach ($case in @('source_changed', 'source_added', 'source_deleted')) { Invoke-WrapperFixture $family $case 'source_changed_during_run' }
        Invoke-WrapperFixture $family 'build_source_changed' 'source_changed_during_build'
        Invoke-WrapperFixture $family 'test_failed' 'test_failed'
        Invoke-WrapperFixture $family 'binary_changed' 'binary_changed_during_run'
        Invoke-WrapperFixture $family 'missing_case' 'missing_case'
        Invoke-WrapperFixture $family 'short_duration' 'short_duration'
        if ($family -in @('runtime', 'q4')) {
            Invoke-WrapperFixture $family 'asset_changed' 'source_changed_during_run'
            foreach ($pair in @(@('missing_observations', 'missing_observations'), @('malformed_observations', 'malformed_observations'), @('array_observations', 'malformed_observations'), @('wrong_profile', 'observation_profile_mismatch'), @('stale_run', 'observation_run_mismatch'), @('measurement_failed', 'measurement_failed'), @('qualified_provisional', 'authoritative_provisional_flag'))) {
                Invoke-WrapperFixture $family $pair[0] $pair[1]
            }
        }
        if ($family -eq 'q6') {
            Invoke-WrapperFixture $family 'wrong_reference' 'reference_pin_mismatch'
            Invoke-WrapperFixture $family 'reference_changed' 'source_changed_during_run'
        }
    }
    # Direct finalizer checks can represent a full elapsed interval without
    # pretending the short mocked process actually ran that workload.
    . (Join-Path $PSScriptRoot 'pc92-qualification-plan.ps1')
    $plan = Get-PC92QualificationPlan runtime q1
    $observation = Join-Path $base 'full-finalizer-observations.json'
    @{ RunID = 'fixture'; Profile = 'q1'; Diagnostic = $false; MeasurementPassed = $true; Failures = 0; LoadSeconds = 2700; DrainSeconds = 660 } | ConvertTo-Json | Set-Content -LiteralPath $observation
    Test-PC92QualificationEvidence $plan fixture '--- PASS: TestPC92RuntimeQualification (3360.00s)' $observation 3360
    $passed++
    Write-Host "PASS full interval finalizer positive control"
    Write-Host "PASS $passed behavioral qualification fixtures; mock-only artifacts: $base"
} finally {
    $env:PATH = $savedPath
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
}

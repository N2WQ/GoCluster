<#
.SYNOPSIS
Exercise final reconciliation with synthetic, explicitly non-qualifying data.
.DESCRIPTION
No durations, logs, reviews or binaries created by this fixture establish a
production result. A full positive control is necessary to test rejection paths.
#>
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'pc92-qualification-finalize.ps1')
$base = Join-Path ([IO.Path]::GetTempPath()) ('pc92-final-fixtures-' + [guid]::NewGuid().ToString('N'))
$root = Join-Path $base 'source'
$null = New-Item -ItemType Directory -Path (Join-Path $root 'data/config') -Force
$null = New-Item -ItemType Directory -Path (Join-Path $root 'data/cty') -Force
$null = New-Item -ItemType Directory -Path (Join-Path $root 'data/h3') -Force
Set-Content -LiteralPath (Join-Path $root 'data/config/pipeline.yaml') -Value 'confusion_model_file: ""'
Set-Content -LiteralPath (Join-Path $root 'data/cty/cty.plist') -Value 'SYNTHETIC FIXTURE'
Set-Content -LiteralPath (Join-Path $root 'subject.go') -Value 'package fixture'
& git -C $root init -q
if ($LASTEXITCODE -ne 0) { throw 'fixture_git_failed' }
$requirements = Get-PC92FinalRequirements
$digest = Get-PC92FinalSourceDigest $root
$evidence = Join-Path $base 'synthetic.txt'
Set-Content -LiteralPath $evidence -Value 'SYNTHETIC ONLY; NOT EXECUTED QUALIFICATION OR PROOF'
$evidenceArtifact = [pscustomobject]@{ path = $evidence; sha256 = (Get-FileHash -LiteralPath $evidence).Hash }
$bundlePath = Join-Path $base 'bundle.json'
$reviewPath = Join-Path $base 'review.json'
$passed = 0
$referenceRoot = Join-Path $base 'synthetic-reference'
$perlExecutable = Join-Path $base 'synthetic-perl.exe'
$null = New-Item -ItemType Directory -Path (Join-Path $referenceRoot 'perl') -Force
$null = New-Item -ItemType Directory -Path (Join-Path $referenceRoot 'data') -Force
Set-Content -LiteralPath $perlExecutable -Value 'SYNTHETIC NONEXECUTABLE'
# Only these fixtures replace the Git pin assertion; the wrapper contract
# suite separately tests its real Git implementation, including a dirty tree.
function Assert-PC92PinnedReference([string]$Root) {
    if ((Get-Content -LiteralPath (Join-Path $Root 'synthetic-pin') -Raw).Trim() -cne 'SYNTHETIC PIN') { throw 'synthetic_reference_pin_changed' }
}
# Inject a mutation after runs have been checked, without a production hook or
# racing background writer. The real digest function still runs each time.
$sourceDigestImplementation = ${function:Get-PC92FinalSourceDigest}
function Get-PC92FinalSourceDigest([string]$Root) {
    $script:finalFixtureDigestCalls++
    if ($script:finalFixtureDigestCalls -eq 2 -and $script:finalFixtureBeforeRecheck) { & $script:finalFixtureBeforeRecheck }
    return (& $sourceDigestImplementation $Root)
}

function Write-FinalFixtureJSON($Value, [string]$Path) {
    $Value | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $Path
}

function New-FinalFixtureRow([string]$ID) {
    return [pscustomobject]@{ id = $ID; status = 'passed'; source_digest = $digest; summary = 'synthetic fixture';
        artifacts = @($evidenceArtifact); open_evidence = @(); exit_code = 0; command = 'synthetic fixture'; bound_bytes = 1 }
}

function Restore-FinalFixture {
    $script:finalFixtureDigestCalls = 0
    $script:finalFixtureBeforeRecheck = $null
    Set-Content -LiteralPath (Join-Path $root 'subject.go') -Value 'package fixture'
    Set-Content -LiteralPath $evidence -Value 'SYNTHETIC ONLY; NOT EXECUTED QUALIFICATION OR PROOF'
    Set-Content -LiteralPath (Join-Path $referenceRoot 'synthetic-pin') -Value 'SYNTHETIC PIN'
    Set-Content -LiteralPath (Join-Path $referenceRoot 'perl/Fixture.pm') -Value 'SYNTHETIC RECEIVER'
    Set-Content -LiteralPath (Join-Path $referenceRoot 'data/prefix_data.pl') -Value 'SYNTHETIC PREFIX'
    $script:review = [pscustomobject]@{ source_digest = $digest; reviewer = 'SYNTHETIC FIXTURE'; open_evidence = @();
        corrections = @($requirements.corrections | ForEach-Object { New-FinalFixtureRow $_ });
        checks = @($requirements.checks | ForEach-Object { New-FinalFixtureRow $_ });
        partitions = @($requirements.partitions.Keys | ForEach-Object { New-FinalFixtureRow $_ }) }
    $script:bundle = [pscustomobject]@{ schema_version = 1; repository_root = $root; source_digest = $digest;
        review = $null; runs = @() }
    foreach ($id in $requirements.profiles) {
        $parts = $id.Split('/')
        $directory = Join-Path $base ($id.Replace('/', '-'))
        $null = New-Item -ItemType Directory -Path $directory -Force
        $plan = Get-PC92QualificationPlan $parts[0] $parts[1]
        $externalInputs = @()
        if ($parts[0] -ceq 'q6') {
            $externalInputs = @((Join-Path $referenceRoot 'perl'), (Join-Path $referenceRoot 'data/prefix_data.pl'), $perlExecutable)
        }
        $manifest = Get-PC92InputManifest $root $parts[0] $externalInputs
        foreach ($name in @('source-before.json', 'source-after-build.json', 'source-after-run.json')) {
            $manifest | Set-Content -LiteralPath (Join-Path $directory $name)
        }
        $binary = Join-Path $directory ($parts[0] + '.test.exe')
        $helper = Join-Path $directory 'peerdiag.exe'
        Set-Content -LiteralPath $binary -Value 'SYNTHETIC NONEXECUTABLE'
        Set-Content -LiteralPath $helper -Value 'SYNTHETIC NONEXECUTABLE'
        $log = Join-Path $directory 'test.log'
        $lines = @(foreach ($test in $plan.tests) {
            $seconds = if ($plan.minimum_case_seconds.ContainsKey($test)) { $plan.minimum_case_seconds[$test] } else { $plan.minimum_seconds }
            '--- PASS: ' + $test + ' (' + $seconds + '.00s)'
        })
        $lines | Set-Content -LiteralPath $log
        $runID = [guid]::NewGuid().ToString('N')
        $run = [pscustomobject]@{ family = $parts[0]; profile = $parts[1]; status = 'measured'; diagnostic = $false;
            measurement_passed = $true; provenance_passed = $true; profile_accepted = $true; exit_code = 0;
            failure_reasons = @(); repository_root = $root; runtime_settings = 'GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1536MiB';
            go_version = 'go version go1.26.4 windows/amd64'; external_inputs = @($externalInputs); executable = $binary;
            helper_executable = $helper; executable_sha256 = (Get-FileHash -LiteralPath $binary).Hash;
            helper_executable_sha256 = (Get-FileHash -LiteralPath $helper).Hash; test_log_sha256 = (Get-FileHash -LiteralPath $log).Hash;
            observations_sha256 = ''; run_id = $runID; elapsed_seconds = $plan.minimum_seconds;
            reference_pin = '3e9b3621d94dd45c68702e4a0f896aac33f2a91d'; reference_root = $referenceRoot;
            perl_executable = $perlExecutable; perl_library = ''; reference_dll_directory = '' }
        if ($plan.observation) {
            $observation = if ($parts[0] -eq 'runtime') {
                @{ RunID = $runID; Profile = $parts[1]; Diagnostic = $false; MeasurementPassed = $true;
                    Failures = 0; LoadSeconds = ($plan.minimum_seconds - 660); DrainSeconds = 660 }
            } else {
                @{ RunID = $runID; Phase = $parts[1]; Diagnostic = $false; MeasurementPassed = $true;
                    Failures = @(); OpenEvidence = @(); DurationSeconds = $plan.minimum_seconds }
            }
            $observationPath = Join-Path $directory 'observations.json'
            Write-FinalFixtureJSON $observation $observationPath
            $run.observations_sha256 = (Get-FileHash -LiteralPath $observationPath).Hash
        }
        Write-FinalFixtureJSON $run (Join-Path $directory 'verdict.json')
        $script:bundle.runs += [pscustomobject]@{ id = $id; directory = $directory }
    }
}

function Edit-FinalFixtureRun([string]$ID, [scriptblock]$Edit) {
    $directory = ($bundle.runs | Where-Object id -CEQ $ID).directory
    $path = Join-Path $directory 'verdict.json'
    $run = Read-PC92FinalObject $path
    & $Edit $run $directory
    Write-FinalFixtureJSON $run $path
}

function Invoke-FinalFixture([string]$Name, [scriptblock]$Edit, [string]$Reason = '', $ExpectedCorrections = $null) {
    Restore-FinalFixture
    & $Edit
    Write-FinalFixtureJSON $review $reviewPath
    $bundle.review = [pscustomobject]@{ path = $reviewPath; sha256 = (Get-FileHash -LiteralPath $reviewPath).Hash }
    Write-FinalFixtureJSON $bundle $bundlePath
    $output = Join-Path $base ('result-' + $Name)
    $caught = ''
    try { Invoke-PC92FinalQualification $bundlePath $output | Out-Null }
    catch { $caught = $_.Exception.Message }
    $verdict = Read-PC92FinalObject (Join-Path $output 'final-verdict.json')
    if ($Reason) {
        if (-not $caught.Contains($Reason) -or $verdict.overall_accepted -or $verdict.status -cne 'failed') { throw "${Name}: wrong rejection [$caught]" }
    } elseif ($caught -or -not $verdict.overall_accepted -or -not $verdict.audit_corrections_complete -or $verdict.status -cne 'accepted') {
        throw "positive control failed: $caught"
    }
    if ($null -ne $ExpectedCorrections -and $verdict.audit_corrections_complete -ne $ExpectedCorrections) { throw "${Name}: incorrect correction-closeout flag" }
    $script:passed++
    Write-Host "PASS $Name"
}

Invoke-FinalFixture positive {}
foreach ($id in $requirements.profiles) {
    Invoke-FinalFixture ('missing-' + $id.Replace('/', '-')) { $bundle.runs = @($bundle.runs | Where-Object id -CNE $id) } 'missing_profile' $true
}
Invoke-FinalFixture duplicate-profile { $bundle.runs += $bundle.runs[0] } 'invalid_profile_set'
Invoke-FinalFixture open-review { $review.open_evidence = @('unproved context descendants') } 'review_open_evidence'
Invoke-FinalFixture missing-native { $review.checks = @($review.checks | Where-Object id -CNE 'linux-native') } 'missing_check'
Invoke-FinalFixture failed-check { $review.checks[0].status = 'failed' } 'review_incomplete'
Invoke-FinalFixture stale-check { $review.checks[0].source_digest = ('0' * 64) } 'review_incomplete'
Invoke-FinalFixture over-budget { ($review.partitions | Where-Object id -CEQ 'sqlite').bound_bytes = 16MB + 1 } 'partition_limit'
Invoke-FinalFixture no-proof-artifact { $review.partitions[0].artifacts = @() } 'review_incomplete'
Invoke-FinalFixture missing-correction { $review.corrections = @($review.corrections | Where-Object id -CNE 'V16-03') } 'missing_correction'
Invoke-FinalFixture source-change { Add-Content -LiteralPath (Join-Path $root 'subject.go') -Value '// changed' } 'bundle_source_mismatch'
Invoke-FinalFixture diagnostic { Edit-FinalFixtureRun runtime/q1 { param($r) $r.diagnostic = $true } } 'run_not_accepted'
Invoke-FinalFixture string-pass { Edit-FinalFixtureRun runtime/q1 { param($r) $r.measurement_passed = 'true' } } 'run_not_accepted'
Invoke-FinalFixture string-run-exit { Edit-FinalFixtureRun runtime/q1 { param($r) $r.exit_code = '0' } } 'run_not_accepted'
Invoke-FinalFixture boolean-check-exit { $review.checks[0].exit_code = $false } 'check_incomplete'
Invoke-FinalFixture boolean-schema { $bundle.schema_version = $true } 'bundle_source_mismatch'
Invoke-FinalFixture shortened { Edit-FinalFixtureRun runtime/q1 { param($r) $r.elapsed_seconds = 3359 } } 'short_duration'
Invoke-FinalFixture load-replaced-by-drain {
    Edit-FinalFixtureRun runtime/q1 { param($r, $d)
        $path = Join-Path $d 'observations.json'; $o = Read-PC92FinalObject $path
        $o.LoadSeconds = 0; $o.DrainSeconds = 3360; Write-FinalFixtureJSON $o $path
        $r.observations_sha256 = (Get-FileHash -LiteralPath $path).Hash
    }
} 'workload_duration_mismatch'
Invoke-FinalFixture wrong-reference { Edit-FinalFixtureRun q6/qualification { param($r) $r.reference_pin = ('0' * 40) } } 'reference_pin_mismatch'
Invoke-FinalFixture missing-reference-root { Edit-FinalFixtureRun q6/qualification { param($r) $r.reference_root = '' } } 'q6_identity_missing'
Invoke-FinalFixture missing-perl { Edit-FinalFixtureRun q6/qualification { param($r) $r.perl_executable = (Join-Path $base 'missing.exe') } } 'q6_identity_missing'
Invoke-FinalFixture omitted-reference-closure { Edit-FinalFixtureRun q6/qualification { param($r) $r.external_inputs = @() } } 'run_external_closure_mismatch'
Invoke-FinalFixture changed-reference-pin { Set-Content -LiteralPath (Join-Path $referenceRoot 'synthetic-pin') -Value 'CHANGED PIN' } 'synthetic_reference_pin_changed'
Invoke-FinalFixture changed-helper { Edit-FinalFixtureRun runtime/q1 { param($r) Add-Content -LiteralPath $r.helper_executable -Value 'changed' } } 'artifact_changed'
Invoke-FinalFixture missing-helper { Edit-FinalFixtureRun runtime/q1 { param($r) Remove-Item -LiteralPath $r.helper_executable } } 'invalid_artifact'
Invoke-FinalFixture changed-log { Edit-FinalFixtureRun runtime/q1 { param($r, $d) Add-Content -LiteralPath (Join-Path $d 'test.log') -Value 'changed' } } 'artifact_changed'
Invoke-FinalFixture stale-manifest { Edit-FinalFixtureRun runtime/q1 { param($r, $d) Set-Content -LiteralPath (Join-Path $d 'source-after-run.json') -Value '[]' } } 'run_source_changed'
Invoke-FinalFixture q4-open-proof {
    Edit-FinalFixtureRun q4/a { param($r, $d)
        $path = Join-Path $d 'observations.json'; $o = Read-PC92FinalObject $path
        $o.OpenEvidence = @('SQLite host inventory incomplete'); Write-FinalFixtureJSON $o $path
        $r.observations_sha256 = (Get-FileHash -LiteralPath $path).Hash
    }
} 'open_allocation_proof'
Invoke-FinalFixture verdict-changed-during-finalization {
    $script:finalFixtureBeforeRecheck = { Edit-FinalFixtureRun runtime/q1 { param($r) $r.measurement_passed = $false } }
} 'artifact_changed'
Invoke-FinalFixture binary-changed-during-finalization {
    $script:finalFixtureBeforeRecheck = { Add-Content -LiteralPath (Join-Path $base 'runtime-q1/runtime.test.exe') -Value 'CHANGED' }
} 'artifact_changed'
Invoke-FinalFixture proof-changed-during-finalization {
    $script:finalFixtureBeforeRecheck = { Add-Content -LiteralPath $evidence -Value 'CHANGED' }
} 'artifact_changed' $false
Invoke-FinalFixture source-changed-during-finalization {
    $script:finalFixtureBeforeRecheck = { Add-Content -LiteralPath (Join-Path $root 'subject.go') -Value '// CHANGED' }
} 'source_changed_during_finalization' $false
Invoke-FinalFixture external-changed-during-finalization {
    $script:finalFixtureBeforeRecheck = { Add-Content -LiteralPath (Join-Path $referenceRoot 'perl/Fixture.pm') -Value 'CHANGED' }
} 'inputs_changed_during_finalization'
Write-Host "PASS $passed final reconciliation fixtures; synthetic artifacts: $base"

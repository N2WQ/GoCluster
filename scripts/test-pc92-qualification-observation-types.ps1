# Narrow behavioral checks of the actual finalizer. These JSON files are mock
# observations, not evidence that any qualification workload ran.
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'pc92-qualification-plan.ps1')
$base = Join-Path ([IO.Path]::GetTempPath()) ('pc92-observation-fixtures-' + [guid]::NewGuid().ToString('N'))
$null = New-Item -ItemType Directory -Path $base
$passed = 0

function Test-ObservationFixture([string]$Family, [string]$Field, $Value, [string]$Reason = '') {
    $observation = @{ RunID = 'fixture'; Diagnostic = $true; MeasurementPassed = $true }
    $profile = 'preflight'
    if ($Family -eq 'runtime') {
        $observation.Profile = $profile; $observation.Failures = 0
        $observation.LoadSeconds = 1; $observation.DrainSeconds = 1
        $log = '--- PASS: TestPC92RuntimeQualification (2.00s)'
    } else {
        $profile = 'preflight-a'; $observation.Phase = 'a'; $observation.Failures = $null
        $observation.OpenEvidence = $null; $observation.DurationSeconds = 2
        $log = '--- PASS: TestPC92Q4RuntimeQualification (2.00s)'
    }
    $observation[$Field] = $Value
    $path = Join-Path $base ("$Family-$Field-$script:passed.json")
    $observation | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $path
    $failure = ''
    try { Test-PC92QualificationEvidence (Get-PC92QualificationPlan $Family $profile) fixture $log $path 2 }
    catch { $failure = $_.Exception.Message }
    if ($Reason) {
        if (-not $failure.StartsWith($Reason)) { throw "Expected $Reason for $Family/$Field, got '$failure'" }
    } elseif ($failure) { throw "Positive $Family/$Field failed: $failure" }
    $script:passed++
}

Test-ObservationFixture runtime Profile 'preflight'
Test-ObservationFixture runtime Failures 0
Test-ObservationFixture q4 Phase 'a'
foreach ($field in @('Failures', 'OpenEvidence')) {
    Test-ObservationFixture q4 $field $null
    Test-ObservationFixture q4 $field @()
    foreach ($bad in @('0', 0, $false, @{ Message = 'wrong shape' }, @($false))) {
        Test-ObservationFixture q4 $field $bad 'malformed_observations'
    }
}
Test-ObservationFixture q4 OpenEvidence @('documented open proof')
Test-ObservationFixture q4 Failures @('failure') 'measurement_failed'
foreach ($pair in @(@('runtime', 'Profile', 'preflight'), @('q4', 'Phase', 'a'))) {
    Test-ObservationFixture $pair[0] $pair[1] @($pair[2]) 'malformed_observations'
    Test-ObservationFixture $pair[0] $pair[1] $null 'malformed_observations'
    Test-ObservationFixture $pair[0] $pair[1] $false 'malformed_observations'
}
foreach ($bad in @('0', $false, -1, 0.5, 0.0, $null, @(0), @{ Value = 0 })) {
    Test-ObservationFixture runtime Failures $bad 'malformed_observations'
}
Write-Output "PASS $passed observation type fixtures; mock-only artifacts: $base"

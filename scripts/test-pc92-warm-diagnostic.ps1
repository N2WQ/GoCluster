<#
.SYNOPSIS
  Exercise warm-profile evidence validation with positive and negative fixtures.
.DESCRIPTION
  Checks the fixed diagnostic workload, CPU windows, sample sequence and
  malformed/missing evidence. Synthetic reports cannot qualify a workload.
.NOTES
  Prerequisites: PowerShell 7. Side effects: writes an isolated temporary
  fixture directory. Does not start GoCluster, run load or alter runtime data.
#>
# These synthetic reports exercise the real evidence checker. They cannot
# qualify any workload and do not pretend that this fixture ran for 26 minutes.
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'pc92-qualification-plan.ps1')
$root = Join-Path ([IO.Path]::GetTempPath()) ('pc92-warm-fixtures-' + [guid]::NewGuid().ToString('N'))
$null = New-Item -ItemType Directory -Path $root
$observation = Join-Path $root 'observations.json'
$plan = Get-PC92QualificationPlan runtime warm-diagnostic
if (-not $plan.diagnostic -or $plan.minimum_seconds -ne 1560) { throw 'warm plan lost diagnostic duration contract' }
$passed = 0

function New-WarmFixture {
    $windows = @(foreach ($pair in @(@(0, 20), @(60, 180), @(660, 780))) {
        $path = Join-Path $root ("profile-$($pair[0]).pprof")
        [IO.File]::WriteAllBytes($path, [byte[]]@(1, 2, 3))
        [ordered]@{ Path = $path; StartSecond = $pair[0]; EndSecond = $pair[1]; StartedNS = [long]$pair[0] * 1000000000; StoppedNS = [long]$pair[1] * 1000000000; Bytes = 3 }
    })
    return [ordered]@{ RunID = 'fixture'; Profile = 'warm-diagnostic'; Diagnostic = $true; MeasurementPassed = $true; Failures = 0; LoadSeconds = 900; DrainSeconds = 660
        LoadProfile = [ordered]@{ Enabled = $true; Warm = [ordered]@{ EpochCounterNS = 1; EpochUTCNS = 1; Count = 900; Complete = $true; Failure = ''; Windows = $windows
            Samples = @(foreach ($second in 1..900) { [ordered]@{ Second = $second; AtNS = [long]$second * 1000000000 } }) } }
    }
}

function Invoke-WarmFixture([string]$Name, [scriptblock]$Change, [string]$Reason = '', [double]$Elapsed = 1560) {
    $report = New-WarmFixture
    & $Change $report
    $report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $observation
    $failure = ''
    try { Test-PC92QualificationEvidence $plan fixture '--- PASS: TestPC92RuntimeQualification (1560.00s)' $observation $Elapsed }
    catch { $failure = $_.Exception.Message }
    if (($Reason -and -not $failure.Contains($Reason)) -or (-not $Reason -and $failure)) { throw "${Name}: expected '$Reason', got '$failure'" }
    $script:passed++
    Write-Host "PASS warm-$Name"
}

Invoke-WarmFixture positive { param($r) }
Invoke-WarmFixture shortened-load { param($r) $r.LoadSeconds = 899 } 'warm_duration_changed'
Invoke-WarmFixture missing-tail { param($r) $r.DrainSeconds = 659 } 'warm_duration_changed'
Invoke-WarmFixture elapsed { param($r) } 'short_duration:' 1559.9
Invoke-WarmFixture promoted { param($r) $r.Diagnostic = $false } 'observation_profile_mismatch'
Invoke-WarmFixture qualified { param($r) $r.Qualified = $true } 'authoritative_provisional_flag'
Invoke-WarmFixture no-profile { param($r) $r.Remove('LoadProfile') } 'warm_profiles_missing'
Invoke-WarmFixture incomplete { param($r) $r.LoadProfile.Warm.Complete = $false } 'warm_evidence_incomplete'
Invoke-WarmFixture clock { param($r) $r.LoadProfile.Warm.EpochCounterNS = 0 } 'warm_evidence_incomplete'
Invoke-WarmFixture missing-window { param($r) $r.LoadProfile.Warm.Windows = @($r.LoadProfile.Warm.Windows[0]) } 'warm_evidence_incomplete'
Invoke-WarmFixture wrong-window { param($r) $r.LoadProfile.Warm.Windows[2].StartSecond = 650 } 'warm_window_invalid'
Invoke-WarmFixture late-window { param($r) $r.LoadProfile.Warm.Windows[1].StartedNS = 61000000000 } 'warm_window_missed'
Invoke-WarmFixture early-stop { param($r) $r.LoadProfile.Warm.Windows[1].StoppedNS = 179000000000 } 'warm_window_missed'
Invoke-WarmFixture missing-file { param($r) Remove-Item -LiteralPath $r.LoadProfile.Warm.Windows[1].Path } 'warm_profile_file_invalid'
Invoke-WarmFixture changed-file { param($r) [IO.File]::WriteAllBytes($r.LoadProfile.Warm.Windows[1].Path, [byte[]]@(1)) } 'warm_profile_file_invalid'
Invoke-WarmFixture missing-sample { param($r) $r.LoadProfile.Warm.Samples = @($r.LoadProfile.Warm.Samples[0]) } 'warm_evidence_incomplete'
Invoke-WarmFixture duplicate-sample { param($r) $r.LoadProfile.Warm.Samples[42] = $r.LoadProfile.Warm.Samples[41] } 'warm_sample_invalid'
Invoke-WarmFixture negative-clock { param($r) $r.LoadProfile.Warm.Samples[42].AtNS = -1 } 'warm_sample_invalid'
Invoke-WarmFixture catch-up { param($r) $r.LoadProfile.Warm.Samples[42].AtNS = 44000000000 } 'warm_sample_invalid'
Write-Host "PASS $passed warm diagnostic checker fixtures; synthetic evidence only: $root"

# Fixed evidence plans shared by the public wrappers. These values are the
# approved workload, not caller-adjustable duration or rate overrides.
function Get-PC92QualificationPlan([string]$Family, [string]$Profile) {
    $plan = [ordered]@{ family = $Family; profile = $Profile; package = './peer'; tags = 'qualification';
        timeout = '65m'; tests = @(); diagnostic = $false; minimum_seconds = 0; minimum_case_seconds = @{}; observation = ''; environment = @{} }
    switch ($Family) {
        'runtime' {
            if ($Profile -notin @('preflight', 'diagnostic-full', 'warm-diagnostic', 'q1', 'q2', 'q3', 'shipped-q1')) { throw 'invalid_profile: runtime' }
            $plan.package = './internal/cluster'; $plan.timeout = '70m'
            $plan.tests = @('TestPC92RuntimeQualification'); $plan.observation = 'runtime'
            $plan.environment.GOCLUSTER_PC92_RUNTIME_PROFILE = $Profile
            $plan.diagnostic = $Profile -in @('preflight', 'diagnostic-full', 'warm-diagnostic')
            $plan.minimum_seconds = if ($Profile -in @('q1', 'shipped-q1')) { 3360 } elseif ($Profile -eq 'warm-diagnostic') { 1560 } elseif ($plan.diagnostic) { 0 } else { 2460 }
        }
        'q4' {
            if ($Profile -notin @('preflight-a', 'preflight-b', 'a', 'b')) { throw 'invalid_profile: q4' }
            $plan.package = './internal/cluster'; $plan.timeout = '50m'
            $plan.tests = @('TestPC92Q4RuntimeQualification'); $plan.observation = 'q4'
            $plan.environment.GOCLUSTER_PC92_Q4_PROFILE = $Profile
            $plan.diagnostic = $Profile.StartsWith('preflight-')
            $plan.minimum_seconds = if ($plan.diagnostic) { 0 } else { 1800 }
        }
        'q5' {
            if ($Profile -notin @('preflight', 'qualification')) { throw 'invalid_profile: q5' }
            $plan.timeout = '60m'; $plan.tests = @('TestPC92QualificationQ5Isolation')
            foreach ($name in @('spot', 'pc92', 'pc93', 'bulletin')) { $plan.tests += "TestPC92QualificationQ5Isolation/$name" }
            $plan.environment.GOCLUSTER_PC92_Q5_PROFILE = $Profile
            $plan.diagnostic = $Profile -eq 'preflight'
            $plan.minimum_seconds = if ($plan.diagnostic) { 0 } else { 2401 }
        }
        'q6' {
            if ($Profile -notin @('preflight', 'qualification')) { throw 'invalid_profile: q6' }
            $plan.timeout = if ($Profile -eq 'qualification') { '40m' } else { '6m' }
            $plan.tests = @('TestPC92QualificationQ6Faults', 'TestPC92QualificationQ6ReceiveOnly')
            $plan.environment.GOCLUSTER_PC92_Q6_PROFILE = $Profile
            $plan.diagnostic = $Profile -eq 'preflight'
            $repetitions = if ($plan.diagnostic) { 1 } else { 2 }
            $faults = @('publication', 'clock-regression', 'clock-frozen', 'clock-frozen-loaded', 'admission', 'staging-capacity', 'stall', 'candidate-race')
            if (-not $plan.diagnostic) { $faults += 'staging-deadline' }
            foreach ($zero in @('false', 'true')) {
                foreach ($repeat in 1..$repetitions) {
                    foreach ($fault in $faults) { $plan.tests += "TestPC92QualificationQ6Faults/zero=$zero/repeat=$repeat/$fault" }
                }
            }
            $plan.minimum_seconds = if ($plan.diagnostic) { 0 } else { 1200 }
        }
        'cache' {
            if ($Profile -notin @('cache-memory', 'cache-sustained')) { throw 'invalid_profile: cache' }
            $plan.tags = ''; $plan.tests = if ($Profile -eq 'cache-memory') { @('TestPC92QualificationCacheMemory') } else { @('TestPC92QualificationCacheSustained') }
            $plan.environment.GOCLUSTER_PC92_QUALIFICATION = $Profile
            $plan.minimum_seconds = if ($Profile -eq 'cache-sustained') { 3360 } else { 0 }
        }
        'retry' {
            if ($Profile -notin @('preflight', 'qualification')) { throw 'invalid_profile: retry' }
            # 3750 seconds of offered load (including each fixed 600-second
            # recovery tail) leaves 150 seconds for setup within 65 minutes.
            # Individual protocol deadlines remain binding inside each case.
            $plan.timeout = '65m'
            $plan.tests = @('TestPC92V14RetryWaveService', 'TestPC92V14RetryWaveService/recovering_63', 'TestPC92V14RetryWaveService/recovering_63_periodic', 'TestPC92V14RetryWaveService/recovering_64')
            $plan.environment.GOCLUSTER_PC92_V14_RETRY_PROFILE = $Profile
            $plan.diagnostic = $Profile -eq 'preflight'
            if (-not $plan.diagnostic) {
                # The zero-periodic 63-peer case sustains mixed overload for
                # 30 minutes. Periodic 63-peer and zero-periodic 64-peer cases
                # each add 75 seconds. All keep offered load running through
                # another 600 seconds for recovery/reset/refail observation.
                $plan.minimum_seconds = 3750
                $plan.minimum_case_seconds['TestPC92V14RetryWaveService/recovering_63'] = 2400
                $plan.minimum_case_seconds['TestPC92V14RetryWaveService/recovering_63_periodic'] = 675
                $plan.minimum_case_seconds['TestPC92V14RetryWaveService/recovering_64'] = 675
            }
        }
        default { throw 'invalid_family' }
    }
    return $plan
}

# A passing process can skip every test or write a stale report. All requirements
# are checked before the caller may finalize any positive profile measurement.
function Test-PC92QualificationEvidence($Plan, [string]$RunID, [string]$Log, [string]$ObservationPath, [double]$Elapsed) {
    foreach ($test in $Plan.tests) {
        $pattern = '(?m)^\s*--- PASS: ' + [regex]::Escape($test) + ' \('
        if (-not [regex]::IsMatch($Log, $pattern)) { throw "missing_case: $test" }
    }
    if ($Elapsed -lt $Plan.minimum_seconds) { throw 'short_duration: execution did not span the approved workload' }
    foreach ($test in $Plan.minimum_case_seconds.Keys) {
        $pattern = '(?m)^\s*--- PASS: ' + [regex]::Escape($test) + ' \(([0-9]+(?:\.[0-9]+)?)s\)\s*$'
        $match = [regex]::Match($Log, $pattern)
        if (-not $match.Success) { throw "missing_case_duration: $test" }
        $seconds = [double]::Parse($match.Groups[1].Value, [Globalization.CultureInfo]::InvariantCulture)
        if (-not [double]::IsFinite($seconds) -or $seconds -lt $Plan.minimum_case_seconds[$test]) { throw "short_case_duration: $test" }
    }
    if (-not $Plan.observation) { return }
    if (-not (Test-Path -LiteralPath $ObservationPath -PathType Leaf)) { throw 'missing_observations' }
    try { $observation = Get-Content -LiteralPath $ObservationPath -Raw | ConvertFrom-Json -ErrorAction Stop }
    catch { throw 'malformed_observations' }
    if ($observation -isnot [pscustomobject]) { throw 'malformed_observations: expected one object' }
    $required = @('RunID', 'Diagnostic', 'MeasurementPassed', 'Failures')
    $required += if ($Plan.observation -eq 'runtime') { @('Profile', 'LoadSeconds', 'DrainSeconds') } else { @('Phase', 'DurationSeconds', 'OpenEvidence') }
    foreach ($field in $required) { if (-not $observation.PSObject.Properties[$field]) { throw "malformed_observations: missing $field" } }
    if ($observation.RunID -isnot [string] -or $observation.Diagnostic -isnot [bool] -or $observation.MeasurementPassed -isnot [bool]) { throw 'malformed_observations: invalid value type' }
    if ($Plan.observation -eq 'runtime') {
        if ($observation.Profile -isnot [string]) { throw 'malformed_observations: invalid Profile' }
        if (($observation.Failures -isnot [long] -and $observation.Failures -isnot [int]) -or $observation.Failures -lt 0) { throw 'malformed_observations: invalid Failures' }
    } else {
        if ($observation.Phase -isnot [string]) { throw 'malformed_observations: invalid Phase' }
        foreach ($field in @('Failures', 'OpenEvidence')) {
            $value = $observation.$field
            if ($null -ne $value -and $value -isnot [array]) { throw "malformed_observations: invalid $field" }
            foreach ($entry in $value) { if ($entry -isnot [string]) { throw "malformed_observations: invalid $field entry" } }
        }
    }
    $durations = if ($Plan.observation -eq 'runtime') { @($observation.LoadSeconds, $observation.DrainSeconds) } else { @($observation.DurationSeconds) }
    foreach ($value in $durations) {
        if ($value -isnot [ValueType] -or $value -is [bool] -or -not [double]::IsFinite([double]$value) -or [double]$value -lt 0) { throw 'malformed_observations: invalid duration' }
    }
    if ($observation.PSObject.Properties['Qualified'] -and $observation.Qualified -eq $true) { throw 'authoritative_provisional_flag' }
    if ($observation.RunID -cne $RunID) { throw 'observation_run_mismatch' }
    if ($observation.Diagnostic -ne $Plan.diagnostic) { throw 'observation_profile_mismatch' }
    if ($Plan.observation -eq 'runtime' -and $Plan.profile -eq 'warm-diagnostic') {
        Test-PC92WarmDiagnosticEvidence $observation $ObservationPath
    }
    if ($observation.MeasurementPassed -ne $true) { throw 'measurement_failed' }
    if ($Plan.observation -eq 'runtime') {
        if ($observation.Profile -cne $Plan.profile) { throw 'observation_profile_mismatch' }
        if ($observation.Failures -ne 0) { throw 'measurement_failed' }
        if (-not $Plan.diagnostic -and ($observation.DrainSeconds -ne 660 -or $observation.LoadSeconds -ne ($Plan.minimum_seconds - 660))) {
            throw 'workload_duration_mismatch: load and drain must each match the approved profile'
        }
        if (($observation.LoadSeconds + $observation.DrainSeconds) -lt $Plan.minimum_seconds) { throw 'short_duration: observation' }
    } else {
        if ($observation.Phase -cne $Plan.profile.Replace('preflight-', '')) { throw 'observation_profile_mismatch' }
        if ($null -ne $observation.Failures -and $observation.Failures.Count -gt 0) { throw 'measurement_failed' }
        if ($observation.DurationSeconds -lt $Plan.minimum_seconds) { throw 'short_duration: observation' }
        if (-not $Plan.diagnostic -and $null -ne $observation.OpenEvidence -and $observation.OpenEvidence.Count -gt 0) { throw 'open_allocation_proof' }
    }
}

# Warm diagnostics are never accepted profiles, but their timing/profiling
# evidence must still be complete. A missing interval cannot be replaced by a
# later counter sample; the timestamp must fall in its declared one-second slot.
function Test-PC92WarmDiagnosticEvidence($Observation, [string]$ObservationPath) {
    if ($Observation.LoadSeconds -ne 900 -or $Observation.DrainSeconds -ne 660) { throw 'warm_duration_changed' }
    if (-not $Observation.PSObject.Properties['LoadProfile'] -or $Observation.LoadProfile.Enabled -ne $true) { throw 'warm_profiles_missing' }
    $warm = $Observation.LoadProfile.Warm
    if ($null -eq $warm -or $warm.Complete -isnot [bool] -or -not $warm.Complete -or $warm.Count -ne 900 -or $warm.Failure -cne '') { throw 'warm_evidence_incomplete' }
    if ($warm.EpochCounterNS -le 0 -or $warm.EpochUTCNS -le 0 -or @($warm.Samples).Count -ne 900 -or @($warm.Windows).Count -ne 3) { throw 'warm_evidence_incomplete' }
    $directory = [IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($ObservationPath))
    $starts, $ends = @(0, 60, 660), @(20, 180, 780)
    for ($i = 0; $i -lt 3; $i++) {
        $window = $warm.Windows[$i]
        if ($window.StartSecond -ne $starts[$i] -or $window.EndSecond -ne $ends[$i] -or $window.Bytes -le 0) { throw 'warm_window_invalid' }
        foreach ($boundary in @(@($window.StartedNS, $starts[$i]), @($window.StoppedNS, $ends[$i]))) {
            $at = $boundary[0]
            $scheduled = [long]$boundary[1] * 1000000000
            if (($at -isnot [long] -and $at -isnot [int]) -or $at -lt $scheduled -or $at -ge ($scheduled + 1000000000)) { throw 'warm_window_missed' }
        }
        if ($window.Path -isnot [string] -or -not [IO.Path]::IsPathFullyQualified($window.Path) -or [IO.Path]::GetDirectoryName($window.Path) -cne $directory) { throw 'warm_profile_path_invalid' }
        if (-not (Test-Path -LiteralPath $window.Path -PathType Leaf) -or (Get-Item -LiteralPath $window.Path).Length -ne $window.Bytes) { throw 'warm_profile_file_invalid' }
    }
    for ($i = 0; $i -lt 900; $i++) {
        $sample = $warm.Samples[$i]
        $start = [long]($i + 1) * 1000000000
        if ($sample.Second -ne $i + 1 -or ($sample.AtNS -isnot [long] -and $sample.AtNS -isnot [int]) -or $sample.AtNS -lt $start -or $sample.AtNS -ge $start + 1000000000) { throw 'warm_sample_invalid' }
    }
}

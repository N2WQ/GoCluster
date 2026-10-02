# Fixed evidence plans shared by the five public wrappers. These values are the
# approved workload, not caller-adjustable duration or rate overrides.
function Get-PC92QualificationPlan([string]$Family, [string]$Profile) {
    $plan = [ordered]@{ family = $Family; profile = $Profile; package = './peer'; tags = 'qualification';
        timeout = '65m'; tests = @(); diagnostic = $false; minimum_seconds = 0; observation = ''; environment = @{} }
    switch ($Family) {
        'runtime' {
            if ($Profile -notin @('preflight', 'diagnostic-full', 'q1', 'q2', 'q3', 'shipped-q1')) { throw 'invalid_profile: runtime' }
            $plan.package = './internal/cluster'; $plan.timeout = '70m'
            $plan.tests = @('TestPC92RuntimeQualification'); $plan.observation = 'runtime'
            $plan.environment.GOCLUSTER_PC92_RUNTIME_PROFILE = $Profile
            $plan.diagnostic = $Profile -in @('preflight', 'diagnostic-full')
            $plan.minimum_seconds = if ($Profile -in @('q1', 'shipped-q1')) { 3360 } elseif ($plan.diagnostic) { 0 } else { 2460 }
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
    if ($observation.MeasurementPassed -ne $true) { throw 'measurement_failed' }
    if ($Plan.observation -eq 'runtime') {
        if ($observation.Profile -cne $Plan.profile) { throw 'observation_profile_mismatch' }
        if ($observation.Failures -ne 0) { throw 'measurement_failed' }
        if (($observation.LoadSeconds + $observation.DrainSeconds) -lt $Plan.minimum_seconds) { throw 'short_duration: observation' }
    } else {
        if ($observation.Phase -cne $Plan.profile.Replace('preflight-', '')) { throw 'observation_profile_mismatch' }
        if ($null -ne $observation.Failures -and $observation.Failures.Count -gt 0) { throw 'measurement_failed' }
        if ($observation.DurationSeconds -lt $Plan.minimum_seconds) { throw 'short_duration: observation' }
        if (-not $Plan.diagnostic -and $null -ne $observation.OpenEvidence -and $observation.OpenEvidence.Count -gt 0) { throw 'open_allocation_proof' }
    }
}

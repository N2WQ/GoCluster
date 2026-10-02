. (Join-Path $PSScriptRoot 'pc92-qualification-plan.ps1')
. (Join-Path $PSScriptRoot 'pc92-qualification-manifest.ps1')

# This runner owns one fresh evidence directory and one final verdict. Process
# output and Go observations are provisional; neither can authorize acceptance.
# Termination before finalization leaves the durable initial verdict incomplete.
function Invoke-PC92QualificationRun {
    param([string]$Family, [string]$Profile, [string]$OutputDirectory, [switch]$CPUProfile,
        [string]$DXSpiderRoot = '', [string]$PerlPath = '', [string]$PerlLibrary = '', [string]$PerlDLLDirectory = '')
    $ErrorActionPreference = 'Stop'
    $root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
    $plan = Get-PC92QualificationPlan $Family $Profile
    if ($CPUProfile -and ($Family -ne 'runtime' -or $Profile -ne 'preflight')) { throw 'invalid_cpu_profile' }
    if (-not $OutputDirectory) { $OutputDirectory = Join-Path ([IO.Path]::GetTempPath()) ('gocluster-' + $Family + '-' + [guid]::NewGuid().ToString('N')) }
    $output = [IO.Path]::GetFullPath($OutputDirectory)
    if ($output.StartsWith($root.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -or $output -eq $root) {
        throw 'output_inside_source: evidence must be outside the source tree'
    }
    if (Test-Path -LiteralPath $output) {
        if (-not (Test-Path -LiteralPath $output -PathType Container) -or @(Get-ChildItem -LiteralPath $output -Force).Count -gt 0) { throw 'output_not_fresh' }
    } else { $null = New-Item -ItemType Directory -Path $output }
    $run = [ordered]@{
        schema_version = 1; run_id = [guid]::NewGuid().ToString('N'); family = $Family; profile = $Profile
        status = 'incomplete'; measurement_passed = $false; provenance_passed = $false; profile_accepted = $false
        qualified = $false; overall_accepted = $false; failure_reasons = @(); diagnostic = $plan.diagnostic
        open_evidence = @('Complete 480 MiB enabled-SQLite, context-backing and retirement ownership proof remains open.', 'Required final-source qualification profiles are separate evidence.')
        started_utc = [DateTime]::UtcNow.ToString('o'); finished_utc = ''; exit_code = $null
        minimum_seconds = $plan.minimum_seconds; elapsed_seconds = 0; expected_cases = $plan.tests
    }
    $verdictPath = Join-Path $output 'verdict.json'
    $run | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $verdictPath
    $previousLocation = Get-Location
    $names = @('GOCLUSTER_PC92_RUN_ID', 'GOCLUSTER_PC92_RUNTIME_PROFILE', 'GOCLUSTER_PC92_RUNTIME_OUTPUT', 'GOCLUSTER_PC92_RUNTIME_CPU_PROFILE',
        'GOCLUSTER_PC92_Q4_PROFILE', 'GOCLUSTER_PC92_Q4_OUTPUT', 'GOCLUSTER_PC92_Q5_PROFILE', 'GOCLUSTER_PC92_Q6_PROFILE', 'GOCLUSTER_PC92_Q6_USERS',
        'GOCLUSTER_PC92_QUALIFICATION', 'DXSPIDER_ROOT', 'DXSPIDER_PERL', 'DXSPIDER_PERL_LIB', 'PATH', 'LC_ALL', 'GOMAXPROCS', 'GOGC', 'GOMEMLIMIT')
    $saved = @{}
    foreach ($name in $names) { $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
    $failure = $null
    try {
        Set-Location -LiteralPath $root
        foreach ($name in $names | Where-Object { $_ -like 'GOCLUSTER_PC92_*' }) { [Environment]::SetEnvironmentVariable($name, $null, 'Process') }
        foreach ($name in $plan.environment.Keys) { [Environment]::SetEnvironmentVariable($name, $plan.environment[$name], 'Process') }
        $env:GOCLUSTER_PC92_RUN_ID = $run.run_id
        $observationPath = Join-Path $output 'observations.json'
        $env:GOCLUSTER_PC92_RUNTIME_OUTPUT = $observationPath
        $env:GOCLUSTER_PC92_Q4_OUTPUT = $observationPath
        $env:GOCLUSTER_PC92_RUNTIME_CPU_PROFILE = if ($CPUProfile) { Join-Path $output 'load-cpu.pprof' } else { '' }
        $env:GOMAXPROCS = '2'; $env:GOGC = '50'; $env:GOMEMLIMIT = '1536MiB'; $env:LC_ALL = 'C'
        $external = @()
        $dll = ''
        if ($Family -eq 'q6') {
            $env:DXSPIDER_ROOT = (Resolve-Path -LiteralPath $DXSpiderRoot).Path
            $env:DXSPIDER_PERL = (Resolve-Path -LiteralPath $PerlPath).Path
            $env:DXSPIDER_PERL_LIB = $PerlLibrary
            Assert-PC92PinnedReference $env:DXSPIDER_ROOT
            $external = @((Join-Path $env:DXSPIDER_ROOT 'perl'), (Join-Path $env:DXSPIDER_ROOT 'data/prefix_data.pl'), $env:DXSPIDER_PERL)
            if ($PerlLibrary) { $external += (Resolve-Path -LiteralPath $PerlLibrary).Path }
            if ($PerlDLLDirectory) {
                $dll = (Resolve-Path -LiteralPath $PerlDLLDirectory).Path
                $external += $dll
            }
            $env:GOCLUSTER_PC92_Q6_USERS = Join-Path $output 'users'
            $null = New-Item -ItemType Directory -Path $env:GOCLUSTER_PC92_Q6_USERS
        }
        $run['source_head'] = (& git -C $root rev-parse HEAD)
        if ($LASTEXITCODE -ne 0) { throw 'source_identity_failed' }
        $run['source_status'] = @(& git -C $root status --short)
        $run['go_version'] = (& go version)
        if ($LASTEXITCODE -ne 0) { throw 'go_version_failed' }
        $run['go_build_environment'] = @(& go env GOOS GOARCH GOAMD64 CGO_ENABLED GOFLAGS GOTOOLCHAIN CC CXX)
        if ($LASTEXITCODE -ne 0) { throw 'go_environment_failed' }
        $run['runtime_settings'] = 'GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1536MiB'
        $run['os'] = [Runtime.InteropServices.RuntimeInformation]::OSDescription
        $run['processors'] = [Environment]::ProcessorCount
        $run['reference_pin'] = if ($Family -eq 'q6') { '3e9b3621d94dd45c68702e4a0f896aac33f2a91d' } else { '' }
        $before = Get-PC92InputManifest $root $Family $external
        $before | Set-Content -LiteralPath (Join-Path $output 'source-before.json')
        $binary = Join-Path $output ($Family + '.test.exe')
        $arguments = @('test', '-c', '-o', $binary)
        if ($plan.tags) { $arguments += @('-tags', $plan.tags) }
        $arguments += $plan.package
        $run['build_arguments'] = $arguments
        & go @arguments 2>&1 | Tee-Object -FilePath (Join-Path $output 'build.log')
        if ($LASTEXITCODE -ne 0) { throw 'build_failed' }
        if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) { throw 'missing_binary' }
        $built = Get-PC92InputManifest $root $Family $external
        $built | Set-Content -LiteralPath (Join-Path $output 'source-after-build.json')
        if ($before -cne $built) { throw 'source_changed_during_build' }
        $hash = (Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash
        $run['executable_sha256'] = $hash
        $roots = @($plan.tests | Where-Object { -not $_.Contains('/') })
        $pattern = '^(' + (($roots | ForEach-Object { [regex]::Escape($_) }) -join '|') + ')$'
        $testArguments = @('-test.count=1', '-test.v', "-test.timeout=$($plan.timeout)", "-test.run=$pattern")
        if ($Family -eq 'q5') { $testArguments += '-test.parallel=4' }
        $run['test_arguments'] = $testArguments
        $run['runtime_dll_directory'] = $dll
        $run | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $verdictPath
        # Perl's DLL directory can also contain GCC. Restrict its PATH effect
        # to execution so qualification does not silently select another CC.
        if ($dll) { $env:PATH = $dll + [IO.Path]::PathSeparator + $env:PATH }
        Set-Location -LiteralPath (Join-Path $root $plan.package)
        $watch = [Diagnostics.Stopwatch]::StartNew()
        & $binary @testArguments 2>&1 | Tee-Object -FilePath (Join-Path $output 'test.log')
        $run.exit_code = $LASTEXITCODE
        $watch.Stop(); $run.elapsed_seconds = $watch.Elapsed.TotalSeconds
        Set-Location -LiteralPath $root
        $after = Get-PC92InputManifest $root $Family $external
        $after | Set-Content -LiteralPath (Join-Path $output 'source-after-run.json')
        if ($before -cne $after) { throw 'source_changed_during_run' }
        if ((Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash -cne $hash) { throw 'binary_changed_during_run' }
        if ($Family -eq 'q6') { Assert-PC92PinnedReference $env:DXSPIDER_ROOT }
        $run.provenance_passed = $true
        if ($run.exit_code -ne 0) { throw "test_failed: exit $($run.exit_code)" }
        Test-PC92QualificationEvidence $plan $run.run_id (Get-Content -LiteralPath (Join-Path $output 'test.log') -Raw) $observationPath $run.elapsed_seconds
        $run.measurement_passed = $true
        $run.profile_accepted = -not $plan.diagnostic
        $run.status = 'measured'
    } catch {
        $failure = $_
        $run.status = 'failed'
        $run.failure_reasons = @($_.Exception.Message)
        $run.measurement_passed = $false; $run.profile_accepted = $false
    } finally {
        try {
            $run.finished_utc = [DateTime]::UtcNow.ToString('o')
            # Same-directory overwrite uses the filesystem rename operation;
            # interruption cannot leave a partially written positive verdict.
            $temporary = Join-Path $output 'verdict.pending.json'
            $run | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $temporary
            [IO.File]::Move($temporary, $verdictPath, $true)
        } finally {
            foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
            Set-Location -LiteralPath $previousLocation.Path
        }
    }
    if ($failure) { throw $failure }
    Write-Output "Evidence: $output; measured checks passed; profile accepted=$($run.profile_accepted); overall acceptance remains incomplete."
}

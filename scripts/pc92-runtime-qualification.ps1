param(
    [ValidateSet('preflight', 'diagnostic-full', 'q1', 'q2', 'q3', 'shipped-q1')]
    [string]$Profile = 'preflight',
    [switch]$CPUProfile,
    [string]$OutputDirectory = (Join-Path $env:TEMP ('gocluster-pc92-runtime-' + (Get-Date -Format 'yyyyMMdd-HHmmss')))
)

# The approved profiles have fixed durations; diagnostics have distinct names
# and always report Qualified=false. Ordinary binaries contain no observation
# callback. This process opts into the qualification build tag explicitly.
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$savedLocation = Get-Location
$environmentNames = @('GOCLUSTER_PC92_RUNTIME_PROFILE', 'GOCLUSTER_PC92_RUNTIME_OUTPUT', 'GOCLUSTER_PC92_RUNTIME_CPU_PROFILE', 'GOMAXPROCS', 'GOGC', 'GOMEMLIMIT')
$savedEnvironment = @{}
foreach ($name in $environmentNames) { $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
try {
    Set-Location -LiteralPath $repoRoot
    if ($CPUProfile -and $Profile -ne 'preflight') { throw 'CPU profiling is a preflight diagnostic only.' }
    New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
    $resolvedOutput = (Resolve-Path -LiteralPath $OutputDirectory).Path
    $env:GOCLUSTER_PC92_RUNTIME_PROFILE = $Profile
    $env:GOCLUSTER_PC92_RUNTIME_OUTPUT = Join-Path $resolvedOutput 'observations.json'
    $env:GOCLUSTER_PC92_RUNTIME_CPU_PROFILE = if ($CPUProfile) { Join-Path $resolvedOutput 'load-cpu.pprof' } else { '' }
    $env:GOMAXPROCS = '2'
    $env:GOGC = '50'
    $env:GOMEMLIMIT = '1536MiB'
    $metadata = @(
        'Profile: ' + $Profile
        'CPU profiling (diagnostic only): ' + $CPUProfile.IsPresent
        'UTC start: ' + [DateTime]::UtcNow.ToString('o')
        'Git HEAD: ' + (& git rev-parse HEAD)
        'Go: ' + (& go version)
        'OS/architecture: ' + [System.Runtime.InteropServices.RuntimeInformation]::OSDescription + ' / ' + [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
        'Logical processors: ' + [Environment]::ProcessorCount
        'Service: GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1536MiB; parent driver uses logical processor count and separately reported memory; native peer frame cap=65536'
        'Windows split process: shared immutable input ledger, shared QPC domain, plus one uncertainty tick with upward interval rounding; no observation event queue'
        '100 local clients;16 peers for Q1,64 peers for Q2/Q3; full graph population occurs through actual peer frames'
        'Q1/shipped-Q1:45min+11min;Q2/Q3:30min+11min;preflight:20s+5s;diagnostic-full:10s+5s'
        '10000 new spot keys/min,100000 duplicate arrivals/min,100newPC92/s,100PC93/min,20WWV/min'
        'Q3: six5min cycles24s@1000new/s,120s no new keys,156s@10000/min; duplicates continue independently'
        'Declared profiles disable batching/stabilizer/temporal holds only in memory; shipped-Q1 keeps all shipped holds'
        'Working tree changes:'
        (& git status --short)
    )
    $metadata | Set-Content -LiteralPath (Join-Path $resolvedOutput 'environment.txt')
    $sourceFiles = @(& rg --files -g '*.go' -g '*.yaml' -g '*.yml' -g '*.ps1' -g 'go.mod' -g 'go.sum' | Sort-Object)
    $sourceBefore = @(foreach ($sourceFile in $sourceFiles) { (Get-FileHash -Algorithm SHA256 -LiteralPath $sourceFile).Hash + '  ' + $sourceFile })
    $sourceBefore | Set-Content -LiteralPath (Join-Path $resolvedOutput 'source-sha256-before.txt')
    if ($IsWindows -or $env:OS -eq 'Windows_NT') {
        Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors | Format-List | Out-File -LiteralPath (Join-Path $resolvedOutput 'hardware.txt')
        Get-CimInstance Win32_ComputerSystem | Select-Object TotalPhysicalMemory | Format-List | Out-File -LiteralPath (Join-Path $resolvedOutput 'hardware.txt') -Append
    }
    $binary = Join-Path $resolvedOutput 'runtime.test.exe'
    & go test -tags qualification ./internal/cluster -o $binary -count=1 -timeout=70m -run '^TestPC92RuntimeQualification$' -v 2>&1 | Tee-Object -FilePath (Join-Path $resolvedOutput 'test-output.txt')
    $testExit = $LASTEXITCODE
    $sourceAfterFiles = @(& rg --files -g '*.go' -g '*.yaml' -g '*.yml' -g '*.ps1' -g 'go.mod' -g 'go.sum' | Sort-Object)
    $sourceAfter = @(foreach ($sourceFile in $sourceAfterFiles) { (Get-FileHash -Algorithm SHA256 -LiteralPath $sourceFile).Hash + '  ' + $sourceFile })
    $sourceAfter | Set-Content -LiteralPath (Join-Path $resolvedOutput 'source-sha256-after.txt')
    @('UTC finish: ' + [DateTime]::UtcNow.ToString('o'); 'Source hashes unchanged: ' + (($sourceBefore -join "`n") -ceq ($sourceAfter -join "`n")); (& git status --short)) | Set-Content -LiteralPath (Join-Path $resolvedOutput 'completion.txt')
    if ($testExit -ne 0) { throw "Runtime profile $Profile failed with exit code $testExit; evidence: $resolvedOutput" }
    $observations = Get-Content -LiteralPath $env:GOCLUSTER_PC92_RUNTIME_OUTPUT -Raw | ConvertFrom-Json
    Write-Output "Runtime evidence: $resolvedOutput"
    Write-Output "Profile: $Profile; diagnostic: $($observations.Diagnostic); this profile qualified: $($observations.Qualified). Other required profiles are separate evidence."
}
finally {
    foreach ($name in $environmentNames) { [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process') }
    Set-Location -LiteralPath $savedLocation.Path
}

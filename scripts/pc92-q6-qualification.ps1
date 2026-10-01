[CmdletBinding()]
param(
    [ValidateSet('preflight', 'qualification')][string]$Profile = 'preflight',
    [Parameter(Mandatory = $true)][string]$DXSpiderRoot,
    [Parameter(Mandatory = $true)][string]$PerlPath,
    [string]$PerlLibrary = '',
    [string]$PerlDLLDirectory = '',
    [string]$OutputDirectory = ''
)

# Q6 owns peer-session failure/recovery and real receive-only ingestion. Q1-Q3
# separately qualify the complete spot pipeline and per-client latency. Preflight
# deliberately cannot satisfy the repetition or 20-minute duration requirements.
$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$referenceRoot = (Resolve-Path -LiteralPath $DXSpiderRoot).Path
$interpreter = (Resolve-Path -LiteralPath $PerlPath).Path
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path ([IO.Path]::GetTempPath()) ('gocluster-q6-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
}
$null = New-Item -ItemType Directory -Path $OutputDirectory -Force
$outputRoot = (Resolve-Path -LiteralPath $OutputDirectory).Path
$environmentNames = @('GOCLUSTER_PC92_Q6_PROFILE', 'GOCLUSTER_PC92_Q6_USERS', 'DXSPIDER_ROOT', 'DXSPIDER_PERL', 'DXSPIDER_PERL_LIB', 'PATH', 'LC_ALL', 'GOMAXPROCS', 'GOGC', 'GOMEMLIMIT')
$previousLocation = Get-Location
$previousEnvironment = @{}
foreach ($name in $environmentNames) {
    $previousEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
try {
    Set-Location -LiteralPath $repositoryRoot
    $env:GOCLUSTER_PC92_Q6_PROFILE = $Profile
    $env:GOCLUSTER_PC92_Q6_USERS = Join-Path $outputRoot 'users'
    $null = New-Item -ItemType Directory -Path $env:GOCLUSTER_PC92_Q6_USERS -Force
    $env:DXSPIDER_ROOT = $referenceRoot
    $env:DXSPIDER_PERL = $interpreter
    $env:DXSPIDER_PERL_LIB = $PerlLibrary
    $env:LC_ALL = 'C'
    $env:GOMAXPROCS = '2'
    $env:GOGC = '50'
    $env:GOMEMLIMIT = '1536MiB'
    if ($PerlDLLDirectory) {
        $env:PATH = (Resolve-Path -LiteralPath $PerlDLLDirectory).Path + [IO.Path]::PathSeparator + $env:PATH
    }
    $metadata = [ordered]@{
        profile = $Profile
        qualification_eligible = ($Profile -eq 'qualification')
        started_utc = [DateTime]::UtcNow.ToString('o')
        source_head = (& git -C $repositoryRoot rev-parse HEAD)
        source_status = @(& git -C $repositoryRoot status --short)
        go_version = (& go version)
        os = [Environment]::OSVersion.VersionString
        processors = [Environment]::ProcessorCount
        runtime_settings = 'GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1536MiB'
        dxspider_head = (& git -C $referenceRoot rev-parse HEAD)
        timers_seconds = @(@{ C = 1800; K = 600 }, @{ C = 0; K = 0 })
        handshake_deadline_seconds = 60
        publication_fault_pc92_max_bytes = 512
        other_fault_pc92_max_bytes = 65536
        fault_fixture_local_connection_limit = 64
        receive_only_seconds = $(if ($Profile -eq 'qualification') { 1200 } else { 5 })
        receive_only_unique_keys_per_minute = 10000
        receive_only_duplicates_per_minute = 100000
        evidence_boundary = 'Real Go TCP peer/telnet components; captured recovery wires replayed into pinned DXSpider handlers; not a full DXSpider network daemon or complete cluster latency qualification.'
    }
    $metadata | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $outputRoot 'metadata.json')
    $sourceFiles = @(& rg --files -g '*.go' -g '*.yaml' -g '*.yml' -g '*.ps1' -g 'go.mod' -g 'go.sum' | Sort-Object)
    $sourceBefore = @(foreach ($sourceFile in $sourceFiles) { (Get-FileHash -Algorithm SHA256 -LiteralPath $sourceFile).Hash + '  ' + $sourceFile })
    $sourceBefore | Set-Content -LiteralPath (Join-Path $outputRoot 'source-sha256-before.txt')
    $testBinary = Join-Path $outputRoot 'q6.test.exe'
    & go test -c -tags qualification -o $testBinary ./peer
    if ($LASTEXITCODE -ne 0) { throw 'Q6 qualification test build failed.' }
    Get-FileHash -Algorithm SHA256 -LiteralPath $testBinary | Format-List | Out-String |
        Set-Content -LiteralPath (Join-Path $outputRoot 'executable-sha256.txt')
    $timeout = if ($Profile -eq 'qualification') { '35m' } else { '5m' }
    Set-Location -LiteralPath (Join-Path $repositoryRoot 'peer')
    & $testBinary '-test.run=^TestPC92QualificationQ6' '-test.count=1' '-test.v' "-test.timeout=$timeout" 2>&1 |
        Tee-Object -FilePath (Join-Path $outputRoot 'test.log')
    $result = $LASTEXITCODE
    Set-Location -LiteralPath $repositoryRoot
    $sourceAfterFiles = @(& rg --files -g '*.go' -g '*.yaml' -g '*.yml' -g '*.ps1' -g 'go.mod' -g 'go.sum' | Sort-Object)
    $sourceAfter = @(foreach ($sourceFile in $sourceAfterFiles) { (Get-FileHash -Algorithm SHA256 -LiteralPath $sourceFile).Hash + '  ' + $sourceFile })
    $sourceAfter | Set-Content -LiteralPath (Join-Path $outputRoot 'source-sha256-after.txt')
    $metadata['source_unchanged'] = (($sourceBefore -join "`n") -ceq ($sourceAfter -join "`n"))
    $metadata['qualification_eligible'] = ($Profile -eq 'qualification' -and $metadata['source_unchanged'])
    $metadata['finished_utc'] = [DateTime]::UtcNow.ToString('o')
    $metadata['exit_code'] = $result
    $metadata | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $outputRoot 'metadata.json')
    if ($result -ne 0) { throw "Q6 $Profile failed ($result); evidence: $outputRoot" }
    if ($Profile -eq 'qualification' -and -not $metadata['source_unchanged']) {
        throw "Source changed during Q6; evidence cannot qualify the final state: $outputRoot"
    }
    $log = Get-Content -LiteralPath (Join-Path $outputRoot 'test.log') -Raw
    $repetitions = if ($Profile -eq 'qualification') { 2 } else { 1 }
    $faults = @('publication', 'clock', 'admission', 'staging-capacity', 'stall', 'candidate-race')
    if ($Profile -eq 'qualification') { $faults += 'staging-deadline' }
    foreach ($zero in @('false', 'true')) {
        foreach ($repeat in 1..$repetitions) {
            foreach ($fault in $faults) {
                $expected = "--- PASS: TestPC92QualificationQ6Faults/zero=$zero/repeat=$repeat/$fault "
                if (-not $log.Contains($expected)) { throw "Missing expected executed case: $expected" }
            }
        }
    }
    if (-not $log.Contains('--- PASS: TestPC92QualificationQ6ReceiveOnly ')) {
        throw 'Missing executed receive-only case; skipped or unmatched tests cannot qualify.'
    }
    $metadata['all_expected_cases_passed'] = $true
    $metadata | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $outputRoot 'metadata.json')
    Write-Output "Q6 $Profile completed; evidence: $outputRoot"
}
finally {
    foreach ($name in $environmentNames) {
        [Environment]::SetEnvironmentVariable($name, $previousEnvironment[$name], 'Process')
    }
    Set-Location -LiteralPath $previousLocation.Path
}

param(
    [ValidateSet('preflight-a', 'preflight-b', 'a', 'b')]
    [string]$Profile = 'preflight-a',
    [string]$OutputDirectory = (Join-Path $env:TEMP ('gocluster-pc92-q4-' + (Get-Date -Format 'yyyyMMdd-HHmmss')))
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$savedLocation = Get-Location
$names = @('GOCLUSTER_PC92_Q4_PROFILE', 'GOCLUSTER_PC92_Q4_OUTPUT', 'GOMAXPROCS', 'GOGC', 'GOMEMLIMIT')
$saved = @{}
foreach ($name in $names) { $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
try {
    Set-Location -LiteralPath $repoRoot
    New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
    $output = (Resolve-Path -LiteralPath $OutputDirectory).Path
    $env:GOCLUSTER_PC92_Q4_PROFILE = $Profile
    $env:GOCLUSTER_PC92_Q4_OUTPUT = Join-Path $output 'observations.json'
    $env:GOMAXPROCS = '2'
    $env:GOGC = '50'
    $env:GOMEMLIMIT = '1536MiB'
    @(
        'Profile: ' + $Profile
        'UTC start: ' + [DateTime]::UtcNow.ToString('o')
        'Git HEAD: ' + (& git rev-parse HEAD)
        'Go: ' + (& go version)
        'OS: ' + [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
        'Logical processors: ' + [Environment]::ProcessorCount
        '1000 real local clients;64 established+128 prelogin for A;63 established+128 authenticated for B'
        'A/B nominal30min after real-wire fullgraph preparation; preflight names are10sec diagnostics'
        'Ordinary authentication, deadlines, duplicate ownership and real600sec cache clocks remain enabled'
        'Cache, queue, active-write, reader and candidate pressure enter through real sockets; final allocation audit remains open'
        (& git status --short)
    ) | Set-Content -LiteralPath (Join-Path $output 'environment.txt')
    $sources = @(& rg --files -g '*.go' -g '*.yaml' -g '*.yml' -g '*.ps1' -g 'go.mod' -g 'go.sum' | Sort-Object)
    $before = @(foreach ($source in $sources) { (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash + '  ' + $source })
    $before | Set-Content -LiteralPath (Join-Path $output 'source-sha256-before.txt')
    & go test -tags qualification ./internal/cluster -run '^TestPC92Q4RuntimeQualification$' -count=1 -timeout=50m -v 2>&1 | Tee-Object -FilePath (Join-Path $output 'test-output.txt')
    $testExit = $LASTEXITCODE
    $afterSources = @(& rg --files -g '*.go' -g '*.yaml' -g '*.yml' -g '*.ps1' -g 'go.mod' -g 'go.sum' | Sort-Object)
    $after = @(foreach ($source in $afterSources) { (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash + '  ' + $source })
    $after | Set-Content -LiteralPath (Join-Path $output 'source-sha256-after.txt')
    @('UTC finish: ' + [DateTime]::UtcNow.ToString('o'); 'Source hashes unchanged: ' + (($before -join "`n") -ceq ($after -join "`n"))) | Set-Content -LiteralPath (Join-Path $output 'completion.txt')
    if ($testExit -ne 0) { throw "Q4 $Profile did not pass; inspect $output" }
    Write-Output "Q4 observations: $output. A diagnostic pass does not establish qualification."
}
finally {
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
    Set-Location -LiteralPath $savedLocation.Path
}

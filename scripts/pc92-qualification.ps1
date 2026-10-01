param(
    [ValidateSet('cache-memory', 'cache-sustained')]
    [string]$Profile = 'cache-memory',
    [string]$OutputDirectory = (Join-Path $env:TEMP ('gocluster-pc92-' + (Get-Date -Format 'yyyyMMdd-HHmmss')))
)

# These are cache-only evidence runs. Full Q1-Q6 qualification still requires
# independent traffic/output correlation and the populations in the companion
# document. No shortened duration or reduced rate is exposed by this wrapper.
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$savedLocation = Get-Location
$savedProfile = $env:GOCLUSTER_PC92_QUALIFICATION
$savedProcs = $env:GOMAXPROCS
$savedGC = $env:GOGC
$savedMemory = $env:GOMEMLIMIT
try {
    Set-Location -LiteralPath $repoRoot
    New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
    $resolvedOutput = (Resolve-Path -LiteralPath $OutputDirectory).Path
    $env:GOCLUSTER_PC92_QUALIFICATION = $Profile
    $env:GOMAXPROCS = '2'
    $env:GOGC = '50'
    $env:GOMEMLIMIT = '1536MiB'
    $metadata = @(
        'Profile: ' + $Profile
        'UTC start: ' + [DateTime]::UtcNow.ToString('o')
        'Git HEAD: ' + (& git rev-parse HEAD)
        'Go: ' + (& go version)
        'OS/architecture: ' + [System.Runtime.InteropServices.RuntimeInformation]::OSDescription + ' / ' + [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
        'Logical processors: ' + [Environment]::ProcessorCount
        'GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1536MiB'
        'Working tree changes:'
        (& git status --short)
    )
    $metadata | Set-Content -LiteralPath (Join-Path $resolvedOutput 'environment.txt')
    $name = if ($Profile -eq 'cache-memory') { '^TestPC92QualificationCacheMemory$' } else { '^TestPC92QualificationCacheSustained$' }
    Get-FileHash -LiteralPath (Join-Path $repoRoot 'peer/dedupe.go'), (Join-Path $repoRoot 'peer/pc92_qualification_test.go') -Algorithm SHA256 |
        Format-List | Out-String | Set-Content -LiteralPath (Join-Path $resolvedOutput 'cache-source-sha256.txt')
    $testBinary = Join-Path $resolvedOutput 'cache.test.exe'
    & go test -c -o $testBinary ./peer
    if ($LASTEXITCODE -ne 0) { throw 'Cache qualification test build failed.' }
    Get-FileHash -LiteralPath $testBinary -Algorithm SHA256 | Format-List | Out-String |
        Set-Content -LiteralPath (Join-Path $resolvedOutput 'executable-sha256.txt')
    Set-Location -LiteralPath (Join-Path $repoRoot 'peer')
    & $testBinary '-test.count=1' '-test.timeout=65m' "-test.run=$name" '-test.v' 2>&1 | Tee-Object -FilePath (Join-Path $resolvedOutput 'test-output.txt')
    $testExit = $LASTEXITCODE
    if ($testExit -ne 0) { throw "Cache qualification failed with exit code $testExit; evidence: $resolvedOutput" }
    $expectedTest = $name.TrimStart('^').TrimEnd('$')
    $log = Get-Content -LiteralPath (Join-Path $resolvedOutput 'test-output.txt') -Raw
    if (-not $log.Contains("--- PASS: $expectedTest ")) { throw 'Expected cache qualification test did not execute and pass.' }
    Write-Output "Cache qualification evidence: $resolvedOutput"
}
finally {
    $env:GOCLUSTER_PC92_QUALIFICATION = $savedProfile
    $env:GOMAXPROCS = $savedProcs
    $env:GOGC = $savedGC
    $env:GOMEMLIMIT = $savedMemory
    Set-Location -LiteralPath $savedLocation.Path
}

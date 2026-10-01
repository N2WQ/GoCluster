param(
    [ValidateSet('preflight', 'qualification')]
    [string]$Profile = 'preflight',
    [string]$OutputDirectory = (Join-Path $env:TEMP ('gocluster-pc92-q5-' + (Get-Date -Format 'yyyyMMdd-HHmmss')))
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$savedLocation = Get-Location
$names = @('GOCLUSTER_PC92_Q5_PROFILE', 'GOMAXPROCS', 'GOGC', 'GOMEMLIMIT')
$savedEnvironment = @{}
foreach ($name in $names) { $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
function Write-SourceManifest([string]$Destination) {
    $paths = @(& git -C $repoRoot ls-files -co --exclude-standard) | Sort-Object -Unique
    $manifest = foreach ($path in $paths) {
        $absolute = Join-Path $repoRoot $path
        if (Test-Path -LiteralPath $absolute -PathType Leaf) {
            [ordered]@{ path = $path; sha256 = (Get-FileHash -LiteralPath $absolute -Algorithm SHA256).Hash }
        }
    }
    $manifest | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $Destination
}
try {
    Set-Location -LiteralPath $repoRoot
    New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
    $resolvedOutput = (Resolve-Path -LiteralPath $OutputDirectory).Path
    $env:GOCLUSTER_PC92_Q5_PROFILE = $Profile
    $env:GOMAXPROCS = '2'
    $env:GOGC = '50'
    $env:GOMEMLIMIT = '1536MiB'
    @(
        'Profile: ' + $Profile
        'UTC start: ' + [DateTime]::UtcNow.ToString('o')
        'Git HEAD: ' + (& git rev-parse HEAD)
        'Go: ' + (& go version)
        'OS: ' + [Environment]::OSVersion.VersionString
        'Logical processors: ' + [Environment]::ProcessorCount
        'GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1536MiB'
        'Four isolated peer Managers, each saturating exactly one class; healthy other classes enter authenticated TCP.'
        'qualification: three complete real600-second retention windows and601-second final drain; no duration override.'
        'preflight: full population but abbreviated hold; never qualification.'
        'This class-isolation test is separate from per-client runtime latency and whole-subsystem allocation proof.'
        (& git status --short)
    ) | Set-Content -LiteralPath (Join-Path $resolvedOutput 'environment.txt')
    Write-SourceManifest (Join-Path $resolvedOutput 'source-before.json')
    $testBinary = Join-Path $resolvedOutput 'q5.test.exe'
    & go test -tags qualification -c -o $testBinary ./peer
    if ($LASTEXITCODE -ne 0) { throw 'Q5 qualification test build failed.' }
    Get-FileHash -LiteralPath $testBinary -Algorithm SHA256 | Format-List | Out-String |
        Set-Content -LiteralPath (Join-Path $resolvedOutput 'executable-sha256.txt')
    Set-Location -LiteralPath (Join-Path $repoRoot 'peer')
    & $testBinary '-test.parallel=4' '-test.count=1' '-test.timeout=60m' '-test.run=^TestPC92QualificationQ5Isolation$' '-test.v' 2>&1 | Tee-Object -FilePath (Join-Path $resolvedOutput 'test-output.txt')
    $testExit = $LASTEXITCODE
    Write-SourceManifest (Join-Path $resolvedOutput 'source-after.json')
    if ($testExit -ne 0) { throw "Q5 $Profile failed with exit code $testExit; evidence: $resolvedOutput" }
    $log = Get-Content -LiteralPath (Join-Path $resolvedOutput 'test-output.txt') -Raw
    foreach ($class in @('spot', 'pc92', 'pc93', 'bulletin')) {
        if (-not $log.Contains("--- PASS: TestPC92QualificationQ5Isolation/$class ")) {
            throw "Missing executed passing Q5 class: $class"
        }
    }
    Write-Output "Q5 $Profile evidence: $resolvedOutput"
}
finally {
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process') }
    Set-Location -LiteralPath $savedLocation.Path
}

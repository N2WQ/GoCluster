<#
.SYNOPSIS
  Check the connected CustomSCP runner's setup and failure handling offline.
.DESCRIPTION
  Parses the runner, then uses a bounded mock of its native-command helper.
  These checks verify selection, overlay isolation, readiness, state restoration,
  and lock ownership; they do not execute Go or establish storage correctness.
.NOTES
  Requires PowerShell 7 on Windows. Uses and removes only its own ignored .tmp
  fixture directory. No real test binaries, production data, or firewall rules
  change. The actual Go producer and delivered-output consumer remain separate.
#>
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$runner = Join-Path $PSScriptRoot 'test-customscp-regression.ps1'
$tokens = $null
$parseErrors = $null
$null = [Management.Automation.Language.Parser]::ParseFile($runner, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count -ne 0) { throw "CustomSCP runner parse failed: $($parseErrors -join '; ')" }
. $runner

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$fixtureRepository = Join-Path $repositoryRoot ('.tmp/customscp-runner-fixture-' + [guid]::NewGuid().ToString('N'))
$originalPath = $env:PATH
$originalDirectory = (Get-Location).Path
$savedEnvironment = @{}
foreach ($name in @('GOCLUSTER_CUSTOMSCP_FIXTURE', 'GOCLUSTER_CUSTOMSCP_RUN_ID')) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
$script:mode = 'pass'
$script:commands = [Collections.Generic.List[object]]::new()
$passed = 0

function Assert-CustomSCPFixture([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

# The mock substitutes exactly the runner's process boundary. All file, lock,
# overlay, readiness, and caller-state handling below is the actual runner code.
function Invoke-CustomSCPCommand([string]$Executable, [string[]]$Arguments, [string]$Directory, [string]$LogPath, [bool]$Quiet = $false) {
    $script:commands.Add([pscustomobject]@{ Executable = $Executable; Arguments = $Arguments; Directory = $Directory; Log = $LogPath })
    $exitCode = 0
    $lines = @()
    if ($Executable -eq 'git') {
        if ($Arguments[0] -eq 'rev-parse') {
            $lines = @($fixtureRepository)
        } elseif ($Arguments[0] -eq 'show') {
            $lines = @('package spot', 'func (s *CustomSCPStore) cleanup(now time.Time) {', "`tif s.db != nil {", "`t`titer, err := s.db.NewIter(nil)", "`t`tdefer iter.Close()", "`t`tbatch := s.db.NewBatch()", "`t}", '}', 'func (s *CustomSCPStore) loadFromDB() error { return nil }')
        } else { throw "Unexpected mock git operation: $Arguments" }
    } elseif ($Executable -eq 'go') {
        if ($Arguments[0] -eq 'env') {
            $target = if ($script:mode -eq 'cross-target') { 'linux' } else { 'windows' }
            $lines = @(@{ GOOS = $target; GOARCH = 'amd64'; GOHOSTARCH = 'amd64'; GOROOT = $fixtureRepository } | ConvertTo-Json -Compress)
        } elseif ($Arguments[0] -eq 'test') {
            $producer = $Arguments[-1] -eq 'dxcluster/spot'
            if ($producer -and $script:mode -eq 'producer-build-fails') {
                $exitCode = 1
            } elseif (-not ($producer -and $script:mode -eq 'missing-binary')) {
                Set-Content -LiteralPath $Arguments[3] -Value 'offline fixture executable placeholder'
            }
        } else { throw "Unexpected mock Go operation: $Arguments" }
    } else {
        $producer = $Executable -eq (Join-Path $fixtureRepository '.tmp/windows-tests/dxcluster/spot/package.test.exe')
        $testName = if ($producer) { 'TestCustomSCPCleanupFixtureProducer' } else { 'TestCustomSCPDeliveredOutput' }
        if ($Arguments[0].StartsWith('-test.list=')) {
            $lines = if ($script:mode -eq 'wrong-selection' -and $producer) { @('TestOther') } else { @($testName) }
        } else {
            Assert-CustomSCPFixture ($Arguments[0] -ceq "-test.run=^$testName$") 'Runner did not select the exact test.'
            Assert-CustomSCPFixture ($Arguments -contains '-test.count=1' -and $Arguments -contains '-test.paniconexit0') 'Runner lost direct-execution safeguards.'
            if ($producer) {
                if ($script:mode -eq 'producer-fails') {
                    $exitCode = 1
                } elseif ($script:mode -ne 'missing-ready') {
                    if ($script:mode -ne 'missing-db') { $null = [IO.Directory]::CreateDirectory((Join-Path $env:GOCLUSTER_CUSTOMSCP_FIXTURE 'scp')) }
                    $identity = if ($script:mode -eq 'wrong-identity') { 'another-run' } else { $env:GOCLUSTER_CUSTOMSCP_RUN_ID }
                    $timestamp = if ($script:mode -eq 'stale-ready') { [DateTimeOffset]::UtcNow.ToUnixTimeSeconds() - 601 } else { [DateTimeOffset]::UtcNow.ToUnixTimeSeconds() - 60 }
                    $lost = $script:mode -eq 'consumer-fails'
                    $manifest = @{
                        run_id = $identity; time_unix = $timestamp
                        observation_present = -not $lost; static_present = -not $lost
                        recovered_support = $(if ($lost) { 0 } else { 2 })
                    }
                    if ($script:mode -eq 'invalid-type') { $manifest.observation_present = 'false' }
                    $json = if ($script:mode -eq 'invalid-json') { '{' } else { $manifest | ConvertTo-Json -Compress }
                    Set-Content -LiteralPath (Join-Path $env:GOCLUSTER_CUSTOMSCP_FIXTURE 'ready.json') -Value $json
                }
            } elseif ($script:mode -eq 'consumer-fails') {
                $exitCode = 1
            }
            $lines = @($testName)
        }
    }
    if ($LogPath) { Set-Content -LiteralPath $LogPath -Value $lines }
    return [pscustomobject]@{ ExitCode = $exitCode; Lines = @($lines) }
}

function Assert-CustomSCPCallerRestored {
    Assert-CustomSCPFixture ($env:PATH -ceq $originalPath -and (Get-Location).Path -eq $originalDirectory) 'Runner changed the caller PATH or location.'
    foreach ($name in $savedEnvironment.Keys) {
        $actual = [Environment]::GetEnvironmentVariable($name, 'Process')
        Assert-CustomSCPFixture ($actual -ceq $savedEnvironment[$name]) "Runner did not restore $name (actual='$actual', expected='$($savedEnvironment[$name])')."
    }
}

function Invoke-CustomSCPExpectedFailure([string]$Mode, [string]$Message, [bool]$UseBaseline = $false) {
    $script:mode = $Mode
    $script:commands.Clear()
    try {
        Invoke-CustomSCPRegression $fixtureRepository $UseBaseline
    } catch {
        Assert-CustomSCPFixture ($_.Exception.Message.Contains($Message)) "Expected '$Message', got: $_"
        Assert-CustomSCPCallerRestored
        return
    }
    throw "Expected CustomSCP runner failure: $Message"
}

try {
    Assert-CustomSCPOutputPath $repositoryRoot $fixtureRepository
    $null = [IO.Directory]::CreateDirectory((Join-Path $fixtureRepository 'spot'))
    $null = [IO.Directory]::CreateDirectory((Join-Path $fixtureRepository 'internal/cluster'))
    Set-Content -LiteralPath (Join-Path $fixtureRepository 'go.mod') 'module dxcluster'
    $producer = @'
package spot
// Unchanged producer prefix.
func TestCustomSCPCleanupFixtureProducer(t *testing.T) {}
func exerciseCustomSCPFixture(t *testing.T, s *CustomSCPStore, now time.Time, refresh func()) {
	t.Helper()
	s.applyCleanupDeletes(nil)
}
'@
    Set-Content -LiteralPath (Join-Path $fixtureRepository 'spot/custom_scp_fixture_test.go') $producer
    foreach ($name in @('custom_scp_cleanup.go', 'custom_scp_cleanup_test.go', 'custom_scp_lifecycle_test.go', 'custom_scp_cleanup_helpers_test.go', 'custom_scp_persistence_bench_test.go')) {
        Set-Content -LiteralPath (Join-Path $fixtureRepository "spot/$name") 'package spot'
    }

    $script:commands.Clear()
    Invoke-CustomSCPRegression $fixtureRepository $false
    Assert-CustomSCPCallerRestored
    $firstRun = (Get-ChildItem -LiteralPath (Join-Path $fixtureRepository '.tmp/customscp-regression') -Directory).Name
    Invoke-CustomSCPRegression $fixtureRepository $false $true
    Assert-CustomSCPCallerRestored
    $runs = @(Get-ChildItem -LiteralPath (Join-Path $fixtureRepository '.tmp/customscp-regression') -Directory)
    Assert-CustomSCPFixture ($runs.Count -eq 2 -and @($runs | Where-Object Name -EQ $firstRun).Count -eq 1) 'Runner reused a fixture directory.'
    $builds = @($script:commands | Where-Object { $_.Executable -eq 'go' -and $_.Arguments[0] -eq 'test' })
    Assert-CustomSCPFixture ($builds.Count -eq 4 -and $builds[2].Arguments -contains '-race' -and $builds[3].Arguments -contains '-race') 'Race mode did not compile both stages.'
    foreach ($build in $builds) { Assert-CustomSCPFixture (-not ($build.Arguments -match '^-overlay=')) 'Fixed execution unexpectedly used an overlay.' }
    $executions = @($script:commands | Where-Object { $_.Executable.EndsWith('package.test.exe') -and $_.Arguments[0].StartsWith('-test.run=') })
    Assert-CustomSCPFixture ($executions.Count -eq 4) 'Fixed execution did not run both stages twice.'
    foreach ($execution in $executions) {
        $expectedDirectory = if ($execution.Arguments[0].Contains('Producer')) { Join-Path $fixtureRepository 'spot' } else { Join-Path $fixtureRepository 'internal/cluster' }
        Assert-CustomSCPFixture ($execution.Directory -eq $expectedDirectory) 'Test execution used the wrong package directory.'
    }
    $passed++

    Invoke-CustomSCPExpectedFailure 'consumer-fails' 'consumer test failed' $true
    $builds = @($script:commands | Where-Object { $_.Executable -eq 'go' -and $_.Arguments[0] -eq 'test' })
    $overlayArgument = @($builds[0].Arguments | Where-Object { $_.StartsWith('-overlay=') })
    Assert-CustomSCPFixture ($builds.Count -eq 2 -and $overlayArgument.Count -eq 1 -and -not ($builds[1].Arguments -match '^-overlay=')) 'Baseline overlay was not limited to the producer.'
    $overlay = Get-Content -LiteralPath $overlayArgument[0].Substring('-overlay='.Length) -Raw | ConvertFrom-Json -AsHashtable
    $store = Get-Content -LiteralPath $overlay.Replace[(Join-Path $fixtureRepository 'spot/custom_scp_store.go')] -Raw
    Assert-CustomSCPFixture ($store.Contains("defer iter.Close()`n`t`tif customSCPFixtureAfterIterator != nil") -and $store.Contains('var customSCPFixtureAfterIterator func()')) 'Baseline hook is not immediately after the original iterator snapshot or lacks its production declaration.'
    $overlaidProducer = Get-Content -LiteralPath $overlay.Replace[(Join-Path $fixtureRepository 'spot/custom_scp_fixture_test.go')] -Raw
    Assert-CustomSCPFixture ($overlaidProducer.Contains('// Unchanged producer prefix.') -and $overlaidProducer.Contains('s.cleanup(now)') -and -not $overlaidProducer.Contains('s.applyCleanupDeletes')) 'Baseline overlay changed more than the scheduling function.'
    $consumerRuns = @($script:commands | Where-Object { $_.Arguments[0] -ceq '-test.run=^TestCustomSCPDeliveredOutput$' })
    Assert-CustomSCPFixture ($consumerRuns.Count -eq 1) 'Actual loss was rejected as setup instead of reaching the consumer.'
    $passed++

    foreach ($failure in @(
        @('producer-build-fails', 'producer build failed'),
        @('wrong-selection', 'producer test selection'),
        @('producer-fails', 'producer test failed'),
        @('missing-ready', 'did not create ready.json'),
        @('wrong-identity', 'different run identity'),
        @('stale-ready', 'timestamp is not fresh'),
        @('missing-db', 'database is missing'),
        @('invalid-type', 'invalid types'),
        @('invalid-json', 'invalid JSON'),
        @('cross-target', 'native Windows target')
    )) {
        Invoke-CustomSCPExpectedFailure $failure[0] $failure[1]
        Assert-CustomSCPFixture (@($script:commands | Where-Object { $_.Arguments[0] -ceq '-test.run=^TestCustomSCPDeliveredOutput$' }).Count -eq 0) 'Setup failure executed the consumer.'
        if ($failure[0] -eq 'producer-build-fails') {
            Assert-CustomSCPFixture (@($script:commands | Where-Object { $_.Executable.EndsWith('package.test.exe') }).Count -eq 0) 'Build failure executed a stale binary.'
        }
        $passed++
    }

    $producerBinary = Join-Path $fixtureRepository '.tmp/windows-tests/dxcluster/spot/package.test.exe'
    Remove-Item -LiteralPath $producerBinary
    Invoke-CustomSCPExpectedFailure 'missing-binary' 'build produced no executable'
    Assert-CustomSCPFixture (@($script:commands | Where-Object { $_.Executable.EndsWith('package.test.exe') }).Count -eq 0) 'Missing executable was run.'
    $passed++

    $lockPath = Join-Path $fixtureRepository '.tmp/windows-tests/runner.lock'
    $heldLock = [IO.File]::Open($lockPath, [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    try { Invoke-CustomSCPExpectedFailure 'pass' 'Cannot acquire Windows test runner lock' } finally { $heldLock.Dispose() }
    Assert-CustomSCPFixture ($script:commands.Count -eq 1 -and $script:commands[0].Executable -eq 'git') 'Lock rejection built or ran binaries.'
    $passed++

    Write-Host "CustomSCP runner offline fixtures passed: $passed checks (no Go commands executed)."
} finally {
    foreach ($name in $savedEnvironment.Keys) {
        if ($null -eq $savedEnvironment[$name]) {
            Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
        } else { [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process') }
    }
    $env:PATH = $originalPath
    Set-Location -LiteralPath $originalDirectory
    if (Test-Path -LiteralPath $fixtureRepository) {
        # Check the resolved owned target before recursive removal, in this shell.
        Assert-CustomSCPOutputPath $repositoryRoot $fixtureRepository
        Remove-Item -LiteralPath $fixtureRepository -Recurse -Force
    }
}

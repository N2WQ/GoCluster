<#
.SYNOPSIS
  Verify the stable Windows test runner with disposable native Go packages.
.DESCRIPTION
  Exercises actual executable paths, child processes, working directories,
  repeated builds, race mode, preparation, failure handling, and lock exclusion.
.NOTES
  Prerequisites: PowerShell 7 and native Windows Go including race prerequisites.
  Side effects: builds/runs network-free fixture binaries under ignored .tmp.
  Safety: no firewall changes or production cluster processes; removes only the
  unique fixture directory created by this invocation and restores caller state.
#>
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$fixtureRoot = Join-Path $repoRoot ('.tmp/windows-test-fixture-' + [guid]::NewGuid().ToString('N'))
$originalPath = $env:PATH
$originalLocation = (Get-Location).Path
$savedEnvironment = @{}
foreach ($name in @('WINDOWS_TEST_FIXTURE_LOG', 'WINDOWS_TEST_FIXTURE_FAIL')) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
$passed = 0

function Assert([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}
function Invoke-FixtureFailure([string]$Expected, [hashtable]$Parameters = @{}) {
    try { & $runner @Parameters } catch {
        Assert ($_.Exception.Message.Contains($Expected)) "Expected '$Expected', got: $_"
        return
    }
    throw "Expected failure: $Expected"
}
function Read-Observations {
    @(Get-Content -LiteralPath $env:WINDOWS_TEST_FIXTURE_LOG | ForEach-Object { $_ | ConvertFrom-Json })
}

try {
    $null = [IO.Directory]::CreateDirectory((Join-Path $fixtureRoot 'scripts'))
    $runner = Join-Path $fixtureRoot 'scripts/test-windows.ps1'
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'test-windows.ps1') -Destination $runner
    Set-Content -LiteralPath (Join-Path $fixtureRoot 'go.mod') "module runnerfixture`n`ngo 1.27.1"
    $source = @'
package fixture

import (
    "encoding/json"
    "flag"
    "os"
    "os/exec"
    "testing"
)

func observe(t *testing.T, role string) {
    t.Helper()
    executable, err := os.Executable()
    if err != nil { t.Fatal(err) }
    directory, err := os.Getwd()
    if err != nil { t.Fatal(err) }
    file, err := os.OpenFile(os.Getenv("WINDOWS_TEST_FIXTURE_LOG"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
    if err != nil { t.Fatal(err) }
    err = json.NewEncoder(file).Encode(map[string]string{
        "Executable": executable, "Directory": directory, "Role": role, "Path": os.Getenv("PATH"),
        "Race": fixtureRace, "Timeout": flag.Lookup("test.timeout").Value.String(), "Count": flag.Lookup("test.count").Value.String(),
    })
    closeErr := file.Close()
    if err != nil { t.Fatal(err) }
    if closeErr != nil { t.Fatal(closeErr) }
}

func TestRunner(t *testing.T) {
    observe(t, "parent")
    if os.Getenv("WINDOWS_TEST_FIXTURE_FAIL") == "1" { t.Fatal("requested fixture failure") }
    child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRunnerChild$", "-test.timeout=10s")
    child.Env = append(os.Environ(), "WINDOWS_TEST_FIXTURE_CHILD=1")
    if output, err := child.CombinedOutput(); err != nil { t.Fatalf("child: %v %s", err, output) }
}

func TestRunnerChild(t *testing.T) {
    if os.Getenv("WINDOWS_TEST_FIXTURE_CHILD") != "1" { t.Skip("subprocess only") }
    observe(t, "child")
}

func TestExcludedByFilter(t *testing.T) { t.Fatal("test name filter was not applied") }
func TestExitZero(t *testing.T) { os.Exit(0) }
'@
    foreach ($relative in @('', 'a/same', 'b/same')) {
        $directory = if ($relative) { Join-Path $fixtureRoot $relative } else { $fixtureRoot }
        $null = [IO.Directory]::CreateDirectory($directory)
        Set-Content -LiteralPath (Join-Path $directory 'runner_test.go') $source
        Set-Content -LiteralPath (Join-Path $directory 'race_enabled.go') "//go:build race`n`npackage fixture`nconst fixtureRace = `"true`""
        Set-Content -LiteralPath (Join-Path $directory 'race_disabled.go') "//go:build !race`n`npackage fixture`nconst fixtureRace = `"false`""
    }
    $null = [IO.Directory]::CreateDirectory((Join-Path $fixtureRoot 'empty'))
    Set-Content -LiteralPath (Join-Path $fixtureRoot 'empty/empty.go') 'package empty'
    $env:WINDOWS_TEST_FIXTURE_LOG = Join-Path $fixtureRoot 'observations.jsonl'
    $env:WINDOWS_TEST_FIXTURE_FAIL = '0'

    & $runner -PrepareOnly
    Assert (-not (Test-Path -LiteralPath $env:WINDOWS_TEST_FIXTURE_LOG)) 'Prepare-only ran tests.'
    $binaryRoot = Join-Path $fixtureRoot '.tmp/windows-tests/runnerfixture'
    Assert (Test-Path -LiteralPath (Join-Path $binaryRoot 'package.test.exe')) 'Root binary missing.'
    Assert (-not (Test-Path -LiteralPath (Join-Path $binaryRoot 'empty/package.test.exe'))) 'Empty package produced a binary.'
    $passed++

    & $runner -Run '^TestRunner$' -Timeout 30s
    & $runner -Run '^TestRunner$' -Timeout 30s
    $observations = Read-Observations
    Assert ($observations.Count -eq 12) 'Repeated runs did not execute all parents and children.'
    $goroot = & go env GOROOT
    if ($LASTEXITCODE -ne 0) { throw 'Fixture go env failed.' }
    foreach ($relative in @('', 'a/same', 'b/same')) {
        $directory = if ($relative) { Join-Path $fixtureRoot $relative } else { $fixtureRoot }
        $binaryDirectory = if ($relative) { Join-Path $binaryRoot $relative } else { $binaryRoot }
        $expectedBinary = Join-Path $binaryDirectory 'package.test.exe'
        $matching = @($observations | Where-Object Directory -EQ $directory)
        Assert ($matching.Count -eq 4) "Working directory mismatch: $directory"
        foreach ($observation in $matching) {
            Assert ($observation.Executable -eq $expectedBinary) "Wrong executable path: $($observation.Executable)"
            Assert ($observation.Path.StartsWith((Join-Path $goroot 'bin') + ';', [StringComparison]::OrdinalIgnoreCase)) 'Toolchain PATH mismatch.'
            Assert ($observation.Race -eq 'false' -and $observation.Count -eq '1') 'Ordinary build or test count mismatch.'
            if ($observation.Role -eq 'parent') { Assert ($observation.Timeout -eq '30s') 'Timeout was not forwarded.' }
        }
        Assert (@($matching | Where-Object Role -EQ 'child').Count -eq 2) 'Child path was not observed twice.'
    }
    Assert ($env:PATH -ceq $originalPath -and (Get-Location).Path -eq $originalLocation) 'Caller state changed after success.'
    $passed++

    & $runner -Packages '.' -Race -Run '^TestRunner$' -Timeout 30s
    $observations = Read-Observations
    Assert ($observations.Count -eq 14 -and $observations[-1].Executable -eq (Join-Path $binaryRoot 'package.test.exe')) 'Race mode changed executable path.'
    Assert ($observations[-1].Race -eq 'true' -and $observations[-2].Race -eq 'true') 'Race mode did not enable the detector.'
    $passed++

    $env:WINDOWS_TEST_FIXTURE_FAIL = '1'
    Invoke-FixtureFailure 'Tests failed: runnerfixture' @{ Packages = @('.'); Run = '^TestRunner$' }
    $env:WINDOWS_TEST_FIXTURE_FAIL = '0'
    Assert ((Read-Observations).Count -eq 15) 'Expected failing test was not executed.'
    Assert ($env:PATH -ceq $originalPath -and (Get-Location).Path -eq $originalLocation) 'Caller state changed after test failure.'
    $passed++

    Invoke-FixtureFailure 'Tests failed: runnerfixture' @{ Packages = @('.'); Run = '^TestExitZero$' }
    Assert ((Read-Observations).Count -eq 15) 'Exit-zero fixture ran unrelated tests.'
    $passed++

    $invalidSource = Join-Path $fixtureRoot 'broken.go'
    Set-Content -LiteralPath $invalidSource "package fixture`nvar broken = missingIdentifier"
    Invoke-FixtureFailure 'Test build failed: runnerfixture' @{ Packages = @('.'); Run = '^TestRunner$' }
    Assert ((Read-Observations).Count -eq 15) 'Build failure executed a stale binary.'
    Remove-Item -LiteralPath $invalidSource
    & $runner -Packages '.' -PrepareOnly
    $passed++

    $invalidEmptySource = Join-Path $fixtureRoot 'empty/broken.go'
    Set-Content -LiteralPath $invalidEmptySource "package empty`nvar broken = missingIdentifier"
    Invoke-FixtureFailure 'Test build failed: runnerfixture/empty' @{ Packages = @('./empty') }
    Remove-Item -LiteralPath $invalidEmptySource
    $passed++

    $heldLock = [IO.File]::Open((Join-Path $fixtureRoot '.tmp/windows-tests/runner.lock'), [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    try { Invoke-FixtureFailure 'Cannot acquire Windows test runner lock' } finally { $heldLock.Dispose() }
    Assert ((Read-Observations).Count -eq 15) 'Concurrent invocation ran tests.'
    $passed++

    Invoke-FixtureFailure 'External package is not supported' @{ Packages = @('fmt') }
    Invoke-FixtureFailure 'go list failed' @{ Packages = @('./missing') }
    $passed++

    # Refuse output redirection through a junction before replacing anything.
    $packageDirectory = Join-Path $binaryRoot 'a/same'
    $sentinel = Join-Path $fixtureRoot 'sentinel'
    $null = [IO.Directory]::CreateDirectory($sentinel)
    Set-Content -LiteralPath (Join-Path $sentinel 'package.test.exe') 'KEEP'
    Remove-Item -LiteralPath (Join-Path $packageDirectory 'package.test.exe')
    Remove-Item -LiteralPath $packageDirectory
    $null = New-Item -ItemType Junction -Path $packageDirectory -Target $sentinel
    try {
        Invoke-FixtureFailure 'junction or symlink' @{ Packages = @('./a/same'); PrepareOnly = $true }
        Assert ((Get-Content -LiteralPath (Join-Path $sentinel 'package.test.exe')) -eq 'KEEP') 'Junction target was replaced.'
    } finally {
        # Remove the junction itself, never recurse through its target.
        [IO.Directory]::Delete($packageDirectory)
    }
    $passed++
    Write-Host "PASS: $passed Windows test runner fixture groups."
} finally {
    foreach ($name in $savedEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process')
    }
    $env:PATH = $originalPath
    Set-Location -LiteralPath $originalLocation
    $resolvedFixture = [IO.Path]::GetFullPath($fixtureRoot)
    $allowedRoot = [IO.Path]::GetFullPath((Join-Path $repoRoot '.tmp')) + [IO.Path]::DirectorySeparatorChar
    if (-not $resolvedFixture.StartsWith($allowedRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing fixture cleanup outside .tmp: $resolvedFixture"
    }
    if (Test-Path -LiteralPath $resolvedFixture) { Remove-Item -LiteralPath $resolvedFixture -Recurse -Force }
}

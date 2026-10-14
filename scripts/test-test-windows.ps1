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
    "fmt"
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
func TestNested(t *testing.T) { t.Run("leaf", func(t *testing.T) {}) }
func TestSkip(t *testing.T) { t.Skip("fixture prerequisite absent") }
func TestSpoof(t *testing.T) {
    child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSpoofChild$", "-test.timeout=10s")
    child.Env = append(os.Environ(), "WINDOWS_TEST_FIXTURE_CHILD=1")
    output, err := child.CombinedOutput(); if err != nil { t.Fatal(err) }; fmt.Print(string(output))
}
func TestSpoofChild(t *testing.T) {
    if os.Getenv("WINDOWS_TEST_FIXTURE_CHILD") != "1" { t.Skip("subprocess only") }
    fmt.Print("=== RUN   TestSpoof/missing\n--- PASS: TestSpoof/missing (0.00s)\n")
}
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
    Add-Content -LiteralPath (Join-Path $fixtureRoot 'runner_test.go') 'func TestRootOnly(t *testing.T) {}'
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

    & $runner -Run '^TestRootOnly$' -RequireCases 'runnerfixture::TestRootOnly'
    # Exact leaf evidence comes from the same compiled binary, not -list.
    & $runner -Packages '.' -Run '^TestNested$/^leaf$' -RequireCases 'runnerfixture::TestNested/leaf'
    Invoke-FixtureFailure 'Slash-qualified filters require' @{ Packages = @('.'); Run = '^TestNested$/missing' }
    Invoke-FixtureFailure 'Required case did not RUN then PASS' @{ Packages = @('.'); Run = '^TestNested$/missing'; RequireCases = @('runnerfixture::TestNested/missing') }
    Invoke-FixtureFailure 'Required case did not RUN then PASS' @{ Packages = @('.'); Run = '^TestNested$/^leaf$'; RequireCases = @('runnerfixture::TestNested/LEAF') }
    Invoke-FixtureFailure 'Filtered execution passed no cases' @{ Packages = @('.'); Run = '^TestDoesNotExist$' }
    Invoke-FixtureFailure 'Filtered execution passed no cases' @{ Packages = @('.'); Run = '^TestSkip$' }
    Invoke-FixtureFailure 'Required case did not RUN then PASS' @{ Packages = @('.'); Run = '^(TestNested|TestSkip)$'; RequireCases = @('runnerfixture::TestSkip') }
    Invoke-FixtureFailure 'Required case did not RUN then PASS' @{ Packages = @('.'); Run = '^TestSpoof$'; RequireCases = @('runnerfixture::TestSpoof/missing') }
    $passed++
    foreach ($relative in @('third_party/go-sqlite3', 'third_party/go-sqlite3/qualification', 'third_party/paho.mqtt.golang')) {
        $directory = Join-Path $fixtureRoot $relative
        $null = [IO.Directory]::CreateDirectory($directory)
        Set-Content -LiteralPath (Join-Path $directory 'go.mod') "module nestedfixture/$($relative.Split('/')[-1])`n`ngo 1.27.1"
        Set-Content -LiteralPath (Join-Path $directory 'tag_test.go') "//go:build runner_tag`n`npackage fixture`nimport `"testing`"`nfunc TestTagged(t *testing.T) {}"
        & $runner -ModuleRoot $relative -Tags runner_tag -Packages '.' -Run '^TestTagged$' -RequireCases "nestedfixture/$($relative.Split('/')[-1])::TestTagged"
    }
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

    # Tool faults must not be hidden by a succeeding process or output pipeline.
    $nativeGoCommand = Get-Command go.exe -CommandType Application -All -ErrorAction Stop |
        Select-Object -First 1
    $nativeGo = $nativeGoCommand.Source
    if ([string]::IsNullOrWhiteSpace($nativeGo) -or -not (Test-Path -LiteralPath $nativeGo -PathType Leaf)) {
        throw 'Could not resolve one Go executable for fault-injection fixtures.'
    }
    try {
        function go {
            if ($args[0] -eq 'tool') {
                $global:LASTEXITCODE = 0
                'not-json'
            } else { & $nativeGo @args }
        }
        Invoke-FixtureFailure 'Malformed test event stream' @{ Packages = @('.'); Run = '^TestNested$' }
        function go {
            if ($args[0] -eq 'tool') {
                $global:LASTEXITCODE = 0
                '{"Action":"start","Package":["runnerfixture"]}'
                '{"Action":"run","Package":["runnerfixture"],"Test":"TestNested"}'
                '{"Action":"pass","Package":["runnerfixture"],"Test":"TestNested"}'
                '{"Action":"pass","Package":["runnerfixture"]}'
            } else { & $nativeGo @args }
        }
        Invoke-FixtureFailure 'Malformed test event identity or action' @{ Packages = @('.'); Run = '^TestNested$'; RequireCases = @('runnerfixture::TestNested') }
        function go {
            if ($args[0] -eq 'tool') {
                $global:LASTEXITCODE = 9
            } else { & $nativeGo @args }
        }
        Invoke-FixtureFailure 'Tests failed: runnerfixture (exit 9)' @{ Packages = @('.'); Run = '^TestNested$' }
    } finally { Remove-Item Function:go }
    Invoke-FixtureFailure 'Filtered execution passed no cases' @{ Packages = @('./empty'); Run = '^TestMissing$' }
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
    $modulePath = Join-Path $fixtureRoot 'third_party/paho.mqtt.golang'
    Remove-Item -LiteralPath (Join-Path $modulePath 'go.mod'), (Join-Path $modulePath 'tag_test.go')
    [IO.Directory]::Delete($modulePath)
    $null = New-Item -ItemType Junction -Path $modulePath -Target $sentinel
    try { Invoke-FixtureFailure 'junction or symlink' @{ ModuleRoot = 'third_party/paho.mqtt.golang'; PrepareOnly = $true } }
    finally { [IO.Directory]::Delete($modulePath) }
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

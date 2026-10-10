<#
.SYNOPSIS
  Connect the CustomSCP cleanup counterexample to delivered-output checks.
.DESCRIPTION
  Builds and directly runs the producer and consumer from canonical Windows
  test paths. Each invocation owns a fresh fixture directory and keeps its
  database, readiness manifest, and logs for inspection. The shared Windows
  runner lock prevents concurrent replacement or execution of these binaries.
.PARAMETER Baseline
  Build only the producer with the defective 4d711a4 store through a temporary
  Go overlay. A scheduling hook forces refresh after the old iterator snapshot.
  The current consumer must reject the resulting lost-history fixture.
.PARAMETER Race
  Compile both stages with the Go race detector, reusing the same binary paths.
.NOTES
  Requires PowerShell 7 and native Windows Go. No checkout, firewall changes,
  external feeds, or production data are used. Baseline is expected to fail the
  positive delivery assertion; setup failures also fail closed.
#>
[CmdletBinding()]
param([switch]$Baseline, [switch]$Race)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false

function Assert-CustomSCPOutputPath([string]$RepositoryRoot, [string]$Path) {
    $root = [IO.Path]::GetFullPath($RepositoryRoot)
    $full = [IO.Path]::GetFullPath($Path)
    if (-not $full.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw "CustomSCP output path is outside the repository: $Path"
    }
    for ($current = $full; $current -ne $root; $current = [IO.Path]::GetDirectoryName($current)) {
        if (Test-Path -LiteralPath $current) {
            if ((Get-Item -LiteralPath $current -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
                throw "CustomSCP output path traverses a junction or symlink: $current"
            }
        }
    }
}

function Invoke-CustomSCPCommand([string]$Executable, [string[]]$Arguments, [string]$Directory, [string]$LogPath, [bool]$Quiet = $false) {
    Push-Location -LiteralPath $Directory
    try {
        $lines = @(& $Executable @Arguments 2>&1 | ForEach-Object { "$_" })
        $commandExit = $LASTEXITCODE
        if ($LogPath) { Set-Content -LiteralPath $LogPath -Value $lines -Encoding utf8 }
        if (-not $Quiet) { foreach ($line in $lines) { Write-Host $line } }
        return [pscustomobject]@{ ExitCode = $commandExit; Lines = $lines }
    } finally {
        Pop-Location
    }
}

function Assert-CustomSCPReady([string]$FixtureRoot, [string]$RunID) {
    $readyPath = Join-Path $FixtureRoot 'ready.json'
    if (-not (Test-Path -LiteralPath $readyPath -PathType Leaf)) { throw 'CustomSCP producer did not create ready.json.' }
    $file = Get-Item -LiteralPath $readyPath -Force
    if ($file.Attributes -band [IO.FileAttributes]::ReparsePoint -or $file.Length -gt 4096) {
        throw 'CustomSCP readiness must be a regular file of at most 4096 bytes.'
    }
    try { $ready = Get-Content -LiteralPath $readyPath -Raw | ConvertFrom-Json -AsHashtable } catch {
        throw "CustomSCP readiness is invalid JSON: $($_.Exception.Message)"
    }
    $expected = @('run_id', 'time_unix', 'observation_present', 'static_present', 'recovered_support')
    if ($ready -isnot [System.Collections.IDictionary] -or $ready.Count -ne $expected.Count) {
        throw 'CustomSCP readiness has an unexpected shape.'
    }
    foreach ($name in $expected) {
        if (-not $ready.Contains($name)) { throw "CustomSCP readiness is missing $name." }
    }
    if ($ready.run_id -isnot [string] -or $ready.run_id -cne $RunID) { throw 'CustomSCP readiness has a different run identity.' }
    if ($ready.time_unix -isnot [long] -and $ready.time_unix -isnot [int]) { throw 'CustomSCP readiness timestamp must be an integer.' }
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    if ($ready.time_unix -le 0 -or $ready.time_unix -lt $now - 600 -or $ready.time_unix -gt $now + 10) {
        throw 'CustomSCP readiness timestamp is not fresh.'
    }
    if ($ready.observation_present -isnot [bool] -or $ready.static_present -isnot [bool] -or
        ($ready.recovered_support -isnot [long] -and $ready.recovered_support -isnot [int]) -or $ready.recovered_support -lt 0) {
        throw 'CustomSCP readiness observations have invalid types.'
    }
    $database = Join-Path $FixtureRoot 'scp'
    if (-not (Test-Path -LiteralPath $database -PathType Container) -or
        (Get-Item -LiteralPath $database -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw 'CustomSCP producer database is missing or redirected.'
    }
    # Preservation flags are evidence, not setup gates. Lost history must reach
    # the consumer, whose positive output expectation is independent of them.
    return $ready
}

function New-CustomSCPBaselineOverlay([string]$RepositoryRoot, [string]$FixtureRoot) {
    $source = Invoke-CustomSCPCommand 'git' @('show', '4d711a4:spot/custom_scp_store.go') $RepositoryRoot (Join-Path $FixtureRoot 'baseline-source.log') $true
    if ($source.ExitCode -ne 0) { throw 'Cannot read the defective 4d711a4 CustomSCP source.' }
    $store = $source.Lines -join "`n"
    $cleanupPattern = '(?ms)^func \(s \*CustomSCPStore\) cleanup\(now time\.Time\) \{.*?(?=^func |\z)'
    $cleanupMatches = [regex]::Matches($store, $cleanupPattern)
    if ($cleanupMatches.Count -ne 1) { throw 'Baseline overlay requires exactly one original cleanup function.' }
    $cleanup = $cleanupMatches[0]
    $iteratorPattern = '(?m)^\t\tdefer iter\.Close\(\)\r?$'
    if ([regex]::Matches($cleanup.Value, $iteratorPattern).Count -ne 1) {
        throw 'Baseline overlay requires exactly one cleanup iterator-close marker.'
    }
    $scheduled = [regex]::Replace($cleanup.Value, $iteratorPattern, "`t`tdefer iter.Close()`n`t`tif customSCPFixtureAfterIterator != nil {`n`t`t`tcustomSCPFixtureAfterIterator()`n`t`t}")
    $store = $store.Substring(0, $cleanup.Index) + $scheduled + $store.Substring($cleanup.Index + $cleanup.Length)
    $store += "`n// Scheduling-only counterexample hook, present exclusively in the temporary baseline overlay.`nvar customSCPFixtureAfterIterator func()`n"
    $producer = Get-Content -LiteralPath (Join-Path $RepositoryRoot 'spot/custom_scp_fixture_test.go') -Raw
    $exercisePattern = '(?ms)^func exerciseCustomSCPFixture\(t \*testing\.T, s \*CustomSCPStore, now time\.Time, refresh func\(\)\) \{.*\z'
    if ([regex]::Matches($producer, $exercisePattern).Count -ne 1) { throw 'Baseline overlay requires the producer scheduling function at the end of its file.' }
    $exercise = @'
func exerciseCustomSCPFixture(t *testing.T, s *CustomSCPStore, now time.Time, refresh func()) {
	t.Helper()
	customSCPFixtureAfterIterator = refresh
	defer func() { customSCPFixtureAfterIterator = nil }()
	s.cleanup(now)
}
'@
    $producer = [regex]::Replace($producer, $exercisePattern, $exercise)
    $overlayDirectory = Join-Path $FixtureRoot 'overlay'
    $null = [IO.Directory]::CreateDirectory($overlayDirectory)
    $storePath = Join-Path $overlayDirectory 'custom_scp_store.go'
    $producerPath = Join-Path $overlayDirectory 'custom_scp_fixture_test.go'
    $emptyPath = Join-Path $overlayDirectory 'empty.go'
    Set-Content -LiteralPath $storePath -Value $store -Encoding utf8
    Set-Content -LiteralPath $producerPath -Value $producer -Encoding utf8
    Set-Content -LiteralPath $emptyPath -Value "package spot`n" -Encoding utf8
    $replacements = @{
        (Join-Path $RepositoryRoot 'spot/custom_scp_store.go') = $storePath
        (Join-Path $RepositoryRoot 'spot/custom_scp_fixture_test.go') = $producerPath
    }
    foreach ($name in @('custom_scp_cleanup.go', 'custom_scp_cleanup_test.go', 'custom_scp_lifecycle_test.go', 'custom_scp_cleanup_helpers_test.go', 'custom_scp_persistence_bench_test.go')) {
        $path = Join-Path $RepositoryRoot "spot/$name"
        if (Test-Path -LiteralPath $path -PathType Leaf) { $replacements[$path] = $emptyPath }
    }
    $overlayPath = Join-Path $overlayDirectory 'overlay.json'
    @{ Replace = $replacements } | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $overlayPath -Encoding utf8
    return $overlayPath
}

function Invoke-CustomSCPRegression([string]$RepositoryRoot, [bool]$UseBaseline, [bool]$UseRace = $false) {
    if ($PSVersionTable.PSVersion.Major -lt 7 -or -not $IsWindows) { throw 'CustomSCP regression requires PowerShell 7 on Windows.' }
    $root = [IO.Path]::GetFullPath($RepositoryRoot)
    $originalPath = $env:PATH
    $originalDirectory = (Get-Location).Path
    $savedEnvironment = @{}
    foreach ($name in @('GOCLUSTER_CUSTOMSCP_FIXTURE', 'GOCLUSTER_CUSTOMSCP_RUN_ID')) {
        $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
    }
    $lock = $null
    try {
        $workspace = Invoke-CustomSCPCommand 'git' @('rev-parse', '--show-toplevel') $root ''
        if ($workspace.ExitCode -ne 0 -or $workspace.Lines.Count -ne 1 -or [IO.Path]::GetFullPath($workspace.Lines[0]) -ne $root) {
            throw 'CustomSCP regression must run in its owning repository workspace.'
        }
        if ((Get-Content -LiteralPath (Join-Path $root 'go.mod') -TotalCount 1) -ne 'module dxcluster') {
            throw 'CustomSCP regression requires the dxcluster module.'
        }
        $binaryRoot = Join-Path $root '.tmp/windows-tests'
        Assert-CustomSCPOutputPath $root $binaryRoot
        $null = [IO.Directory]::CreateDirectory($binaryRoot)
        $lockPath = Join-Path $binaryRoot 'runner.lock'
        Assert-CustomSCPOutputPath $root $lockPath
        try { $lock = [IO.File]::Open($lockPath, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None) } catch [IO.IOException] {
            throw "Cannot acquire Windows test runner lock: $lockPath. $($_.Exception.Message)"
        }
        $runID = [guid]::NewGuid().ToString('N')
        $fixtureRoot = Join-Path $root ".tmp/customscp-regression/$runID"
        Assert-CustomSCPOutputPath $root $fixtureRoot
        if (Test-Path -LiteralPath $fixtureRoot) { throw 'CustomSCP fixture must be a fresh directory.' }
        $null = [IO.Directory]::CreateDirectory($fixtureRoot)
        Write-Host "CustomSCP fixture evidence: $fixtureRoot; baseline=$UseBaseline"
        $env:GOCLUSTER_CUSTOMSCP_FIXTURE = $fixtureRoot
        $env:GOCLUSTER_CUSTOMSCP_RUN_ID = $runID
        $environment = Invoke-CustomSCPCommand 'go' @('env', '-json', 'GOOS', 'GOARCH', 'GOHOSTARCH', 'GOROOT') $root (Join-Path $fixtureRoot 'go-environment.log')
        if ($environment.ExitCode -ne 0) { throw 'CustomSCP go env failed.' }
        $goEnvironment = $environment.Lines -join "`n" | ConvertFrom-Json
        if ($goEnvironment.GOOS -ne 'windows' -or $goEnvironment.GOARCH -ne $goEnvironment.GOHOSTARCH) {
            throw 'CustomSCP regression requires a native Windows target.'
        }
        $env:PATH = (Join-Path $goEnvironment.GOROOT 'bin') + [IO.Path]::PathSeparator + $originalPath
        $overlay = if ($UseBaseline) { New-CustomSCPBaselineOverlay $root $fixtureRoot } else { '' }
        foreach ($stage in @(
            [pscustomobject]@{ Name = 'producer'; Package = 'dxcluster/spot'; Directory = 'spot'; Test = 'TestCustomSCPCleanupFixtureProducer' },
            [pscustomobject]@{ Name = 'consumer'; Package = 'dxcluster/internal/cluster'; Directory = 'internal/cluster'; Test = 'TestCustomSCPDeliveredOutput' }
        )) {
            $binary = Join-Path $binaryRoot "$($stage.Package)/package.test.exe"
            Assert-CustomSCPOutputPath $root $binary
            $null = [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($binary))
            $buildArguments = @('test', '-c', '-o', $binary)
            if ($UseRace) { $buildArguments += '-race' }
            if ($stage.Name -eq 'producer' -and $overlay) { $buildArguments += "-overlay=$overlay" }
            $buildArguments += $stage.Package
            $build = Invoke-CustomSCPCommand 'go' $buildArguments $root (Join-Path $fixtureRoot "$($stage.Name)-build.log")
            if ($build.ExitCode -ne 0) { throw "CustomSCP $($stage.Name) build failed; no saved binary was run." }
            if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) { throw "CustomSCP $($stage.Name) build produced no executable." }
            $directory = Join-Path $root $stage.Directory
            $list = Invoke-CustomSCPCommand $binary @("-test.list=^$($stage.Test)$") $directory (Join-Path $fixtureRoot "$($stage.Name)-list.log")
            if ($list.ExitCode -ne 0 -or $list.Lines.Count -ne 1 -or $list.Lines[0] -cne $stage.Test) {
                throw "CustomSCP $($stage.Name) test selection did not match exactly $($stage.Test)."
            }
            $run = Invoke-CustomSCPCommand $binary @("-test.run=^$($stage.Test)$", '-test.count=1', '-test.paniconexit0', '-test.timeout=2m', '-test.v') $directory (Join-Path $fixtureRoot "$($stage.Name)-test.log")
            if ($run.ExitCode -ne 0) { throw "CustomSCP $($stage.Name) test failed (exit $($run.ExitCode)); evidence: $fixtureRoot" }
            if ($stage.Name -eq 'producer') {
                $ready = Assert-CustomSCPReady $fixtureRoot $runID
                Write-Host "Producer observed: persisted_observation=$($ready.observation_present); persisted_static=$($ready.static_present); reopened_qualified_support=$($ready.recovered_support)"
            }
        }
        Write-Host "CustomSCP connected regression passed; evidence: $fixtureRoot"
    } finally {
        foreach ($name in $savedEnvironment.Keys) {
            if ($null -eq $savedEnvironment[$name]) {
                Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
            } else { [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process') }
        }
        $env:PATH = $originalPath
        if ($null -ne $lock) { $lock.Dispose() }
        Set-Location -LiteralPath $originalDirectory
    }
}

# Dot-sourcing exposes only these narrow helpers to the script's offline fixtures.
if ($MyInvocation.InvocationName -ne '.') {
    Invoke-CustomSCPRegression ([IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))) $Baseline.IsPresent $Race.IsPresent
}

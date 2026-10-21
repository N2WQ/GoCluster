<#
.SYNOPSIS
  Run Go tests from stable Windows executable paths for firewall approval.
.DESCRIPTION
  Compiles each selected package, then runs its saved binary in the package's
  source directory. Ordinary and race builds reuse the same paths. A shared
  file lock refuses overlapping runner invocations. No firewall rules change.
.PARAMETER Packages
  Repository package selectors. Defaults to ./...; external packages are refused.
.PARAMETER Run
  Optional Go test name regular expression.
.PARAMETER ModuleRoot
  One supported repository module root. Defaults to the first-party root.
.PARAMETER Tags
  Optional comma-separated build tags.
.PARAMETER RequireCases
  Exact Package::Test/subtest identities that must RUN then PASS. Slash-qualified
  filters require explicit identities; preparation only compiles these selections.
.PARAMETER Timeout
  Per-package Go test timeout. Defaults to 10m.
.PARAMETER Race
  Compile with the Go race detector; requires its native Windows prerequisites.
.PARAMETER PrepareOnly
  Build and print executable paths without running tests, for firewall setup.
.NOTES
  Prerequisites: PowerShell 7, native Windows Go toolchain, repository dependencies.
  Side effects: rebuilds ignored .tmp/windows-tests binaries; tests retain their
  normal side effects. Leaves the lock file in place, releasing its handle on exit.
  Safety: does not run stale binaries after failed builds, change GOOS/GOARCH,
  alter firewall policy, or delete previous binaries. Dirty worktrees are allowed.
#>
[CmdletBinding()]
param(
    [ValidateNotNullOrEmpty()][string[]]$Packages = @('./...'),
    [string]$Run = '',
    [ValidateSet('.', 'third_party/go-sqlite3', 'third_party/go-sqlite3/qualification', 'third_party/paho.mqtt.golang')]
    [string]$ModuleRoot = '.',
    [ValidatePattern('^[A-Za-z0-9_.,-]*$')][string]$Tags = '',
    [string[]]$RequireCases = @(),
    [ValidatePattern('^(0|([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+)$')]
    [string]$Timeout = '10m',
    [switch]$Race,
    [switch]$PrepareOnly
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false
if ($PSVersionTable.PSVersion.Major -lt 7 -or -not $IsWindows) {
    throw 'test-windows.ps1 requires PowerShell 7 on Windows.'
}

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$outputRoot = Join-Path $repoRoot '.tmp/windows-tests'
$moduleDirectory = [IO.Path]::GetFullPath((Join-Path $repoRoot $ModuleRoot))
if ($Run.Contains('/') -and $RequireCases.Count -eq 0) { throw 'Slash-qualified filters require exact Package::Test/subtest identities in -RequireCases.' }

function Assert-PlainOutputPath([string]$Path) {
    # Never replace binaries through a junction/symlink, including one above a
    # package directory. All writable output paths are strictly below this root.
    $fullPath = [IO.Path]::GetFullPath($Path)
    if (-not $fullPath.StartsWith($repoRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Output path is outside the repository: $Path"
    }
    $current = $fullPath
    while ($current -ne $repoRoot) {
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
                throw "Output path traverses a junction or symlink: $current"
            }
        }
        $current = [IO.Path]::GetDirectoryName($current)
    }
}

$lock = $null
$originalPath = $env:PATH
$powerShellIdentity = 'GOCLUSTER_TEST_POWERSHELL'
$originalPowerShell = [Environment]::GetEnvironmentVariable($powerShellIdentity, 'Process')
# Windows names are case-insensitive, unlike the environment snapshot's hashtable.
$hadPowerShell = [Environment]::GetEnvironmentVariables('Process').Keys -icontains $powerShellIdentity
if ($moduleDirectory -ne $repoRoot) { Assert-PlainOutputPath $moduleDirectory }
Push-Location -LiteralPath $moduleDirectory
try {
    $runnerImage = [Environment]::ProcessPath
    $expectedImage = Join-Path $PSHOME 'pwsh.exe'
    if ([string]::IsNullOrWhiteSpace($runnerImage) -or -not [IO.Path]::IsPathFullyQualified($runnerImage) -or
        -not [string]::Equals($runnerImage, $expectedImage, [StringComparison]::OrdinalIgnoreCase) -or
        -not (Test-Path -LiteralPath $runnerImage -PathType Leaf)) {
        throw 'Cannot establish the actual PowerShell 7 runner executable.'
    }
    $runnerArchitecture = [Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture
    $hostArchitecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    if ($runnerArchitecture -ne $hostArchitecture) {
        throw "PowerShell runner must be native to Windows (runner=$runnerArchitecture; host=$hostArchitecture)."
    }
    [Environment]::SetEnvironmentVariable($powerShellIdentity, $runnerImage, 'Process')
    Assert-PlainOutputPath $outputRoot
    $null = [IO.Directory]::CreateDirectory($outputRoot)
    $lockPath = Join-Path $outputRoot 'runner.lock'
    Assert-PlainOutputPath $lockPath
    try {
        $lock = [IO.File]::Open($lockPath, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    } catch [IO.IOException] {
        throw "Cannot acquire Windows test runner lock (another run may be active): $lockPath. $($_.Exception.Message)"
    }

    $goEnvironment = & go env -json GOOS GOARCH GOHOSTARCH GOROOT
    if ($LASTEXITCODE -ne 0) { throw 'go env failed.' }
    $goEnvironment = ($goEnvironment -join "`n") | ConvertFrom-Json
    if ($goEnvironment.GOOS -ne 'windows' -or $goEnvironment.GOARCH -ne $goEnvironment.GOHOSTARCH) {
        throw 'Tests require a native Windows target; remove cross-compilation GOOS/GOARCH settings.'
    }
    Write-Host "RUNNER PowerShell=$runnerImage; Windows=$hostArchitecture; PowerShell=$runnerArchitecture; Go=$($goEnvironment.GOARCH)/$($goEnvironment.GOHOSTARCH)"
    # Match go test's toolchain selection for tests that spawn the go command.
    $env:PATH = (Join-Path $goEnvironment.GOROOT 'bin') + [IO.Path]::PathSeparator + $originalPath
    $listArguments = @('list', '-f', '{{.ImportPath}}|{{.Dir}}|{{if or .TestGoFiles .XTestGoFiles}}tests{{end}}|{{if .Module}}{{.Module.Dir}}{{end}}')
    if ($Tags) { $listArguments += @('-tags', $Tags) }
    $rows = @(& go @listArguments @Packages)
    if ($LASTEXITCODE -ne 0) { throw 'go list failed.' }
    if ($rows.Count -eq 0) { throw 'No repository packages matched.' }

    $targets = @{}
    $selected = @(foreach ($row in $rows) {
        $fields = $row.Split('|')
        if ($fields.Count -ne 4) { throw "Unexpected go list output: $row" }
        $importPath, $directory, $hasTests, $listedModule = $fields
        $directory = [IO.Path]::GetFullPath($directory)
        if ($listedModule -ne $moduleDirectory -or ($directory -ne $moduleDirectory -and -not $directory.StartsWith($moduleDirectory + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase))) {
            throw "External package is not supported: $importPath ($directory)"
        }
        if ($directory -ne $repoRoot) { Assert-PlainOutputPath $directory }
        # Full import paths distinguish packages sharing a basename, including
        # the module root. Restrict components to unambiguous Windows names.
        foreach ($component in $importPath.Split('/')) {
            if ($component -notmatch '^[A-Za-z0-9_~.-]+$' -or $component -in @('.', '..') -or
                $component.EndsWith('.') -or $component -match '^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\.|$)') {
                throw "Import path cannot map to a stable Windows directory: $importPath"
            }
        }
        $binary = Join-Path (Join-Path $outputRoot $importPath) 'package.test.exe'
        Assert-PlainOutputPath $binary
        if ($targets.ContainsKey($binary)) { throw "Duplicate executable path: $binary" }
        $targets[$binary] = $true
        [pscustomobject]@{ ImportPath = $importPath; Directory = $directory; Binary = $binary; HasTests = ($hasTests -eq 'tests') }
    })

    $requirements = [Collections.Generic.Dictionary[string,bool]]::new([StringComparer]::Ordinal)
    foreach ($identity in $RequireCases) {
        $parts = $identity.Split('::', [StringSplitOptions]::None)
        if ($parts.Count -ne 2 -or $parts[1] -cnotmatch '^Test[^/]*(/[^/]+)*$' -or $requirements.ContainsKey($identity) -or $parts[0] -cnotin $selected.ImportPath) {
            throw "Invalid, duplicate or unselected required case: $identity"
        }
        $requirements[$identity] = $false
    }
    $executed = 0; $buildOnly = 0; $passedCases = 0; $skippedCases = 0
    foreach ($package in $selected) {
        $null = [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($package.Binary))
        $buildArguments = @('test', '-c', '-o', $package.Binary)
        if ($Race) { $buildArguments += '-race' }
        if ($Tags) { $buildArguments += @('-tags', $Tags) }
        $buildArguments += $package.ImportPath
        Write-Host "BUILD $($package.ImportPath) -> $($package.Binary)"
        & go @buildArguments
        if ($LASTEXITCODE -ne 0) { throw "Test build failed: $($package.ImportPath)" }
        if (-not $package.HasTests) {
            $buildOnly++
            Write-Host "BUILD-ONLY $($package.ImportPath) [no test files; build checked]"
            continue
        }
        if (-not (Test-Path -LiteralPath $package.Binary -PathType Leaf)) {
            throw "Test build produced no executable: $($package.Binary)"
        }

        if ($package.ImportPath -eq 'dxcluster/internal/peerdiag') {
            $companion = Join-Path ([IO.Path]::GetDirectoryName($package.Binary)) 'peerdiag.exe'
            Assert-PlainOutputPath $companion
            Write-Host "COMPANION $companion"
            if ($PrepareOnly) {
                & go build -o $companion ./cmd/peerdiag
                if ($LASTEXITCODE -ne 0) { throw 'Peer diagnostic companion build failed.' }
            }
        }
        if ($PrepareOnly) { continue }

        # test2json executes this saved binary itself. Its marker mode prevents
        # child stdout containing ordinary PASS/RUN-looking text from qualifying.
        $testArguments = @('-test.v=test2json', '-test.count=1', '-test.paniconexit0', "-test.timeout=$Timeout")
        if ($Run) { $testArguments += "-test.run=$Run" }
        Write-Host "RUN $($package.ImportPath)"
        Push-Location -LiteralPath $package.Directory
        try {
            $runs = [Collections.Generic.Dictionary[string,bool]]::new([StringComparer]::Ordinal)
            $passes = [Collections.Generic.Dictionary[string,bool]]::new([StringComparer]::Ordinal); $packageStarted = $false; $packagePassed = $false
            & go tool test2json -t -p $package.ImportPath $package.Binary @testArguments 2>&1 | ForEach-Object {
                try { $event = "$_" | ConvertFrom-Json -ErrorAction Stop } catch { throw "Malformed test event stream: $_" }
                if ($event -isnot [pscustomobject] -or -not $event.PSObject.Properties['Action'] -or $event.Action -isnot [string] -or
                    -not $event.PSObject.Properties['Package'] -or $event.Package -isnot [string] -or $event.Package -cne $package.ImportPath -or
                    $event.Action -cnotin @('start', 'run', 'pause', 'cont', 'pass', 'bench', 'fail', 'skip', 'output')) {
                    throw 'Malformed test event identity or action.'
                }
                $name = if ($event.PSObject.Properties['Test']) { $event.Test } else { '' }
                if ($name -isnot [string]) { throw 'Malformed test event name.' }
                if ($event.Action -ceq 'start') { if ($packageStarted) { throw 'Duplicate package start.' }; $packageStarted = $true }
                if (-not $packageStarted -or $packagePassed) { throw 'Test event outside package execution.' }
                if ($event.Action -ceq 'output') { Write-Host $event.Output -NoNewline }
                if ($name -and $event.Action -ceq 'run') {
                    if ($runs.ContainsKey($name)) { throw "Duplicate case RUN: $name" }; $runs[$name] = $true
                }
                if ($name -and $event.Action -ceq 'pass') {
                    if (-not $runs.ContainsKey($name) -or $passes.ContainsKey($name)) { throw "Case PASS without unique RUN: $name" }
                    $passes[$name] = $true; $passedCases++
                    $identity = "$($package.ImportPath)::$name"
                    if ($requirements.ContainsKey($identity)) { $requirements[$identity] = $true }
                }
                if ($name -and $event.Action -ceq 'skip') { $skippedCases++ }
                if (-not $name -and $event.Action -ceq 'pass') { $packagePassed = $true }
            }
            if ($LASTEXITCODE -ne 0 -or -not $packagePassed) { throw "Tests failed: $($package.ImportPath) (exit $LASTEXITCODE)" }
            $executed++
        } finally { Pop-Location }

    }
    if (-not $PrepareOnly) {
        if ($Run -and $passedCases -eq 0) { throw 'Filtered execution passed no cases across selected packages.' }
        foreach ($identity in $requirements.Keys) { if (-not $requirements[$identity]) { throw "Required case did not RUN then PASS: $identity" } }
    }
    Write-Host "Windows tests complete: executed=$executed; build-only=$buildOnly; passed-cases=$passedCases; skipped-cases=$skippedCases; $($selected.Count) packages; prepare-only=$($PrepareOnly.IsPresent); race=$($Race.IsPresent)."
} finally {
    # PowerShell binds $null as an empty string; NullString removes an absent key.
    [Environment]::SetEnvironmentVariable($powerShellIdentity, $(if ($hadPowerShell) { $originalPowerShell } else { [NullString]::Value }), 'Process')
    $env:PATH = $originalPath
    if ($null -ne $lock) { $lock.Dispose() }
    Pop-Location
}

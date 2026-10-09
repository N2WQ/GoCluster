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
Push-Location -LiteralPath $repoRoot
try {
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
    # Match go test's toolchain selection for tests that spawn the go command.
    $env:PATH = (Join-Path $goEnvironment.GOROOT 'bin') + [IO.Path]::PathSeparator + $originalPath
    $rows = @(& go list -f '{{.ImportPath}}|{{.Dir}}|{{if or .TestGoFiles .XTestGoFiles}}tests{{end}}' @Packages)
    if ($LASTEXITCODE -ne 0) { throw 'go list failed.' }
    if ($rows.Count -eq 0) { throw 'No repository packages matched.' }

    $targets = @{}
    $selected = @(foreach ($row in $rows) {
        $fields = $row.Split('|')
        if ($fields.Count -ne 3) { throw "Unexpected go list output: $row" }
        $importPath, $directory, $hasTests = $fields
        $directory = [IO.Path]::GetFullPath($directory)
        if ($directory -ne $repoRoot -and -not $directory.StartsWith($repoRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
            throw "External package is not supported: $importPath ($directory)"
        }
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

    foreach ($package in $selected) {
        $null = [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($package.Binary))
        $buildArguments = @('test', '-c', '-o', $package.Binary)
        if ($Race) { $buildArguments += '-race' }
        $buildArguments += $package.ImportPath
        Write-Host "BUILD $($package.ImportPath) -> $($package.Binary)"
        & go @buildArguments
        if ($LASTEXITCODE -ne 0) { throw "Test build failed: $($package.ImportPath)" }
        if (-not $package.HasTests) {
            Write-Host "SKIP $($package.ImportPath) [no test files; build checked]"
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

        # Preserve go test's safeguard against tests silently exiting early.
        $testArguments = @('-test.count=1', '-test.paniconexit0', "-test.timeout=$Timeout")
        if ($Run) { $testArguments += "-test.run=$Run" }
        Write-Host "RUN $($package.ImportPath)"
        Push-Location -LiteralPath $package.Directory
        try {
            & $package.Binary @testArguments
            if ($LASTEXITCODE -ne 0) { throw "Tests failed: $($package.ImportPath) (exit $LASTEXITCODE)" }
        } finally {
            Pop-Location
        }
    }
    Write-Host "Windows tests complete: $($selected.Count) packages; prepare-only=$($PrepareOnly.IsPresent); race=$($Race.IsPresent)."
} finally {
    $env:PATH = $originalPath
    if ($null -ne $lock) { $lock.Dispose() }
    Pop-Location
}

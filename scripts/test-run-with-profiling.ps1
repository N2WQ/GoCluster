<#
.SYNOPSIS
  Verifies the profiling controller starts independently from its caller.
.DESCRIPTION
  Exercises the exact Start-ProfilerController function from
  run-with-profiling.ps1 with a child fixture, then verifies caller exit does
  not retire that child and startup failures remain observable.
.NOTES
  This fixture starts only temporary PowerShell processes and files. It does
  not launch GoCluster or contact the configured pprof endpoint.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$scriptPath = Join-Path $PSScriptRoot 'run-with-profiling.ps1'
$parseErrors = $null
$scriptAst = [System.Management.Automation.Language.Parser]::ParseFile($scriptPath, [ref]$null, [ref]$parseErrors)
if ($parseErrors.Count -gt 0) {
    throw "Profiling script has PowerShell parse errors: $($parseErrors -join '; ')"
}

$functionAst = $scriptAst.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Start-ProfilerController'
}, $true)
if (-not $functionAst) {
    throw 'Start-ProfilerController was not found in the profiling script.'
}
$source = Get-Content -LiteralPath $scriptPath -Raw
$dispatchOffset = $source.IndexOf('if (-not $Controller)', [StringComparison]::Ordinal)
$launchCallOffset = $source.IndexOf('Start-ProfilerController -ScriptPath $PSCommandPath', [StringComparison]::Ordinal)
$clusterLaunchOffset = $source.IndexOf('# Start the cluster through a wrapper', [StringComparison]::Ordinal)
if ($dispatchOffset -lt 0 -or $launchCallOffset -le $dispatchOffset -or $launchCallOffset -ge $clusterLaunchOffset) {
    throw 'The controller dispatch must happen before cluster startup.'
}
if (-not $source.Contains('Failed to start profiling controller window:')) {
    throw 'Controller launch failures must be reported by the entry point.'
}

$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("gocluster-profile-controller-test-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot | Out-Null
try {
    $helperPath = Join-Path $tempRoot 'controller-helper.ps1'
    $childDirectory = Join-Path $tempRoot 'child script'
    New-Item -ItemType Directory -Path $childDirectory | Out-Null
    $childPath = Join-Path $childDirectory 'controller-child.ps1'
    $parentPath = Join-Path $tempRoot 'controller-parent.ps1'
    $readyPath = Join-Path $tempRoot 'child-ready.txt'
    $releasePath = Join-Path $tempRoot 'release-child'
    $donePath = Join-Path $tempRoot 'child-done.txt'
    $parentExitPath = Join-Path $tempRoot 'parent-exit.txt'
    $timestamp = '20261011-153000'

    Set-Content -LiteralPath $helperPath -Value $functionAst.Extent.Text -Encoding UTF8
    @"
param([switch]`$Controller, [string]`$LaunchTimestamp)
Set-Content -LiteralPath '$readyPath' -Value "`$PID|`$Controller|`$LaunchTimestamp"
while (-not (Test-Path -LiteralPath '$releasePath')) { Start-Sleep -Milliseconds 50 }
Set-Content -LiteralPath '$donePath' -Value "`$PID|`$Controller|`$LaunchTimestamp"
"@ | Set-Content -LiteralPath $childPath -Encoding UTF8
    @"
`$ErrorActionPreference = 'Stop'
. '$helperPath'
`$started = Start-ProfilerController -ScriptPath '$childPath' -Timestamp '$timestamp' -WorkingDirectory '$repoRoot' -WindowStyle Hidden
Start-Sleep -Milliseconds 250
`$started.Refresh()
Set-Content -LiteralPath '$parentExitPath' -Value ('{0}|{1}' -f `$started.Id, `$started.HasExited)
"@ | Set-Content -LiteralPath $parentPath -Encoding UTF8

    $engineName = if ($PSVersionTable.PSEdition -eq 'Core') { 'pwsh.exe' } else { 'powershell.exe' }
    $enginePath = Join-Path $PSHOME $engineName
    $parentArgs = '-NoLogo -NoProfile -ExecutionPolicy Bypass -File "{0}"' -f $parentPath
    $parent = Start-Process -FilePath $enginePath -ArgumentList $parentArgs -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru
    if (-not $parent.WaitForExit(10000)) { throw 'The fixture launcher did not return within 10 seconds.' }
    if ($parent.ExitCode -ne 0 -or -not (Test-Path -LiteralPath $parentExitPath)) {
        throw "The fixture launcher failed (exit=$($parent.ExitCode))."
    }
    $readyDeadline = [DateTime]::UtcNow.AddSeconds(10)
    while (-not (Test-Path -LiteralPath $readyPath) -and [DateTime]::UtcNow -lt $readyDeadline) {
        Start-Sleep -Milliseconds 50
    }
    if (-not (Test-Path -LiteralPath $readyPath)) {
        $reported = (Get-Content -LiteralPath $parentExitPath -Raw).Trim()
        throw "The detached controller did not start (Start-Process PID/Exited=$reported)."
    }
    $ready = (Get-Content -LiteralPath $readyPath -Raw).Trim().Split('|')
    $childProcess = Get-Process -Id ([int]$ready[0]) -ErrorAction SilentlyContinue
    if (-not $childProcess) { throw 'The controller child exited when its caller returned.' }
    if ($ready[1] -ne 'True' -or $ready[2] -ne $timestamp) {
        throw "The controller arguments were not preserved: $($ready -join '|')"
    }

    New-Item -ItemType File -Path $releasePath | Out-Null
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    while (-not (Test-Path -LiteralPath $donePath) -and [DateTime]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 50
    }
    if (-not (Test-Path -LiteralPath $donePath)) { throw 'The detached controller did not complete after caller exit.' }
    if (-not $childProcess.WaitForExit(5000)) { throw 'The detached controller did not exit after completing its fixture.' }

    $failedAsExpected = $false
    try {
        Start-ProfilerController -ScriptPath $childPath -Timestamp $timestamp -WorkingDirectory $tempRoot -PowerShellPath (Join-Path $tempRoot 'missing-powershell.exe') -WindowStyle Hidden | Out-Null
    } catch {
        $failedAsExpected = $true
    }
    if (-not $failedAsExpected) { throw 'A missing PowerShell executable did not fail controller startup.' }

    Write-Host 'PASS: controller remains alive after the launcher exits, receives its mode and run ID, and reports process-start failure.'
} finally {
    if (Test-Path -LiteralPath $tempRoot) {
        if (-not (Test-Path -LiteralPath $releasePath)) {
            New-Item -ItemType File -Path $releasePath -Force | Out-Null
        }
        if (Test-Path -LiteralPath $readyPath) {
            $fixturePidText = (Get-Content -LiteralPath $readyPath -Raw).Trim().Split('|')[0]
            $fixtureProcess = Get-Process -Id ([int]$fixturePidText) -ErrorAction SilentlyContinue
            if ($fixtureProcess -and -not $fixtureProcess.WaitForExit(5000)) {
                $fixtureCim = Get-CimInstance -ClassName Win32_Process -Filter "ProcessId=$($fixtureProcess.Id)" -ErrorAction SilentlyContinue
                if ($fixtureCim -and $fixtureCim.CommandLine.Contains($childPath)) {
                    Stop-Process -Id $fixtureProcess.Id -Force
                    $fixtureProcess.WaitForExit(5000) | Out-Null
                } else {
                    throw "Refusing to stop process $($fixtureProcess.Id) because its command line does not match the fixture."
                }
            }
        }
        $tempParent = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
        $resolvedTempRoot = [IO.Path]::GetFullPath($tempRoot)
        if (-not $resolvedTempRoot.StartsWith($tempParent, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Refusing to remove test output outside the temp directory: $resolvedTempRoot"
        }
        Remove-Item -LiteralPath $tempRoot -Recurse -Force
    }
}

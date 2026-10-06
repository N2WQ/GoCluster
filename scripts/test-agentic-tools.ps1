<#
.SYNOPSIS
    Exercise verifier failures, quiet behavior, host PATH handling and restoration.
#>
param([string]$Case = '')
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$verifier = Join-Path $PSScriptRoot 'verify-agentic-tools.ps1'

if ($Case) {
    if ($Case -eq 'windows-null-locations') {
        Remove-Item Env:LOCALAPPDATA, Env:ProgramFiles -ErrorAction SilentlyContinue
    }
    $pathVariable = if ([Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT) { 'Path' } else { 'PATH' }
    $before = [Environment]::GetEnvironmentVariable($pathVariable, 'Process')
    $global:FixtureProbeCount = 0
    $global:FixtureSeenPath = $null
    function ProbeSuccess {
        $global:FixtureProbeCount++
        $global:FixtureSeenPath = [Environment]::GetEnvironmentVariable($pathVariable, 'Process')
        $global:LASTEXITCODE = 0
        'fixture version 1'
    }
    function ProbeFailure { $global:LASTEXITCODE = 23; 'fixture probe failed' }
    function ProbeException { throw 'fixture probe exception' }
    $successCommand = Microsoft.PowerShell.Core\Get-Command ProbeSuccess
    $failureCommand = Microsoft.PowerShell.Core\Get-Command ProbeFailure
    $exceptionCommand = Microsoft.PowerShell.Core\Get-Command ProbeException
    function Get-Command {
        param([string]$Name, $ErrorAction)
        if ($Case -eq 'discovery-exception' -and $Name -eq 'go') { throw 'fixture discovery exception' }
        if ($Case -eq 'missing-required' -and $Name -eq 'go') { return $null }
        if ($Case -eq 'missing-optional' -and $Name -eq 'semgrep') { return $null }
        if ($Case -in @('failed-required','quiet-failed-required') -and $Name -eq 'go') { return $failureCommand }
        if ($Case -eq 'probe-exception' -and $Name -eq 'go') { return $exceptionCommand }
        if ($Case -eq 'failed-optional' -and $Name -eq 'semgrep') { return $failureCommand }
        return $successCommand
    }
    $caught = $null
    try {
        $output = (& $verifier -Quiet:($Case -like 'quiet-*') 6>&1 | Out-String)
        $actual = $LASTEXITCODE
    } catch { $caught = $_ }
    if ([Environment]::GetEnvironmentVariable($pathVariable, 'Process') -cne $before) { throw "$Case changed caller PATH" }
    if ($Case -eq 'discovery-exception') {
        if (-not $caught -or -not $caught.ToString().Contains('fixture discovery exception')) { throw 'Expected discovery exception' }
    } else {
        if ($caught) { throw $caught }
        $expected = if ($Case -in @('missing-required','failed-required','quiet-failed-required','probe-exception')) { 1 } else { 0 }
        if ($actual -ne $expected) { throw "$Case expected exit $expected, got $actual`n$output" }
        $text = switch ($Case) {
            'missing-required' { 'FAIL  go (missing)' }
            'missing-optional' { 'WARN  semgrep (missing)' }
            'failed-required' { 'FAIL  go (version probe failed with exit code 23)' }
            'quiet-failed-required' { 'FAIL  go (version probe failed with exit code 23)' }
            'failed-optional' { 'WARN  semgrep (version probe failed with exit code 23)' }
            'probe-exception' { 'FAIL  go (version probe failed: fixture probe exception)' }
            default { 'PASS required agentic workflow tools are available.' }
        }
        if (-not $output.Contains($text)) { throw "$Case missing '$text'`n$output" }
        if ($Case -eq 'success' -and -not $output.Contains('callgraph - presence only')) { throw 'Presence-only checks must be explicit' }
        if ($Case -eq 'quiet-success' -and ($global:FixtureProbeCount -ne 20 -or $output.Contains('PASS  go -'))) { throw "Quiet skipped probes or displayed successes: $global:FixtureProbeCount" }
        if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT -and $global:FixtureSeenPath -cne $before) { throw 'Non-Windows PATH was changed during probes' }
    }
    Write-Host "PASS $Case"
    exit 0
}

$engine = (Get-Process -Id $PID).Path
foreach ($name in @('success','quiet-success','missing-required','failed-required','quiet-failed-required','probe-exception','missing-optional','failed-optional','discovery-exception','windows-null-locations')) {
    & $engine -NoProfile -File $PSCommandPath -Case $name
    if ($LASTEXITCODE -ne 0) { throw "Fixture $name failed" }
}

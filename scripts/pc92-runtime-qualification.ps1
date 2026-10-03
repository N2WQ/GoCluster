[CmdletBinding()]
param(
    [ValidateSet('preflight', 'diagnostic-full', 'warm-diagnostic', 'q1', 'q2', 'q3', 'shipped-q1')][string]$Profile = 'preflight',
    [switch]$CPUProfile,
    [string]$OutputDirectory = ''
)
. (Join-Path $PSScriptRoot 'pc92-qualification-run.ps1')
Invoke-PC92QualificationRun -Family runtime -Profile $Profile -OutputDirectory $OutputDirectory -CPUProfile:$CPUProfile

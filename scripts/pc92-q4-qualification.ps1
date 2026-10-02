[CmdletBinding()]
param(
    [ValidateSet('preflight-a', 'preflight-b', 'a', 'b')][string]$Profile = 'preflight-a',
    [string]$OutputDirectory = ''
)
. (Join-Path $PSScriptRoot 'pc92-qualification-run.ps1')
Invoke-PC92QualificationRun -Family q4 -Profile $Profile -OutputDirectory $OutputDirectory

[CmdletBinding()]
param(
    [ValidateSet('cache-memory', 'cache-sustained')][string]$Profile = 'cache-memory',
    [string]$OutputDirectory = ''
)
. (Join-Path $PSScriptRoot 'pc92-qualification-run.ps1')
Invoke-PC92QualificationRun -Family cache -Profile $Profile -OutputDirectory $OutputDirectory

[CmdletBinding()]
param(
    [ValidateSet('preflight', 'qualification')][string]$Profile = 'preflight',
    [string]$OutputDirectory = ''
)
. (Join-Path $PSScriptRoot 'pc92-qualification-run.ps1')
Invoke-PC92QualificationRun -Family q5 -Profile $Profile -OutputDirectory $OutputDirectory

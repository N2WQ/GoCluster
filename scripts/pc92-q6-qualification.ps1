[CmdletBinding()]
param(
    [ValidateSet('preflight', 'qualification')][string]$Profile = 'preflight',
    [Parameter(Mandatory = $true)][string]$DXSpiderRoot,
    [Parameter(Mandatory = $true)][string]$PerlPath,
    [string]$PerlLibrary = '',
    [string]$PerlDLLDirectory = '',
    [string]$OutputDirectory = ''
)
. (Join-Path $PSScriptRoot 'pc92-qualification-run.ps1')
Invoke-PC92QualificationRun -Family q6 -Profile $Profile -OutputDirectory $OutputDirectory -DXSpiderRoot $DXSpiderRoot -PerlPath $PerlPath -PerlLibrary $PerlLibrary -PerlDLLDirectory $PerlDLLDirectory

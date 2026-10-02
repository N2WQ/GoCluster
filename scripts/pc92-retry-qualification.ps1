[CmdletBinding()]
param(
    [ValidateSet('preflight', 'qualification')][string]$Profile = 'preflight',
    [string]$OutputDirectory = ''
)
# Retained-binary retry correction evidence. Overall PC18/PC92 acceptance still
# requires the separate receiver, workload and aggregate allocation proofs.
. (Join-Path $PSScriptRoot 'pc92-qualification-run.ps1')
Invoke-PC92QualificationRun -Family retry -Profile $Profile -OutputDirectory $OutputDirectory

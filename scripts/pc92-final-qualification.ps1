<#
.SYNOPSIS
Reconcile the full PC18/PC92 final-source evidence bundle.
.DESCRIPTION
Accepts only original full profiles plus a source-bound engineering review of
all corrections, allocation partitions and required checks. The review remains
a human engineering attestation; hashing cannot establish its scientific truth.
Never use synthetic fixture records as execution evidence.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$BundlePath,
    [Parameter(Mandatory)][string]$OutputDirectory
)
. (Join-Path $PSScriptRoot 'pc92-qualification-finalize.ps1')
Invoke-PC92FinalQualification -BundlePath $BundlePath -OutputDirectory $OutputDirectory

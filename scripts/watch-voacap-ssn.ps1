<#
.SYNOPSIS
  Watch NOAA SSN fetches and the VOACAP EWMA SSN generation.

.DESCRIPTION
  Runs the repo-local lightweight SSN watcher in cmd/voacap_sunspot_watch.
  The watcher polls NOAA SWPC sunspot_report.json, prints the latest raw SSN,
  rounded EWMA SSN, recompute delta, and recompute marker, and stores fetch
  validators/state under .tmp by default. It does not run VOACAP forecasts.

.PARAMETER Interval
  Fetch interval passed to the watcher. Default: 30m.

.PARAMETER HalfLife
  EWMA half-life passed to the watcher. Default: 8h.

.PARAMETER Threshold
  Relative recompute threshold passed to the watcher. Default: 0.12.

.PARAMETER Timeout
  HTTP request timeout passed to the watcher. Default: 30s.

.PARAMETER StatePath
  State file path for ETag/Last-Modified and EWMA state. Default:
  .tmp/voacap-ssn-watch-state.json.

.PARAMETER Once
  Fetch once and exit.

.NOTES
  Prerequisites: Go toolchain and network access to NOAA SWPC.
  Side effects: creates or updates the StatePath file, normally under .tmp.
  Safety: local observation tool only; server runtime config and VOACAP cache
  behavior are unchanged.
#>

[CmdletBinding()]
param(
    [string]$Interval = "30m",
    [string]$HalfLife = "8h",
    [double]$Threshold = 0.12,
    [string]$Timeout = "30s",
    [string]$StatePath = ".tmp/voacap-ssn-watch-state.json",
    [switch]$Once
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $PSCommandPath
$repoRoot = Resolve-Path (Join-Path $scriptDir "..")

$args = @(
    "run",
    "./cmd/voacap_sunspot_watch",
    "-interval", $Interval,
    "-half-life", $HalfLife,
    "-threshold", $Threshold,
    "-timeout", $Timeout,
    "-state", $StatePath
)

if ($Once) {
    $args += "-once"
}

Push-Location $repoRoot
try {
    & go @args
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}

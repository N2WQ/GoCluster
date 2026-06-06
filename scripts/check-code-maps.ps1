<#
.SYNOPSIS
	Check whether checked-in GoCluster code maps are fresh.

.DESCRIPTION
	Runs the deterministic Go code-map checker. The checker regenerates map
	content in memory and compares it with checked-in Markdown files under
	docs/code-maps. It does not modify repository files.

.PARAMETER All
	Check all maps declared in docs/code-maps/manifest.json.

.PARAMETER Map
	Check one map by id.

.NOTES
	Prerequisites: Go toolchain and the repository checkout.
	Side effects: none; this script is read-only.
	Safety: stale maps fail the script and should be regenerated with
	scripts/update-code-maps.ps1, reviewed, and committed intentionally.
#>

Param(
    [switch]$All,
    [string]$Map = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-RepoRoot {
    $root = & git rev-parse --show-toplevel
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($root)) {
        throw "Unable to resolve repository root with git."
    }
    return $root.Trim()
}

if ($All -and -not [string]::IsNullOrWhiteSpace($Map)) {
    throw "Use either -All or -Map, not both."
}

$selector = @()
if ($All) {
    $selector += "-all"
} elseif (-not [string]::IsNullOrWhiteSpace($Map)) {
    $selector += @("-map", $Map)
} else {
    $selector += "-all"
}

$repoRoot = Resolve-RepoRoot
Push-Location $repoRoot
try {
    $args = @("run", "./cmd/codemap", "check") + $selector
    & go @args
    if ($LASTEXITCODE -ne 0) {
        throw "go $($args -join ' ') failed."
    }
}
finally {
    Pop-Location
}

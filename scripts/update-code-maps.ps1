<#
.SYNOPSIS
	Regenerate checked-in GoCluster code maps.

.DESCRIPTION
	Runs the deterministic Go code-map generator and writes generated Markdown
	files under docs/code-maps. Code maps are generated from current Go package
	metadata and ADR records; do not edit generated map files by hand.

.PARAMETER All
	Regenerate all maps declared in docs/code-maps/manifest.json.

.PARAMETER Map
	Regenerate one map by id.

.NOTES
	Prerequisites: Go toolchain and the repository checkout.
	Side effects: updates generated Markdown files under docs/code-maps.
	Safety: review and commit generated diffs intentionally before release.
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
    $args = @("run", "./cmd/codemap", "generate") + $selector
    & go @args
    if ($LASTEXITCODE -ne 0) {
        throw "go $($args -join ' ') failed."
    }
}
finally {
    Pop-Location
}

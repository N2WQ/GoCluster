Param(
    [string[]]$Skills = @("gh-fix-ci", "sentry"),
    [switch]$Mirror
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-CodexHome {
    if ($env:CODEX_HOME -and $env:CODEX_HOME.Trim() -ne "") {
        return $env:CODEX_HOME
    }
    if ($env:USERPROFILE -and $env:USERPROFILE.Trim() -ne "") {
        return (Join-Path $env:USERPROFILE ".codex")
    }
    throw "Unable to resolve Codex home. Set CODEX_HOME or USERPROFILE."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$sourceRoot = Join-Path $repoRoot "codex-skills"
if (-not (Test-Path -LiteralPath $sourceRoot)) {
    throw "Missing source directory: $sourceRoot"
}

$codexHome = Get-CodexHome
$destRoot = Join-Path $codexHome "skills"
New-Item -ItemType Directory -Path $destRoot -Force | Out-Null

$installed = New-Object System.Collections.Generic.List[string]
foreach ($skill in $Skills) {
    $sourceDir = Join-Path $sourceRoot $skill
    if (-not (Test-Path -LiteralPath $sourceDir)) {
        throw "Skill '$skill' not found in repo source: $sourceDir"
    }

    $destDir = Join-Path $destRoot $skill
    if ($Mirror -and (Test-Path -LiteralPath $destDir)) {
        Remove-Item -LiteralPath $destDir -Recurse -Force
    }

    New-Item -ItemType Directory -Path $destDir -Force | Out-Null
    Copy-Item -Path (Join-Path $sourceDir "*") -Destination $destDir -Recurse -Force
    $installed.Add($skill) | Out-Null
}

Write-Host "Installed/updated skills to ${destRoot}:"
foreach ($skill in $installed) {
    Write-Host " - $skill"
}
if ($Mirror) {
    Write-Host "Mode: mirror (skill directories replaced before copy)."
} else {
    Write-Host "Mode: in-place update."
}

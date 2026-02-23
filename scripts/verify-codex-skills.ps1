Param(
    [string[]]$Skills = @("gh-fix-ci", "sentry")
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

function Get-RelativePath {
    Param(
        [string]$BasePath,
        [string]$ChildPath
    )
    $baseUri = New-Object System.Uri(($BasePath.TrimEnd('\') + '\'))
    $childUri = New-Object System.Uri($ChildPath)
    $relativeUri = $baseUri.MakeRelativeUri($childUri)
    return [System.Uri]::UnescapeDataString($relativeUri.ToString()).Replace('/', '\')
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$sourceRoot = Join-Path $repoRoot "codex-skills"
if (-not (Test-Path -LiteralPath $sourceRoot)) {
    throw "Missing source directory: $sourceRoot"
}

$destRoot = Join-Path (Get-CodexHome) "skills"
$hasFailures = $false

foreach ($skill in $Skills) {
    $sourceDir = Join-Path $sourceRoot $skill
    $destDir = Join-Path $destRoot $skill

    if (-not (Test-Path -LiteralPath $sourceDir)) {
        Write-Host "FAIL [$skill] missing repo source: $sourceDir"
        $hasFailures = $true
        continue
    }
    if (-not (Test-Path -LiteralPath $destDir)) {
        Write-Host "FAIL [$skill] not installed at: $destDir"
        $hasFailures = $true
        continue
    }

    $sourceFiles = Get-ChildItem -LiteralPath $sourceDir -Recurse -File
    $skillFailure = $false
    foreach ($sourceFile in $sourceFiles) {
        $relPath = Get-RelativePath -BasePath $sourceDir -ChildPath $sourceFile.FullName
        $destFile = Join-Path $destDir $relPath
        if (-not (Test-Path -LiteralPath $destFile)) {
            Write-Host "FAIL [$skill] missing file: $relPath"
            $skillFailure = $true
            continue
        }

        $sourceHash = (Get-FileHash -LiteralPath $sourceFile.FullName -Algorithm SHA256).Hash
        $destHash = (Get-FileHash -LiteralPath $destFile -Algorithm SHA256).Hash
        if ($sourceHash -ne $destHash) {
            Write-Host "FAIL [$skill] hash mismatch: $relPath"
            $skillFailure = $true
        }
    }

    if ($skillFailure) {
        $hasFailures = $true
    } else {
        Write-Host "PASS [$skill] installed and in sync."
    }
}

if ($hasFailures) {
    exit 1
}

Write-Host "PASS all requested skills verified."
exit 0

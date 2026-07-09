<#
.SYNOPSIS
  Test the Fable workflow-contract checker against positive and negative
  fixtures.

.DESCRIPTION
  Copies the current mechanical contract surface into a temporary directory,
  verifies the positive case, then proves missing markers, mismatched
  validation labels, missing Document Map targets, missing SELF-AUDIT IDs,
  and duplicate SELF-AUDIT IDs all fail. Fable-native counterpart to
  scripts/test-workflow-contract.ps1 (Codex).

.NOTES
  Prerequisites: PowerShell 7 and scripts/check-fable-workflow-contract.ps1.
  Side effects: creates and removes a temporary fixture directory.
  Safety: never modifies the repository fixture source.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$checker = Join-Path $PSScriptRoot "check-fable-workflow-contract.ps1"
$engine = (Get-Process -Id $PID).Path
$fixtureRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("gocluster-fable-workflow-contract-" + [guid]::NewGuid().ToString("N"))

function Copy-RepoFile {
  Param([string]$RelativePath)
  $source = Join-Path $repoRoot $RelativePath
  $target = Join-Path $fixtureRoot $RelativePath
  $parent = Split-Path -Parent $target
  New-Item -ItemType Directory -Force -Path $parent | Out-Null
  Copy-Item -LiteralPath $source -Destination $target
}

function New-RepoDir {
  Param([string]$RelativePath)
  New-Item -ItemType Directory -Force -Path (Join-Path $fixtureRoot $RelativePath) | Out-Null
}

function Invoke-Checker {
  Param([int]$ExpectedExit, [string]$Label)
  & $engine -NoProfile -File $checker -RepoRoot $fixtureRoot *> $null
  $actual = $LASTEXITCODE
  if ($actual -ne $ExpectedExit) {
    throw "$Label expected exit $ExpectedExit, got $actual"
  }
  Write-Host "PASS $Label"
}

try {
  $requiredFiles = @(
    "AGENTS.md",
    "CLAUDE.md",
    "docs/WORKING_WITH_CODEX.md",
    "docs/agent-lessons/README.md",
    "docs/code-quality.md",
    "docs/decision-log.md",
    "docs/decision-memory.md",
    "docs/dev-runbook.md",
    "docs/domain-contract.md",
    "docs/fable-review-checklist.md",
    "docs/fable-validation.md",
    "docs/fable-workflow.md",
    "docs/templates/fable-non-trivial-change-template.md",
    "docs/troubleshooting-log.md"
  )
  foreach ($file in $requiredFiles) {
    Copy-RepoFile $file
  }
  foreach ($dir in @("customgpt", "docs/decisions", "docs/troubleshooting")) {
    New-RepoDir $dir
  }

  Invoke-Checker 0 "positive fixture"

  $templatePath = Join-Path $fixtureRoot "docs/templates/fable-non-trivial-change-template.md"
  $originalTemplate = Get-Content -LiteralPath $templatePath -Raw
  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("### TRACEABILITY", "### REMOVED_TRACEABILITY")) -NoNewline
  Invoke-Checker 1 "missing marker"

  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("Validation Score: X/6", "Validation Result: X/6")) -NoNewline
  Invoke-Checker 1 "mismatched validation label"

  Set-Content -LiteralPath $templatePath -Value $originalTemplate -NoNewline
  Remove-Item -LiteralPath (Join-Path $fixtureRoot "docs/code-quality.md")
  Invoke-Checker 1 "missing Document Map target"

  Copy-RepoFile "docs/code-quality.md"
  $reviewPath = Join-Path $fixtureRoot "docs/fable-review-checklist.md"
  $originalReview = Get-Content -LiteralPath $reviewPath -Raw
  Set-Content -LiteralPath $reviewPath -Value ($originalReview.Replace("- SA15 Validation block completeness", "- REMOVED_SA15 Validation block completeness")) -NoNewline
  Invoke-Checker 1 "missing SELF-AUDIT ID"

  Set-Content -LiteralPath $reviewPath -Value ($originalReview + "`n- SA15 Duplicate validation block completeness`n") -NoNewline
  Invoke-Checker 1 "duplicate SELF-AUDIT ID"

  Set-Content -LiteralPath $reviewPath -Value $originalReview -NoNewline

  $templatePath2 = Join-Path $fixtureRoot "docs/templates/fable-non-trivial-change-template.md"
  $currentTemplate = Get-Content -LiteralPath $templatePath2 -Raw
  Set-Content -LiteralPath $templatePath2 -Value ($currentTemplate + "`n### GATE`n") -NoNewline
  Invoke-Checker 1 "GATE marker count regression (3 instead of 2)"
  Set-Content -LiteralPath $templatePath2 -Value $currentTemplate -NoNewline

  & $engine -NoProfile -File $checker -RepoRoot $repoRoot
  if ($LASTEXITCODE -ne 0) {
    throw "current repository contract failed checker"
  }
  Write-Host "PASS current repository contract"
  Write-Host "PASS all Fable workflow-contract checker tests passed."
} finally {
  Remove-Item -LiteralPath $fixtureRoot -Recurse -Force -ErrorAction SilentlyContinue
}

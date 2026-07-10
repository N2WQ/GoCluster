<#
.SYNOPSIS
  Test the workflow-contract checker against positive and negative fixtures.

.DESCRIPTION
  Copies the current mechanical contract surface into a temporary directory,
  verifies the positive case, then proves missing markers, changed validation
  labels, and missing Document Map targets fail.

.NOTES
  Prerequisites: PowerShell 7 and scripts/check-workflow-contract.ps1.
  Side effects: creates and removes a temporary fixture directory.
  Safety: never modifies the repository fixture source.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$checker = Join-Path $PSScriptRoot "check-workflow-contract.ps1"
$engine = (Get-Process -Id $PID).Path
$fixtureRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("gocluster-workflow-contract-" + [guid]::NewGuid().ToString("N"))

function Copy-RepoFile {
  Param([string]$RelativePath)
  $source = Join-Path $repoRoot $RelativePath
  $target = Join-Path $fixtureRoot $RelativePath
  $parent = Split-Path -Parent $target
  New-Item -ItemType Directory -Force -Path $parent | Out-Null
  Copy-Item -LiteralPath $source -Destination $target
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
    "VALIDATION.md",
    "docs/change-workflow.md",
    "docs/code-quality.md",
    "docs/review-checklist.md",
    "docs/dev-runbook.md",
    "docs/WORKING_WITH_CODEX.md",
    "docs/domain-contract.md",
    "docs/decision-memory.md",
    "docs/agent-lessons/README.md",
    "docs/templates/non-trivial-change-template.md",
    "docs/templates/adr-template.md",
    "docs/troubleshooting/TSR-TEMPLATE.md"
  )
  foreach ($file in $requiredFiles) {
    Copy-RepoFile $file
  }

  Invoke-Checker 0 "positive fixture"

  $templatePath = Join-Path $fixtureRoot "docs/templates/non-trivial-change-template.md"
  $originalTemplate = Get-Content -LiteralPath $templatePath -Raw
  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("### TRACEABILITY", "### REMOVED_TRACEABILITY")) -NoNewline
  Invoke-Checker 1 "missing marker"

  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("Validation Score: X/6", "Validation Result: X/6")) -NoNewline
  Invoke-Checker 1 "mismatched validation label"

  Set-Content -LiteralPath $templatePath -Value $originalTemplate -NoNewline
  Remove-Item -LiteralPath (Join-Path $fixtureRoot "docs/code-quality.md")
  Invoke-Checker 1 "missing Document Map target"

  Copy-RepoFile "docs/code-quality.md"
  $reviewPath = Join-Path $fixtureRoot "docs/review-checklist.md"
  $originalReview = Get-Content -LiteralPath $reviewPath -Raw
  Set-Content -LiteralPath $reviewPath -Value ($originalReview.Replace("- SA15 Validation block completeness", "- REMOVED_SA15 Validation block completeness")) -NoNewline
  Invoke-Checker 1 "missing SELF-AUDIT ID"

  Set-Content -LiteralPath $reviewPath -Value ($originalReview + "`n- SA15 Duplicate validation block completeness`n") -NoNewline
  Invoke-Checker 1 "duplicate SELF-AUDIT ID"

  Set-Content -LiteralPath $reviewPath -Value $originalReview -NoNewline
  $agentsPath = Join-Path $fixtureRoot "AGENTS.md"
  $originalAgents = Get-Content -LiteralPath $agentsPath -Raw
  Set-Content -LiteralPath $agentsPath -Value ($originalAgents.Replace("test-strategy-adversary", "REMOVED_TEST_STRATEGY_ROLE")) -NoNewline
  Invoke-Checker 1 "missing pre-code role routing"

  & $engine -NoProfile -File $checker -RepoRoot $repoRoot
  if ($LASTEXITCODE -ne 0) {
    throw "current repository contract failed checker"
  }
  Write-Host "PASS current repository contract"
  Write-Host "PASS all workflow-contract checker tests passed."
} finally {
  Remove-Item -LiteralPath $fixtureRoot -Recurse -Force -ErrorAction SilentlyContinue
}

<#
.SYNOPSIS
  Check mechanical coherence of the Fable workflow contract.

.DESCRIPTION
  Verifies required files, exact operational strings, evidence markers, the
  final validation block, and CLAUDE.md Document Map targets. This checker
  cannot prove that a user supplied ExitPlanMode approval; the lead agent's
  approval and workflow-drift review remain authoritative. Fable-native
  counterpart to scripts/check-workflow-contract.ps1 (Codex) — same
  technique, adapted where Fable's template structure genuinely differs
  (GATE appears twice, once per Plan Mode phase; the exact validation block
  is stated in two files, not four).

.PARAMETER RepoRoot
  Repository root to inspect. Defaults to the parent of this script directory.

.NOTES
  Prerequisites: PowerShell 7 and a gocluster-style workflow tree.
  Side effects: reads files and writes only console output.
  Safety: does not modify workflow files or treat static text as approval.
#>

Param(
  [string]$RepoRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($RepoRoot -eq "") {
  $RepoRoot = Join-Path $PSScriptRoot ".."
}
$root = (Resolve-Path -LiteralPath $RepoRoot).Path
$failures = [System.Collections.Generic.List[string]]::new()

function Add-Failure {
  Param([string]$Message)
  $failures.Add($Message)
  Write-Host "FAIL $Message"
}

function Get-RepoText {
  Param([string]$RelativePath)
  $full = Join-Path $root $RelativePath
  if (-not (Test-Path -LiteralPath $full -PathType Leaf)) {
    Add-Failure "missing required file: $RelativePath"
    return ""
  }
  return (Get-Content -LiteralPath $full -Raw).Replace("`r`n", "`n")
}

function Require-Text {
  Param(
    [string]$RelativePath,
    [string]$Text,
    [string]$Required
  )
  if (-not $Text.Contains($Required)) {
    Add-Failure "$RelativePath missing exact workflow text: $Required"
  }
}

function Require-Line {
  Param(
    [string]$RelativePath,
    [string]$Text,
    [string]$Required
  )
  $lines = $Text -split "`n"
  if ($lines -notcontains $Required) {
    Add-Failure "$RelativePath missing exact workflow line: $Required"
  }
}

$claude = Get-RepoText "CLAUDE.md"
$template = Get-RepoText "docs/templates/fable-non-trivial-change-template.md"
$validation = Get-RepoText "docs/fable-validation.md"
$review = Get-RepoText "docs/fable-review-checklist.md"

$requiredClaudeText = @(
  "``EnterPlanMode``",
  "``ExitPlanMode``",
  "``Skill check: selected <skill>``",
  "``Skill check: none applicable``",
  "``SCOPE ADVERSARIAL REVIEW``",
  "Scope-to-Code Traceability"
)
foreach ($required in $requiredClaudeText) {
  Require-Text "CLAUDE.md" $claude $required
}

# Fable's template splits Non-trivial reporting into a Plan Mode plan (Phase
# A) and a closing response (Phase B). GATE is the only marker required in
# both phases; the other 11 appear exactly once, in Phase A or Phase B.
$markerExpectedCount = @{
  "GATE" = 2
  "DISCOVERY" = 1
  "SCOPE" = 1
  "SCOPE ADVERSARIAL REVIEW" = 1
  "PREFLIGHT" = 1
  "DESIGN" = 1
  "IMPLEMENTATION" = 1
  "REVIEW" = 1
  "SELF-AUDIT" = 1
  "CLOSEOUT" = 1
  "TRACEABILITY" = 1
  "VALIDATION" = 1
}
foreach ($marker in $markerExpectedCount.Keys) {
  $expected = $markerExpectedCount[$marker]
  $actual = ([regex]::Matches($template, "(?m)^### $([regex]::Escape($marker))$")).Count
  if ($actual -ne $expected) {
    Add-Failure "docs/templates/fable-non-trivial-change-template.md marker '$marker' expected $expected occurrence(s), found $actual"
  }
}

foreach ($number in 1..15) {
  $id = "SA$number"
  $reviewCount = ([regex]::Matches($review, "(?m)^- $id ")).Count
  if ($reviewCount -ne 1) {
    Add-Failure "docs/fable-review-checklist.md must define $id exactly once; found $reviewCount"
  }
}
Require-Line "docs/templates/fable-non-trivial-change-template.md" $template "- Applicability manifest:"
Require-Line "docs/templates/fable-non-trivial-change-template.md" $template "  - applicable: <SA IDs>"
Require-Line "docs/templates/fable-non-trivial-change-template.md" $template "  - not applicable: <SA IDs> - <shared reason; repeat for different reasons>"
Require-Line "docs/templates/fable-non-trivial-change-template.md" $template "- Results:"
Require-Text "docs/templates/fable-non-trivial-change-template.md" $template "Classify every SA1-SA15 ID exactly once."

# Unlike AGENTS.md, CLAUDE.md deliberately does not repeat the final
# validation block inline (it points to docs/fable-validation.md instead).
# Only the template and the validation rubric itself state it verbatim.
$validationBlock = @"
Validation Score: X/6
Failed items: none | <comma-separated failed item numbers/names>
Auto-fail conditions triggered: no | yes (<conditions>)
"@.Trim().Replace("`r`n", "`n")
foreach ($entry in @(
  @{ Path = "docs/templates/fable-non-trivial-change-template.md"; Text = $template },
  @{ Path = "docs/fable-validation.md"; Text = $validation }
)) {
  Require-Text $entry.Path $entry.Text $validationBlock
}

$mapHeading = "## Document Map"
$mapStart = $claude.IndexOf($mapHeading, [System.StringComparison]::Ordinal)
if ($mapStart -lt 0) {
  Add-Failure "CLAUDE.md missing Document Map heading"
} else {
  $mapText = $claude.Substring($mapStart + $mapHeading.Length)
  $targets = [regex]::Matches($mapText, "``([^``]+)``") |
    ForEach-Object { $_.Groups[1].Value } |
    Sort-Object -Unique
  if ($targets.Count -eq 0) {
    Add-Failure "CLAUDE.md Document Map has no file/directory targets"
  }
  foreach ($target in $targets) {
    if (-not (Test-Path -LiteralPath (Join-Path $root $target))) {
      Add-Failure "CLAUDE.md Document Map target does not exist: $target"
    }
  }
}

Write-Host "INFO static contract checks cannot prove conversational ExitPlanMode approval."
if ($failures.Count -gt 0) {
  Write-Host "FAIL workflow contract check found $($failures.Count) issue(s)."
  exit 1
}

Write-Host "PASS workflow contract mechanical checks passed."
exit 0

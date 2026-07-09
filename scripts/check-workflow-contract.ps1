<#
.SYNOPSIS
  Check mechanical coherence of the Codex workflow contract.

.DESCRIPTION
  Verifies required files, exact operational strings, evidence markers, the
  final validation block, and AGENTS.md Document Map targets. This checker
  cannot prove that a user supplied conversational approval; the lead agent's
  approval and workflow-drift review remain authoritative.

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

$agents = Get-RepoText "AGENTS.md"
$template = Get-RepoText "docs/templates/non-trivial-change-template.md"
$validation = Get-RepoText "VALIDATION.md"
$review = Get-RepoText "docs/review-checklist.md"

$requiredAgentText = @(
  "Approved vN",
  "Ledger status: Approved vN found: yes/no",
  "Skill check: selected <skill>",
  "Skill check: none applicable",
  "SCOPE ADVERSARIAL REVIEW",
  "Scope-to-Code Traceability"
)
foreach ($required in $requiredAgentText) {
  Require-Text "AGENTS.md" $agents $required
}

$markers = @(
  "GATE",
  "DISCOVERY",
  "SCOPE",
  "SCOPE ADVERSARIAL REVIEW",
  "PREFLIGHT",
  "DESIGN",
  "IMPLEMENTATION",
  "REVIEW",
  "SELF-AUDIT",
  "CLOSEOUT",
  "TRACEABILITY",
  "VALIDATION"
)
foreach ($marker in $markers) {
  Require-Text "AGENTS.md" $agents "``$marker``"
  Require-Line "docs/templates/non-trivial-change-template.md" $template "### $marker"
}

foreach ($number in 1..15) {
  $id = "SA$number"
  $reviewCount = ([regex]::Matches($review, "(?m)^- $id ")).Count
  if ($reviewCount -ne 1) {
    Add-Failure "docs/review-checklist.md must define $id exactly once; found $reviewCount"
  }
}
Require-Line "docs/templates/non-trivial-change-template.md" $template "- Applicability manifest:"
Require-Line "docs/templates/non-trivial-change-template.md" $template "  - applicable: <SA IDs>"
Require-Line "docs/templates/non-trivial-change-template.md" $template "  - not applicable: <SA IDs> - <shared reason; repeat for different reasons>"
Require-Line "docs/templates/non-trivial-change-template.md" $template "- Results:"
Require-Text "docs/templates/non-trivial-change-template.md" $template "Classify every SA1-SA15 ID exactly once."

$validationBlock = @"
Validation Score: X/6
Failed items: none | <comma-separated failed item numbers/names>
Auto-fail conditions triggered: no | yes (<conditions>)
"@.Trim()
foreach ($entry in @(
  @{ Path = "AGENTS.md"; Text = $agents },
  @{ Path = "docs/templates/non-trivial-change-template.md"; Text = $template },
  @{ Path = "VALIDATION.md"; Text = $validation },
  @{ Path = "docs/review-checklist.md"; Text = $review }
)) {
  Require-Text $entry.Path $entry.Text $validationBlock
}

$mapHeading = "## Document Map"
$mapStart = $agents.IndexOf($mapHeading, [System.StringComparison]::Ordinal)
if ($mapStart -lt 0) {
  Add-Failure "AGENTS.md missing Document Map heading"
} else {
  $mapText = $agents.Substring($mapStart + $mapHeading.Length)
  $targets = [regex]::Matches($mapText, "``([^``]+)``") |
    ForEach-Object { $_.Groups[1].Value } |
    Sort-Object -Unique
  if ($targets.Count -eq 0) {
    Add-Failure "AGENTS.md Document Map has no file targets"
  }
  foreach ($target in $targets) {
    if (-not (Test-Path -LiteralPath (Join-Path $root $target) -PathType Leaf)) {
      Add-Failure "AGENTS.md Document Map target does not exist: $target"
    }
  }
}

Write-Host "INFO static contract checks cannot prove conversational Approved vN evidence."
if ($failures.Count -gt 0) {
  Write-Host "FAIL workflow contract check found $($failures.Count) issue(s)."
  exit 1
}

Write-Host "PASS workflow contract mechanical checks passed."
exit 0

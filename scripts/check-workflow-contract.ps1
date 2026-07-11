<#
.SYNOPSIS
  Check mechanically representable Codex workflow invariants.
.DESCRIPTION
  Verifies canonical files, authority routes, positive and negative risk
  routing, retired Codex-only requirements, references, and optional changed-
  path exclusions. It cannot prove conversational compliance or engineering
  quality.
.PARAMETER RepoRoot
  Repository root to inspect.
.PARAMETER BaselineRevision
  Optional Git revision used to derive changed paths.
.PARAMETER ChangedPaths
  Optional explicit changed-path list, primarily for fixtures.
#>

[CmdletBinding()]
Param(
  [string]$RepoRoot = "",
  [string]$BaselineRevision = "",
  [string[]]$ChangedPaths = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if ($RepoRoot -eq "") { $RepoRoot = Join-Path $PSScriptRoot ".." }
$root = (Resolve-Path -LiteralPath $RepoRoot).Path
$failures = [System.Collections.Generic.List[string]]::new()

function Add-Failure([string]$Message) {
  $failures.Add($Message)
  Write-Host "FAIL $Message"
}

function Get-RepoText([string]$Path) {
  $full = Join-Path $root $Path
  if (-not (Test-Path -LiteralPath $full -PathType Leaf)) {
    Add-Failure "missing required file: $Path"
    return ""
  }
  return (Get-Content -LiteralPath $full -Raw).Replace("`r`n", "`n")
}

function Require-Text([string]$Path, [string]$Text, [string]$Required, [string]$Label) {
  if (-not $Text.Contains($Required)) { Add-Failure "$Label [$Path]" }
}

function Require-Pattern([string]$Path, [string]$Text, [string]$Pattern, [string]$Label) {
  if ($Text -notmatch $Pattern) { Add-Failure "$Label [$Path]" }
}

function Forbid-Pattern([string]$Path, [string]$Text, [string]$Pattern, [string]$Label) {
  if ($Text -match $Pattern) { Add-Failure "$Label [$Path]" }
}

function Get-MarkdownSection([string]$Text, [string]$Heading) {
  $pattern = "(?ms)^$([regex]::Escape($Heading))\s*`n(.*?)(?=^#{1,6}\s|\z)"
  $match = [regex]::Match($Text, $pattern)
  if (-not $match.Success) { return "" }
  return $match.Groups[1].Value
}

$requiredFiles = @(
  "AGENTS.md",
  "VALIDATION.md",
  "docs/change-workflow.md",
  "docs/code-quality.md",
  "docs/review-checklist.md",
  "docs/dev-runbook.md",
  "docs/WORKING_WITH_CODEX.md",
  "docs/decision-memory.md",
  "docs/templates/non-trivial-change-template.md",
  "docs/runbooks/codex-workflow-checks.md",
  "docs/runbooks/codex-triggered-validation-tools.md",
  "docs/workflow-eval-cases.md",
  "codex-skills/README.md",
  "scripts/README.md"
)
$text = @{}
foreach ($path in $requiredFiles) { $text[$path] = Get-RepoText $path }

$agents = $text["AGENTS.md"]
$workflow = $text["docs/change-workflow.md"]
$runbook = $text["docs/dev-runbook.md"]
$decisionMemory = $text["docs/decision-memory.md"]

Require-Text "AGENTS.md" $agents 'Only exact `Approved vN` authorizes the matching agreed scope.' "approval authority route missing"
Require-Text "AGENTS.md" $agents "Only explicitly agreed items are executable." "agreed-scope authority missing"
Require-Pattern "AGENTS.md" $agents 'Stop\s+and obtain revised approval' "scope-expansion reapproval route missing"
Require-Text "AGENTS.md" $agents "validation follows the touched surface" "touched-surface validation authority missing"
Require-Text "AGENTS.md" $agents "Read-only explanation, review, audit, diagnosis, and prioritization" "read-only route missing"
Require-Text "AGENTS.md" $agents "A bounded coherent change may remain one slice." "meaningful slicing safeguard missing"
Require-Pattern "AGENTS.md" $agents 'Broad\s+refactor-shaped scope is not approval-ready' "broad-refactor refusal missing"
Require-Pattern "AGENTS.md" $agents 'never claim a check or behavior\s+that was not actually observed' "claim-evidence rule missing"
Require-Text "AGENTS.md" $agents "High-risk work requires a fresh final verification pass." "high-risk fresh verification missing"

Require-Pattern "docs/change-workflow.md" $workflow 'Only exact `Approved vN` for the current ledger authorizes Non-trivial\s+mutation\.' "detailed approval route missing"
Require-Pattern "docs/change-workflow.md" $workflow 'Only explicitly\s+agreed items may be implemented\.' "detailed agreed-scope rule missing"
Require-Text "docs/change-workflow.md" $workflow "Standard and High-risk are internal routes." "internal risk-routing rule missing"
Require-Pattern "docs/change-workflow.md" $workflow 'Uncertainty that could affect safety,\s+scope, validation, or compatibility is High-risk until resolved\.' "uncertainty High-risk route missing"
Require-Text "docs/change-workflow.md" $workflow "Line count alone does not determine substantiality." "substantial-Go rule missing"
Require-Text "docs/change-workflow.md" $workflow "Retained specialist skills preserve their unique engineering methods" "specialist-method preservation missing"
Require-Pattern "docs/change-workflow.md" $workflow 'Run the complete\s+selected lane once on the final relevant state\.' "final-lane rule missing"

$standard = Get-MarkdownSection $workflow "### Standard"
if ($standard -eq "") { Add-Failure "Standard routing section missing" }
Require-Text "docs/change-workflow.md#Standard" $standard "Do not invoke specialists, parallel discovery, or`nindependent agents merely because work is Non-trivial." "Standard route does not reject default specialists"
Forbid-Pattern "docs/change-workflow.md#Standard" $standard '(?i)use (all |every )?(specialists|independent agents) by default' "Standard route reintroduces default specialists"

$highRisk = Get-MarkdownSection $workflow "### High-risk And Specialist Triggers"
if ($highRisk -eq "") { Add-Failure "High-risk routing section missing" }
foreach ($required in @(
  "Requirements ambiguity",
  "Scientific/model oracle",
  "Design challenge",
  "Test-strategy adversary",
  "Scope adversary",
  "Go code-quality review",
  "Code walk",
  "Blast-radius audit"
)) { Require-Text "docs/change-workflow.md#High-risk" $highRisk $required "positive specialist trigger missing: $required" }

Require-Text "docs/dev-runbook.md" $runbook "Task classification controls approval; touched surface and engineering risk`n  control validation." "runbook lane authority missing"
Require-Text "docs/dev-runbook.md" $runbook "Do not run Go validation solely because workflow Markdown" "workflow lane incorrectly permits inferred Go validation"
Require-Text "docs/dev-runbook.md" $runbook "Fable's existing contract`nremains owned by" "shared runbook Fable preservation missing"

$codexDecision = Get-MarkdownSection $decisionMemory "## Codex Application"
if ($codexDecision -eq "") { Add-Failure "Codex decision-memory section missing" }
if ($codexDecision -ne "") {
  Require-Text "docs/decision-memory.md#Codex" $codexDecision "Create or update an ADR only when a durable decision changes." "Codex durable-decision rule missing"
  Forbid-Pattern "docs/decision-memory.md#Codex" $codexDecision '(?i)(lightweight ADR stub|No durable decision change.{0,40}(file|ADR)|every Non-trivial.{0,60}(new|updated|stub))' "Codex no-change ADR requirement remains active"
}
$fableDecision = Get-MarkdownSection $decisionMemory "## Fable Application"
if ($fableDecision -eq "") { Add-Failure "Fable decision-memory preservation section missing" }
Require-Pattern "docs/decision-memory.md#Fable" $fableDecision 'new\s+ADR, updated ADR, or lightweight ADR stub' "Fable decision-memory behavior changed"

$activeCodexPaths = @(
  "AGENTS.md",
  "VALIDATION.md",
  "docs/change-workflow.md",
  "docs/review-checklist.md",
  "docs/WORKING_WITH_CODEX.md",
  "docs/templates/non-trivial-change-template.md",
  "codex-skills/README.md"
)
$activeCodexPaths += Get-ChildItem -LiteralPath (Join-Path $root "codex-skills") -Recurse -File |
  Where-Object { $_.Name -in @("SKILL.md", "openai.yaml") } |
  ForEach-Object { $_.FullName.Substring($root.Length).TrimStart('\').Replace('\','/') }

$retiredPatterns = @(
  @{ Pattern = '(?m)^\s*Skill check:'; Label = "retired skill marker remains" },
  @{ Pattern = 'Validation Score:'; Label = "retired numeric validation score remains" },
  @{ Pattern = '\bSA(?:[1-9]|1[0-5])\b'; Label = "retired SA taxonomy remains" },
  @{ Pattern = 'SELF-AUDIT'; Label = "retired Self-Audit reporting remains" },
  @{ Pattern = 'Ledger status: Approved'; Label = "retired ledger-status echo remains" },
  @{ Pattern = 'Reasoning budget:'; Label = "retired reasoning-budget field remains" },
  @{ Pattern = 'canonical four-field independent-result envelope'; Label = "retired agent result envelope remains" }
)
foreach ($path in $activeCodexPaths | Sort-Object -Unique) {
  $content = if ($text.ContainsKey($path)) { $text[$path] } else { Get-RepoText $path }
  foreach ($retired in $retiredPatterns) { Forbid-Pattern $path $content $retired.Pattern $retired.Label }
}

Require-Text "docs/workflow-eval-cases.md" $text["docs/workflow-eval-cases.md"] "optional, non-authoritative" "workflow evaluation cases are not marked optional"

$map = Get-MarkdownSection $agents "## Detailed Routes"
if ($map -eq "") { Add-Failure "AGENTS.md detailed route map missing" }
foreach ($target in [regex]::Matches($map, '`([^`]+)`') | ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique) {
  if (-not (Test-Path -LiteralPath (Join-Path $root $target) -PathType Leaf)) { Add-Failure "AGENTS.md route target missing: $target" }
}

if ($BaselineRevision -ne "") {
  $ChangedPaths += @(& git -C $root diff --name-only $BaselineRevision --)
  if ($LASTEXITCODE -ne 0) { Add-Failure "unable to derive changed paths from baseline $BaselineRevision" }
}
$protectedPatterns = @(
  '^CLAUDE\.md$',
  '^\.claude/',
  '^docs/fable-',
  '^docs/templates/fable-',
  '^scripts/.*fable',
  '^docs/decisions/ADR-(0206|0208|0215|0217)-',
  '^scripts/create-release\.ps1$'
)
foreach ($changed in $ChangedPaths | Sort-Object -Unique) {
  $normalized = $changed.Replace('\','/')
  if ($protectedPatterns | Where-Object { $normalized -match $_ }) { Add-Failure "protected Fable or release path changed: $normalized" }
}

Write-Host "INFO static checks prove text, ownership, references, trigger representation, and supplied path boundaries only."
Write-Host "INFO they do not prove conversational approval, classification, discovery, validation sufficiency, independence, or engineering quality."
if ($failures.Count -gt 0) {
  Write-Host "FAIL workflow contract check found $($failures.Count) issue(s)."
  exit 1
}
Write-Host "PASS Codex workflow static invariants passed."
exit 0

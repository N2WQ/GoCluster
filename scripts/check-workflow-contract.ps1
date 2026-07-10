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
$workflow = Get-RepoText "docs/change-workflow.md"
$template = Get-RepoText "docs/templates/non-trivial-change-template.md"
$validation = Get-RepoText "VALIDATION.md"
$review = Get-RepoText "docs/review-checklist.md"
$runbook = Get-RepoText "docs/dev-runbook.md"
$working = Get-RepoText "docs/WORKING_WITH_CODEX.md"
$evalCases = Get-RepoText "docs/workflow-eval-cases.md"
$scriptsReadme = Get-RepoText "scripts/README.md"
$developerGuide = Get-RepoText "customgpt/developer-guide-index.md"
$sourceMap = Get-RepoText "customgpt/source-map.md"
$commonQuestions = Get-RepoText "customgpt/common-questions.md"
$skillStatusPaths = @(
  "codex-skills/requirements-ambiguity-review/SKILL.md",
  "codex-skills/scientific-model-oracle/SKILL.md",
  "codex-skills/design-challenger/SKILL.md",
  "codex-skills/test-strategy-adversary/SKILL.md",
  "codex-skills/go-code-quality-review/SKILL.md",
  "codex-skills/scope-ledger-adversarial-review/SKILL.md"
)
$skillStatusText = @{}
foreach ($path in $skillStatusPaths) {
  $skillStatusText[$path] = Get-RepoText $path
}

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

Require-Text "AGENTS.md" $agents "Emit exactly one standalone skill marker per assistant turn"
$selectedMarkerCount = ([regex]::Matches($template, "(?m)^  - Skill check: selected <skill>$")).Count
$noneMarkerCount = ([regex]::Matches($template, "(?m)^  - Skill check: none applicable$")).Count
if ($selectedMarkerCount -ne 2 -or $noneMarkerCount -ne 2) {
  Add-Failure "docs/templates/non-trivial-change-template.md must contain each exact standalone skill-marker alternative twice; selected=$selectedMarkerCount none=$noneMarkerCount"
}

$canonicalAgentStatus = "- Agent status: completed | unsupported | not authorized/not requested | explicitly prohibited | failed | timed out | inconclusive"
foreach ($path in $skillStatusPaths) {
  Require-Text $path $skillStatusText[$path] "canonical four-field independent-result envelope"
}
Require-Line "AGENTS.md" $agents "  - ``Agent status: completed | unsupported | not authorized/not requested | explicitly prohibited | failed | timed out | inconclusive``"
Require-Text "docs/templates/non-trivial-change-template.md" $template "- Independent-agent status: completed | unsupported |"
Require-Text "docs/templates/non-trivial-change-template.md" $template "- Waiver disposition: none | <scope, owner, mitigation, expiry>"
$forbiddenTemplateStatusText = @(
  "Independent-agent status: supported, authorized, and not prohibited",
  "failed/timed out",
  "unsupported/not authorized/not requested/prohibited",
  "used/status",
  "inconclusive - no independent context",
  "inconclusive - context contaminated"
)
foreach ($forbidden in $forbiddenTemplateStatusText) {
  if ($template.Contains($forbidden)) {
    Add-Failure "docs/templates/non-trivial-change-template.md contains noncanonical report-field status text: $forbidden"
  }
}

$readOnlyContract = @(
  @{ Path = "AGENTS.md"; Text = $agents; Required = "## Read-only Review/Audit Mode" },
  @{ Path = "AGENTS.md"; Text = $agents; Required = "any later mutation must first enter its Small or" },
  @{ Path = "docs/change-workflow.md"; Text = $workflow; Required = "### Read-only review/audit" },
  @{ Path = "docs/change-workflow.md"; Text = $workflow; Required = "Findings, priorities, and requested recommendations are evidence, not latent" },
  @{ Path = "VALIDATION.md"; Text = $validation; Required = "Use this scorecard after any Non-trivial Codex change" },
  @{ Path = "docs/templates/non-trivial-change-template.md"; Text = $template; Required = "This template applies only to Non-trivial changes." },
  @{ Path = "docs/review-checklist.md"; Text = $review; Required = "## Read-only Review/Audit Evidence" },
  @{ Path = "docs/WORKING_WITH_CODEX.md"; Text = $working; Required = "Non-mutating explanation, review, audit, diagnosis, prioritization, and" },
  @{ Path = "docs/workflow-eval-cases.md"; Text = $evalCases; Required = "### E1 Read-Only Explanation And Audit Route" },
  @{ Path = "scripts/README.md"; Text = $scriptsReadme; Required = "read-only route and transition boundary" },
  @{ Path = "customgpt/developer-guide-index.md"; Text = $developerGuide; Required = "Non-mutating explanation, review, audit, diagnosis, prioritization, and" },
  @{ Path = "customgpt/source-map.md"; Text = $sourceMap; Required = "Read-only review, audit, diagnosis, and transition to implementation" },
  @{ Path = "customgpt/common-questions.md"; Text = $commonQuestions; Required = "When does read-only review or audit avoid change approval" }
)
foreach ($entry in $readOnlyContract) {
  Require-Text $entry.Path $entry.Text $entry.Required
}

$laneContract = @(
  @{ Path = "AGENTS.md"; Text = $agents; Required = "``docs/dev-runbook.md`` owns" },
  @{ Path = "docs/change-workflow.md"; Text = $workflow; Required = "``docs/dev-runbook.md`` owns" },
  @{ Path = "docs/dev-runbook.md"; Text = $runbook; Required = "Task classification controls approval rigor; touched surface controls" },
  @{ Path = "docs/dev-runbook.md"; Text = $runbook; Required = "### Small code change" },
  @{ Path = "docs/dev-runbook.md"; Text = $runbook; Required = "Workflow contracts, executor guidance, runbooks, rubrics, templates, and" },
  @{ Path = "docs/dev-runbook.md"; Text = $runbook; Required = "### Script-only change" },
  @{ Path = "docs/dev-runbook.md"; Text = $runbook; Required = "Do not infer Go" },
  @{ Path = "docs/dev-runbook.md"; Text = $runbook; Required = "### Non-trivial code, mixed, or runtime-contract change" },
  @{ Path = "VALIDATION.md"; Text = $validation; Required = "use the workflow/skill-doc lane even when Markdown-only" },
  @{ Path = "docs/WORKING_WITH_CODEX.md"; Text = $working; Required = "size controls approval; touched surface controls validation commands." }
)
foreach ($entry in $laneContract) {
  Require-Text $entry.Path $entry.Text $entry.Required
}

$scopeStatusContract = @(
  @{ Path = "AGENTS.md"; Text = $agents; Required = "only ``Agreed`` items are executable" },
  @{ Path = "docs/change-workflow.md"; Text = $workflow; Required = "``Pending`` means unresolved and blocks presentation or use of the approval" },
  @{ Path = "docs/templates/non-trivial-change-template.md"; Text = $template; Required = "Do not present the approval token while any item or implementation slice is" },
  @{ Path = "docs/templates/non-trivial-change-template.md"; Text = $template; Required = "For every Scope Ledger item that was ``Agreed`` at the start of implementation:" },
  @{ Path = "docs/review-checklist.md"; Text = $review; Required = "Map every Scope Ledger item with status ``Agreed`` as of the start of the" },
  @{ Path = "VALIDATION.md"; Text = $validation; Required = "no ``Pending`` item remained when approval was presented or" }
)
foreach ($entry in $scopeStatusContract) {
  Require-Text $entry.Path $entry.Text $entry.Required
}

$preCodeRoles = @(
  "parallel-discovery",
  "requirements-ambiguity-review",
  "scientific-model-oracle",
  "design-challenger",
  "test-strategy-adversary"
)
foreach ($role in $preCodeRoles) {
  Require-Text "AGENTS.md" $agents $role
  Require-Text "docs/change-workflow.md" $workflow $role
  Require-Text "docs/templates/non-trivial-change-template.md" $template $role
}
foreach ($role in $preCodeRoles | Where-Object { $_ -ne "parallel-discovery" }) {
  Require-Text "VALIDATION.md" $validation $role
  Require-Text "docs/review-checklist.md" $review $role
  Require-Text "docs/WORKING_WITH_CODEX.md" $working $role
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
  Require-Line "docs/templates/non-trivial-change-template.md" $template "### $marker"
}
Require-Text "AGENTS.md" $agents "exact required`nmarker set, ordering, phase placement"

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
$normalizedValidationBlock = $validationBlock.Replace("`r", "")
if (-not $validation.Replace("`r", "").Contains($normalizedValidationBlock)) {
  Add-Failure "VALIDATION.md missing exact canonical validation block"
}
foreach ($entry in @(
  @{ Path = "AGENTS.md"; Text = $agents },
  @{ Path = "docs/templates/non-trivial-change-template.md"; Text = $template },
  @{ Path = "docs/review-checklist.md"; Text = $review }
)) { Require-Text $entry.Path $entry.Text "VALIDATION.md" }
$validationCopies = @($agents,$template,$validation,$review | Where-Object { $_.Replace("`r", "").Contains($normalizedValidationBlock) }).Count
if ($validationCopies -ne 1) { Add-Failure "exact validation block must have one owner; found $validationCopies" }

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

Write-Host "INFO static contract checks cannot prove conversational Approved vN evidence, route transitions, or genuine agent independence."
if ($failures.Count -gt 0) {
  Write-Host "FAIL workflow contract check found $($failures.Count) issue(s)."
  exit 1
}

Write-Host "PASS workflow contract mechanical checks passed."
exit 0

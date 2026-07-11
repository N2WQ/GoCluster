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
    "docs/runbooks/codex-workflow-checks.md",
    "docs/runbooks/codex-triggered-validation-tools.md",
    "docs/WORKING_WITH_CODEX.md",
    "docs/workflow-eval-cases.md",
    "docs/domain-contract.md",
    "docs/decision-memory.md",
    "docs/agent-lessons/README.md",
    "docs/templates/non-trivial-change-template.md",
    "docs/templates/adr-template.md",
    "docs/troubleshooting/TSR-TEMPLATE.md",
    "scripts/README.md",
    "customgpt/developer-guide-index.md",
    "customgpt/source-map.md",
    "customgpt/common-questions.md",
    "codex-skills/requirements-ambiguity-review/SKILL.md",
    "codex-skills/scientific-model-oracle/SKILL.md",
    "codex-skills/design-challenger/SKILL.md",
    "codex-skills/test-strategy-adversary/SKILL.md",
    "codex-skills/go-code-quality-review/SKILL.md",
    "codex-skills/scope-ledger-adversarial-review/SKILL.md"
  )
  foreach ($file in $requiredFiles) {
    Copy-RepoFile $file
  }

  Invoke-Checker 0 "positive fixture"

  $templatePath = Join-Path $fixtureRoot "docs/templates/non-trivial-change-template.md"
  $originalTemplate = Get-Content -LiteralPath $templatePath -Raw
  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("### TRACEABILITY", "### REMOVED_TRACEABILITY")) -NoNewline
  Invoke-Checker 1 "missing marker"

  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("### TRACEABILITY", "### REVIEW`n### TRACEABILITY")) -NoNewline
  Invoke-Checker 1 "duplicate marker"

  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("### TRACEABILITY", "### VALIDATION_MOVED").Replace("### VALIDATION", "### TRACEABILITY").Replace("### VALIDATION_MOVED", "### VALIDATION")) -NoNewline
  Invoke-Checker 1 "reordered markers"

  $validationPath = Join-Path $fixtureRoot "VALIDATION.md"
  $originalValidation = Get-Content -LiteralPath $validationPath -Raw
  Set-Content -LiteralPath $validationPath -Value ($originalValidation.Replace("Validation Score: X/6", "Validation Result: X/6")) -NoNewline
  Invoke-Checker 1 "mismatched validation label"

  Set-Content -LiteralPath $validationPath -Value ($originalValidation + "`nValidation Score: X/6`nFailed items: none | <comma-separated failed item numbers/names>`nAuto-fail conditions triggered: no | yes (<conditions>)`n") -NoNewline
  Invoke-Checker 1 "duplicate canonical validation block"
  Set-Content -LiteralPath $validationPath -Value $originalValidation -NoNewline

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

  Set-Content -LiteralPath $agentsPath -Value ($originalAgents.Replace("## Read-only Review/Audit Mode", "## REMOVED Read-only Review/Audit Mode")) -NoNewline
  Invoke-Checker 1 "missing read-only route"

  Set-Content -LiteralPath $agentsPath -Value ($originalAgents.Replace("any later mutation must first enter its Small or", "a later mutation may proceed without a gate")) -NoNewline
  Invoke-Checker 1 "missing read-only transition boundary"

  Set-Content -LiteralPath $agentsPath -Value $originalAgents -NoNewline

  Set-Content -LiteralPath $agentsPath -Value ($originalAgents.Replace("Approved vN", "Approved approximately")) -NoNewline
  Invoke-Checker 1 "approval token mutation"

  Set-Content -LiteralPath $agentsPath -Value $originalAgents -NoNewline
  $workflowPath = Join-Path $fixtureRoot "docs/change-workflow.md"
  $originalWorkflow = Get-Content -LiteralPath $workflowPath -Raw
  Set-Content -LiteralPath $workflowPath -Value ($originalWorkflow + "`nThe common independent-review contract is owned here.`n") -NoNewline
  Invoke-Checker 1 "competing independent-review owner"

  Set-Content -LiteralPath $workflowPath -Value ($originalWorkflow.Replace("``Rejected`` is explicitly excluded", "``Rejected`` may be implemented")) -NoNewline
  Invoke-Checker 1 "Rejected status transition weakened"

  Set-Content -LiteralPath $workflowPath -Value ($originalWorkflow.Replace("``Deferred`` item becomes necessary", "item becomes necessary")) -NoNewline
  Invoke-Checker 1 "Deferred status transition weakened"
  Set-Content -LiteralPath $workflowPath -Value $originalWorkflow -NoNewline

  $runbookPath = Join-Path $fixtureRoot "docs/dev-runbook.md"
  $originalRunbook = Get-Content -LiteralPath $runbookPath -Raw
  Set-Content -LiteralPath $runbookPath -Value ($originalRunbook.Replace("### Small code change", "### Small change")) -NoNewline
  Invoke-Checker 1 "Small commands are not scoped to code"

  Set-Content -LiteralPath $runbookPath -Value ($originalRunbook.Replace("Workflow contracts, executor guidance, runbooks, rubrics, templates, and", "Some workflow documents and")) -NoNewline
  Invoke-Checker 1 "workflow Markdown lane precedence removed"

  Set-Content -LiteralPath $runbookPath -Value ($originalRunbook.Replace("### Script-only change", "### Unspecified change")) -NoNewline
  Invoke-Checker 1 "script-only lane removed"

  Set-Content -LiteralPath $runbookPath -Value ($originalRunbook.Replace("Do not infer Go", "Infer Go")) -NoNewline
  Invoke-Checker 1 "script-only lane incorrectly infers Go validation"

  Set-Content -LiteralPath $runbookPath -Value $originalRunbook -NoNewline

  $checksPath = Join-Path $fixtureRoot "docs/runbooks/codex-workflow-checks.md"
  $originalChecks = Get-Content -LiteralPath $checksPath -Raw
  Set-Content -LiteralPath $checksPath -Value ($originalChecks.Replace("It does not select a validation lane", "It selects a validation lane")) -NoNewline
  Invoke-Checker 1 "Codex component selects a lane"
  Set-Content -LiteralPath $checksPath -Value $originalChecks -NoNewline

  $toolsPath = Join-Path $fixtureRoot "docs/runbooks/codex-triggered-validation-tools.md"
  $originalTools = Get-Content -LiteralPath $toolsPath -Raw
  Set-Content -LiteralPath $toolsPath -Value ($originalTools.Replace("Open this recipe only after ``docs/dev-runbook.md`` or a triggered audit requires", "Open this recipe unconditionally before work requires")) -NoNewline
  Invoke-Checker 1 "triggered recipes become unconditional"
  Set-Content -LiteralPath $toolsPath -Value $originalTools -NoNewline

  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("Do not present the approval token while any item or implementation slice is", "The approval token may be presented while an item is")) -NoNewline
  Invoke-Checker 1 "Pending blocks approval"

  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("For every Scope Ledger item that was ``Agreed`` at the start of implementation:", "For every Scope Ledger item that was ``Agreed`` or ``Pending`` at the start of implementation:")) -NoNewline
  Invoke-Checker 1 "traceability is Agreed-only"

  Set-Content -LiteralPath $templatePath -Value $originalTemplate -NoNewline

  $specialistPath = Join-Path $fixtureRoot "codex-skills/design-challenger/SKILL.md"
  $originalSpecialist = Get-Content -LiteralPath $specialistPath -Raw
  foreach ($mutation in @(
    @{ From="Trigger when two or more viable architectures"; To="Consider when architectures"; Label="specialist trigger mutation" },
    @{ From="Run before drafting the Proposed Scope"; To="Run at any phase before scope"; Label="specialist phase mutation" },
    @{ From="Do not approve scope"; To="Approve scope"; Label="specialist refusal mutation" },
    @{ From="disposition with the lead"; To="disposition with the reviewer"; Label="specialist lead-ownership mutation" },
    @{ From="canonical four-field independent-result envelope"; To="independent result"; Label="specialist stale canonical route" }
  )) {
    Set-Content -LiteralPath $specialistPath -Value ($originalSpecialist.Replace($mutation.From,$mutation.To)) -NoNewline
    Invoke-Checker 1 $mutation.Label
  }
  Set-Content -LiteralPath $specialistPath -Value $originalSpecialist -NoNewline

  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("  - Skill check: selected <skill>", "  - Skill check: selected <skill>`n  - Skill check: selected <skill>")) -NoNewline
  Invoke-Checker 1 "duplicate standalone skill marker field"

  Set-Content -LiteralPath $templatePath -Value $originalTemplate -NoNewline

  Set-Content -LiteralPath $templatePath -Value ($originalTemplate.Replace("failed | timed out |", "failed/timed out |")) -NoNewline
  Invoke-Checker 1 "combined template status alias rejected"

  Set-Content -LiteralPath $templatePath -Value $originalTemplate -NoNewline

  $canonicalStatus = "  - ``Agent status: completed | unsupported | not authorized/not requested | explicitly prohibited | failed | timed out | inconclusive``"
  $statusMutations = @(
    @{ Replacement = "  - ``Agent status: used | unsupported | not authorized/not requested | explicitly prohibited | failed | timed out | inconclusive``"; Label = "used is not an agent status" },
    @{ Replacement = "  - ``Agent status: independent | unsupported | not authorized/not requested | explicitly prohibited | failed | timed out | inconclusive``"; Label = "independent is not an agent status" },
    @{ Replacement = "  - ``Agent status: completed | unsupported | not authorized/not requested | prohibited | failed | timed out | inconclusive``"; Label = "prohibited alias rejected" },
    @{ Replacement = "  - ``Agent status: completed | unsupported | not authorized/not requested | explicitly prohibited | failed | timed-out | inconclusive``"; Label = "timed-out alias rejected" },
    @{ Replacement = "  - ``Agent status: completed | unsupported | not authorized/not requested | explicitly prohibited | failed/timed out | inconclusive``"; Label = "combined failure alias rejected" },
    @{ Replacement = "  - ``Agent status: completed | unsupported | not authorized/not requested | explicitly prohibited | failed | timed out | inconclusive - no independent context``"; Label = "status reason must be separate" }
  )
  foreach ($mutation in $statusMutations) {
    $skillPath = $agentsPath
    $originalSkill = Get-Content -LiteralPath $skillPath -Raw
    Set-Content -LiteralPath $skillPath -Value ($originalSkill.Replace($canonicalStatus, $mutation.Replacement)) -NoNewline
    Invoke-Checker 1 $mutation.Label
    Set-Content -LiteralPath $skillPath -Value $originalSkill -NoNewline
  }

  & $engine -NoProfile -File $checker -RepoRoot $repoRoot
  if ($LASTEXITCODE -ne 0) {
    throw "current repository contract failed checker"
  }
  Write-Host "PASS current repository contract"
  Write-Host "PASS all workflow-contract checker tests passed."
} finally {
  Remove-Item -LiteralPath $fixtureRoot -Recurse -Force -ErrorAction SilentlyContinue
}

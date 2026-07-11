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

function Get-PatternGroup([string]$Path, [string]$Text, [string]$Pattern, [string]$Group, [string]$Label) {
  $match = [regex]::Match($Text, $Pattern)
  if (-not $match.Success) {
    Add-Failure "$Label [$Path]"
    return ""
  }
  return $match.Groups[$Group].Value
}

function Normalize-RelationshipText([string]$Text) {
  return ([regex]::Replace($Text, '\s+', ' ')).Trim()
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
  "docs/decision-log.md",
  "docs/decisions/ADR-0221-codex-authority-and-evidence-workflow.md",
  "docs/decisions/ADR-0222-corrective-codex-authority-and-validation.md",
  "docs/decisions/ADR-0223-bounded-specialist-context-and-independent-evidence.md",
  "docs/decisions/ADR-0224-evidence-before-scope-and-material-reapproval.md",
  "docs/decisions/ADR-0225-remove-codex-target-reasoning-recommendation.md",
  "codex-skills/README.md",
  "scripts/README.md"
)
$text = @{}
foreach ($path in $requiredFiles) { $text[$path] = Get-RepoText $path }

$agents = $text["AGENTS.md"]
$workflow = $text["docs/change-workflow.md"]
$runbook = $text["docs/dev-runbook.md"]
$codeQuality = $text["docs/code-quality.md"]
$decisionMemory = $text["docs/decision-memory.md"]
$adr0221 = $text["docs/decisions/ADR-0221-codex-authority-and-evidence-workflow.md"]
$adr0222 = $text["docs/decisions/ADR-0222-corrective-codex-authority-and-validation.md"]
$adr0223 = $text["docs/decisions/ADR-0223-bounded-specialist-context-and-independent-evidence.md"]
$adr0224 = $text["docs/decisions/ADR-0224-evidence-before-scope-and-material-reapproval.md"]
$adr0225 = $text["docs/decisions/ADR-0225-remove-codex-target-reasoning-recommendation.md"]
$adr0223Decision = Get-MarkdownSection $adr0223 "## Decision"
$adr0225Decision = Get-MarkdownSection $adr0225 "## Decision"
$decisionLog = $text["docs/decision-log.md"]

Require-Text "AGENTS.md" $agents 'Only exact `Approved vN` authorizes the matching agreed scope.' "approval authority route missing"
Require-Text "AGENTS.md" $agents "Only explicitly agreed items are executable." "agreed-scope authority missing"
Require-Pattern "AGENTS.md" $agents 'Stop\s+and obtain revised approval' "scope-expansion reapproval route missing"
Require-Text "AGENTS.md" $agents "validation follows the touched surface" "touched-surface validation authority missing"
Require-Text "AGENTS.md" $agents "Read-only explanation, review, audit, diagnosis, and prioritization" "read-only route missing"
Require-Text "AGENTS.md" $agents "A bounded coherent change may remain one slice." "meaningful slicing safeguard missing"
Require-Pattern "AGENTS.md" $agents 'Broad\s+refactor-shaped scope is not approval-ready' "broad-refactor refusal missing"
Require-Pattern "AGENTS.md" $agents 'never claim a check or behavior\s+that was not actually observed' "claim-evidence rule missing"
Require-Pattern "AGENTS.md" $agents 'High-risk work requires a fresh final verification pass' "high-risk fresh verification missing"

$agentRoutes = Get-MarkdownSection $agents "## Work Routes"
$agentSmallExclusions = Get-PatternGroup "AGENTS.md#Work Routes" $agentRoutes '(?is)Small\s+(?:also\s+)?(?:cannot|must not)\s+change(?<excluded>.*?)(?:If that|Stop and reclassify)' "excluded" "sensitive Small exclusion relationship missing"
$positiveSensitiveChange = '(?i)\b(?:(?:may|can|could|might|will|shall|must)\s+(?!(?:not|never)\b)|is\s+(?:allowed|permitted|authorized)\s+to\s+)change\b'
$prohibitionContrast = '(?i)\b(?:but|except|however|yet)\b'
foreach ($concept in @(
  @{ Pattern='runtime config'; Label='runtime config' },
  @{ Pattern='schema'; Label='schema' },
  @{ Pattern='default'; Label='default' },
  @{ Pattern='sentinel'; Label='sentinel' },
  @{ Pattern='parser'; Label='parser' },
  @{ Pattern='authentication|admission'; Label='authentication/admission' },
  @{ Pattern='persisted state'; Label='persisted state' },
  @{ Pattern='scientific/model'; Label='scientific/model' },
  @{ Pattern='hot-path'; Label='hot path' },
  @{ Pattern='shared contracts'; Label='shared contracts' },
  @{ Pattern='operator-visible'; Label='operator-visible behavior' },
  @{ Pattern='durable decisions'; Label='durable decisions' }
)) { Require-Pattern "AGENTS.md#Work Routes" $agentSmallExclusions $concept.Pattern "sensitive Small exclusion missing: $($concept.Label)" }
Forbid-Pattern "AGENTS.md#Work Routes" $agentSmallExclusions $positiveSensitiveChange "sensitive Small exclusion contains a positive inversion"
Forbid-Pattern "AGENTS.md#Work Routes" $agentSmallExclusions $prohibitionContrast "sensitive Small exclusion contains a positive inversion"

$agentRisk = Get-MarkdownSection $agents "## Risk Routing"
Require-Pattern "AGENTS.md#Risk Routing" $agentRisk '(?s)standing authorization.*active platform policy permits' "standing subagent authorization missing"
$agentAuthorizationLimits = Get-PatternGroup "AGENTS.md#Risk Routing" $agentRisk '(?is)This authorization\s+(?:does not|must not)(?<limits>.*?)(?:\.\s|$)' "limits" "standing authorization negation relationship missing"
foreach ($concept in @(
  @{ Pattern='require\s+subagents'; Label='require subagents' },
  @{ Pattern='expand\s+scope'; Label='expand scope' },
  @{ Pattern='bypass\s+approval'; Label='bypass approval' },
  @{ Pattern='authorize\s+pre-approval\s+edits'; Label='authorize pre-approval edits' },
  @{ Pattern='transfer\s+lead\s+authority'; Label='transfer lead authority' }
)) {
  Require-Pattern "AGENTS.md#Risk Routing" $agentAuthorizationLimits $concept.Pattern "standing authorization prohibition missing: $($concept.Label)"
}
$agentAuthorizationPositive = '(?i)\b(?:(?:does|may|can|could|might|will|shall|must)\s+(?!(?:not|never)\b)|is\s+(?:allowed|permitted|authorized)\s+to\s+)(?:require\s+subagents|expand\s+scope|bypass\s+approval|authorize\s+pre-approval\s+edits|transfer\s+lead\s+authority)'
Forbid-Pattern "AGENTS.md#Risk Routing" $agentAuthorizationLimits $agentAuthorizationPositive "standing authorization contains a positive inversion"
Forbid-Pattern "AGENTS.md#Risk Routing" $agentAuthorizationLimits $prohibitionContrast "standing authorization contains a positive inversion"
$agentContextRoute = Get-PatternGroup "AGENTS.md#Risk Routing" $agentRisk '(?is)(?<route>When a triggered specialist\s+investigation materially benefits from context partitioning,.*?)(?:\n\n|$)' "route" "positive context-partitioning route missing"
$agentContextRoute = Normalize-RelationshipText $agentContextRoute
$positiveBoundedSubagent = '(?i)\b(?:use\s+a|delegate(?:\s+the investigation)?\s+to\s+a)\s+bounded\s+subagent\b.*\b(?:supported|platform supports)\b'
$negativeBoundedSubagent = '(?i)\b(?:(?:do not|never|must not|cannot|should not|may not|need not)\s+(?:use\s+a|delegate(?:\s+the investigation)?\s+to\s+a)|(?:the lead\s+)?is not required to\s+(?:use\s+a|delegate(?:\s+the investigation)?\s+to\s+a))\s+bounded\s+subagent\b.*?\b(?:when supported|when the platform supports it)\b'
Require-Pattern "AGENTS.md#Context Routing" $agentContextRoute $positiveBoundedSubagent "positive context-partitioning route missing"
Forbid-Pattern "AGENTS.md#Context Routing" $agentContextRoute $negativeBoundedSubagent "positive context-partitioning route contains a negative inversion"
$agentFreshLead = Get-PatternGroup "AGENTS.md#Risk Routing" $agentRisk '(?is)(?<fresh>Require a separate non-steered context.*?fresh lead (?:pass|review).*?)(?:\n\n|$)' "fresh" "fresh lead review relationship missing"
$agentFreshLead = Normalize-RelationshipText $agentFreshLead
$freshLeadNotIndependent = '(?i)(?:fresh lead (?:pass|review)|fresh pass).*?(?:is not\s+independent review|does not constitute\s+independent review|must not be (?:described|reported) as\s+independent review|must not describe it as independent)'
$freshLeadReclassified = '(?i)(?:fresh lead (?:pass|review)|fresh pass).*?(?:\bis\b|constitutes|counts as|may be (?:described|reported) as|can be (?:described|reported) as)\s+independent review'
Require-Pattern "AGENTS.md#Fresh Lead Review" $agentFreshLead $freshLeadNotIndependent "fresh lead pass incorrectly substitutes for independent review"
Forbid-Pattern "AGENTS.md#Fresh Lead Review" $agentFreshLead $freshLeadReclassified "fresh lead pass contains a positive independent-review inversion"

Require-Pattern "docs/change-workflow.md" $workflow 'Only exact `Approved vN` for the current ledger authorizes Non-trivial\s+mutation\.' "detailed approval route missing"
Require-Pattern "docs/change-workflow.md" $workflow 'Only explicitly\s+agreed items may be implemented\.' "detailed agreed-scope rule missing"
Require-Text "docs/change-workflow.md" $workflow "Standard and High-risk are internal routes." "internal risk-routing rule missing"
Require-Pattern "docs/change-workflow.md" $workflow 'Uncertainty that could affect safety,\s+scope, validation, or compatibility is High-risk until resolved\.' "uncertainty High-risk route missing"
Require-Text "docs/change-workflow.md" $workflow "Line count alone does not determine substantiality." "substantial-Go rule missing"
Require-Text "docs/change-workflow.md" $workflow "Retained specialist skills preserve their unique engineering methods" "specialist-method preservation missing"
Require-Pattern "docs/change-workflow.md" $workflow 'Run the complete\s+selected lane once on the final relevant state\.' "final-lane rule missing"

$workflowClassification = Get-MarkdownSection $workflow "## Change Classification"
$workflowSmallExclusions = Get-PatternGroup "docs/change-workflow.md#Change Classification" $workflowClassification '(?is)(?:Small|It)\s+(?:also\s+)?(?:cannot|must not)\s+change(?<excluded>.*?)(?:Anything\s+else|If that)' "excluded" "detailed sensitive Small exclusion relationship missing"
foreach ($concept in @('runtime config','schema','default','sentinel','parser','authentication|admission','persisted state','scientific/model','hot-path','shared\s+contracts','operator-visible','durable decisions')) {
  Require-Pattern "docs/change-workflow.md#Change Classification" $workflowSmallExclusions $concept "detailed sensitive Small exclusion missing: $concept"
}
Forbid-Pattern "docs/change-workflow.md#Change Classification" $workflowSmallExclusions $positiveSensitiveChange "detailed sensitive Small exclusion contains a positive inversion"
Forbid-Pattern "docs/change-workflow.md#Change Classification" $workflowSmallExclusions $prohibitionContrast "detailed sensitive Small exclusion contains a positive inversion"
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
Require-Pattern "docs/change-workflow.md#High-risk" $highRisk '(?s)multiple production\s+packages.*shared behavior.*ownership.*interfaces.*contracts.*cross-package uncertainty' "shared-impact multiple-package review trigger missing"

$subagents = Get-MarkdownSection $workflow "## Subagents When Used"
Require-Pattern "docs/change-workflow.md#Subagents" $subagents '(?s)standing authorization.*active platform policy permits' "detailed standing authorization missing"
$workflowAuthorizationLimits = Get-PatternGroup "docs/change-workflow.md#Subagents" $subagents '(?is)Standing authorization\s+(?:does not|must not)(?<limits>.*?)(?:\.\s|$)' "limits" "detailed standing authorization negation relationship missing"
foreach ($concept in @(
  @{ Pattern='require\s+use'; Label='require use' },
  @{ Pattern='expand\s+scope'; Label='expand scope' },
  @{ Pattern='bypass\s+approval'; Label='bypass approval' },
  @{ Pattern='authorize\s+pre-approval\s+edits'; Label='authorize pre-approval edits' },
  @{ Pattern='transfer\s+lead\s+authority'; Label='transfer lead authority' }
)) {
  Require-Pattern "docs/change-workflow.md#Subagents" $workflowAuthorizationLimits $concept.Pattern "detailed standing authorization prohibition missing: $($concept.Label)"
}
$workflowAuthorizationPositive = '(?i)\b(?:(?:does|may|can|could|might|will|shall|must)\s+(?!(?:not|never)\b)|is\s+(?:allowed|permitted|authorized)\s+to\s+)(?:require\s+use|expand\s+scope|bypass\s+approval|authorize\s+pre-approval\s+edits|transfer\s+lead\s+authority)'
Forbid-Pattern "docs/change-workflow.md#Subagents" $workflowAuthorizationLimits $workflowAuthorizationPositive "detailed standing authorization contains a positive inversion"
Forbid-Pattern "docs/change-workflow.md#Subagents" $workflowAuthorizationLimits $prohibitionContrast "detailed standing authorization contains a positive inversion"
$workflowContextRoute = Get-PatternGroup "docs/change-workflow.md#Subagents" $subagents '(?is)(?<route>When a triggered specialist investigation materially\s+benefits from context partitioning,.*?)(?:\n\n|$)' "route" "detailed positive context-partitioning route missing"
$workflowContextRoute = Normalize-RelationshipText $workflowContextRoute
Require-Pattern "docs/change-workflow.md#Context Routing" $workflowContextRoute $positiveBoundedSubagent "detailed positive context-partitioning route missing"
Forbid-Pattern "docs/change-workflow.md#Context Routing" $workflowContextRoute $negativeBoundedSubagent "detailed positive context-partitioning route contains a negative inversion"
$workflowFallback = Get-PatternGroup "docs/change-workflow.md#Subagents" $subagents '(?is)(?<fallback>When preferred delegation is unavailable,.*?)(?:\n\n|$)' "fallback" "preferred-delegation fallback relationship missing"
$workflowFallback = Normalize-RelationshipText $workflowFallback
Require-Pattern "docs/change-workflow.md#Fresh Lead Review" $workflowFallback $freshLeadNotIndependent "detailed fresh lead pass incorrectly substitutes for independent review"
Forbid-Pattern "docs/change-workflow.md#Fresh Lead Review" $workflowFallback $freshLeadReclassified "detailed fresh lead pass contains a positive independent-review inversion"
$requiredIndependenceBoundary = '(?is)\b(?:when|if)\s+required independent context\s+(?:is unavailable|cannot be obtained).*?\b(?:pause|stop)\b.*?\b(?:explicit user approval|user explicitly approves)\b.*?\blimit\w*\b.*?\bclaim\b'
$silentLeadSubstitution = '(?i)(?:(?:the )?lead(?:-owned review)?\s+(?:may|can)\s+(?!not\b)(?:silently\s+)?(?:substitut\w*|replace|stand in for)|(?:the )?lead(?:-owned review)?\s+is\s+(?!not\b)(?:allowed|permitted|authorized)\s+to\s+(?:substitut\w*|replace|stand in for)|lead-owned review\s+is\s+an equivalent substitute)'
Require-Pattern "docs/change-workflow.md#Required Independence" $workflowFallback $requiredIndependenceBoundary "required independent evidence substitution boundary missing"
Forbid-Pattern "docs/change-workflow.md#Required Independence" $workflowFallback $silentLeadSubstitution "required independent evidence boundary permits lead substitution"

Require-Text "docs/dev-runbook.md" $runbook "Task classification controls approval; touched surface and engineering risk`n  control validation." "runbook lane authority missing"
Require-Text "docs/dev-runbook.md" $runbook "Do not run Go validation solely because workflow Markdown" "workflow lane incorrectly permits inferred Go validation"
Require-Text "docs/dev-runbook.md" $runbook "Fable's existing contract`nremains owned by" "shared runbook Fable preservation missing"

$productionGo = Get-MarkdownSection $runbook "## Non-trivial Code, Config, Or Mixed Change"
Require-Pattern "docs/dev-runbook.md#Non-trivial production Go" $productionGo 'every Non-trivial change that touches production Go' "production-Go final lane trigger missing"
Require-Pattern "docs/dev-runbook.md#Non-trivial production Go" $productionGo '(?s)Fable''s code/mixed/runtime-contract lane remains defined.*uses the same complete baseline' "shared Fable validation lane changed"
foreach ($command in @('go test ./...','go vet ./...','staticcheck ./...','golangci-lint run ./... --config=.golangci.yaml')) {
  Require-Text "docs/dev-runbook.md#Non-trivial production Go" $productionGo $command "production-Go final lane command missing: $command"
}
Require-Pattern "docs/dev-runbook.md#Non-trivial production Go" $productionGo 'complete baseline once on the\s+final relevant state' "production-Go final-once rule missing"

$networkQuality = Get-MarkdownSection $codeQuality "## Network And Lifecycle"
Require-Pattern "docs/code-quality.md#Network" $networkQuality '(?s)context cancellation.*deadlines.*idle/stall timeouts' "shared network/lifecycle obligation changed"
$retainedQuality = Get-MarkdownSection $codeQuality "## Bounded Retained State"
foreach ($concept in @('cardinality cap','expiry','ownership-coupled deletion','reference-counted reclamation','bounded by another structure')) {
  Require-Pattern "docs/code-quality.md#Bounded Retained State" $retainedQuality $concept "shared retained-state obligation missing: $concept"
}
$hotQuality = Get-MarkdownSection $codeQuality "## Hot Paths And Performance"
foreach ($concept in @('single-victim','zero or near-zero allocation','Performance claims require measurements')) {
  Require-Text "docs/code-quality.md#Hot Paths" $hotQuality $concept "shared hot-path obligation missing: $concept"
}
$modelQuality = Get-MarkdownSection $codeQuality "## Scientific And Model Claims"
foreach ($concept in @('definitions','units','boundaries','provenance-independent golden vectors','remaining uncertainty')) {
  Require-Text "docs/code-quality.md#Scientific" $modelQuality $concept "shared scientific obligation missing: $concept"
}
Require-Pattern "docs/code-quality.md" $codeQuality '## Support-critical Comments \(Go Comment Intent\)' "shared Go Comment Intent route missing"

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
  @{ Pattern = 'canonical four-field independent-result envelope'; Label = "retired agent result envelope remains" },
  @{ Pattern = '(?i)\b(?:lowest\s+sufficient\s+(?:target\s+)?reasoning\s+(?:level|effort)|target\s+reasoning\s*:|reasoning\s+budget\s*:|(?:target|recommended)\s+model\s+effort\s*:)'; Label = "retired target-reasoning requirement remains" }
)
foreach ($path in $activeCodexPaths | Sort-Object -Unique) {
  $content = if ($text.ContainsKey($path)) { $text[$path] } else { Get-RepoText $path }
  foreach ($retired in $retiredPatterns) { Forbid-Pattern $path $content $retired.Pattern $retired.Label }
}

$obsoleteReferencePaths = @($activeCodexPaths | Where-Object { $_ -like 'codex-skills/*' })
$obsoleteReferencePaths += "docs/runbooks/codex-triggered-validation-tools.md"
foreach ($path in $obsoleteReferencePaths | Sort-Object -Unique) {
  $content = if ($text.ContainsKey($path)) { $text[$path] } else { Get-RepoText $path }
  Forbid-Pattern $path $content 'Verification command reporting' "obsolete command-evidence reference remains"
  Forbid-Pattern $path $content 'workflow markers' "obsolete workflow-marker reference remains"
  Forbid-Pattern $path $content '`DESIGN`' "obsolete DESIGN-marker reference remains"
}

Require-Pattern "ADR-0221" $adr0221 '(?s)Status: Accepted.*Selective supersession: ADR-0222.*All other ADR-0221\s+decisions remain accepted' "ADR-0221 selective reciprocal note missing"
Require-Pattern "ADR-0221" $adr0221 '(?s)ADR-0223 refines Decision 2.*specialist triggering.*context selection.*independent-evidence requirements' "ADR-0221 ADR-0223 reciprocal note missing"
Require-Pattern "ADR-0222" $adr0222 '(?s)Status: Accepted.*Selectively supersede ADR-0221 only as follows' "ADR-0222 selective authority missing"
Require-Pattern "ADR-0222" $adr0222 '(?s)ADR-0223 supersedes Decision 7.*separate independent reviewer' "ADR-0222 ADR-0223 reciprocal note missing"
Require-Pattern "ADR-0222" $adr0222 '(?s)ADR-0225 supersedes Decision 1.*target reasoning recommendation' "ADR-0222 ADR-0225 reciprocal note missing"
Require-Pattern "ADR-0223#Decision" $adr0223Decision '(?s)selectively refines ADR-0221 Decision 2.*selectively supersedes ADR-0222 Decision 7' "ADR-0223 selective authority missing"
Require-Pattern "ADR-0224" $adr0224 '(?s)Status: Accepted.*refines ADR-0221.*current-evidence and planning' "ADR-0224 refinement authority missing"
Require-Pattern "ADR-0225#Decision" $adr0225Decision '(?s)selectively supersedes ADR-0222 Decision 1.*does not change.*exact.*Approved vN.*Fable' "ADR-0225 selective authority missing"
Require-Pattern "docs/decision-log.md" $decisionLog '(?m)^\| ADR-0225 \|.*\| Accepted \|.*\| ADR-0222 Decision 1 \| - \|' "ADR-0225 decision-index row missing"
Require-Pattern "docs/decision-log.md" $decisionLog '(?m)^\| ADR-0223 \|.*\| Accepted \|.*\| ADR-0221 Decision 2 \(refines\); ADR-0222 Decision 7 \| - \|' "ADR-0223 decision-index row missing"
Require-Pattern "docs/decision-log.md" $decisionLog '(?m)^\| ADR-0222 \|.*\| Accepted \|.*ADR-0221 \(selected clauses\).*\| (?=[^|\r\n]*ADR-0223 \(Decision 7\))(?=[^|\r\n]*ADR-0225 \(Decision 1\))[^|\r\n]*\|' "ADR-0222 decision-index reciprocal link missing"
Require-Pattern "docs/decision-log.md" $decisionLog '(?m)^\| ADR-0221 \|.*\| Accepted \|.*\| (?=[^|\r\n]*ADR-0222 \(selected clauses\))(?=[^|\r\n]*ADR-0223 \(refines Decision 2\))(?=[^|\r\n]*ADR-0224 \(refines evidence/planning\))[^|\r\n]*\|' "ADR-0221 reciprocal decision-index link missing"

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

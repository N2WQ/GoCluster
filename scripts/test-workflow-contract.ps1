<#
.SYNOPSIS
  Exercise positive and named negative Codex workflow fixtures.
.DESCRIPTION
  Every negative fixture asserts the invariant-specific failure text so an
  unrelated checker failure cannot create a false green.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$checker = Join-Path $PSScriptRoot "check-workflow-contract.ps1"
$skillVerifier = Join-Path $PSScriptRoot "verify-codex-skills.ps1"
$engine = (Get-Process -Id $PID).Path
$fixtureRoot = Join-Path ([IO.Path]::GetTempPath()) ("gocluster-workflow-" + [guid]::NewGuid().ToString("N"))

function Copy-ItemTree([string]$RelativePath) {
  $source = Join-Path $repoRoot $RelativePath
  $target = Join-Path $fixtureRoot $RelativePath
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $target) | Out-Null
  Copy-Item -LiteralPath $source -Destination $target -Recurse
}

function Invoke-Fixture([int]$ExpectedExit, [string]$ExpectedText, [string]$Label, [string[]]$ChangedPaths = @()) {
  $arguments = @('-NoProfile','-File',$checker,'-RepoRoot',$fixtureRoot)
  if ($ChangedPaths.Count -gt 0) { $arguments += '-ChangedPaths'; $arguments += $ChangedPaths }
  $output = (& $engine @arguments 2>&1 | Out-String)
  $actual = $LASTEXITCODE
  if ($actual -ne $ExpectedExit) { throw "$Label expected exit $ExpectedExit, got $actual`n$output" }
  if (-not $output.Contains($ExpectedText)) { throw "$Label missing expected text '$ExpectedText'`n$output" }
  Write-Host "PASS $Label"
}

function Replace-Once([string]$RelativePath, [string]$From, [string]$To) {
  $path = Join-Path $fixtureRoot $RelativePath
  $content = Get-Content -LiteralPath $path -Raw
  $count = ([regex]::Matches($content, [regex]::Escape($From))).Count
  if ($count -ne 1) { throw "$RelativePath mutation expected one occurrence of '$From', found $count" }
  Set-Content -LiteralPath $path -Value $content.Replace($From, $To) -NoNewline
  return $content
}

function Invoke-SkillFixture([int]$ExpectedExit, [string]$ExpectedText, [string]$Label) {
  $output = (& $engine -NoProfile -File $skillVerifier -RepoRoot $fixtureRoot 2>&1 | Out-String)
  $actual = $LASTEXITCODE
  if ($actual -ne $ExpectedExit) { throw "$Label expected exit $ExpectedExit, got $actual`n$output" }
  if (-not $output.Contains($ExpectedText)) { throw "$Label missing expected text '$ExpectedText'`n$output" }
  Write-Host "PASS $Label"
}

try {
  foreach ($path in @(
    "AGENTS.md", "VALIDATION.md", "docs/change-workflow.md",
    "docs/code-quality.md", "docs/domain-contract.md", "docs/review-checklist.md", "docs/dev-runbook.md",
    "docs/WORKING_WITH_CODEX.md", "docs/decision-memory.md",
    "docs/templates/non-trivial-change-template.md",
    "docs/runbooks/codex-workflow-checks.md",
    "docs/runbooks/codex-triggered-validation-tools.md",
    "docs/workflow-eval-cases.md", "codex-skills", "scripts/README.md"
  )) { Copy-ItemTree $path }

  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "positive contract"
  Invoke-SkillFixture 0 "PASS all requested repo skills verified." "positive skill methods"

  $original = Replace-Once "AGENTS.md" 'Only exact `Approved vN` authorizes the matching agreed scope.' 'Discussion or requests to proceed authorize the matching agreed scope.'
  Invoke-Fixture 1 "approval authority route missing" "approval cannot be loosened"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $standardFrom = "Do not invoke specialists, parallel discovery, or`nindependent agents merely because work is Non-trivial."
  $original = Replace-Once "docs/change-workflow.md" $standardFrom "Use specialists by default for Non-trivial work."
  Invoke-Fixture 1 "Standard route does not reject default specialists" "Standard rejects default specialists"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "Uncertainty that could affect safety,`nscope, validation, or compatibility is High-risk until resolved." "Uncertainty may remain Standard."
  Invoke-Fixture 1 "uncertainty High-risk route missing" "uncertainty cannot silently remain Standard"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $templatePath = Join-Path $fixtureRoot "docs/templates/non-trivial-change-template.md"
  $baseTemplate = Get-Content -LiteralPath $templatePath -Raw
  foreach ($case in @(
    @{ Text="`nSkill check: selected anything`n"; Expected="retired skill marker remains"; Label="skill marker retired" },
    @{ Text="`nValidation Score: 6/6`n"; Expected="retired numeric validation score remains"; Label="numeric score retired" },
    @{ Text="`nSA1 PASS`n"; Expected="retired SA taxonomy remains"; Label="SA taxonomy retired" },
    @{ Text="`nSELF-AUDIT`n"; Expected="retired Self-Audit reporting remains"; Label="Self-Audit reporting retired" },
    @{ Text="`nReasoning budget: high`n"; Expected="retired reasoning-budget field remains"; Label="reasoning field retired" }
  )) {
    Set-Content -LiteralPath $templatePath -Value ($baseTemplate + $case.Text) -NoNewline
    Invoke-Fixture 1 $case.Expected $case.Label
  }
  Set-Content -LiteralPath $templatePath -Value $baseTemplate -NoNewline

  $decisionPath = Join-Path $fixtureRoot "docs/decision-memory.md"
  $baseDecision = Get-Content -LiteralPath $decisionPath -Raw
  Set-Content -LiteralPath $decisionPath -Value $baseDecision.Replace("Create or update an ADR only when a durable decision changes.", "Every Non-trivial task creates a lightweight ADR stub.") -NoNewline
  Invoke-Fixture 1 "Codex durable-decision rule missing" "Codex no-change stub cannot return"
  Set-Content -LiteralPath $decisionPath -Value $baseDecision -NoNewline

  $oraclePath="codex-skills/scientific-model-oracle/SKILL.md"
  $oracleFullPath=Join-Path $fixtureRoot $oraclePath
  $original=Get-Content -Raw -LiteralPath $oracleFullPath
  $oracleCount=([regex]::Matches($original,[regex]::Escape("independent golden vectors"))).Count
  if($oracleCount -ne 3){throw "scientific oracle mutation expected three method occurrences, found $oracleCount"}
  Set-Content -LiteralPath $oracleFullPath -Value $original.Replace("independent golden vectors","implementation-derived vectors") -NoNewline
  Invoke-SkillFixture 1 "missing preserved trigger or method invariant: independent golden vectors" "scientific method cannot disappear"
  Set-Content -LiteralPath $oracleFullPath -Value $original -NoNewline

  $scopeSkillPath="codex-skills/scope-ledger-adversarial-review/SKILL.md"
  $original=Replace-Once $scopeSkillPath "Do not trigger for every Non-trivial ledger." "Trigger for every Non-trivial ledger."
  Invoke-SkillFixture 1 "missing preserved trigger or method invariant: Do not trigger for every" "scope specialist cannot become default"
  Set-Content -LiteralPath (Join-Path $fixtureRoot $scopeSkillPath) -Value $original -NoNewline

  Invoke-Fixture 1 "protected Fable or release path changed: CLAUDE.md" "Fable path protected" @("CLAUDE.md")
  Invoke-Fixture 1 "protected Fable or release path changed: scripts/create-release.ps1" "release script protected" @("scripts/create-release.ps1")

  & $engine -NoProfile -File $checker -RepoRoot $repoRoot
  if ($LASTEXITCODE -ne 0) { throw "current repository contract failed checker" }
  Write-Host "PASS current repository contract"
  Write-Host "PASS all workflow-contract fixtures passed."
} finally {
  Remove-Item -LiteralPath $fixtureRoot -Recurse -Force -ErrorAction SilentlyContinue
}

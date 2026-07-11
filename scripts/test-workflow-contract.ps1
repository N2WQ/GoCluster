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
    "docs/decision-log.md",
    "docs/decisions/ADR-0221-codex-authority-and-evidence-workflow.md",
    "docs/decisions/ADR-0222-corrective-codex-authority-and-validation.md",
    "docs/templates/non-trivial-change-template.md",
    "docs/runbooks/codex-workflow-checks.md",
    "docs/runbooks/codex-triggered-validation-tools.md",
    "docs/workflow-eval-cases.md", "codex-skills", "scripts/README.md"
  )) { Copy-ItemTree $path }

  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "positive contract"
  Invoke-SkillFixture 0 "PASS all requested repo skills verified." "positive skill methods"

  $original = Replace-Once "AGENTS.md" "lowest`n   sufficient target reasoning level" "target model effort"
  Invoke-Fixture 1 "reasoning recommendation missing" "reasoning recommendation remains reachable"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "docs/templates/non-trivial-change-template.md" "Target reasoning:" "Recommended model effort:"
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "reasoning narration remains flexible"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/templates/non-trivial-change-template.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "Small`nalso cannot change" "Small may change"
  Invoke-Fixture 1 "sensitive Small exclusion relationship missing" "AGENTS sensitive exclusion rejects inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "It also cannot change" "It may change"
  Invoke-Fixture 1 "detailed sensitive Small exclusion relationship missing" "workflow sensitive exclusion rejects inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "Small`nalso cannot change" "Small must not change"
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "AGENTS sensitive exclusion allows equivalent wording"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "It also cannot change" "It must not change"
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "workflow sensitive exclusion allows equivalent wording"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  foreach ($case in @(
    @{ From="runtime config"; To="runtime settings"; Expected="sensitive Small exclusion missing: runtime config"; Label="runtime config cannot be Small" },
    @{ From="parser behavior"; To="text handling"; Expected="sensitive Small exclusion missing: parser"; Label="parser behavior cannot be Small" },
    @{ From="authentication or admission"; To="access handling"; Expected="sensitive Small exclusion missing: authentication/admission"; Label="authentication cannot be Small" },
    @{ From="persisted state"; To="stored information"; Expected="sensitive Small exclusion missing: persisted state"; Label="persisted state cannot be Small" },
    @{ From="scientific/model semantics; hot-path behavior"; To="domain behavior; optimized code"; Expected="sensitive Small exclusion missing: scientific/model"; Label="model semantics cannot be Small" }
  )) {
    $original = Replace-Once "AGENTS.md" $case.From $case.To
    Invoke-Fixture 1 $case.Expected $case.Label
    Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline
  }

  $original = Replace-Once "AGENTS.md" "provides standing authorization for subagent use" "may provide task authorization for subagent use"
  Invoke-Fixture 1 "standing subagent authorization missing" "standing authorization remains reachable"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "This authorization does not require subagents," "This authorization requires subagents,"
  Invoke-Fixture 1 "standing authorization negation relationship missing" "standing authorization does not force use"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "This authorization does not" "This authorization must not"
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "AGENTS standing limits allow equivalent wording"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "Standing authorization does not" "Standing authorization must not"
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "workflow standing limits allow equivalent wording"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  foreach ($case in @(
    @{ Path="AGENTS.md"; From="expand scope"; To="does expand scope"; Expected="standing authorization contains a positive inversion"; Label="AGENTS standing authorization cannot expand scope" },
    @{ Path="docs/change-workflow.md"; From="bypass approval"; To="does bypass approval"; Expected="detailed standing authorization contains a positive inversion"; Label="workflow standing authorization cannot bypass approval" },
    @{ Path="AGENTS.md"; From="authorize pre-approval edits"; To="does authorize pre-approval edits"; Expected="standing authorization contains a positive inversion"; Label="AGENTS standing authorization cannot authorize preapproval edits" },
    @{ Path="docs/change-workflow.md"; From="transfer lead authority"; To="does transfer lead authority"; Expected="detailed standing authorization contains a positive inversion"; Label="workflow standing authorization cannot transfer lead authority" }
  )) {
    $original = Replace-Once $case.Path $case.From $case.To
    Invoke-Fixture 1 $case.Expected $case.Label
    Set-Content -LiteralPath (Join-Path $fixtureRoot $case.Path) -Value $original -NoNewline
  }

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

  $sharedImpact = "Changes to multiple production`npackages also trigger independent Go review when shared behavior, ownership,`ninterfaces, contracts, or meaningful cross-package uncertainty are affected."
  $original = Replace-Once "docs/change-workflow.md" $sharedImpact "Changes to multiple production packages always trigger independent Go review."
  Invoke-Fixture 1 "shared-impact multiple-package review trigger missing" "multiple-package review requires shared impact"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  foreach ($case in @(
    @{ From='1. `go test ./...`;'; To='1. targeted tests only;'; Expected='production-Go final lane command missing: go test ./...'; Label='production Go requires full tests' },
    @{ From='2. `go vet ./...`;'; To='2. optional vet;'; Expected='production-Go final lane command missing: go vet ./...'; Label='production Go requires vet' },
    @{ From='3. `staticcheck ./...`;'; To='3. optional static analysis;'; Expected='production-Go final lane command missing: staticcheck ./...'; Label='production Go requires staticcheck' },
    @{ From='4. `golangci-lint run ./... --config=.golangci.yaml`.'; To='4. optional lint.'; Expected='production-Go final lane command missing: golangci-lint run ./... --config=.golangci.yaml'; Label='production Go requires configured lint' }
  )) {
    $original = Replace-Once "docs/dev-runbook.md" $case.From $case.To
    Invoke-Fixture 1 $case.Expected $case.Label
    Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/dev-runbook.md") -Value $original -NoNewline
  }

  $original = Replace-Once "docs/dev-runbook.md" "Fable's code/mixed/runtime-contract lane remains defined" "Fable may choose a narrower lane"
  Invoke-Fixture 1 "shared Fable validation lane changed" "shared Fable validation lane preserved"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/dev-runbook.md") -Value $original -NoNewline

  $original = Replace-Once "docs/code-quality.md" "provenance-independent golden vectors" "implementation-derived expected values"
  Invoke-Fixture 1 "shared scientific obligation missing: provenance-independent golden vectors" "shared Fable scientific obligation preserved"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/code-quality.md") -Value $original -NoNewline

  $templatePath = Join-Path $fixtureRoot "docs/templates/non-trivial-change-template.md"
  $baseTemplate = Get-Content -LiteralPath $templatePath -Raw
  foreach ($case in @(
    @{ Text="`nSkill check: selected anything`n"; Expected="retired skill marker remains"; Label="skill marker retired" },
    @{ Text="`nValidation Score: 6/6`n"; Expected="retired numeric validation score remains"; Label="numeric score retired" },
    @{ Text="`nSA1 PASS`n"; Expected="retired SA taxonomy remains"; Label="SA taxonomy retired" },
    @{ Text="`nSELF-AUDIT`n"; Expected="retired Self-Audit reporting remains"; Label="Self-Audit reporting retired" }
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

  $leakSkillPath = Join-Path $fixtureRoot "codex-skills/go-leak-detection/SKILL.md"
  $original = Get-Content -LiteralPath $leakSkillPath -Raw
  Set-Content -LiteralPath $leakSkillPath -Value ($original + "`nVerification command reporting`n") -NoNewline
  Invoke-SkillFixture 1 "obsolete command-evidence reference" "obsolete specialist integration cannot return"
  Set-Content -LiteralPath $leakSkillPath -Value $original -NoNewline

  $recipePath = "docs/runbooks/codex-triggered-validation-tools.md"
  $original = Replace-Once $recipePath "material command`nevidence required by" '`Verification command reporting` required by'
  Invoke-Fixture 1 "obsolete command-evidence reference remains" "obsolete validation-recipe integration cannot return"
  Set-Content -LiteralPath (Join-Path $fixtureRoot $recipePath) -Value $original -NoNewline

  $original = Replace-Once "docs/decisions/ADR-0221-codex-authority-and-evidence-workflow.md" "Selective supersession: ADR-0222" "Historical note: ADR-0222"
  Invoke-Fixture 1 "ADR-0221 selective reciprocal note missing" "ADR selective link remains reciprocal"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/decisions/ADR-0221-codex-authority-and-evidence-workflow.md") -Value $original -NoNewline

  Invoke-Fixture 1 "protected Fable or release path changed: CLAUDE.md" "Fable path protected" @("CLAUDE.md")
  Invoke-Fixture 1 "protected Fable or release path changed: scripts/create-release.ps1" "release script protected" @("scripts/create-release.ps1")

  & $engine -NoProfile -File $checker -RepoRoot $repoRoot
  if ($LASTEXITCODE -ne 0) { throw "current repository contract failed checker" }
  Write-Host "PASS current repository contract"
  Write-Host "PASS all workflow-contract fixtures passed."
} finally {
  Remove-Item -LiteralPath $fixtureRoot -Recurse -Force -ErrorAction SilentlyContinue
}

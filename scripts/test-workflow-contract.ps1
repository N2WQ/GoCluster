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
  $content = (Get-Content -LiteralPath $path -Raw).Replace("`r`n", "`n")
  $count = ([regex]::Matches($content, [regex]::Escape($From))).Count
  if ($count -ne 1) { throw "$RelativePath mutation expected one occurrence of '$From', found $count" }
  Set-Content -LiteralPath $path -Value $content.Replace($From, $To) -NoNewline
  return $content
}

function Replace-All([string]$RelativePath, [string]$From, [string]$To) {
  $path = Join-Path $fixtureRoot $RelativePath
  $content = (Get-Content -LiteralPath $path -Raw).Replace("`r`n", "`n")
  $count = ([regex]::Matches($content, [regex]::Escape($From))).Count
  if ($count -lt 1) { throw "$RelativePath mutation expected at least one occurrence of '$From'" }
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

function Invoke-ApprovedChangedPathsFixture() {
  $checkerArg = $checker.Replace("'", "''")
  $rootArg = $fixtureRoot.Replace("'", "''")
  $command = "& '$checkerArg' -RepoRoot '$rootArg' -ChangedPaths @('scripts/check-workflow-contract.ps1','scripts/test-workflow-contract.ps1')"
  $output = (& $engine -NoProfile -Command $command 2>&1 | Out-String)
  $actual = $LASTEXITCODE
  if ($actual -ne 0) { throw "approved workflow-script paths accepted expected exit 0, got $actual`n$output" }
  if (-not $output.Contains("PASS Codex workflow static invariants passed.")) {
    throw "approved workflow-script paths accepted missing checker PASS`n$output"
  }
  Write-Host "PASS approved workflow-script paths accepted"
}

function Invoke-ContextMeasurementSelectionFixtures() {
  $measurementPaths = @(
    'scripts/measure-codex-workflow-context.ps1',
    'scripts/test-measure-codex-workflow-context.ps1'
  )
  foreach ($case in @(
    @{ Paths=@('AGENTS.md'); Expected=$false; Label='unrelated workflow change skips context measurement' },
    @{ Paths=@('scripts/measure-codex-workflow-context.ps1'); Expected=$true; Label='measurement implementation triggers context measurement' },
    @{ Paths=@('scripts/test-measure-codex-workflow-context.ps1'); Expected=$true; Label='measurement fixture triggers context measurement' }
  )) {
    $changed = $case.Paths | ForEach-Object { $_.Replace('\', '/') } |
      Where-Object { $_ -in $measurementPaths }
    $actual = $null -ne $changed
    if ($actual -ne $case.Expected) { throw "$($case.Label) expected $($case.Expected), got $actual" }
    Write-Host "PASS $($case.Label)"
  }
}

try {
  foreach ($path in @(
    "AGENTS.md", "VALIDATION.md", "docs/change-workflow.md",
    "docs/code-quality.md", "docs/domain-contract.md", "docs/review-checklist.md", "docs/dev-runbook.md",
    "docs/WORKING_WITH_CODEX.md", "docs/decision-memory.md",
    "docs/decision-log.md",
    ".github/workflows/ci.yml",
    ".github/workflows/codex-workflow-contract.yml",
    ".github/workflows/nightly-race.yml",
    "docs/decisions/ADR-0221-codex-authority-and-evidence-workflow.md",
    "docs/decisions/ADR-0222-corrective-codex-authority-and-validation.md",
    "docs/decisions/ADR-0223-bounded-specialist-context-and-independent-evidence.md",
    "docs/decisions/ADR-0224-evidence-before-scope-and-material-reapproval.md",
    "docs/decisions/ADR-0225-remove-codex-target-reasoning-recommendation.md",
    "docs/decisions/ADR-0227-push-to-main-ci-validation-backstops.md",
    "docs/decisions/ADR-0228-corrective-ci-enforcement.md",
    "docs/templates/non-trivial-change-template.md",
    "docs/runbooks/codex-workflow-checks.md",
    "docs/runbooks/codex-triggered-validation-tools.md",
    "docs/workflow-eval-cases.md", "codex-skills", "scripts/README.md"
  )) { Copy-ItemTree $path }

  $fixtureAgents = Join-Path $fixtureRoot "AGENTS.md"
  $agentsLF = (Get-Content -LiteralPath $fixtureAgents -Raw).Replace("`r`n", "`n")
  Set-Content -LiteralPath $fixtureAgents -Value $agentsLF.Replace("`n", "`r`n") -NoNewline

  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "positive contract"
  Invoke-SkillFixture 0 "PASS all requested repo skills verified." "positive skill methods"
  Write-Host "PASS CRLF fixture source accepted"
  Invoke-ContextMeasurementSelectionFixtures

  $original = Replace-Once ".github/workflows/ci.yml" "on:`n  push:" "on:`n  pull_request:`n  push:"
  Invoke-Fixture 1 "CI pull-request trigger remains" "CI rejects pull-request trigger"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/ci.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/ci.yml" "  workflow_dispatch:" "  disabled_dispatch:"
  Invoke-Fixture 1 "CI push/manual triggers missing" "CI requires manual dispatch"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/ci.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/ci.yml" "fetch-depth: 0" "fetch-depth: 1"
  Invoke-Fixture 1 "CI full-history checkout missing" "CI requires full history"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/ci.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/ci.yml" "staticcheck@v0.7.0" "staticcheck@latest"
  Invoke-Fixture 1 "CI Staticcheck uses latest" "CI rejects unpinned Staticcheck"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/ci.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/ci.yml" "actionlint@v1.7.12" "actionlint@latest"
  Invoke-Fixture 1 "CI Actionlint uses latest" "CI rejects unpinned Actionlint"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/ci.yml") -Value $original -NoNewline

  foreach ($case in @(
    @{ From='go run ./cmd/codemap check -all'; To='go run ./cmd/codemap list'; Expected='CI required command missing: go run ./cmd/codemap check -all'; Label='CI requires code-map check' },
    @{ From='go test ./...'; To='go test ./cmd/codemap'; Expected='CI required command missing: go test ./...'; Label='CI requires full tests' },
    @{ From='go vet ./...'; To='go vet ./cmd/codemap'; Expected='CI required command missing: go vet ./...'; Label='CI requires full vet' },
    @{ From='staticcheck ./...'; To='staticcheck ./cmd/codemap'; Expected='CI required command missing: staticcheck ./...'; Label='CI requires full Staticcheck' },
    @{ From='golangci-lint run ./... --config=.golangci.yaml'; To='golangci-lint run ./cmd/codemap'; Expected='CI required command missing: golangci-lint run ./... --config=.golangci.yaml'; Label='CI requires configured lint' },
    @{ From='actionlint .github/workflows/*.yml'; To='actionlint .github/workflows/ci.yml'; Expected='CI Actionlint command missing'; Label='CI requires all workflow syntax checks' }
  )) {
    $original = Replace-Once ".github/workflows/ci.yml" $case.From $case.To
    Invoke-Fixture 1 $case.Expected $case.Label
    Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/ci.yml") -Value $original -NoNewline
  }

  $original = Replace-Once ".github/workflows/ci.yml" 'base="HEAD^"' 'base="HEAD"'
  Invoke-Fixture 1 "CI pushed-range safeguard missing" "manual CI requires HEAD parent range"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/ci.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/codex-workflow-contract.yml" "git fetch --no-tags --depth=1 origin `$env:BEFORE_SHA" "Write-Host 'baseline unavailable'"
  Invoke-Fixture 1 "Codex contract workflow requirement missing" "contract CI requires explicit baseline fetch"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/codex-workflow-contract.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/codex-workflow-contract.yml" ".github/workflows/**" ".github/workflows/ci.yml"
  Invoke-Fixture 1 "Codex contract path ownership missing: .github/workflows/**" "contract CI owns all workflow files"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/codex-workflow-contract.yml") -Value $original -NoNewline

  foreach ($path in @('scripts/measure-codex-workflow-context.ps1','scripts/test-measure-codex-workflow-context.ps1')) {
    $original = Replace-Once ".github/workflows/codex-workflow-contract.yml" "      - $path" "      - disabled/$path"
    Invoke-Fixture 1 "Codex contract path ownership missing: $path" "contract CI owns measurement path $path"
    Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/codex-workflow-contract.yml") -Value $original -NoNewline

    $original = Replace-Once ".github/workflows/codex-workflow-contract.yml" "            '$path'" "            'scripts/disabled-context-measurement.ps1'"
    Invoke-Fixture 1 "conditional context-measurement path missing: $path" "measurement detector owns $path"
    Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/codex-workflow-contract.yml") -Value $original -NoNewline
  }

  $measurementCondition = "        if: steps.context_measurement.outputs.changed == 'true'"
  $original = Replace-Once ".github/workflows/codex-workflow-contract.yml" $measurementCondition ""
  Invoke-Fixture 1 "context-measurement fixture is unconditional" "measurement fixture cannot become unconditional"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/codex-workflow-contract.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/codex-workflow-contract.yml" "runs-on: windows-latest" "runs-on: ubuntu-latest"
  Invoke-Fixture 1 "Codex contract Windows runner missing" "contract CI requires Windows runner"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/codex-workflow-contract.yml") -Value $original -NoNewline

  $original = Replace-All ".github/workflows/codex-workflow-contract.yml" "shell: pwsh" "shell: bash"
  Invoke-Fixture 1 "Codex contract PowerShell invocation missing" "contract CI requires PowerShell"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/codex-workflow-contract.yml") -Value $original -NoNewline

  foreach ($case in @(
    @{ From='run: ./scripts/check-workflow-contract.ps1 -BaselineRevision $env:BEFORE_SHA'; To='run: Write-Host skipped'; Label='contract CI requires contract checker' },
    @{ From='run: ./scripts/test-workflow-contract.ps1'; To='run: Write-Host skipped'; Label='contract CI requires contract fixtures' },
    @{ From='          ./scripts/test-measure-codex-workflow-context.ps1'; To='          Write-Host skipped'; Label='contract CI retains conditional measurement fixtures' },
    @{ From='run: ./scripts/verify-codex-skills.ps1'; To='run: Write-Host skipped'; Label='contract CI requires skill verification' }
  )) {
    $original = Replace-Once ".github/workflows/codex-workflow-contract.yml" $case.From $case.To
    Invoke-Fixture 1 "Codex contract workflow requirement missing" $case.Label
    Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/codex-workflow-contract.yml") -Value $original -NoNewline
  }

  foreach ($workflowPath in @('.github/workflows/ci.yml','.github/workflows/codex-workflow-contract.yml','.github/workflows/nightly-race.yml')) {
    $original = Replace-Once $workflowPath "  contents: read" "  contents: write"
    Invoke-Fixture 1 "write permission is not allowed" "$workflowPath rejects contents write"
    Set-Content -LiteralPath (Join-Path $fixtureRoot $workflowPath) -Value $original -NoNewline
  }

  $original = Replace-Once ".github/workflows/ci.yml" "  contents: read" "  contents: read`n  actions: write"
  Invoke-Fixture 1 "write permission is not allowed" "CI rejects additive workflow write permission"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/ci.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/ci.yml" "jobs:`n  test-and-lint:" "jobs:`n  test-and-lint:`n    permissions:`n      actions: write"
  Invoke-Fixture 1 "write permission is not allowed" "CI rejects job-level write permission"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/ci.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/ci.yml" "permissions:`n  contents: read" "permissions: write-all"
  Invoke-Fixture 1 "write permission is not allowed" "CI rejects write-all permissions"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/ci.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/nightly-race.yml" "go test -race -count=1 ./..." "go test ./..."
  Invoke-Fixture 1 "nightly race command missing" "nightly workflow requires uncached full race command"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/nightly-race.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/nightly-race.yml" '- cron: "17 7 * * *"' '- cron: "17 8 * * *"'
  Invoke-Fixture 1 "nightly race schedule/manual triggers missing" "nightly workflow requires documented schedule"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/nightly-race.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/nightly-race.yml" "  workflow_dispatch:" "  disabled_dispatch:"
  Invoke-Fixture 1 "nightly race schedule/manual triggers missing" "nightly workflow requires manual dispatch"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/nightly-race.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/nightly-race.yml" "go-version-file: go.mod" "go-version: stable"
  Invoke-Fixture 1 "nightly race go.mod toolchain missing" "nightly workflow requires go.mod toolchain"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/nightly-race.yml") -Value $original -NoNewline

  $original = Replace-Once ".github/workflows/nightly-race.yml" "timeout-minutes: 60" "timeout-minutes: 60`n    continue-on-error: true"
  Invoke-Fixture 1 "CI continue-on-error is not allowed" "CI cannot tolerate required-check failures"
  Set-Content -LiteralPath (Join-Path $fixtureRoot ".github/workflows/nightly-race.yml") -Value $original -NoNewline

  $original = Replace-Once "docs/dev-runbook.md" "CI path filters and filenames cannot determine semantic engineering risk." "CI paths determine semantic engineering risk."
  Invoke-Fixture 1 "CI semantic-risk limitation missing" "CI cannot infer semantic risk"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/dev-runbook.md") -Value $original -NoNewline

  $original = Replace-Once "docs/decision-log.md" "| ADR-0228 | Corrective CI Enforcement |" "| ADR-0999 | Corrective CI Enforcement |"
  Invoke-Fixture 1 "ADR-0228 decision-index row missing" "ADR-0228 remains indexed"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/decision-log.md") -Value $original -NoNewline

  foreach ($case in @(
    @{ Text="Target reasoning: low (lowest sufficient)."; Label="target reasoning requirement cannot return" },
    @{ Text="Recommended model effort: low."; Label="replacement model-effort field cannot return" }
  )) {
    $original = Replace-Once "docs/templates/non-trivial-change-template.md" "Scope challenge:" "$($case.Text)`nScope challenge:"
    Invoke-Fixture 1 "retired target-reasoning requirement remains" $case.Label
    Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/templates/non-trivial-change-template.md") -Value $original -NoNewline
  }

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

  $original = Replace-Once "AGENTS.md" "runtime config, schema" "runtime config, but may change parser behavior; schema"
  Invoke-Fixture 1 "sensitive Small exclusion contains a positive inversion" "AGENTS sensitive exclusion rejects mixed inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "runtime config, schema" "runtime config, however can change parser behavior; schema"
  Invoke-Fixture 1 "detailed sensitive Small exclusion contains a positive inversion" "workflow sensitive exclusion rejects mixed inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "parser behavior" "must never change parser behavior"
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "AGENTS sensitive exclusion allows explicitly negated must"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

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

  $original = Replace-Once "AGENTS.md" "expand scope" "but could expand scope"
  Invoke-Fixture 1 "standing authorization contains a positive inversion" "AGENTS standing authorization rejects mixed inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "expand scope" "yet is permitted to expand scope"
  Invoke-Fixture 1 "detailed standing authorization contains a positive inversion" "workflow standing authorization rejects mixed inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "expand scope" "shall not expand scope"
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "workflow standing authorization allows explicitly negated shall"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" 'Only exact `Approved vN` authorizes the matching agreed scope.' 'Discussion or requests to proceed authorize the matching agreed scope.'
  Invoke-Fixture 1 "approval authority route missing" "approval cannot be loosened"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $standardFrom = "Do not invoke specialists, parallel discovery, or`nindependent agents merely because work is Non-trivial."
  $original = Replace-Once "docs/change-workflow.md" $standardFrom "Use specialists by default for Non-trivial work."
  Invoke-Fixture 1 "Standard route does not reject default specialists" "Standard rejects default specialists"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "use a bounded`nsubagent when supported" "keep the investigation`nlead-owned when supported"
  Invoke-Fixture 1 "positive context-partitioning route missing" "valuable context partitioning routes to bounded subagent"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "use a bounded`nsubagent when supported" "do not use a bounded`nsubagent when supported"
  Invoke-Fixture 1 "positive context-partitioning route contains a negative inversion" "AGENTS context route rejects direct inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "use a bounded`nsubagent when supported." "use a bounded`nsubagent when supported. Never use a bounded subagent when supported."
  Invoke-Fixture 1 "positive context-partitioning route contains a negative inversion" "AGENTS context route rejects mixed inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "use a bounded subagent when supported" "do not use a bounded subagent when supported"
  Invoke-Fixture 1 "detailed positive context-partitioning route contains a negative inversion" "workflow context route rejects direct inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "use a bounded subagent when supported so" "use a bounded subagent when supported, but never use a bounded subagent when supported, so"
  Invoke-Fixture 1 "detailed positive context-partitioning route contains a negative inversion" "workflow context route rejects mixed inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  foreach ($case in @(
    @{ Path="AGENTS.md"; From="use a bounded`nsubagent when supported"; To="the lead may not use a bounded`nsubagent when supported"; Expected="positive context-partitioning route contains a negative inversion"; Label="AGENTS context route rejects may-not inversion" },
    @{ Path="AGENTS.md"; From="use a bounded`nsubagent when supported"; To="need not use a bounded`nsubagent when supported"; Expected="positive context-partitioning route contains a negative inversion"; Label="AGENTS context route rejects need-not inversion" },
    @{ Path="AGENTS.md"; From="use a bounded`nsubagent when supported"; To="is not required to use a bounded`nsubagent when supported"; Expected="positive context-partitioning route contains a negative inversion"; Label="AGENTS context route rejects not-required inversion" },
    @{ Path="docs/change-workflow.md"; From="use a bounded subagent when supported"; To="the lead may not use a bounded subagent when supported"; Expected="detailed positive context-partitioning route contains a negative inversion"; Label="workflow context route rejects may-not inversion" },
    @{ Path="docs/change-workflow.md"; From="use a bounded subagent when supported"; To="need not use a bounded subagent when supported"; Expected="detailed positive context-partitioning route contains a negative inversion"; Label="workflow context route rejects need-not inversion" },
    @{ Path="docs/change-workflow.md"; From="use a bounded subagent when supported"; To="is not required to use a bounded subagent when supported"; Expected="detailed positive context-partitioning route contains a negative inversion"; Label="workflow context route rejects not-required inversion" }
  )) {
    $original = Replace-Once $case.Path $case.From $case.To
    Invoke-Fixture 1 $case.Expected $case.Label
    Set-Content -LiteralPath (Join-Path $fixtureRoot $case.Path) -Value $original -NoNewline
  }

  $original = Replace-Once "AGENTS.md" "use a bounded`nsubagent when supported." "use a bounded`nsubagent when supported. Do not use a bounded subagent when the platform does not support it."
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "AGENTS context route accepts unsupported-platform caveat"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "use a bounded subagent when supported so" "use a bounded subagent when supported so broad evidence is partitioned. Do not use a bounded subagent when the platform does not support it. The lead still ensures"
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "workflow context route accepts unsupported-platform caveat"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "use a bounded`nsubagent when supported" "delegate the investigation to a bounded`nsubagent when the platform supports it"
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "context route accepts equivalent positive wording"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "A`nfresh lead pass is not independent review." "A`nfresh lead pass is independent review."
  Invoke-Fixture 1 "fresh lead pass incorrectly substitutes for independent review" "fresh lead pass is not independent review"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "A`nfresh lead pass is not independent review." "A`nfresh lead pass is not independent review.`nIt may be reported as independent review."
  Invoke-Fixture 1 "fresh lead pass contains a positive independent-review inversion" "AGENTS fresh lead rule rejects mixed inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "fresh pass and must not describe it as independent." "fresh pass and may be reported as independent review."
  Invoke-Fixture 1 "detailed fresh lead pass incorrectly substitutes for independent review" "workflow fresh lead rule rejects direct inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" "fresh pass and must not describe it as independent." "fresh pass and must not describe it as independent.`nThe fresh pass may be reported as independent review."
  Invoke-Fixture 1 "detailed fresh lead pass contains a positive independent-review inversion" "workflow fresh lead rule rejects mixed inversion"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "AGENTS.md" "A`nfresh lead pass is not independent review." "A`nfresh lead review does not constitute independent review."
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "fresh lead rule accepts equivalent positive wording"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "AGENTS.md") -Value $original -NoNewline

  $requiredIndependence = "When required independent context is unavailable, pause or proceed only with`nexplicit user approval and clearly limit the affected claim."
  $original = Replace-Once "docs/change-workflow.md" $requiredIndependence "When required independent context is unavailable, the lead may silently substitute its own review."
  Invoke-Fixture 1 "required independent evidence substitution boundary missing" "required independence cannot be silently replaced"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" $requiredIndependence ($requiredIndependence + " The lead may`nsilently substitute its own review.")
  Invoke-Fixture 1 "required independent evidence boundary permits lead substitution" "required independence rejects mixed lead substitution"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $original = Replace-Once "docs/change-workflow.md" $requiredIndependence "If required independent context cannot be obtained, stop unless the user explicitly approves proceeding with a limited claim."
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "required independence accepts equivalent positive wording"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  foreach ($case in @(
    @{ Text="The lead is permitted to substitute its own review."; Label="required independence rejects permitted substitution" },
    @{ Text="Lead-owned review may replace the unavailable independent review."; Label="required independence rejects replacement" },
    @{ Text="Lead-owned review may stand in for the independent review."; Label="required independence rejects stand-in substitution" }
  )) {
    $original = Replace-Once "docs/change-workflow.md" $requiredIndependence ($requiredIndependence + " " + $case.Text)
    Invoke-Fixture 1 "required independent evidence boundary permits lead substitution" $case.Label
    Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline
  }

  foreach ($case in @(
    @{ Text="Lead-owned review may not replace the unavailable independent review."; Label="required independence accepts prohibited replacement" },
    @{ Text="The lead is not authorized to substitute its own review."; Label="required independence accepts prohibited authorization" }
  )) {
    $original = Replace-Once "docs/change-workflow.md" $requiredIndependence ($requiredIndependence + " " + $case.Text)
    Invoke-Fixture 0 "PASS Codex workflow static invariants passed." $case.Label
    Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline
  }

  $original = Replace-Once "docs/change-workflow.md" "Uncertainty that could affect safety,`nscope, validation, or compatibility is High-risk until resolved." "Uncertainty may remain Standard."
  Invoke-Fixture 1 "uncertainty High-risk route missing" "uncertainty cannot silently remain Standard"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/change-workflow.md") -Value $original -NoNewline

  $sharedImpact = "Changes to multiple production`npackages also trigger the Go code-quality review method when shared behavior,`nownership, interfaces, contracts, or meaningful cross-package uncertainty are`naffected."
  $original = Replace-Once "docs/change-workflow.md" $sharedImpact "Changes to multiple production packages always trigger the Go code-quality review method."
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

  $original = Replace-Once "docs/decisions/ADR-0223-bounded-specialist-context-and-independent-evidence.md" "It`nselectively supersedes ADR-0222 Decision 7's interpretation" "It`nis related to ADR-0222 Decision 7's interpretation"
  Invoke-Fixture 1 "ADR-0223 selective authority missing" "ADR-0223 selective authority remains explicit"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/decisions/ADR-0223-bounded-specialist-context-and-independent-evidence.md") -Value $original -NoNewline

  $original = Replace-Once "docs/decisions/ADR-0222-corrective-codex-authority-and-validation.md" "ADR-0225 supersedes Decision 1's target reasoning recommendation." "ADR-0225 is related to Decision 1's former recommendation."
  Invoke-Fixture 1 "ADR-0222 ADR-0225 reciprocal note missing" "ADR-0222 records ADR-0225 supersession"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/decisions/ADR-0222-corrective-codex-authority-and-validation.md") -Value $original -NoNewline

  $original = Replace-Once "docs/decisions/ADR-0225-remove-codex-target-reasoning-recommendation.md" "1. This decision selectively supersedes ADR-0222 Decision 1." "1. This decision is related to ADR-0222 Decision 1."
  Invoke-Fixture 1 "ADR-0225 selective authority missing" "ADR-0225 selective authority remains explicit"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/decisions/ADR-0225-remove-codex-target-reasoning-recommendation.md") -Value $original -NoNewline

  $original = Replace-Once "docs/decision-log.md" 'ADR-0222 Decision 1 | - | `docs/decisions/ADR-0225-remove-codex-target-reasoning-recommendation.md`' 'ADR-0222 | - | `docs/decisions/ADR-0225-remove-codex-target-reasoning-recommendation.md`'
  Invoke-Fixture 1 "ADR-0225 decision-index row missing" "ADR-0225 decision-index supersession remains explicit"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/decision-log.md") -Value $original -NoNewline

  $original = Replace-Once "docs/decision-log.md" "ADR-0223 (refines Decision 2)" "ADR-0223 Decision 2"
  Invoke-Fixture 1 "ADR-0221 reciprocal decision-index link missing" "ADR-0221 refinement direction remains explicit"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/decision-log.md") -Value $original -NoNewline

  $original = Replace-Once "docs/decision-log.md" "ADR-0224 (refines evidence/planning)" "ADR-0224 (refines evidence/planning); ADR-0999 (additional refinement)"
  Invoke-Fixture 0 "PASS Codex workflow static invariants passed." "ADR-0221 reciprocal link allows additional refinements"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/decision-log.md") -Value $original -NoNewline

  $original = Replace-Once "docs/decision-log.md" "ADR-0221 Decision 2 (refines); ADR-0222 Decision 7" "ADR-0221 Decision 2; ADR-0222 Decision 7"
  Invoke-Fixture 1 "ADR-0223 decision-index row missing" "ADR-0223 refinement direction remains explicit"
  Set-Content -LiteralPath (Join-Path $fixtureRoot "docs/decision-log.md") -Value $original -NoNewline

  Invoke-ApprovedChangedPathsFixture

  Invoke-Fixture 1 "protected Fable or release path changed: CLAUDE.md" "Fable path protected" @("CLAUDE.md")
  Invoke-Fixture 1 "protected Fable or release path changed: scripts/create-release.ps1" "release script protected" @("scripts/create-release.ps1")

  & $engine -NoProfile -File $checker -RepoRoot $repoRoot
  if ($LASTEXITCODE -ne 0) { throw "current repository contract failed checker" }
  Write-Host "PASS current repository contract"
  Write-Host "PASS all workflow-contract fixtures passed."
} finally {
  Remove-Item -LiteralPath $fixtureRoot -Recurse -Force -ErrorAction SilentlyContinue
}

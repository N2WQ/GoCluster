<#
.SYNOPSIS
  Test informational Codex context measurement against immutable Git objects.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script = Join-Path $PSScriptRoot 'measure-codex-workflow-context.ps1'
$root = Join-Path ([IO.Path]::GetTempPath()) ('gocluster-context-' + [guid]::NewGuid().ToString('N'))
$engine = (Get-Process -Id $PID).Path

function Invoke-Measurement([string]$Baseline, [string]$Candidate, [switch]$Json) {
  $args = @('-NoProfile','-File',$script,'-BaselineRevision',$Baseline,'-CandidateRevision',$Candidate,'-RepoRoot',$root)
  if ($Json) { $args += '-AsJson' }
  $priorPreference=$ErrorActionPreference
  $ErrorActionPreference='Continue'
  try { $output = (& $engine @args 2>&1 | Out-String) } finally { $ErrorActionPreference=$priorPreference }
  return @{ ExitCode=$LASTEXITCODE; Output=$output }
}

function Assert-Result([hashtable]$Result, [int]$Exit, [string]$Pattern, [string]$Label) {
  if ($Result.ExitCode -ne $Exit) { throw "$Label exit=$($Result.ExitCode), expected=$Exit`n$($Result.Output)" }
  if ($Pattern -and $Result.Output -notmatch $Pattern) { throw "$Label missing '$Pattern'`n$($Result.Output)" }
  Write-Host "PASS $Label"
}

try {
  New-Item -ItemType Directory -Path $root | Out-Null
  git -C $root init -q
  git -C $root config user.email test@example.invalid
  git -C $root config user.name test

  $core=@('AGENTS.md','docs/change-workflow.md','docs/templates/non-trivial-change-template.md','docs/code-quality.md','docs/review-checklist.md','VALIDATION.md','docs/dev-runbook.md','codex-skills/README.md','docs/runbooks/codex-triggered-validation-tools.md')
  $skills=@('decision-memory-audit','workflow-contract-audit','requirements-ambiguity-review','scientific-model-oracle','design-challenger','scope-ledger-adversarial-review','test-strategy-adversary','go-code-quality-review','go-code-walk','go-blast-radius-audit','go-config-contract-audit','go-connection-lifecycle-audit','go-leak-detection','go-retained-state-audit','pprof-impact-review')
  $paths=$core + @($skills | ForEach-Object { "codex-skills/$_/SKILL.md" })

  $weight=1
  foreach($path in $paths) {
    $target=Join-Path $root $path
    New-Item -ItemType Directory -Force -Path (Split-Path $target) | Out-Null
    Set-Content -LiteralPath $target -Value ('word ' * $weight) -NoNewline
    $weight++
  }
  git -C $root add .; git -C $root commit -qm baseline
  $baseline=(git -C $root rev-parse HEAD).Trim()

  foreach($path in $paths) {
    $current=Get-Content -Raw -LiteralPath (Join-Path $root $path)
    Set-Content -LiteralPath (Join-Path $root $path) -Value ($current + $current) -NoNewline
  }
  git -C $root add .; git -C $root commit -qm growth
  $growth=(git -C $root rev-parse HEAD).Trim()

  $result=Invoke-Measurement $baseline $growth -Json
  Assert-Result $result 0 '"Scenario"' 'growth remains informational'
  $jsonStart=$result.Output.IndexOf('[')
  $jsonEnd=$result.Output.LastIndexOf(']')
  if($jsonStart -lt 0 -or $jsonEnd -lt $jsonStart){throw "measurement JSON not found`n$($result.Output)"}
  $rows=$result.Output.Substring($jsonStart,$jsonEnd-$jsonStart+1) | ConvertFrom-Json
  foreach($name in @('always-loaded','standard-planning','standard-execution-closeout','high-risk-planning','high-risk-execution-closeout')) {
    if ($rows.Scenario -notcontains $name) { throw "missing scenario $name`n$($result.Output)" }
  }
  foreach($skill in $skills) {
    if ($rows.Scenario -notcontains "specialist-$skill") { throw "missing specialist scenario $skill" }
  }
  foreach($row in $rows) {
    if ($row.CandidateWords -ne 2 * $row.BaselineWords) { throw "incorrect word total for $($row.Scenario)" }
    if ($row.WordDelta -ne $row.BaselineWords) { throw "incorrect word delta for $($row.Scenario)" }
  }
  if ($result.Output -match 'gate:|Mean reduction|threshold') { throw 'diagnostic output contains an adoption gate' }
  Write-Host 'PASS exact per-scenario totals'

  Set-Content -LiteralPath (Join-Path $root 'AGENTS.md') -Value ('dirty ' * 1000) -NoNewline
  Assert-Result (Invoke-Measurement $baseline $growth) 0 'informational' 'dirty worktree cannot affect pinned revisions'

  git -C $root reset --hard -q $growth
  git -C $root rm -q AGENTS.md; git -C $root commit -qm missing
  $missing=(git -C $root rev-parse HEAD).Trim()
  Assert-Result (Invoke-Measurement $baseline $missing) 1 'cannot read Git blob' 'missing blob fails'

  git -C $root reset --hard -q $growth
  [IO.File]::WriteAllBytes((Join-Path $root 'AGENTS.md'), [byte[]](0xC3,0x28))
  git -C $root add AGENTS.md; git -C $root commit -qm invalid
  $invalid=(git -C $root rev-parse HEAD).Trim()
  Assert-Result (Invoke-Measurement $baseline $invalid) 1 'Unable to translate bytes' 'invalid UTF-8 fails'

  Write-Host 'PASS informational context measurement fixtures'
} finally {
  if(Test-Path $root){Remove-Item -LiteralPath $root -Recurse -Force}
}

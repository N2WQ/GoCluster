<#
.SYNOPSIS
  Test deterministic Codex context measurement against temporary Git objects.
.DESCRIPTION
  Builds baseline/candidate commits containing the production manifest paths.
  Exercises the success gate, pinned-revision isolation, missing and invalid
  blobs, candidate-only components, and the fixed reduction threshold.
.NOTES
  Side effects: creates and removes one temporary Git repository.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script = Join-Path $PSScriptRoot 'measure-codex-workflow-context.ps1'
$root = Join-Path ([IO.Path]::GetTempPath()) ('gocluster-context-' + [guid]::NewGuid().ToString('N'))

function Invoke-Measurement {
  Param([string]$Baseline, [string]$Candidate)
  $hostExe = (Get-Process -Id $PID).Path
  $priorPreference = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    $output = & $hostExe -NoProfile -ExecutionPolicy Bypass -File $script -BaselineRevision $Baseline -CandidateRevision $Candidate -RepoRoot $root 2>&1
  } finally {
    $ErrorActionPreference = $priorPreference
  }
  return @{ ExitCode=$LASTEXITCODE; Output=($output -join "`n") }
}

function Assert-Exit {
  Param([hashtable]$Result, [int]$Expected, [string]$Label, [string]$Pattern='')
  if ($Result.ExitCode -ne $Expected) { throw "$Label exit=$($Result.ExitCode), expected=$Expected`n$($Result.Output)" }
  if ($Pattern -and $Result.Output -notmatch $Pattern) { throw "$Label missing output pattern '$Pattern'`n$($Result.Output)" }
}

try {
  New-Item -ItemType Directory -Path $root | Out-Null
  git -C $root init -q
  git -C $root config user.email test@example.invalid
  git -C $root config user.name test
  $paths=@('AGENTS.md','docs/change-workflow.md','docs/templates/non-trivial-change-template.md','docs/review-checklist.md','VALIDATION.md','docs/dev-runbook.md')
  $skills=@('design-challenger','go-code-quality-review','requirements-ambiguity-review','scientific-model-oracle','scope-ledger-adversarial-review','test-strategy-adversary')
  $paths += $skills | ForEach-Object { "codex-skills/$_/SKILL.md" }
  foreach($path in $paths){$target=Join-Path $root $path; New-Item -ItemType Directory -Force -Path (Split-Path $target) | Out-Null; Set-Content -LiteralPath $target -Value (('baseline words ' * 80) + "`n") -NoNewline}
  git -C $root add .; git -C $root commit -qm baseline
  $baseline=(git -C $root rev-parse HEAD).Trim()
  foreach($path in $paths){Set-Content -LiteralPath (Join-Path $root $path) -Value (('short words ' * 20) + "`n") -NoNewline}
  foreach($path in @('docs/runbooks/codex-workflow-checks.md','docs/runbooks/codex-triggered-validation-tools.md')){$target=Join-Path $root $path; New-Item -ItemType Directory -Force -Path (Split-Path $target) | Out-Null; Set-Content -LiteralPath $target -Value "route only`n" -NoNewline}
  git -C $root add .; git -C $root commit -qm candidate
  $candidate=(git -C $root rev-parse HEAD).Trim()
  Assert-Exit (Invoke-Measurement $baseline $candidate) 0 'shrinking candidate' 'gate: PASS'

  Set-Content -LiteralPath (Join-Path $root 'AGENTS.md') -Value ('dirty growth ' * 1000) -NoNewline
  Assert-Exit (Invoke-Measurement $baseline $candidate) 0 'pinned candidate isolation' 'gate: PASS'

  git -C $root reset --hard -q $candidate
  git -C $root rm -q AGENTS.md
  git -C $root commit -qm missing-path
  $missing=(git -C $root rev-parse HEAD).Trim()
  Assert-Exit (Invoke-Measurement $baseline $missing) 1 'missing manifest path' 'cannot read Git blob'

  git -C $root reset --hard -q $candidate
  [IO.File]::WriteAllBytes((Join-Path $root 'AGENTS.md'), [byte[]](0xC3,0x28))
  git -C $root add AGENTS.md; git -C $root commit -qm invalid-utf8
  $invalid=(git -C $root rev-parse HEAD).Trim()
  Assert-Exit (Invoke-Measurement $baseline $invalid) 1 'invalid UTF-8 blob' 'Unable to translate bytes'

  git -C $root reset --hard -q $candidate
  Set-Content -LiteralPath (Join-Path $root 'docs/runbooks/codex-workflow-checks.md') -Value ('candidate only growth ' * 2000) -NoNewline
  git -C $root add .; git -C $root commit -qm candidate-component-growth
  $componentGrowth=(git -C $root rev-parse HEAD).Trim()
  Assert-Exit (Invoke-Measurement $baseline $componentGrowth) 1 'candidate-only component growth' 'gate: FAIL'

  git -C $root reset --hard -q $baseline
  foreach($path in @('docs/runbooks/codex-workflow-checks.md','docs/runbooks/codex-triggered-validation-tools.md')){$target=Join-Path $root $path; New-Item -ItemType Directory -Force -Path (Split-Path $target) | Out-Null; Set-Content -LiteralPath $target -Value "route only`n" -NoNewline}
  git -C $root add .; git -C $root commit -qm below-threshold
  $belowThreshold=(git -C $root rev-parse HEAD).Trim()
  Assert-Exit (Invoke-Measurement $baseline $belowThreshold) 1 'fixed reduction threshold' 'gate: FAIL'

  Write-Host 'PASS deterministic context measurement fixtures'
} finally {
  if(Test-Path $root){Remove-Item -LiteralPath $root -Recurse -Force}
}

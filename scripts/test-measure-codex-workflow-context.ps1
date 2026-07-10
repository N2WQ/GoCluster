<#
.SYNOPSIS
  Test deterministic Codex context measurement against temporary Git objects.
.DESCRIPTION
  Builds baseline/candidate commits containing the production manifest paths,
  verifies a shrinking candidate passes, and verifies dirty worktree changes do
  not affect pinned-revision measurement.
.NOTES
  Side effects: creates and removes one temporary Git repository.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script = Join-Path $PSScriptRoot 'measure-codex-workflow-context.ps1'
$root = Join-Path ([IO.Path]::GetTempPath()) ('gocluster-context-' + [guid]::NewGuid().ToString('N'))
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
  & $script -BaselineRevision $baseline -CandidateRevision $candidate -RepoRoot $root | Out-Host
  if($LASTEXITCODE -ne 0){throw 'shrinking candidate should pass'}
  Set-Content -LiteralPath (Join-Path $root 'AGENTS.md') -Value ('dirty growth ' * 1000) -NoNewline
  & $script -BaselineRevision $baseline -CandidateRevision $candidate -RepoRoot $root | Out-Null
  if($LASTEXITCODE -ne 0){throw 'pinned candidate must ignore dirty worktree'}
  Write-Host 'PASS deterministic context measurement fixtures'
} finally {
  if(Test-Path $root){Remove-Item -LiteralPath $root -Recurse -Force}
}

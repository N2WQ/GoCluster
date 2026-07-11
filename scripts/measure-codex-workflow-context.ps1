<#
.SYNOPSIS
  Report deterministic Codex instruction-context deltas between revisions.
.DESCRIPTION
  Reads strict UTF-8 Git blobs from immutable revisions and reports each
  declared scenario independently. Results are informational context-footprint
  proxies, not adoption gates, billed tokens, reasoning usage, or quality proof.
.PARAMETER BaselineRevision
  Immutable baseline Git revision.
.PARAMETER CandidateRevision
  Immutable candidate Git revision.
.PARAMETER RepoRoot
  Repository root.
.PARAMETER AsJson
  Emit machine-readable JSON instead of a table.
#>

[CmdletBinding()]
Param(
  [Parameter(Mandatory)][string]$BaselineRevision,
  [Parameter(Mandatory)][string]$CandidateRevision,
  [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
  [switch]$AsJson
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$utf8 = [Text.UTF8Encoding]::new($false, $true)
$wordPattern = '[\p{L}\p{N}_]+(?:[-/][\p{L}\p{N}_]+)*'

function Get-GitBlobBytes([string]$Revision, [string]$Path) {
  $spec = "${Revision}:$Path"
  $start = [Diagnostics.ProcessStartInfo]::new("git")
  $start.WorkingDirectory = $RepoRoot
  $start.Arguments = "cat-file blob `"$spec`""
  $start.RedirectStandardOutput = $true
  $start.RedirectStandardError = $true
  $start.UseShellExecute = $false
  $process = [Diagnostics.Process]::Start($start)
  $memory = [IO.MemoryStream]::new()
  $process.StandardOutput.BaseStream.CopyTo($memory)
  $errorText = $process.StandardError.ReadToEnd()
  $process.WaitForExit()
  if ($process.ExitCode -ne 0) { throw "cannot read Git blob ${spec}: $errorText" }
  return $memory.ToArray()
}

function Measure-Paths([string]$Revision, [string[]]$Paths) {
  if ($Paths.Count -eq 0 -or @($Paths | Sort-Object -Unique).Count -ne $Paths.Count) {
    throw "scenario paths must be non-empty and unique"
  }
  [long]$bytes=0; [long]$characters=0; [long]$words=0
  foreach ($path in $Paths) {
    $blob = Get-GitBlobBytes $Revision $path
    $value = $utf8.GetString($blob)
    if ($value.Length -gt 0 -and $value[0] -eq [char]0xFEFF) { $value = $value.Substring(1) }
    $bytes += $blob.LongLength
    $characters += $value.Length
    $words += [regex]::Matches($value, $wordPattern).Count
  }
  return @{ Words=$words; Characters=$characters; Bytes=$bytes }
}

$standardPlanning = @('AGENTS.md','docs/change-workflow.md','docs/templates/non-trivial-change-template.md')
$standardExecution = @('AGENTS.md','docs/change-workflow.md','docs/code-quality.md','docs/review-checklist.md','VALIDATION.md','docs/dev-runbook.md')
$highRiskPlanning = $standardPlanning + @('codex-skills/README.md')
$highRiskExecution = $standardExecution + @('codex-skills/README.md','docs/runbooks/codex-triggered-validation-tools.md')
$skillNames = @(
  'decision-memory-audit','workflow-contract-audit',
  'requirements-ambiguity-review','scientific-model-oracle',
  'design-challenger','scope-ledger-adversarial-review',
  'test-strategy-adversary','go-code-quality-review','go-code-walk',
  'go-blast-radius-audit','go-config-contract-audit',
  'go-connection-lifecycle-audit','go-leak-detection',
  'go-retained-state-audit','pprof-impact-review'
)
$scenarios = @(
  @{ Name='always-loaded'; Paths=@('AGENTS.md') },
  @{ Name='standard-planning'; Paths=$standardPlanning },
  @{ Name='standard-execution-closeout'; Paths=$standardExecution },
  @{ Name='high-risk-planning'; Paths=$highRiskPlanning },
  @{ Name='high-risk-execution-closeout'; Paths=$highRiskExecution }
)
foreach ($skill in $skillNames) {
  $scenarios += @{ Name="specialist-$skill"; Paths=@('AGENTS.md',"codex-skills/$skill/SKILL.md") }
}

$names = @($scenarios | ForEach-Object { $_.Name })
if (@($names | Sort-Object -Unique).Count -ne $names.Count) { throw "scenario names must be unique" }

$rows = foreach ($scenario in $scenarios) {
  $before = Measure-Paths $BaselineRevision $scenario.Paths
  $after = Measure-Paths $CandidateRevision $scenario.Paths
  [pscustomobject]@{
    Scenario = $scenario.Name
    BaselineWords = $before.Words
    CandidateWords = $after.Words
    WordDelta = $after.Words - $before.Words
    BaselineBytes = $before.Bytes
    CandidateBytes = $after.Bytes
    ByteDelta = $after.Bytes - $before.Bytes
    CandidateCharacters = $after.Characters
  }
}

if ($AsJson) {
  $rows | ConvertTo-Json -Depth 3
} else {
  $rows | Format-Table -AutoSize
  Write-Host "INFO context-footprint results are informational; growth or reduction does not decide whether a rule is useful."
}
exit 0

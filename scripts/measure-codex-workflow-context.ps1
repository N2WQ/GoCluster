<#
.SYNOPSIS
  Compare deterministic Codex instruction-context manifests between revisions.
.DESCRIPTION
  Reads strict UTF-8 Git blobs from immutable baseline and candidate revisions,
  then reports words, characters, UTF-8 bytes, and fixed adoption gates. This
  is a context-footprint proxy, not model-token or effectiveness evidence.
.PARAMETER BaselineRevision
  Immutable baseline Git revision.
.PARAMETER CandidateRevision
  Immutable candidate Git revision.
.PARAMETER RepoRoot
  Repository root. Defaults to the parent of this script directory.
.NOTES
  Side effects: none. The script reads Git objects only.
#>
[CmdletBinding()]
Param(
  [Parameter(Mandatory)][string]$BaselineRevision,
  [Parameter(Mandatory)][string]$CandidateRevision,
  [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$utf8 = [System.Text.UTF8Encoding]::new($false, $true)
$wordPattern = '[\p{L}\p{N}_]+(?:[-/][\p{L}\p{N}_]+)*'

function Get-GitBlobBytes {
  Param([string]$Revision, [string]$Path)
  $spec = "${Revision}:$Path"
  $start = [System.Diagnostics.ProcessStartInfo]::new("git")
  $start.WorkingDirectory = $RepoRoot
  $start.ArgumentList.Add("cat-file")
  $start.ArgumentList.Add("blob")
  $start.ArgumentList.Add($spec)
  $start.RedirectStandardOutput = $true
  $start.RedirectStandardError = $true
  $start.UseShellExecute = $false
  $process = [System.Diagnostics.Process]::Start($start)
  $memory = [System.IO.MemoryStream]::new()
  $process.StandardOutput.BaseStream.CopyTo($memory)
  $errorText = $process.StandardError.ReadToEnd()
  $process.WaitForExit()
  if ($process.ExitCode -ne 0) { throw "cannot read Git blob ${spec}: $errorText" }
  return $memory.ToArray()
}

function Measure-Paths {
  Param([string]$Revision, [string[]]$Paths)
  if ($Paths.Count -eq 0 -or (@($Paths | Sort-Object -Unique).Count -ne $Paths.Count)) {
    throw "manifest paths must be non-empty and unique"
  }
  [long]$bytes = 0; [long]$characters = 0; [long]$words = 0
  foreach ($path in $Paths) {
    $blob = Get-GitBlobBytes $Revision $path
    $text = $utf8.GetString($blob)
    if ($text.Length -gt 0 -and $text[0] -eq [char]0xFEFF) { $text = $text.Substring(1) }
    $bytes += $blob.LongLength
    $characters += $text.Length
    $words += [regex]::Matches($text, $wordPattern).Count
  }
  return @{ Words=$words; Characters=$characters; Bytes=$bytes }
}

$core = @('AGENTS.md','docs/change-workflow.md','docs/templates/non-trivial-change-template.md','docs/review-checklist.md','VALIDATION.md','docs/dev-runbook.md')
$skills = @('design-challenger','go-code-quality-review','requirements-ambiguity-review','scientific-model-oracle','scope-ledger-adversarial-review','test-strategy-adversary')
$manifests = @(
  @{ Name='always'; Baseline=@('AGENTS.md'); Candidate=@('AGENTS.md') },
  @{ Name='planning'; Baseline=@('AGENTS.md','docs/change-workflow.md','docs/templates/non-trivial-change-template.md'); Candidate=@('AGENTS.md','docs/change-workflow.md','docs/templates/non-trivial-change-template.md') },
  @{ Name='full-core'; Baseline=$core; Candidate=$core },
  @{ Name='codex-workflow'; Baseline=$core; Candidate=$core + @('docs/runbooks/codex-workflow-checks.md') },
  @{ Name='triggered'; Baseline=$core; Candidate=$core + @('docs/runbooks/codex-workflow-checks.md','docs/runbooks/codex-triggered-validation-tools.md') }
)
foreach ($skill in $skills) {
  $path = "codex-skills/$skill/SKILL.md"
  $manifests += @{ Name="skill-$skill"; Baseline=@('AGENTS.md',$path); Candidate=@('AGENTS.md',$path) }
}

$rows = @()
foreach ($manifest in $manifests) {
  $before = Measure-Paths $BaselineRevision $manifest.Baseline
  $after = Measure-Paths $CandidateRevision $manifest.Candidate
  if ($before.Words -eq 0 -or $before.Bytes -eq 0) { throw "zero baseline in $($manifest.Name)" }
  $wordReduction = 100.0 * ($before.Words - $after.Words) / $before.Words
  $byteReduction = 100.0 * ($before.Bytes - $after.Bytes) / $before.Bytes
  $rows += [pscustomobject]@{
    Manifest=$manifest.Name; BaselineWords=$before.Words; CandidateWords=$after.Words
    WordReductionPct=$wordReduction; BaselineBytes=$before.Bytes; CandidateBytes=$after.Bytes
    ByteReductionPct=$byteReduction; CandidateCharacters=$after.Characters
  }
}
$meanWords = ($rows | Measure-Object WordReductionPct -Average).Average
$meanBytes = ($rows | Measure-Object ByteReductionPct -Average).Average
$growth = @($rows | Where-Object { $_.CandidateWords -gt $_.BaselineWords -or $_.CandidateBytes -gt $_.BaselineBytes })
$skillFailure = @($rows | Where-Object { $_.Manifest -like 'skill-*' -and ($_.CandidateWords -ge $_.BaselineWords -or $_.CandidateBytes -ge $_.BaselineBytes) })
$passed = $growth.Count -eq 0 -and $skillFailure.Count -eq 0 -and $meanWords -ge 5.0 -and $meanBytes -ge 5.0
$rows | Format-Table -AutoSize
Write-Host ("Mean reductions: words={0:N3}% bytes={1:N3}%" -f $meanWords,$meanBytes)
Write-Host "Deterministic context-footprint gate: $(if($passed){'PASS'}else{'FAIL'})"
if (-not $passed) { exit 1 }

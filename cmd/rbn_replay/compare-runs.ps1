param(
  [string]$HistoryDir = "archive data/rbn_replay_history",
  [string]$RunIdA = "",
  [string]$RunIdB = "",
  [string]$Date = ""
)

$ErrorActionPreference = "Stop"

$indexPath = Join-Path $HistoryDir "runs.jsonl"
if (!(Test-Path $indexPath)) {
  throw "Run history index not found: $indexPath"
}

$rows = Get-Content $indexPath | Where-Object { $_.Trim() -ne "" } | ForEach-Object { $_ | ConvertFrom-Json }
if ($Date -ne "") {
  $rows = $rows | Where-Object { $_.date_utc -eq $Date }
}
if (!$rows -or $rows.Count -lt 2) {
  throw "Need at least two runs in history (after filters) to compare."
}

$toTuple = {
  param($r)
  [datetime]::Parse($r.finished_at_utc).ToUniversalTime()
}

if ($RunIdA -ne "" -and $RunIdB -ne "") {
  $a = $rows | Where-Object { $_.run_id -eq $RunIdA } | Select-Object -First 1
  $b = $rows | Where-Object { $_.run_id -eq $RunIdB } | Select-Object -First 1
  if (!$a -or !$b) {
    throw "Could not find both run IDs in history."
  }
} else {
  $sorted = $rows | Sort-Object { & $toTuple $_ } -Descending
  $b = $sorted[0]
  $a = $sorted[1]
}

function Num([object]$v) {
  if ($null -eq $v -or $v -eq "") { return 0.0 }
  return [double]$v
}

$report = [pscustomobject]@{
  RunA = "$($a.run_id) [$($a.date_utc)]"
  RunB = "$($b.run_id) [$($b.date_utc)]"
  'CurrentApplied_A' = [int]$a.method_stability.current_path.total_applied
  'CurrentApplied_B' = [int]$b.method_stability.current_path.total_applied
  'CurrentApplied_Delta' = [int]$b.method_stability.current_path.total_applied - [int]$a.method_stability.current_path.total_applied
  'CurrentStablePct_A' = Num $a.method_stability.current_path.stable_pct
  'CurrentStablePct_B' = Num $b.method_stability.current_path.stable_pct
  'CurrentStablePct_Delta' = [math]::Round((Num $b.method_stability.current_path.stable_pct) - (Num $a.method_stability.current_path.stable_pct), 3)
  'ResolverApplied_A' = [int]$a.method_stability.resolver.total_applied
  'ResolverApplied_B' = [int]$b.method_stability.resolver.total_applied
  'ResolverApplied_Delta' = [int]$b.method_stability.resolver.total_applied - [int]$a.method_stability.resolver.total_applied
  'ResolverStablePct_A' = Num $a.method_stability.resolver.stable_pct
  'ResolverStablePct_B' = Num $b.method_stability.resolver.stable_pct
  'ResolverStablePct_Delta' = [math]::Round((Num $b.method_stability.resolver.stable_pct) - (Num $a.method_stability.resolver.stable_pct), 3)
  'AgreementPct_A' = Num $a.agreement_pct
  'AgreementPct_B' = Num $b.agreement_pct
  'AgreementPct_Delta' = [math]::Round((Num $b.agreement_pct) - (Num $a.agreement_pct), 3)
  'DWPct_A' = Num $a.dw_pct
  'DWPct_B' = Num $b.dw_pct
  'DWPct_Delta' = [math]::Round((Num $b.dw_pct) - (Num $a.dw_pct), 3)
  'UCPct_A' = Num $a.uc_pct
  'UCPct_B' = Num $b.uc_pct
  'UCPct_Delta' = [math]::Round((Num $b.uc_pct) - (Num $a.uc_pct), 3)
}

$report | Format-List

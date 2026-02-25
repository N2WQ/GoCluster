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

function IntVal([object]$v) {
  if ($null -eq $v -or $v -eq "") { return 0 }
  return [int64]$v
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

  'CurrentConfUnknown_A' = IntVal $a.ab_metrics.current_path.confidence_counts.unknown
  'CurrentConfUnknown_B' = IntVal $b.ab_metrics.current_path.confidence_counts.unknown
  'CurrentConfUnknown_Delta' = (IntVal $b.ab_metrics.current_path.confidence_counts.unknown) - (IntVal $a.ab_metrics.current_path.confidence_counts.unknown)
  'CurrentConfP_A' = IntVal $a.ab_metrics.current_path.confidence_counts.p
  'CurrentConfP_B' = IntVal $b.ab_metrics.current_path.confidence_counts.p
  'CurrentConfP_Delta' = (IntVal $b.ab_metrics.current_path.confidence_counts.p) - (IntVal $a.ab_metrics.current_path.confidence_counts.p)
  'CurrentLegacyUnknownNowP_A' = IntVal $a.ab_metrics.current_path.legacy_unknown_now_p
  'CurrentLegacyUnknownNowP_B' = IntVal $b.ab_metrics.current_path.legacy_unknown_now_p
  'CurrentLegacyUnknownNowP_Delta' = (IntVal $b.ab_metrics.current_path.legacy_unknown_now_p) - (IntVal $a.ab_metrics.current_path.legacy_unknown_now_p)

  'ResolverStateConfident_A' = IntVal $a.ab_metrics.resolver.state_counts.confident
  'ResolverStateConfident_B' = IntVal $b.ab_metrics.resolver.state_counts.confident
  'ResolverStateConfident_Delta' = (IntVal $b.ab_metrics.resolver.state_counts.confident) - (IntVal $a.ab_metrics.resolver.state_counts.confident)
  'ResolverStateProbable_A' = IntVal $a.ab_metrics.resolver.state_counts.probable
  'ResolverStateProbable_B' = IntVal $b.ab_metrics.resolver.state_counts.probable
  'ResolverStateProbable_Delta' = (IntVal $b.ab_metrics.resolver.state_counts.probable) - (IntVal $a.ab_metrics.resolver.state_counts.probable)
  'ResolverStateUncertain_A' = IntVal $a.ab_metrics.resolver.state_counts.uncertain
  'ResolverStateUncertain_B' = IntVal $b.ab_metrics.resolver.state_counts.uncertain
  'ResolverStateUncertain_Delta' = (IntVal $b.ab_metrics.resolver.state_counts.uncertain) - (IntVal $a.ab_metrics.resolver.state_counts.uncertain)
  'ResolverStateSplit_A' = IntVal $a.ab_metrics.resolver.state_counts.split
  'ResolverStateSplit_B' = IntVal $b.ab_metrics.resolver.state_counts.split
  'ResolverStateSplit_Delta' = (IntVal $b.ab_metrics.resolver.state_counts.split) - (IntVal $a.ab_metrics.resolver.state_counts.split)
  'ResolverProjUnknown_A' = IntVal $a.ab_metrics.resolver.projected_confidence_counts.unknown
  'ResolverProjUnknown_B' = IntVal $b.ab_metrics.resolver.projected_confidence_counts.unknown
  'ResolverProjUnknown_Delta' = (IntVal $b.ab_metrics.resolver.projected_confidence_counts.unknown) - (IntVal $a.ab_metrics.resolver.projected_confidence_counts.unknown)
  'ResolverProjP_A' = IntVal $a.ab_metrics.resolver.projected_confidence_counts.p
  'ResolverProjP_B' = IntVal $b.ab_metrics.resolver.projected_confidence_counts.p
  'ResolverProjP_Delta' = (IntVal $b.ab_metrics.resolver.projected_confidence_counts.p) - (IntVal $a.ab_metrics.resolver.projected_confidence_counts.p)
  'ResolverLegacyUnknownNowP_A' = IntVal $a.ab_metrics.resolver.legacy_unknown_now_p
  'ResolverLegacyUnknownNowP_B' = IntVal $b.ab_metrics.resolver.legacy_unknown_now_p
  'ResolverLegacyUnknownNowP_Delta' = (IntVal $b.ab_metrics.resolver.legacy_unknown_now_p) - (IntVal $a.ab_metrics.resolver.legacy_unknown_now_p)

  'StabilizerWouldDelayOld_A' = IntVal $a.ab_metrics.stabilizer_delay_proxy.would_delay_old
  'StabilizerWouldDelayOld_B' = IntVal $b.ab_metrics.stabilizer_delay_proxy.would_delay_old
  'StabilizerWouldDelayOld_Delta' = (IntVal $b.ab_metrics.stabilizer_delay_proxy.would_delay_old) - (IntVal $a.ab_metrics.stabilizer_delay_proxy.would_delay_old)
  'StabilizerWouldDelayNew_A' = IntVal $a.ab_metrics.stabilizer_delay_proxy.would_delay_new
  'StabilizerWouldDelayNew_B' = IntVal $b.ab_metrics.stabilizer_delay_proxy.would_delay_new
  'StabilizerWouldDelayNew_Delta' = (IntVal $b.ab_metrics.stabilizer_delay_proxy.would_delay_new) - (IntVal $a.ab_metrics.stabilizer_delay_proxy.would_delay_new)
  'StabilizerNewlyNotDelayed_A' = IntVal $a.ab_metrics.stabilizer_delay_proxy.newly_not_delayed_under_new_rule
  'StabilizerNewlyNotDelayed_B' = IntVal $b.ab_metrics.stabilizer_delay_proxy.newly_not_delayed_under_new_rule
  'StabilizerNewlyNotDelayed_Delta' = (IntVal $b.ab_metrics.stabilizer_delay_proxy.newly_not_delayed_under_new_rule) - (IntVal $a.ab_metrics.stabilizer_delay_proxy.newly_not_delayed_under_new_rule)
  'StabilizerDelayDelta_A' = IntVal $a.ab_metrics.stabilizer_delay_proxy.delay_delta
  'StabilizerDelayDelta_B' = IntVal $b.ab_metrics.stabilizer_delay_proxy.delay_delta
  'StabilizerDelayDelta_Delta' = (IntVal $b.ab_metrics.stabilizer_delay_proxy.delay_delta) - (IntVal $a.ab_metrics.stabilizer_delay_proxy.delay_delta)
}

$report | Format-List

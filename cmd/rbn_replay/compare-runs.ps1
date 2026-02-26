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
  'ResolverNeighborhoodUsed_A' = IntVal $a.ab_metrics.resolver.neighborhood_used
  'ResolverNeighborhoodUsed_B' = IntVal $b.ab_metrics.resolver.neighborhood_used
  'ResolverNeighborhoodUsed_Delta' = (IntVal $b.ab_metrics.resolver.neighborhood_used) - (IntVal $a.ab_metrics.resolver.neighborhood_used)
  'ResolverNeighborhoodOverride_A' = IntVal $a.ab_metrics.resolver.neighborhood_winner_override
  'ResolverNeighborhoodOverride_B' = IntVal $b.ab_metrics.resolver.neighborhood_winner_override
  'ResolverNeighborhoodOverride_Delta' = (IntVal $b.ab_metrics.resolver.neighborhood_winner_override) - (IntVal $a.ab_metrics.resolver.neighborhood_winner_override)
  'ResolverNeighborhoodSplit_A' = IntVal $a.ab_metrics.resolver.neighborhood_conflict_split
  'ResolverNeighborhoodSplit_B' = IntVal $b.ab_metrics.resolver.neighborhood_conflict_split
  'ResolverNeighborhoodSplit_Delta' = (IntVal $b.ab_metrics.resolver.neighborhood_conflict_split) - (IntVal $a.ab_metrics.resolver.neighborhood_conflict_split)
  'ResolverNeighborhoodExcludedUnrelated_A' = IntVal $a.ab_metrics.resolver.neighborhood_excluded_unrelated
  'ResolverNeighborhoodExcludedUnrelated_B' = IntVal $b.ab_metrics.resolver.neighborhood_excluded_unrelated
  'ResolverNeighborhoodExcludedUnrelated_Delta' = (IntVal $b.ab_metrics.resolver.neighborhood_excluded_unrelated) - (IntVal $a.ab_metrics.resolver.neighborhood_excluded_unrelated)
  'ResolverNeighborhoodExcludedDistance_A' = IntVal $a.ab_metrics.resolver.neighborhood_excluded_distance
  'ResolverNeighborhoodExcludedDistance_B' = IntVal $b.ab_metrics.resolver.neighborhood_excluded_distance
  'ResolverNeighborhoodExcludedDistance_Delta' = (IntVal $b.ab_metrics.resolver.neighborhood_excluded_distance) - (IntVal $a.ab_metrics.resolver.neighborhood_excluded_distance)
  'ResolverNeighborhoodExcludedAnchorMissing_A' = IntVal $a.ab_metrics.resolver.neighborhood_excluded_anchor_missing
  'ResolverNeighborhoodExcludedAnchorMissing_B' = IntVal $b.ab_metrics.resolver.neighborhood_excluded_anchor_missing
  'ResolverNeighborhoodExcludedAnchorMissing_Delta' = (IntVal $b.ab_metrics.resolver.neighborhood_excluded_anchor_missing) - (IntVal $a.ab_metrics.resolver.neighborhood_excluded_anchor_missing)
  'ResolverRecentPlus1Applied_A' = IntVal $a.ab_metrics.resolver.recent_plus1_applied
  'ResolverRecentPlus1Applied_B' = IntVal $b.ab_metrics.resolver.recent_plus1_applied
  'ResolverRecentPlus1Applied_Delta' = (IntVal $b.ab_metrics.resolver.recent_plus1_applied) - (IntVal $a.ab_metrics.resolver.recent_plus1_applied)
  'ResolverRecentPlus1Rejected_A' = IntVal $a.ab_metrics.resolver.recent_plus1_rejected
  'ResolverRecentPlus1Rejected_B' = IntVal $b.ab_metrics.resolver.recent_plus1_rejected
  'ResolverRecentPlus1Rejected_Delta' = (IntVal $b.ab_metrics.resolver.recent_plus1_rejected) - (IntVal $a.ab_metrics.resolver.recent_plus1_rejected)
  'ResolverRecentPlus1RejectEdit_A' = IntVal $a.ab_metrics.resolver.recent_plus1_reject_edit_neighbor_contested
  'ResolverRecentPlus1RejectEdit_B' = IntVal $b.ab_metrics.resolver.recent_plus1_reject_edit_neighbor_contested
  'ResolverRecentPlus1RejectEdit_Delta' = (IntVal $b.ab_metrics.resolver.recent_plus1_reject_edit_neighbor_contested) - (IntVal $a.ab_metrics.resolver.recent_plus1_reject_edit_neighbor_contested)
  'ResolverRecentPlus1RejectDistance_A' = IntVal $a.ab_metrics.resolver.recent_plus1_reject_distance_or_family
  'ResolverRecentPlus1RejectDistance_B' = IntVal $b.ab_metrics.resolver.recent_plus1_reject_distance_or_family
  'ResolverRecentPlus1RejectDistance_Delta' = (IntVal $b.ab_metrics.resolver.recent_plus1_reject_distance_or_family) - (IntVal $a.ab_metrics.resolver.recent_plus1_reject_distance_or_family)
  'ResolverRecentPlus1RejectWinner_A' = IntVal $a.ab_metrics.resolver.recent_plus1_reject_winner_recent_insufficient
  'ResolverRecentPlus1RejectWinner_B' = IntVal $b.ab_metrics.resolver.recent_plus1_reject_winner_recent_insufficient
  'ResolverRecentPlus1RejectWinner_Delta' = (IntVal $b.ab_metrics.resolver.recent_plus1_reject_winner_recent_insufficient) - (IntVal $a.ab_metrics.resolver.recent_plus1_reject_winner_recent_insufficient)
  'ResolverRecentPlus1RejectSubject_A' = IntVal $a.ab_metrics.resolver.recent_plus1_reject_subject_not_weaker
  'ResolverRecentPlus1RejectSubject_B' = IntVal $b.ab_metrics.resolver.recent_plus1_reject_subject_not_weaker
  'ResolverRecentPlus1RejectSubject_Delta' = (IntVal $b.ab_metrics.resolver.recent_plus1_reject_subject_not_weaker) - (IntVal $a.ab_metrics.resolver.recent_plus1_reject_subject_not_weaker)
  'ResolverRecentPlus1RejectOther_A' = IntVal $a.ab_metrics.resolver.recent_plus1_reject_other
  'ResolverRecentPlus1RejectOther_B' = IntVal $b.ab_metrics.resolver.recent_plus1_reject_other
  'ResolverRecentPlus1RejectOther_Delta' = (IntVal $b.ab_metrics.resolver.recent_plus1_reject_other) - (IntVal $a.ab_metrics.resolver.recent_plus1_reject_other)

  'StabilizerWouldDelay_A' = IntVal $a.ab_metrics.stabilizer_delay_proxy.would_delay
  'StabilizerWouldDelay_B' = IntVal $b.ab_metrics.stabilizer_delay_proxy.would_delay
  'StabilizerWouldDelay_Delta' = (IntVal $b.ab_metrics.stabilizer_delay_proxy.would_delay) - (IntVal $a.ab_metrics.stabilizer_delay_proxy.would_delay)
  'StabilizerReasonUnknown_A' = IntVal $a.ab_metrics.stabilizer_delay_proxy.reason_unknown_or_nonrecent
  'StabilizerReasonUnknown_B' = IntVal $b.ab_metrics.stabilizer_delay_proxy.reason_unknown_or_nonrecent
  'StabilizerReasonUnknown_Delta' = (IntVal $b.ab_metrics.stabilizer_delay_proxy.reason_unknown_or_nonrecent) - (IntVal $a.ab_metrics.stabilizer_delay_proxy.reason_unknown_or_nonrecent)
  'StabilizerReasonAmbiguous_A' = IntVal $a.ab_metrics.stabilizer_delay_proxy.reason_ambiguous_resolver
  'StabilizerReasonAmbiguous_B' = IntVal $b.ab_metrics.stabilizer_delay_proxy.reason_ambiguous_resolver
  'StabilizerReasonAmbiguous_Delta' = (IntVal $b.ab_metrics.stabilizer_delay_proxy.reason_ambiguous_resolver) - (IntVal $a.ab_metrics.stabilizer_delay_proxy.reason_ambiguous_resolver)
  'StabilizerReasonLowP_A' = IntVal $a.ab_metrics.stabilizer_delay_proxy.reason_p_low_confidence
  'StabilizerReasonLowP_B' = IntVal $b.ab_metrics.stabilizer_delay_proxy.reason_p_low_confidence
  'StabilizerReasonLowP_Delta' = (IntVal $b.ab_metrics.stabilizer_delay_proxy.reason_p_low_confidence) - (IntVal $a.ab_metrics.stabilizer_delay_proxy.reason_p_low_confidence)
  'StabilizerReasonEditNeighbor_A' = IntVal $a.ab_metrics.stabilizer_delay_proxy.reason_edit_neighbor_contested
  'StabilizerReasonEditNeighbor_B' = IntVal $b.ab_metrics.stabilizer_delay_proxy.reason_edit_neighbor_contested
  'StabilizerReasonEditNeighbor_Delta' = (IntVal $b.ab_metrics.stabilizer_delay_proxy.reason_edit_neighbor_contested) - (IntVal $a.ab_metrics.stabilizer_delay_proxy.reason_edit_neighbor_contested)
}

$report | Format-List

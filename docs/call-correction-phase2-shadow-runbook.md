# Phase 2 Shadow Runbook - Signal Resolver

Status: Active
Date: 2026-02-23
Scope: Phase 2 shadow mode only (no correction cutover)
Decision refs: ADR-0022, TSR-0003

## 1) Purpose

This runbook defines how to operate and evaluate the Phase 2 signal resolver while it runs in shadow mode.

Goals:
- Detect safety/resource issues early.
- Quantify resolver vs current-path decision alignment.
- Decide if Phase 3 cutover is safe.

Non-goals:
- Changing correction/stabilizer/glyph behavior in Phase 2.
- Adding YAML knobs in Phase 2.

## 2) Required runtime profile (before starting)

The resolver summary line is emitted by the periodic stats logger. In this codebase, that line is only written as a log line when `ui.mode: headless`.

Set these values in `data/config/app.yaml` (or your alternate config directory):

```yaml
stats:
  display_interval_seconds: 60  # 30 or 60 recommended for shadow runs

ui:
  mode: "headless"              # required for Resolver/CorrGate/Stabilizer stats lines in logs

logging:
  enabled: true
  dir: "data/logs"
  retention_days: 21            # must cover your shadow window
```

Optional (recommended) decision DB for deep call-correction analysis in `data/config/pipeline.yaml`:

```yaml
call_correction:
  debug_log: true
  debug_log_file: "data/logs/callcorr_debug.log"
```

## 3) Start command (copy/paste)

From repo root:

```pwsh
$env:DXC_CONFIG_PATH = "data/config"   # optional if using default data/config
go run .
```

Alternative binary run:

```pwsh
.\gocluster.exe
```

## 4) Where logs are and file names

System log files:
- Directory: `logging.dir` from `data/config/app.yaml` (default `data/logs`).
- File pattern: `DD-Mon-YYYY.log` in UTC date (example: `23-Feb-2026.log`).
- Each line timestamp is UTC (`YYYY/MM/DD HH:MM:SS`).

Decision DB files when `call_correction.debug_log: true`:
- If `debug_log_file` is blank: `data/analysis/callcorr_YYYY-MM-DD.db`.
- If `debug_log_file: "data/logs/callcorr_debug.log"`: `data/logs/callcorr_debug_YYYY-MM-DD.db`.

Handy variables:

```pwsh
$logDir  = "data/logs"  # set to logging.dir
$logFile = Join-Path $logDir ((Get-Date).ToUniversalTime().ToString("dd-MMM-yyyy") + ".log")
$dbFile  = Join-Path $logDir ("callcorr_debug_" + (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd") + ".db")
```

## 5) Live monitoring commands

Tail only relevant lines:

```pwsh
Get-Content -Path $logFile -Wait |
  Where-Object { $_ -match 'Resolver:|CorrGate:|Stabilizer:|Pipeline:|Telnet:' }
```

Capture a 30-minute evidence slice (stop with `Ctrl+C` after 30 minutes):

```pwsh
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$slice = "data/analysis/shadow_slice_$stamp.log"
Get-Content -Path $logFile -Wait |
  Where-Object { $_ -match 'Resolver:|CorrGate:|Stabilizer:|Pipeline:|Telnet:' } |
  Tee-Object -FilePath $slice
```

## 6) Process data (PowerShell, no external dependencies)

This produces:
- interval CSV (`data/analysis/resolver_intervals_*.csv`)
- threshold hit lists for quick triage

```pwsh
$logPath = $logFile
$queueSize = 8192
$outDir = "data/analysis"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

function ToInt([string]$v) { [int64]($v -replace ',', '') }

$re = [regex]'^(?<ts>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) Resolver: (?<C>[\d,]+) \(C\) / (?<P>[\d,]+) \(P\) / (?<U>[\d,]+) \(U\) / (?<S>[\d,]+) \(S\) \| agr (?<A>[\d,]+)/(?<CMP>[\d,]+) \(\d+%\) \| d (?<SP>[\d,]+) \(SP\) / (?<DW>[\d,]+) \(DW\) / (?<UC>[\d,]+) \(UC\) \| q=(?<DEPTH>\d+) drop (?<Q>[\d,]+) \(Q\) / (?<K>[\d,]+) \(K\) / (?<CCAP>[\d,]+) \(C\) / (?<R>[\d,]+) \(R\)$'

$samples = @()
Get-Content -Path $logPath | ForEach-Object {
  $m = $re.Match($_)
  if ($m.Success) {
    $samples += [pscustomobject]@{
      TS    = [datetime]::ParseExact($m.Groups['ts'].Value, 'yyyy/MM/dd HH:mm:ss', $null)
      C     = ToInt $m.Groups['C'].Value
      P     = ToInt $m.Groups['P'].Value
      U     = ToInt $m.Groups['U'].Value
      S     = ToInt $m.Groups['S'].Value
      A     = ToInt $m.Groups['A'].Value
      CMP   = ToInt $m.Groups['CMP'].Value
      SP    = ToInt $m.Groups['SP'].Value
      DW    = ToInt $m.Groups['DW'].Value
      UC    = ToInt $m.Groups['UC'].Value
      DEPTH = [int]$m.Groups['DEPTH'].Value
      Q     = ToInt $m.Groups['Q'].Value
      K     = ToInt $m.Groups['K'].Value
      CCAP  = ToInt $m.Groups['CCAP'].Value
      R     = ToInt $m.Groups['R'].Value
    }
  }
}

if ($samples.Count -lt 2) {
  throw "Need at least 2 resolver samples in $logPath"
}

$intervals = @()
for ($i = 1; $i -lt $samples.Count; $i++) {
  $a = $samples[$i - 1]
  $b = $samples[$i]
  $dCMP = [int64]($b.CMP - $a.CMP)
  $dA = [int64]($b.A - $a.A)
  $dSP = [int64]($b.SP - $a.SP)
  $dDW = [int64]($b.DW - $a.DW)
  $dUC = [int64]($b.UC - $a.UC)
  $dQ = [int64]($b.Q - $a.Q)
  $dK = [int64]($b.K - $a.K)
  $dCcap = [int64]($b.CCAP - $a.CCAP)
  $dR = [int64]($b.R - $a.R)

  $agreementPct = if ($dCMP -gt 0) { [math]::Round((100.0 * $dA / $dCMP), 3) } else { $null }
  $dwPct = if ($dCMP -gt 0) { [math]::Round((100.0 * $dDW / $dCMP), 3) } else { $null }
  $spPct = if ($dCMP -gt 0) { [math]::Round((100.0 * $dSP / $dCMP), 3) } else { $null }
  $ucPct = if ($dCMP -gt 0) { [math]::Round((100.0 * $dUC / $dCMP), 3) } else { $null }
  $queueRatio = [math]::Round(($b.DEPTH / [double]$queueSize), 6)
  $splitSharePct = [math]::Round((100.0 * $b.S / [double][math]::Max(($b.C + $b.P + $b.U + $b.S), 1)), 3)

  $intervals += [pscustomobject]@{
    T0 = $a.TS
    T1 = $b.TS
    Minutes = [math]::Round((New-TimeSpan -Start $a.TS -End $b.TS).TotalMinutes, 3)
    dA = $dA
    dCMP = $dCMP
    dSP = $dSP
    dDW = $dDW
    dUC = $dUC
    dQ = $dQ
    dK = $dK
    dCcap = $dCcap
    dR = $dR
    AgreementPct = $agreementPct
    DWPct = $dwPct
    SPPct = $spPct
    UCPct = $ucPct
    DEPTH = $b.DEPTH
    QueueDepthRatio = $queueRatio
    SplitSharePct = $splitSharePct
    SampleGuardPass = ($dCMP -ge 200)
  }
}

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$csv = Join-Path $outDir "resolver_intervals_$stamp.csv"
$intervals | Export-Csv -Path $csv -NoTypeInformation

$totalCMP = ($intervals | Measure-Object dCMP -Sum).Sum
$totalA = ($intervals | Measure-Object dA -Sum).Sum
$totalDW = ($intervals | Measure-Object dDW -Sum).Sum
$totalSP = ($intervals | Measure-Object dSP -Sum).Sum
$totalUC = ($intervals | Measure-Object dUC -Sum).Sum
$totalQ = ($intervals | Measure-Object dQ -Sum).Sum
$maxDepth = ($intervals | Measure-Object DEPTH -Maximum).Maximum

$overallAgreementPct = if ($totalCMP -gt 0) { [math]::Round((100.0 * $totalA / $totalCMP), 3) } else { 0.0 }
$overallDWPct = if ($totalCMP -gt 0) { [math]::Round((100.0 * $totalDW / $totalCMP), 3) } else { 0.0 }
$overallSPPct = if ($totalCMP -gt 0) { [math]::Round((100.0 * $totalSP / $totalCMP), 3) } else { 0.0 }
$overallUCPct = if ($totalCMP -gt 0) { [math]::Round((100.0 * $totalUC / $totalCMP), 3) } else { 0.0 }

Write-Host "Interval CSV: $csv"
Write-Host "Comparable decisions: $totalCMP"
Write-Host "AgreementPct: $overallAgreementPct"
Write-Host "DWPct/SPPct/UCPct: $overallDWPct / $overallSPPct / $overallUCPct"
Write-Host "Max queue depth: $maxDepth ($([math]::Round(($maxDepth / [double]$queueSize) * 100.0, 2))%)"
Write-Host "Total queue-full drops (dQ sum): $totalQ"

# Warning/critical windows with sample guard.
$warnOrCrit = $intervals | Where-Object {
  $_.SampleGuardPass -and (
    $_.AgreementPct -lt 98.5 -or
    $_.DWPct -gt 0.5 -or
    $_.SPPct -gt 1.0 -or
    $_.UCPct -gt 2.0 -or
    $_.QueueDepthRatio -ge 0.25 -or
    $_.dQ -ge 1 -or $_.dK -ge 1 -or $_.dCcap -ge 1 -or $_.dR -ge 1
  )
}

$hitsCsv = Join-Path $outDir "resolver_threshold_hits_$stamp.csv"
$warnOrCrit | Export-Csv -Path $hitsCsv -NoTypeInformation
Write-Host "Threshold hits CSV: $hitsCsv"
```

Optional decision DB sanity check:

```pwsh
go run ./cmd/inspect_decisions/main.go -db $dbFile
```

## 7) Metric line definition

Primary source line:

`Resolver: C/P/U/S | agr A/CMP (%) | d SP/DW/UC | q=DEPTH drop Q/K/C/R`

Fields:
- `C`: confident state count (instantaneous active keys).
- `P`: probable state count.
- `U`: uncertain state count.
- `S`: split state count.
- `A`: agreement count (cumulative comparable decisions that match resolver winner).
- `CMP`: comparable decision count (cumulative; resolver state was `confident|probable`).
- `SP`: disagreements where resolver said split while current path corrected.
- `DW`: disagreements where resolver said confident with different winner.
- `UC`: disagreements where resolver said uncertain while current path corrected.
- `DEPTH`: resolver input queue depth (instantaneous).
- `Q`: queue-full drops (cumulative).
- `K`: max-active-key drops (cumulative).
- `C`: max-candidates-per-key drops (cumulative).
- `R`: max-reporters-per-candidate drops (cumulative).

Related context lines to watch together:
- `CorrGate: ...` (current correction decision totals and reasons).
- `Stabilizer: ...` (delivery-policy behavior).
- `Pipeline: ...` and `Telnet: ...` (fan-out health baseline).

## 8) Sampling model

Use deltas between consecutive resolver lines.

Definitions for interval `t0 -> t1`:
- `dA = A1 - A0`
- `dCMP = CMP1 - CMP0`
- `dSP = SP1 - SP0`
- `dDW = DW1 - DW0`
- `dUC = UC1 - UC0`
- `dQ = Q1 - Q0`
- `dK = K1 - K0`
- `dCcap = C1 - C0`
- `dR = R1 - R0`

Derived rates:
- `AgreementRate = dA / max(dCMP, 1)`
- `DWRate = dDW / max(dCMP, 1)`
- `SPRate = dSP / max(dCMP, 1)`
- `UCRate = dUC / max(dCMP, 1)`
- `QueueDepthRatio = DEPTH / 8192` (current Phase 2 bound)
- `SplitShare = S / max(C+P+U+S, 1)`

Minimum sample guard:
- Do not evaluate quality gates on windows where `dCMP < 200`.

Recommended windows:
- Fast: 5 minutes (alerting).
- Operational: 1 hour.
- Decision-making: 24 hours and rolling 7 days.

## 9) Thresholds and actions

## 9.1 Alignment quality thresholds

| Metric | Warning | Critical | Action |
|---|---|---|---|
| `AgreementRate` | `< 98.5%` for 10 min | `< 97.5%` for 10 min | Inspect `CorrGate` reasons, top disagreement classes, and recent deploy/config changes. |
| `DWRate` | `> 0.5%` for 10 min | `> 1.0%` for 10 min | High-severity investigation: winner conflicts imply potential cutover risk. |
| `SPRate` | `> 1.0%` for 10 min | `> 2.0%` for 10 min | Review ambiguous clusters and overlap behavior by band/mode. |
| `UCRate` | `> 2.0%` for 10 min | `> 4.0%` for 10 min | Review candidate support sparsity and tolerance/keying effects. |

## 9.2 Resource and safety thresholds

| Metric | Warning | Critical | Action |
|---|---|---|---|
| `QueueDepthRatio` | `>= 25%` for 5 min | `>= 50%` at any point or `>= 25%` for 30 min | Investigate burst profile and consumer lag; verify no ingest blocking. |
| `dQ` (queue-full drops) | `>= 1` in 5 min | `>= 10` in 5 min | Treat as resolver backpressure event; verify fail-open behavior and no fan-out impact. |
| `dK` (max keys) | `>= 1` in 1 hour | `>= 1` in 5 min | Active-key cardinality pressure; inspect key churn and TTL behavior. |
| `dCcap` (max candidates/key) | `>= 1` in 1 hour | `>= 1` in 5 min | Dense-cluster pressure; inspect same-frequency multiplicity and candidate cap fit. |
| `dR` (max reporters/candidate) | `>= 1` in 1 hour | `>= 1` in 5 min | Very dense reporting pressure; inspect reporter cap fit for contest load. |

## 9.3 State mix guardrails (advisory)

These are advisory signals, not hard blockers by themselves:
- `SplitShare > 20%` for 30 min: investigate cluster overlap assumptions.
- `U` dominates (`U/(C+P+U+S) > 70%`) with high `dCMP`: investigate tolerance/keying and recency interactions.

## 10) Incident workflow (shadow mode)

When any critical threshold trips:
1. Confirm no user-visible regression (`CorrGate`, `Stabilizer`, `Pipeline`, `Telnet` lines).
2. Capture a 30-minute slice of resolver/corrgate/stabilizer lines.
3. Classify dominant issue:
   - capacity (`Q/K/C/R`),
   - alignment (`DW/SP/UC`),
   - mixed.
4. Open/update TSR evidence with:
   - exact UTC timestamps,
   - interval deltas and rates,
   - affected bands/modes if identifiable.
5. If resolver pressure persists, keep shadow mode enabled (do not cut over), and adjust implementation in a follow-up scope.

## 11) Daily operator checklist

- `AgreementRate` healthy at 1h and 24h windows.
- `DWRate` stable and low.
- `Q/K/C/R` flat or near-flat.
- Queue depth not persistently elevated.
- No resolver panic logs.
- No correlated increase in telnet drops/disconnect symptoms.

## 12) Phase 3 cutover readiness criteria

All gates below must pass simultaneously before proposing Phase 3 cutover ADR.

## 12.1 Data sufficiency gate

- At least 14 consecutive days of shadow data.
- At least `50,000` comparable decisions in the 14-day window.
- At least one high-load period (contest-like burst) included.

## 12.2 Quality gate

- Rolling 7-day `AgreementRate >= 98.5%`.
- No day with `AgreementRate < 97.5%`.
- Rolling 7-day `DWRate <= 0.30%`.
- Rolling 7-day `SPRate <= 0.50%`.
- Rolling 7-day `UCRate <= 1.50%`.

## 12.3 Resource/safety gate

- `dK = 0`, `dCcap = 0`, `dR = 0` for rolling 7 days.
- `dQ` near-zero and never in sustained bursts (no 5-minute window with `dQ >= 10`).
- Queue depth not persistently elevated (`QueueDepthRatio < 25%` over p99 1h windows).
- No resolver panic/restart events in 14 days.

## 12.4 Operational impact gate

- No measurable degradation in existing operational health signals:
  - telnet drop profile,
  - pipeline stall/idle warnings,
  - memory/GC trend lines.
- No evidence that resolver shadow feed causes ingest blocking.

## 12.5 Human review gate

- Review a representative sample of disagreements:
  - at least 200 `DW`,
  - at least 200 `SP`/`UC` combined.
- Confirm no systemic band/mode bias that would harm correctness on cutover.

## 13) Phase 3 proposal package (required artifacts)

Before requesting cutover approval, prepare:
- 14-day metric summary table (daily + rolling windows).
- Disagreement-class breakdown by band/mode.
- Capacity chart (`Q/K/C/R`, queue depth).
- Benchmark guardrail summary for resolver hot path.
- Recommended cutover plan and rollback trigger thresholds.

## 14) Rollback triggers for a future Phase 3 pilot

If a Phase 3 pilot is attempted later, immediate rollback triggers should include:
- `AgreementRate < 97.0%` for 15 min.
- `DWRate > 1.5%` for 15 min.
- Any sustained resolver-capacity drops (`Q/K/C/R`) affecting hot-path confidence.
- Any sign of ingest blocking or fan-out instability.

## 15) Notes

- Phase 2 has no user-visible behavior changes by design.
- Thresholds are intentionally conservative to protect correctness and operational safety.

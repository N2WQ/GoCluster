# Historical RBN replay (`rbn_replay`)

`cmd/rbn_replay` is an offline deterministic runner for one UTC day of public RBN history.  
It now evaluates and applies **resolver-primary only** (no legacy/current-path comparison mode).

## Usage

Run one UTC day:

```powershell
go run ./cmd/rbn_replay -date 2026-02-08
```

Flags:
- `-date`: required (`YYYY-MM-DD` or `YYYYMMDD`)
- `-replay-config`: replay YAML path (default `cmd/rbn_replay/replay.yaml`)
- `-config`: override cluster config directory
- `-archive-dir`: override archive root
- `-force-download`: force RBN ZIP re-download

Replay config:
- `cmd/rbn_replay/replay.yaml`

## Resolver-only contract

- Replay requires `call_correction.enabled=true`.
- Replay uses resolver-primary rails for apply/suppress/reject decisions, including:
  - resolver neighborhood selection
  - resolver primary gates (min reports/adv/confidence, family/distance rails, +1 rail)
  - optional fixed-lag temporal decoding (`call_correction.temporal_decoder.*`) before final winner commit
  - invalid-base rejection rail
  - CTY rejection rail

## Determinism model

- Evidence eligibility is driven by row timestamps from the RBN CSV.
- Resolver is driven by deterministic driver stepping (`observe-before-drain` ordering).
- Temporal decoder decisions are also event-time only (hold/defer/commit windows use CSV timestamps, not wall clock).
- Runbook sample cadence is fixed at 60 seconds.
- Replay fails fast on timestamp regressions, out-of-day records, resolver enqueue failures, and resolver hard-drop counters.

Cap-loss generation idle retirement uses monotonic process elapsed time, not CSV
timestamps. At 10 minutes without owner-admitted evidence a loss-affected
generation loses authority, even if sweeping is delayed; a paused offline job
can therefore begin a new retained population when it resumes. Temporal hold
and evidence windows remain event-time based. Deterministic tests can select the
driver's manual process clock before its first owner-admitted receipt. This does
not redesign historical CustomSCP query clocks.

## Inputs and dependency model

Replay downloads:
- `<archive_dir>/<YYYYMMDD>.zip`

RBN history CSV mode fields are intentionally distinct:
- `mode` is the RBN spot class. Replay ingests only `CQ`, `BEACON`, and `NCDXF B`; it drops `DX`, blank, and unknown classes before resolver evidence.
- `tx_mode` is the RF/transmission mode and is the only CSV field used for `Spot.Mode`.
- `BEACON` and `NCDXF B` rows are accepted from the CSV parser but tagged as beacons and excluded from call-correction evidence.

All other dependencies are local snapshots and must already exist when enabled:
- `cty.file`
- `fcc_uls.allowlist_path`
- `fcc_uls.db_path`
- optional confusion model and spotter-reliability files

Invalid applicable configured assets stop initialization before history scratch
replacement or resolver ownership. Every nonblank reliability path is attempted;
the model's own enabled flag requires a valid asset even at weight zero. Errors
identify the configuration key and path. Nonfinite reliability, negative model
counts, invalid edges and overflowing int64 totals are refused; accepted finite
clamps and model results remain. The offline `callcorr_reveng_rebuilt` evaluator
uses the same failure contract. See [numerical inputs](../spot/README.md#numerical-inputs-and-configured-assets).

Accepted CSV reports are present, including zero and negative values. Blank,
unparseable, nonfinite or nonrepresentable floating reports reject the row.
Integer parsing remains first; the float fallback keeps its existing
`int(value+0.5)` behavior, including negative-decimal asymmetry and binary64
precision. Both offline tools check the transformed value before conversion.
Resolver model scoring omits missing or out-of-domain SNR without removing its
receiver support; valid bins remain `(lower, upper]`.

## Output artifacts

Outputs:
- `<archive_dir>/rbn_replay/<YYYY-MM-DD>/runbook_samples.log`
- `<archive_dir>/rbn_replay/<YYYY-MM-DD>/resolver_intervals.csv`
- `<archive_dir>/rbn_replay/<YYYY-MM-DD>/resolver_threshold_hits.csv`
- `<archive_dir>/rbn_replay/<YYYY-MM-DD>/gates.json`
- `<archive_dir>/rbn_replay/<YYYY-MM-DD>/manifest.json`

Run history:
- `<archive_dir>/rbn_replay_history/runs.jsonl`
- `<archive_dir>/rbn_replay_history/runs/<YYYY-MM-DD>_<run-id>.json`

## JSON/CSV schema notes (breaking change)

Legacy comparison artifacts were removed from replay artifacts. Removed fields include:
- `agreement_pct`, `dw_pct`, `sp_pct`, `uc_pct`
- `comparable_decisions`
- `method_stability.current_path` / `method_stability.resolver`
- legacy confidence projection counters (for example `legacy_unknown_now_p`)
- disagreement sample CSV artifact

`ab_metrics` is resolver-only:
- `output.confidence_counts`
- `resolver.state_counts`
- `resolver.projected_confidence_counts`
- `resolver.evidence_loss_selections`
- `resolver.neighborhood_*`
- `resolver.recent_plus1_*`
- `resolver.bayes_report_*`
- `resolver.bayes_advantage_*`
- `stabilizer_delay_proxy.*`
- `temporal.pending`
- `temporal.proposed`
- `temporal.committed`
- `temporal.handoff_attempts`
- `temporal.fallback_resolver`
- `temporal.abstain_low_margin`
- `temporal.overflow_bypass`
- `temporal.path_switches`
- `temporal.commit_latency_ms.*`
- `temporal.confirmed_history_keys`
- `temporal.confirmed_history_evictions`

Proposed means a decoder Apply suggestion; committed means an accepted identity
mutation after current eligible evidence and ordinary gates. Path switches follow
accepted identities only. Handoff attempts count the replay output boundary,
without claiming network receipt. The legacy latency field measures terminal
decision latency, including rejected proposals. Force retirement selects every
pending hold at the existing EOF clock, even when its deadline lies later.
Fresh evidence in the existing key/window does not establish a common RF episode.

Existing interval and threshold-hit CSVs append `dCapLossDiscards`,
`dLossPublicationRecoveries`, `dLossIdleRetirements`, and `PublishedLossKeys`;
previous columns retain their order. The first three are interval deltas; the
last is the interval's final readable protected-publication gauge. `gates.json`
`overall` contains cumulative final-sample `cap_loss_discards`,
`loss_publication_recoveries`, `loss_idle_retirements`, and final
`published_loss_keys`. Query-only expiry does not increment owner counters.
The runbook includes these meanings and a dedicated `evidence_loss` rejection
count outside its top-two reason summary. `evidence_loss_selections` counts
protected resolver selections, not corrections or unique delivered spots.
See [exact diagnostic units](decisions/ADR-0293-correction-decision-diagnostics.md).
Replay does not apply the runtime S recognition floor or add per-row support
percentages.

`stability` now represents resolver-applied winner follow-on stability for the emitted output path.

## Compare runs

Use:

```powershell
.\cmd\rbn_replay\compare-runs.ps1
```

Options:
- `-Date YYYY-MM-DD`
- `-RunIdA <id> -RunIdB <id>`
- `-HistoryDir "archive data/rbn_replay_history"`

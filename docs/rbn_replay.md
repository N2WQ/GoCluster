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
  - replacement license rejection before mutation and authoritative final-DX
    license rejection before output/learning

## Determinism model

- Evidence eligibility is driven by row timestamps from the RBN CSV.
- Resolver is driven by deterministic driver stepping (`observe-before-drain` ordering).
- Temporal decoder decisions use scheduled event time, not the next eligible row
  or machine wall clock. Samples and held-spot deadlines run chronologically;
  a deferred hold is revisited at its maximum wait. At equal timestamps, the
  order is sample, temporal release, then incoming rows. A report arriving
  exactly at the deadline does not influence that release.
- Runbook sample cadence is fixed at 60 seconds.
- Every valid, chronologically accepted row advances scheduled events before
  class/mode exclusions. At EOF, scheduled events advance through the existing
  `dayEnd + 24 hours` clock; samples stop at `dayEnd`, inclusive. Any holds still
  due after that EOF clock are force-retired at that unchanged clock.
- Legacy history admission, queries and maintenance use evaluation time. Zero
  source timestamps resolve to admission time; later reports are refused before
  replacing valid evidence. Legacy cleanup is synchronous at the existing
  interval, with one deadline and one sweep when due, including sampling/gaps
  and terminal temporal evaluation. It starts no machine-wall cleanup worker.
  CustomSCP retains its separate historical clock/maintenance contract.
- Replay fails fast on timestamp regressions, out-of-day records, resolver enqueue failures, and resolver hard-drop counters.

These guarantees concern fixed ordered replay input and evaluation clocks.
Receiver SNR joins are order-independent for the same retained timestamped
reports, but live ingestion still reads the latest asynchronous publication
without waiting for the current receipt. Replay does not promise to reconstruct
live arrival scheduling or every already emitted live outcome.

With identical target reports, unrelated traffic must not change a held spot's
scheduled release time or correction outcome when that traffic neither triggers
capacity limits nor changes relevant evidence. Correct deadline handling can
change replay corrections, latency metrics and interval values compared with
older runs; it does not change the correction method or live spot latency.
See [TSR-0101](troubleshooting/TSR-0101-replay-deadline-scheduling.md).

Cap-loss generation idle retirement uses monotonic process elapsed time, not CSV
timestamps. At 10 minutes without owner-admitted evidence a loss-affected
generation loses authority, even if sweeping is delayed; a paused offline job
can therefore begin a new retained population when it resumes. Temporal hold
and evidence windows remain event-time based. Deterministic tests can select the
driver's manual process clock before its first owner-admitted receipt. This does
not redesign historical CustomSCP query clocks.

Actual legacy owner requirements must fit its 64-receiver capacity before
resolver/history construction or scratch replacement. Replay consumes stabilizer
and resolver gates, without the live S floor or separate FT history. Unused
settings do not impose those capacities on replay. See
[legacy history contract](../spot/README.md#historical-receiver-freshness).

## Inputs and dependency model

Replay downloads:
- `<archive_dir>/<YYYYMMDD>.zip`

RBN history CSV mode fields are intentionally distinct:
- `mode` is the RBN spot class. Replay ingests only `CQ`, `BEACON`, and `NCDXF B`; it drops `DX`, blank, and unknown classes before resolver evidence.
- `tx_mode` is the RF/transmission mode and is the only CSV field used for `Spot.Mode`.
- `BEACON` and `NCDXF B` rows are accepted from the CSV parser but tagged as beacons and excluded from call-correction evidence.

All other dependencies are local snapshots and must already exist when enabled:
- `h3_table_path` containing `res1.bin` and `res2.bin` when CustomSCP is enabled;
  these shared geography inputs are required even with path prediction off
- `cty.file`
- `fcc_uls.allowlist_path`
- `fcc_uls.db_path`
- `ised.db_path` when enabled
- optional confusion model and spotter-reliability files

Required H3 tables are validated before output-directory creation, RBN download
or history scratch replacement. Missing, unreadable, malformed and wrong-sized
files fail startup with all detected errors reported. Each read is bounded to expected
bytes plus one; runtime never generates or rewrites tables. Paths inside the
cluster config retain process-working-directory meaning; `-config` does not
rebase them. The PowerShell replay wrapper starts from `RepoRoot`. Geography
initialization does not enable path prediction or establish full live/replay
stabilizer parity. See [ADR-0344](decisions/ADR-0344-required-shared-geography.md).

Each run stream-copies configured present FCC/ISED databases and allowlist into
owned frozen inputs before lookup. Manifest and run history `license_inputs`
record source path, SHA-256 of the bytes actually read, byte count, enforcement
and actual authority readiness. Disabled checking, unavailable authority and a
confirmed missing call are different states. Enabled missing files retain setup
errors; a malformed allowlist retains the warning/empty-exception policy and is
reported unavailable. No license update download occurs during replay.

Canadian dated eligibility uses simulated UTC at each check, including delayed
releases. Cache TTL uses process time; historical day is entry metadata rather
than an additional retained cache key. Completion and partial initialization
clear database handles/paths, allowlist, settings and cached authority before
removing run-owned copies. Cleanup failures surface and retain deletion ownership
for retry; consecutive runs cannot inherit previous authority.

Activity is counted once from each eligible admitted nonbeacon row at its
original timestamp, before dedup/correction holds. Retries do not refresh it.
Replacement rejection retains the original fields/accounting without another
candidate search. The ordinary final gate may still reject the original or an
accepted replacement after eligibility changes. Accepted temporal mutation
accounting remains separate from final output and learning.

Frozen snapshots cannot reproduce database changes at the historical date.
Replay still omits live DE/login admission, flood/primary queue pressure, State
enrichment and other production preprocessing; it does not claim full production
equivalence. See [ADR-0302](decisions/ADR-0302-correction-evidence-admission-and-license-authority.md).

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

Each receiver's newest SNR is retained separately. Equal-time numeric or
presence disagreement stays unknown until a strictly newer report supersedes
it. Canonical slash permutations share a distinct-receiver voting identity,
while mutations use a current observed display spelling. Alias-only changes do
not count as corrections or temporal switches. Fresh temporal release regroups
eligible published rows and can retain or revoke settled authority, without
settling a new winner. See the [shared evidence contract](../spot/README.md#resolver-capable-modes).

## Bayes evaluation and evidence limits

Replay and the offline evaluator use the same independent bounded report and
advantage rails, ordinary guards and typed cell mask as live correction. The
score remains a capped weighted-support/history heuristic, not a calibrated
probability or a full generative RF model. The report rail does not stack another
report on a floor already filled by ordinary recent +1. The advantage rail can
qualify an already full-report raw tie, with its own extra checks. Mask omission
preserves runtime CW/RTTY/USB/LSB eligibility; explicit `{}` enables none.
The archive study's declared maps remain CW/RTTY only. See the
[mask and capacity contract](../data/config/README.md#call-correction-bayes-cells).

Within one gate, at most eight subject/candidate alias counts are captured once
and reused across the historical qualifiers. Separate alias keys need not come
from one atomic store publication. Later correction and temporal/stabilizer
retries query afresh; no historical count cache survives the decision.

The public full-history CSV projection currently carries receiver calls and
SNR, but no DE grid or receiver coordinates, and `newReplaySpot` supplies no
geographic enrichment in ordinary replay. Loading H3 tables alone cannot supply
a receiver's cell. The owner separately accepted the frozen CTY-derived study
scenario in [approved v4](call-correction-v2-scope-ledger-v4.md). This accepts
representative prefix locations as conditional study inputs, independently of
the already accepted uncertainty in RBN reference labels. It does not establish
historical receiver sites, reported grids, station independence or live location
availability/timing. Common imputation can overstate or understate geographic
diversity; using it in every arm does not cancel nonlinear eligibility errors.

CSV replay also retains a parsed numeric 0 dB report as a measured value. The
full live RBN client currently discards that report. This existing population
difference can affect support, correction and subsequent learning; identical
archive input between arms does not establish full live-input equivalence.

### Explicit study input scenario (schema 2)

Study plans, completion reports and comparisons now require `schema_version: 2`
and an explicit `input_scenario`:

| Field | Required meaning |
| --- | --- |
| `receiver_geography` | `cty_representative_v1` |
| `cty_sha256` | Frozen CTY image digest |
| `h3_res1_sha256` | Frozen `res1.bin` digest |
| `h3_res2_sha256` | Frozen `res2.bin` digest |

These are semantic roles bound to each arm's actual configured paths and asset
manifest, rather than an unordered set of hashes. The CLI validates the schema,
scenario and frozen inputs, copies verified assets into run-owned storage, loads
a nonempty CTY image and initializes H3 before parsing raw archive contents.
Missing, corrupt, mismatched or wrongly bound assets are setup failures, not
unknown receiver locations. An invalid run never publishes `complete.json`;
partial/`FAILED.json` evidence cannot qualify a candidate.

Only the study hook derives a four-character receiver grid from the frozen CTY
image, before focal-A activity/history/learning. It uses the live receiver
normalization and portable CTY precedence, preserving location segments such as
`K1ABC/DL-#` while removing the final hyphen suffix. Known grids are marked
derived. A missing match, invalid coordinates or empty receiver remains unknown;
legitimate zero coordinates are valid. The identical rule and CTY/H3 inputs apply
to shipped, repaired-off and repaired-on arms. Ordinary replay/server behavior
and history minima are unchanged; no receiver-location registry is added.

Every focal-A decision records `receiver_location` with `lookup_call`,
`cty_prefix`, `grid`, `known`, `derived` and `reason`. Report `input_coverage`
reconciles `known_derived_grid`, `unknown_cty_match`,
`unknown_invalid_coordinates` and `unknown_empty_receiver` with
`all_focal_a_rows`, `focal_rows` and `terminal_rows`. Coverage includes warmup,
suppressed and license-rejected outcomes; unknown locations are not filtered
from the population. Withheld B rows remain raw reference inputs and do not
enter predictor learning.

Comparison validates complete coverage against decision streams and binds the
same per-row location covariates across arms alongside raw hashes, episode IDs,
R1 and R2Frozen. Terminal R2 still follows the existing 75-second decision cutoff.
Old/missing schemas, scenario drift, asset drift and paired-covariate drift are
refused. Earlier artifacts remain historical evidence; schema-1 reports cannot
be mixed into a schema-2 comparison. Comparator binaries, configurations, assets,
candidates and maps must be frozen again for the selected input scenario.

Constructed seeded-cell reachability tests establish their own wiring. Separate
CSV-to-resolver-V-to-CustomSCP tests now exercise the study hook without manually
seeded DE grids/history: four receivers across two derived cells meet the mapped
four-receiver/two-cell gate, while same-cell and unknown controls do not. Neither
test establishes real archive coverage or empirical qualification.

Constructed tests establish reachable branches and accounting, not archive
accuracy, RF truth, production performance or empirical activation. Qualification
must cover the exact scalar vector and complete active-cell combination with
untouched reference evidence, including incremental reference-supported corrected
cases and wrong renames. Neither legacy defaults nor arbitrary operator tuning
inherits that evidence. No study qualification or activation follows from this
documentation; the [execution record](call-correction-v2-execution.md) owns the
current empirical disposition.

### Frozen receipt stability audit

The separate audit entry point re-scores existing frozen
shipped/repaired-OFF/repaired-ON terminal receipts. Use it for the
exposed-data investigation authorized by [v5](call-correction-v2-scope-ledger-v5.md).
It never downloads archives, opens a predictor database, loads a historical
model or changes an original study artifact.

Supply only these flags; ordinary replay/study flags and positional arguments
are refused:

| Flag | Value |
| --- | --- |
| `-study-audit-plan` | Local, already-frozen audit JSON path |
| `-study-audit-freeze` | Its externally recorded SHA256, checked before decoding |
| `-study-audit-out` | New local directory whose parent already exists |

The strict audit schema is version 1 with `frozen: true`, `exposed: true`, and
explicit `stability` values `window_minutes: 15`, `min_follow_on: 3`,
`freq_tolerance_hz: 500`. Each `groups` entry declares a hash-bound original
`plan`, a `phase` (`tuning` or `holdout`), a `candidate` ID and `reports`.
Each report entry binds both the original completion `report` and its adjacent
`manifest` using `path`/`sha256`. Every selected original phase/date requires
all three arms. The total is at most 32 phase/date instances; reused archives
or duplicated phase/date instances are refused rather than pooled twice.

The audit verifies archive ordinals, raw payloads, receiver families, episodes,
terminal coverage and historical provenance. Every old 60-minute/two-row/1,000-Hz
integer total must reconcile before the corresponding date is scored at
15/3/500. The evidence population remains accepted A-only raw rows, including
beacon evidence before its later focal exclusion. Scoring retains literal
normalization, decision timestamps, inclusive endpoints, repeated-row counting
and the original day-end boundary; no extra mode/band filter is added.

The new directory contains `applied_receipts.jsonl`, `paired_union.jsonl`,
`aggregates.json` and a final `COMPLETE.json`. Pairing covers every immutable
focal raw ID; delayed streams are not zipped by position. Compact receipts join
by raw ID to the hash-bound original decision exports, which retain R1/R2
reference labels, unknown reasons and covariates. Aggregates report exact
fractions, added/lost receipts, semantic/literal/scorer-key changes, retiming,
stability transitions, date/mode and status counts. A zero denominator is
undefined. These are descriptive stability results, not RF truth or a new
activation qualification; retain the [reference evidence and its limitations](call-correction-v2-qualification.md).

Run-owned output bytes share a literal 4 GiB quota, with space reserved for
metadata/failure reporting. Labels are limited to 1,024 bytes, encoded rows to
64 KiB and records to 8 MiB, with preallocation checks. Day-owned archive and
pairing state retains the original 8,000,000-row/512 MiB decoded-CSV limits;
these do not establish a process-RSS limit. All output must flush, sync, close
and hash successfully before completion is published. Malformed, integrity,
budget and storage failures return an error without `COMPLETE.json`; partial
files remain and `FAILED.json` is best effort. Treat only a valid completion
binding as usable evidence.

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

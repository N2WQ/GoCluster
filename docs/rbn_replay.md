# Historical RBN replay (`rbn_replay`)

This repo includes an **offline**, **deterministic** replay runner that feeds a single day of **public RBN history** through the existing **call-correction + Phase-2 signal resolver** instrumentation, producing runbook-compatible artifacts quickly (without waiting for live shadow mode).

## Usage

Run a single UTC day:

```powershell
go run ./cmd/rbn_replay -date 2026-02-08
```

Flags:
- `-date`: Required. `YYYY-MM-DD` or `YYYYMMDD` (UTC day).
- `-replay-config`: Replay config YAML path (default: `cmd/rbn_replay/replay.yaml`).
- `-config`: Cluster config directory override (defaults to `cluster_config_dir` in replay config).
- `-archive-dir`: Archive directory override (defaults to `archive_dir` in replay config).
- `-force-download`: Re-download the RBN zip even if already present.

Replay config file:
- `cmd/rbn_replay/replay.yaml` (standalone replay settings)

## Hermetic dependency model

The runner **downloads only** the public RBN history zip for the requested day into:
- `<archive-dir>/<YYYYMMDD>.zip` (plus a sidecar status/metadata JSON written by the downloader)

All other dependencies are treated as **local snapshots** and must already exist on disk when enabled via config:
- `cty.file`
- `fcc_uls.allowlist_path`
- `fcc_uls.db_path`
- `known_calls.file`
- call-correction priors/confusion-model files when enabled

The runner does **not** start any background refresh/schedulers for these dependencies; it fails fast when an enabled dependency is missing/unreadable.

## Determinism model (event-time)

Replay is driven entirely by the RBN record timestamps (UTC):
- Call-correction and call-quality updates operate in **event-time**.
- Resolver evaluation is driven by an explicit deterministic driver (no wall-clock tickers).
- Resolver observation uses **observe-before-drain** ordering to conservatively mimic live async enqueue/observe timing.

The runbook sample cadence is fixed at **60s**.

## Output artifacts

Outputs are written under:
- `<archive-dir>/rbn_replay/<YYYY-MM-DD>/`

Files:
- `runbook_samples.log`: Timestamped `CorrGate:` and `Resolver:` summary lines in the same format the Phase-2 runbook tooling expects.
- `resolver_intervals.csv`: Derived 60s interval deltas/rates from the `Resolver:` samples.
- `resolver_threshold_hits.csv`: Interval rows where warning/critical thresholds tripped.
- `gates.json`: Aggregate totals/rates and simple “Phase 3 readiness” gate summaries.
- `disagreements_sample.csv`: Bounded sample rows of disagreement cases (`SP`, `DW`, `UC`) for debugging.
- `manifest.json`: Replay metadata (config source, input zip metadata, CSV header, record counts, outputs, final aggregate stats).

Run-memory artifacts (append-only, persisted across reruns):
- `<archive-dir>/rbn_replay_history/runs.jsonl`: one JSON record per replay execution.
- `<archive-dir>/rbn_replay_history/runs/<YYYY-MM-DD>_<run-id>.json`: immutable per-run snapshot.

Current-path stability metrics (winner follow-on ratio) are included in both `gates.json` and `manifest.json` under `overall.stability` / `results.stability`:
- `window_minutes` (default `60`)
- `min_follow_on` (default `2`)
- `freq_tolerance_hz` (default `1000`)
- `total_applied`, `stable_applied`, `stable_pct`

Dual-method summaries are included under `overall.method_stability` / `results.method_stability`:
- `current_path`: `total_applied`, `stable_applied`, `stable_pct`
- `resolver`: `total_applied`, `stable_applied`, `stable_pct`

Resolver `total_applied` is counted when resolver state is `confident|probable`, has a winner different from the pre-correction subject call, and resolver-primary gate evaluation admits that winner.

Replay A/B instrumentation summaries are included under `overall.ab_metrics` / `results.ab_metrics`:
- `current_path.confidence_counts`: final glyph histogram (`unknown`=`?`, `s`, `p`, `v`, `c`, `b`, `other`)
- `current_path.legacy_unknown_now_p`: count of current-path `P` outcomes that would have been `?` under the legacy confidence mapping
- `resolver.state_counts`: resolver snapshot state histogram (`confident`, `probable`, `uncertain`, `split`)
- `resolver.projected_confidence_counts`: resolver state-derived projected confidence histogram using replay confidence mapping
- `resolver.legacy_unknown_now_p`: resolver projected `P` outcomes that would have been `?` under legacy mapping
- `resolver.neighborhood_*`: resolver neighborhood-policy counters:
  - `neighborhood_used`
  - `neighborhood_winner_override`
  - `neighborhood_conflict_split`
  - `neighborhood_excluded_unrelated`
  - `neighborhood_excluded_distance`
  - `neighborhood_excluded_anchor_missing`
- `resolver.recent_plus1_*`: resolver recent-on-band `+1` corroborator counters:
  - `recent_plus1_applied`
  - `recent_plus1_rejected`
  - `recent_plus1_reject_edit_neighbor_contested`
  - `recent_plus1_reject_distance_or_family`
  - `recent_plus1_reject_winner_recent_insufficient`
  - `recent_plus1_reject_subject_not_weaker`
  - `recent_plus1_reject_other`
- `stabilizer_delay_proxy`: replay-side proxy counters for stabilizer delay semantics:
  - `would_delay`
  - `reason_unknown_or_nonrecent`
  - `reason_ambiguous_resolver`
  - `reason_p_low_confidence`
  - `reason_edit_neighbor_contested`

## Comparing runs (without chat history)

Use the built-in comparer against run history:

```powershell
.\cmd\rbn_replay\compare-runs.ps1
```

Options:
- `-Date 2026-02-21` compare latest two runs for one day.
- `-RunIdA <id> -RunIdB <id>` compare specific run IDs from `runs.jsonl`.
- `-HistoryDir "archive data/rbn_replay_history"` override history location.

The comparer now also emits replay A/B deltas for:
- current-path `unknown`/`p` confidence counts and `legacy_unknown_now_p`
- resolver state, projected-confidence counts, neighborhood-policy counters (including neighborhood exclusion reasons), and recent-plus1 counters
- stabilizer proxy delay counters and reason distribution

## Failure semantics (strict correctness mode)

Replay exits non-zero on:
- Any CSV timestamp regression or record outside the requested UTC day.
- Any resolver enqueue failure.
- Any resolver hard-drop counter observed (`Q/K/C/R`).

This is intentional: correctness runs treat bounded-resource drops as invalid evidence.

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

- Event-time only: replay is driven by row timestamps from the RBN CSV.
- Resolver is driven by deterministic driver stepping (`observe-before-drain` ordering).
- Temporal decoder decisions are also event-time only (hold/defer/commit windows use CSV timestamps, not wall clock).
- Runbook sample cadence is fixed at 60 seconds.
- Replay fails fast on timestamp regressions, out-of-day records, resolver enqueue failures, and resolver hard-drop counters.

## Inputs and dependency model

Replay downloads:
- `<archive_dir>/<YYYYMMDD>.zip`

All other dependencies are local snapshots and must already exist when enabled:
- `cty.file`
- `fcc_uls.allowlist_path`
- `fcc_uls.db_path`
- `known_calls.file`
- optional confusion model and spotter-reliability files

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
- `resolver.neighborhood_*`
- `resolver.recent_plus1_*`
- `resolver.bayes_report_*`
- `resolver.bayes_advantage_*`
- `stabilizer_delay_proxy.*`
- `temporal.pending`
- `temporal.committed`
- `temporal.fallback_resolver`
- `temporal.abstain_low_margin`
- `temporal.overflow_bypass`
- `temporal.path_switches`
- `temporal.commit_latency_ms.*`

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

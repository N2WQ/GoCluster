# Historical RBN replay (`rbn_replay`)

This repo includes an **offline**, **deterministic** replay runner that feeds a single day of **public RBN history** through the existing **call-correction + Phase-2 signal resolver** instrumentation, producing runbook-compatible artifacts quickly (without waiting for live shadow mode).

## Usage

Run a single UTC day:

```powershell
go run ./cmd/rbn_replay -date 2026-02-08 -config data/config -archive-dir "archive data"
```

Flags:
- `-date`: Required. `YYYY-MM-DD` or `YYYYMMDD` (UTC day).
- `-config`: Config directory (defaults to `DXC_CONFIG_PATH` or `data/config`).
- `-archive-dir`: Where inputs/outputs live (default: `archive data`).
- `-force-download`: Re-download the RBN zip even if already present.

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

## Failure semantics (strict correctness mode)

Replay exits non-zero on:
- Any CSV timestamp regression or record outside the requested UTC day.
- Any resolver enqueue failure.
- Any resolver hard-drop counter observed (`Q/K/C/R`).

This is intentional: correctness runs treat bounded-resource drops as invalid evidence.

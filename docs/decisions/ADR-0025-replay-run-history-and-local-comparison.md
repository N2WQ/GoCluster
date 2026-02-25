# ADR-0025 - Replay Run History and Local Comparison Ledger

Status: Accepted
Date: 2026-02-24
Decision Makers: Cluster maintainers
Technical Area: cmd/rbn_replay, replay operations
Decision Origin: Design
Troubleshooting Record(s): n/a
Tags: replay, reproducibility, comparison, evidence

## Context

- Replay output directories are date-based and overwritten on rerun for the same date.
- Users run multiple configuration experiments and need durable, local evidence independent of chat history.
- Phase 3 method selection requires reproducible run-to-run comparisons across changing configs.

## Decision

- Persist an append-only replay run ledger under `<archive_dir>/rbn_replay_history`:
  - `runs.jsonl` with one record per run,
  - immutable per-run snapshots under `runs/<date>_<run-id>.json`.
- Record in each run entry:
  - run identity/time/date,
  - config and artifact paths,
  - config hashes (`replay.yaml`, `pipeline.yaml`, call-correction config hash),
  - key resolver-vs-current metrics and method-level stability summaries.
- Add a local comparison utility:
  - `cmd/rbn_replay/compare-runs.ps1`
  - compares latest two runs (or explicit run IDs) using persisted ledger data.

## Alternatives Considered

1. Keep date-folder outputs only
   - Pros: no extra storage schema.
   - Cons: reruns destroy evidence continuity.
2. Require external notebook/spreadsheet tracking
   - Pros: no code changes.
   - Cons: brittle/manual and hard to reproduce.
3. Introduce SQLite-only run DB now
   - Pros: richer querying.
   - Cons: unnecessary complexity for immediate operational need.

## Consequences

- Positive outcomes:
  - Deterministic local memory of all replay runs across reruns.
  - Fast, scriptable comparison without relying on chat transcripts.
  - Better traceability from metric deltas to config fingerprints.
- Risks:
  - History file growth over time (acceptable; JSONL is append-only and compact).
  - Consumers must understand resolver applied/stability are replay-side method proxies.
- Operational impact:
  - No production runtime behavior changes.
  - Replay now writes history side effects by default.

## Links

- Decision index: `docs/decision-log.md`
- Replay docs: `docs/rbn_replay.md`
- Related ADRs:
  - `docs/decisions/ADR-0023-replay-analysis-tool-consolidation.md`
  - `docs/decisions/ADR-0024-replay-dual-method-stability-metrics.md`

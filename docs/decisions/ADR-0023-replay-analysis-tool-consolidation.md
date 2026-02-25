# ADR-0023 - Consolidate Historical Analysis into Replay Tool

Status: Accepted
Date: 2026-02-24
Decision Makers: Cluster maintainers
Technical Area: cmd/rbn_replay, analysis workflow, docs
Decision Origin: Design
Troubleshooting Record(s): n/a
Tags: replay, analysis, simplification, determinism

## Context

- The repository had two historical-analysis paths:
  - `cmd/rbn_replay` for deterministic replay and resolver/correction observability.
  - `cmd/daily_analysis` (+ wrappers/docs) for offline analysis including follow-on stability scoring.
- Maintaining two tools with overlapping purpose increased operator ambiguity and duplicated maintenance effort.
- Replay already became the preferred execution path for Phase 2/3 evidence generation.
- Stability scoring was recently made reusable via `internal/stability`.

## Decision

- Consolidate to a single historical analysis entrypoint: `cmd/rbn_replay`.
- Remove `cmd/daily_analysis`, `cmd/run_daily_analysis.go`, `cmd/run-daily-analysis.ps1`, and daily-analysis-specific docs.
- Keep and use a standalone replay config file co-located with replay code:
  - `cmd/rbn_replay/replay.yaml`.
- Replay consumes:
  - replay-run settings from `cmd/rbn_replay/replay.yaml`,
  - cluster pipeline/runtime settings from configured cluster YAML directory.
- Replay remains deterministic and now includes follow-on stability output in primary artifacts (`manifest.json`/`gates.json`).

## Alternatives Considered

1. Keep both tools and document division of responsibility
   - Pros: no migration work.
   - Cons: persistent duplication and operator confusion.
2. Keep daily-analysis as wrapper around replay
   - Pros: backward command compatibility.
   - Cons: still two public entrypoints and maintenance surface.
3. Keep replay and defer deletion
   - Pros: lowest short-term risk.
   - Cons: drags cleanup and prolongs split ownership.

## Consequences

- Positive outcomes:
  - Single source of truth for historical evidence generation.
  - Lower maintenance burden and fewer divergent semantics.
  - Clear replay-local config surface.
- Risks:
  - Users/scripts relying on removed daily-analysis commands must migrate.
  - Consolidated tool now carries broader responsibility and needs stricter compatibility discipline.
- Operational impact:
  - Operators run only `cmd/rbn_replay`.
  - Stability metrics available directly in replay outputs.

## Links

- Decision index: `docs/decision-log.md`
- Replay docs: `docs/rbn_replay.md`
- Implementation paths:
  - `cmd/rbn_replay/`
  - `internal/stability/`

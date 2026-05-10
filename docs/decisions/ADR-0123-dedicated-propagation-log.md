# ADR-0123 - Dedicated Propagation Log

Status: Accepted
Date: 2026-05-08
Decision Makers: Founder, Codex
Technical Area: logging, pathreliability, propagation reports
Decision Origin: Design
Troubleshooting Record(s): none
Tags: propagation, logging, PATHP50, reports

Note: ADR-0126 supersedes the PATHP50 shadow-comparison portions of this ADR.
The dedicated propagation log remains accepted, but runtime no longer emits
`Path p50 shadow` lines.

Note: ADR-0128 supersedes the `Path p50 diag (5m)` aggregate and propagation
report parsing portions of this ADR. The dedicated propagation log remains
accepted.

## Context

Path reliability diagnostics and daily propagation reports previously depended
on file-only lines written into the system log. As path diagnostics gained more
detail, mixing propagation evidence with general runtime logging made reports
and support investigation less clear.

PATHP50 diagnostics also need aggregate evidence, but normal path display must
not pay additional p50 computation cost just to feed a log.

## Decision

Write path/propagation aggregate lines to a dedicated daily propagation log
under `logging.propagation.dir`, shipped as `data/logs/propagation`.

Move existing path aggregate lines to that sink:

- `Path predictions (5m)`
- `Path source mix (5m)`
- `Path buckets (5m)`
- `Path weight dist (5m)`
- `Path ge10 variance (5m)`
- `Path unique spotters (hour)`
- `Path unique grid pairs (hour)`

Add `Path p50 diag (5m)` as a diagnostic-observed aggregate. The counters are
updated only when a PATHP50 diagnostic prediction already computed p50. Normal
spot display still uses the non-distribution prediction path and does not
compute p50 for logging.

Add companion `Path p50 shadow` aggregate lines to compare the active
mean-based glyph class with the p50 shadow glyph class. The log records fixed
same/different outcome counters, sample-count buckets, band buckets, mode
family buckets, source buckets, and the mean/p50 glyph-pair matrix. These
counters are updated only on the existing PATHP50 diagnostic path. As refined
by ADR-0125, this shadow glyph comparison gates p50 with the same active
eligibility used by normal path display.

Daily propagation report rotation and scheduled generation read the propagation
log path. Manual `prop_report -log` remains available for historical system log
files.

## Alternatives Considered

1. Keep propagation lines in the system log.
   - Pros: no new file sink.
   - Cons: report inputs and support investigation remain mixed with unrelated
     runtime events.
2. Duplicate propagation lines to both system and propagation logs.
   - Pros: backward-visible in the system log.
   - Cons: doubles write volume for no analytical benefit and violates the
     intent to separate logs.
3. Compute p50 for every normal path prediction.
   - Pros: unbiased fleet-wide aggregate.
   - Cons: adds CPU to the normal path hot path. Deferred until separately
     approved with performance evidence.

## Consequences

- Positive outcomes:
  - Propagation reports have a dedicated input file.
  - System logs are quieter.
  - PATHP50 aggregate evidence is available during diagnostic runs.
  - Shadow comparison lines expose whether p50 differs mostly by low sample
    count, band, mode, source, or broad glyph-pair class.
- Negative outcomes / risks:
  - A new file sink adds constant startup/runtime overhead.
  - PATHP50 aggregate data is diagnostic-observed, not fleet-wide.
  - Shadow comparison does not prove correctness without a later outcome proxy;
    it only shows when the two candidate methods disagree.
  - Operators must look in `data/logs/propagation` for path aggregate lines.
- Operational impact:
  - New config block: `logging.propagation`.
  - `cmd/prop_report` defaults to `data/logs/propagation/<DD-Mon-YYYY>.log`.
  - Historical reports can still use explicit `-log` paths.

## Validation

Required validation includes:

- config loader tests for propagation log defaults and invalid retention.
- runtime tests for fixed PATHP50 diagnostic counter bucketing/reset.
- propagation-report parser tests for `Path p50 diag (5m)` and `Path p50
  shadow` lines.
- targeted package tests across config, telnet, internal/cluster,
  internal/propreport, and cmd/prop_report.
- full repo tests, race check, vet, staticcheck, golangci-lint.
- benchmark evidence showing normal path prediction remains allocation-free and
  the p50 distribution benchmark remains allocation-free.

## Rollout and Reversal

- Rollout plan:
  - Ship with `logging.propagation.enabled: true`.
  - Generate daily propagation reports from the propagation log.
- Backward compatibility impact:
  - Existing historical system logs can still be parsed by passing `-log`.
- Reversal plan:
  - Set `logging.propagation.enabled: false` to disable propagation file writes.
  - Rewire report scheduling to a different explicit log path in a later
    decision if needed.

## References

- Issue(s): none
- PR(s): pending
- Commit(s): pending
- Related ADR(s): ADR-0093, ADR-0095, ADR-0096, ADR-0122
- Troubleshooting Record(s): none
- Docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/README.md`

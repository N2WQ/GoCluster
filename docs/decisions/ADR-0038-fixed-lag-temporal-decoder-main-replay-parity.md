# ADR-0038 - Fixed-Lag Temporal Decoder for Resolver-Primary Runtime/Replay Parity

Status: Accepted
Date: 2026-02-27
Decision Makers: Cluster maintainers
Technical Area: internal/correctionflow, main output pipeline, cmd/rbn_replay, config, stats
Decision Origin: Design
Troubleshooting Record(s): none
Tags: resolver-primary, temporal-decoder, replay-parity, bounded-resources, determinism

## Context

- Resolver-primary correction previously committed from the latest aggregate snapshot only.
- Late corroborating evidence can improve recall, but immediate commits can miss those opportunities.
- Telnet stabilizer delay is output-channel policy only and does not change resolver winner selection.
- Runtime and replay must stay behaviorally aligned so replay remains rollout-valid.
- Any new sequence logic must stay deterministic and resource-bounded (bounded pending requests, bounded active keys, bounded per-key event depth).

## Decision

- Add shared fixed-lag temporal decoding in `internal/correctionflow/temporal_decoder.go`.
- Introduce `call_correction.temporal_decoder.*` config surface with normalized defaults and validation.
- Route resolver-primary candidates through temporal hold/commit before final correction apply when enabled:
  - runtime: `processOutputSpots` path in `main.go`
  - replay: event-time path in `cmd/rbn_replay/main.go`
- Keep existing resolver primary rails unchanged after temporal commit:
  - resolver gates
  - invalid-base rejection rail
  - CTY rejection rail
  - existing suppress/broadcast behavior.
- Enforce bounded fail-open behavior on temporal overflow via explicit action:
  - `fallback_resolver`
  - `abstain`
  - `bypass`.
- Add temporal observability in runtime tracker and replay `ab_metrics.temporal`.

## Alternatives Considered

1. Keep immediate resolver-only commit (no temporal decoder)
   - Pros:
     - no additional latency or state.
     - lowest implementation complexity.
   - Cons:
     - misses late-evidence recall opportunities in ambiguous windows.

2. Add temporal decoding in runtime only
   - Pros:
     - smaller replay surface change.
   - Cons:
     - breaks runtime/replay parity and weakens replay validation value.

3. Use unbounded sequence history
   - Pros:
     - maximal temporal context.
   - Cons:
     - higher memory/CPU risk and harder deterministic behavior guarantees.

## Consequences

- Positive outcomes:
  - Recall can improve for late/ambiguous evidence while preserving existing correction rails.
  - Runtime and replay share the same temporal decision semantics.
  - Operators can gate rollout using one config block and bounded overflow actions.
- Negative outcomes / risks:
  - Any non-zero lag adds decision latency for held candidates.
  - Extra per-key state/heap management increases implementation complexity.
- Operational impact:
  - New optional config block (`call_correction.temporal_decoder`).
  - New runtime/replay temporal counters and latency buckets.
  - No protocol wire-format changes.
- Follow-up work required:
  - Replay-based tuning of lag/margin/penalty defaults before broad production enablement.

## Validation

- Added/updated tests:
  - `internal/correctionflow/temporal_decoder_test.go`
  - `config/call_correction_stabilizer_test.go`
  - `cmd/rbn_replay/ab_metrics_test.go`
  - `stats/tracker_temporal_test.go`
  - existing runtime/replay correction tests remain green.
- Checker evidence:
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`
- Invalidation signals:
  - Recall decrease or stability regressions in replay canary comparisons.
  - Sustained temporal overflow actions under nominal load.

## Rollout and Reversal

- Rollout plan:
  - Keep `call_correction.temporal_decoder.enabled=false` by default.
  - Enable first with conservative settings (`scope=uncertain_only`, bounded lag/margin).
  - Evaluate replay and live counters before widening scope.
- Backward compatibility impact:
  - Backward-compatible by default (`enabled=false`).
  - When enabled, correction commit timing and some winners can differ by design.
- Reversal plan:
  - Set `call_correction.temporal_decoder.enabled=false` to return to immediate resolver-primary behavior.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0028-shared-correctionflow-main-replay.md`
  - `docs/decisions/ADR-0030-shared-stabilizer-policy-parity-main-replay.md`
  - `docs/decisions/ADR-0036-resolver-confusion-tiebreak-runtime-replay-parity.md`
- Troubleshooting Record(s): none
- Docs:
  - `docs/decision-log.md`
  - `docs/rbn_replay.md`
  - `README.md`

# ADR-0033 - Resolver Neighborhood Selection and Contested Edit-Neighbor Rails

Status: Accepted
Date: 2026-02-26
Decision Makers: Cluster maintainers
Technical Area: main output pipeline, internal/correctionflow, telnet suppression, cmd/rbn_replay, config
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0006
Tags: resolver-primary, neighborhood, stabilizer, suppression, replay-parity

## Context

- Resolver-primary is the long-term authority path, but one-character variants could still leak under bucket-boundary and timing conditions.
- Stabilizer/suppressor family rails handled slash/truncation better than substitution-class neighbors.
- Replay acceptance analysis must use the same policy decisions as runtime to avoid drift in rollout evidence.
- Any change must preserve bounded resolver runtime contracts and deterministic tie-breaking.

## Decision

- Adopt shared resolver neighborhood selection across adjacent buckets for resolver-primary and replay:
  - `internal/correctionflow.SelectResolverPrimarySnapshot(...)` now aggregates adjacent bucket winners (bounded radius), applies deterministic ranking, and emits explicit neighborhood split/override signals.
- Apply adaptive min-reports parity in resolver-primary apply gating:
  - resolver-primary now uses `ResolveRuntimeSettings(..., adaptive, ..., true)` so adaptive strictness matches correction expectations.
- Add resolver-contested edit-neighbor stabilizer delay:
  - new delay reason `edit_neighbor_contested`,
  - new bounded checks/spotter thresholds,
  - feature-gated by new stabilizer config knobs.
- Add resolver-driven edit-neighbor telnet suppression safety rail:
  - only for contested neighbors and only when enabled,
  - deterministic tie-break (higher support wins; ties keep earlier emitted call).
- Extend config and observability contracts:
  - new config knobs for neighborhood and edit-neighbor policies,
  - new runtime/replay reason counters and replay comparison output for neighborhood/edit-neighbor outcomes.

## Alternatives Considered

1. Keep family-only rails and tune thresholds
   - Pros:
     - Fewer code/config changes.
   - Cons:
     - Leaves substitution-class blind spots and contested leakage unresolved.
2. Always suppress late edit-distance neighbors regardless of resolver context
   - Pros:
     - Simpler suppression logic.
   - Cons:
     - Higher false suppression risk; not aligned with resolver evidence confidence.
3. Expand to full Levenshtein neighborhood search in hot path
   - Pros:
     - Broader typo coverage.
   - Cons:
     - Higher compute/complexity risk and weaker determinism in rollout phase.

## Consequences

- Positive outcomes:
  - Better resolver-primary stability at bucket boundaries.
  - Better contested-variant containment for one-character substitution cases.
  - Stronger runtime/replay policy parity for rollout decisions.
- Negative outcomes / risks:
  - More policy complexity and new config surface area.
  - Potential over-delay/over-suppression if thresholds are set too aggressively.
- Operational impact:
  - New metrics/reason labels must be monitored during canary.
  - Feature gates enable staged rollout and fast rollback for neighborhood/edit-neighbor behavior.
- Follow-up work required:
  - Complete P3 retirement/deprecation decisions and burn-in acceptance gates from scope ledger v5.

## Validation

- Added/updated tests:
  - `internal/correctionflow/shared_test.go` (neighborhood winner/split + variant generation),
  - `main_test.go` (resolver adaptive gate parity, neighborhood apply/conflict, edit-neighbor stabilizer),
  - `internal/correctionflow/stabilizer_test.go` (edit-neighbor delay policy),
  - `telnet_family_suppressor_test.go` (resolver-contested edit-neighbor suppression),
  - `config/call_correction_stabilizer_test.go` (new knob defaults/overrides/sanitization),
  - `cmd/rbn_replay/ab_metrics_test.go` (neighborhood counters).
- Checker evidence:
  - `go test ./... -count=1`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./... -count=1`

## Rollout and Reversal

- Rollout plan:
  - Keep new neighborhood/edit-neighbor toggles off by default in production until replay evidence and canary windows are acceptable.
  - Use replay counters/reason labels to evaluate impact before broad enablement.
- Backward compatibility impact:
  - No protocol wire format changes.
  - Config schema extends with new optional keys.
  - User-visible correction/suppression timing can change when gates are enabled.
- Reversal plan:
  - Disable neighborhood/edit-neighbor knobs to return to prior runtime behavior.
  - Revert shared selection/suppression wiring if needed.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
  - `docs/decisions/ADR-0028-shared-correctionflow-main-replay.md`
  - `docs/decisions/ADR-0030-shared-stabilizer-policy-parity-main-replay.md`
  - `docs/decisions/ADR-0032-resolver-primary-family-gate-parity-and-conservative-contested-glyphs.md`
- Troubleshooting Record(s):
  - `docs/troubleshooting/TSR-0006-resolver-neighborhood-and-edit-neighbor-contested-rails.md`
- Docs:
  - `docs/call-correction-resolver-scope-ledger-v5.md`
  - `docs/rbn_replay.md`
  - `docs/OPERATOR_GUIDE.md`
  - `docs/troubleshooting-log.md`
  - `docs/decision-log.md`

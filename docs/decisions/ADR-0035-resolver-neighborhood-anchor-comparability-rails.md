# ADR-0035 - Resolver Neighborhood Anchor-Scoped Comparability Rails

Status: Accepted
Date: 2026-02-26
Decision Makers: Cluster maintainers
Technical Area: internal/correctionflow, main output pipeline, cmd/rbn_replay, config
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0008
Tags: resolver-primary, neighborhood, comparability, replay-parity

## Context

- ADR-0033 introduced resolver neighborhood competition to reduce bucket-boundary winner forks.
- Replay analysis after rollout showed degraded resolver outcomes driven by increased neighborhood conflict splits.
- Root cause: neighborhood arbitration considered adjacent winners that were not comparable to the subject signal identity, allowing unrelated neighbors to participate in split forcing.
- Runtime and replay must remain parity-aligned for evidence-based rollout decisions.

## Decision

- Introduce call-anchored neighborhood selection:
  - `internal/correctionflow.SelectResolverPrimarySnapshotForCall(...)` takes the pre-correction subject call as the neighborhood anchor.
  - `SelectResolverPrimarySnapshot(...)` remains as a wrapper for non-anchor call sites.
- Apply comparability rails to neighborhood arbitration:
  - A neighborhood winner is admitted only if comparable to the anchor by one of:
    - same normalized vote key,
    - slash-family relation,
    - truncation-family relation when enabled,
    - mode-aware edit distance <= configured max distance.
- Restrict split and override arbitration:
  - Runner-up conflict split checks apply only when top/runner are comparable.
  - Neighborhood winner override over exact snapshot winner is allowed only when exact/synth winners are comparable.
  - Otherwise selection fails closed to exact snapshot.
- Extend config contract for explicit neighborhood comparability control:
  - `call_correction.resolver_neighborhood_max_distance` (default `1`, sanitize <=0 to `1`)
  - `call_correction.resolver_neighborhood_allow_truncation_family` (default `true` when unset)
- Extend replay observability and comparison output:
  - Add resolver AB counters:
    - `neighborhood_excluded_unrelated`
    - `neighborhood_excluded_distance`
    - `neighborhood_excluded_anchor_missing`
  - Surface these counters in compare tooling and replay docs.

## Alternatives Considered

1. Keep current neighborhood logic and tune `freq_guard_runnerup_ratio`
   - Pros:
     - No new policy dimensions.
   - Cons:
     - Does not fix unrelated-signal split forcing; only shifts threshold sensitivity.
2. Disable neighborhood competition entirely
   - Pros:
     - Removes new regression source quickly.
   - Cons:
     - Loses boundary-fork benefits that improved true nearby winner consolidation.
3. Compare all adjacent calls only by global edit distance
   - Pros:
     - Simple single-rule admission.
   - Cons:
     - Loses slash/truncation semantics and mode-aware family contracts.

## Consequences

- Positive outcomes:
  - Preserves neighborhood boundary benefits for comparable calls.
  - Prevents unrelated adjacent buckets from forcing split states.
  - Adds replay visibility into exclusion causes for tuning/rollback decisions.
- Negative outcomes / risks:
  - More policy knobs and exclusion paths to monitor.
  - Potential under-aggregation if `resolver_neighborhood_max_distance` is set too low.
- Operational impact:
  - Replay comparisons should show reduced `neighborhood_conflict_split` where unrelated conflicts previously dominated.
  - Exclusion counters become primary diagnostics for neighborhood behavior tuning.

## Validation

- Added/updated tests:
  - `internal/correctionflow/shared_test.go`
    - `TestSelectResolverPrimarySnapshotForCallIgnoresUnrelatedNeighbor`
    - `TestSelectResolverPrimarySnapshotForCallPreservesTruncationBenefit`
  - `config/call_correction_stabilizer_test.go` (new neighborhood knob defaults/overrides/sanitization)
  - `cmd/rbn_replay/ab_metrics_test.go` (neighborhood exclusion counters)
- Checker evidence:
  - `go test ./internal/correctionflow ./config ./cmd/rbn_replay -count=1`
  - `go test . -count=1`
  - `go test ./... -count=1`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./... -count=1`

## Rollout and Reversal

- Rollout plan:
  - Keep neighborhood enabled where already canaried, and monitor replay exclusion counters alongside split/apply rates.
  - Adjust `resolver_neighborhood_max_distance` only if exclusion counters show over-restriction.
- Backward compatibility impact:
  - No wire/protocol format changes.
  - Config schema adds optional keys with safe defaults.
  - User-visible correction behavior changes when neighborhood policy is enabled.
- Reversal plan:
  - Disable neighborhood mode to return to exact-bucket-only behavior, or revert to ADR-0033 semantics if required.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Supersedes: none
- Related ADR(s):
  - `docs/decisions/ADR-0033-resolver-neighborhood-and-contested-edit-neighbor-rails.md`
  - `docs/decisions/ADR-0034-resolver-recent-plus1-corroborator-rail.md`
- Troubleshooting Record(s):
  - `docs/troubleshooting/TSR-0008-resolver-neighborhood-unrelated-adjacent-split-regression.md`
- Docs:
  - `docs/call-correction-resolver-scope-ledger-v7.md`
  - `docs/rbn_replay.md`
  - `docs/decision-log.md`
  - `docs/troubleshooting-log.md`

# Call-Correction Resolver Scope Ledger v7

Status: Approved
Date: 2026-02-26
Classification: Non-trivial
Dependency rigor: Full (shared pipeline behavior, resolver/replay/config/observability contracts)
Pre-edit checklist: Scope Ledger approved? yes (`Approved v7`)
Scope: v6 baseline plus neighborhood comparability hardening to prevent unrelated adjacent-bucket split forcing while preserving neighborhood benefits.
Decision refs: ADR-0026, ADR-0028, ADR-0030, ADR-0032, ADR-0033, ADR-0034, ADR-0035, TSR-0004, TSR-0005, TSR-0006, TSR-0007, TSR-0008
Supersedes: `docs/call-correction-resolver-scope-ledger-v6.md`

## Inherited Scope

- All v6 items remain in effect.
- Status unchanged from v6 except for new v7 additions below.
- Still pending (unchanged):
  - A12: migration switch to disable legacy shadow comparator after burn-in.
  - A14: legacy-shadow retirement gate criteria.

## v7 Additions

| ID | Status | Keep/Add/Defer | Scope Item | Why / Contract Impact |
|---|---|---|---|---|
| A18 | Implemented | Add | Resolver neighborhood anchor-scoped comparability rails | Prevent unrelated adjacent buckets from entering winner-override/split arbitration; preserves comparable neighborhood benefits. |
| A19 | Implemented | Add | Neighborhood fail-closed override contract | If neighborhood winner is not comparable to exact winner, selection falls back to exact snapshot deterministically. |
| A20 | Implemented | Add | Config knobs for neighborhood comparability (`resolver_neighborhood_max_distance`, `resolver_neighborhood_allow_truncation_family`) | Gives deterministic rollout control over neighborhood admission policy. |
| A21 | Implemented | Add | Replay neighborhood exclusion telemetry + comparer parity (`excluded_unrelated`, `excluded_distance`, `excluded_anchor_missing`) | Enables causal analysis of replay deltas and confirms replay/main behavior parity. |
| A22 | Implemented | Add | Decision memory updates (TSR-0008, ADR-0035) | Captures durable troubleshooting-origin contract change. |

## Explicitly Not Ported (Unchanged)

- Same deferred/not-ported set as v6 (N1-N8), including resolver-only path not porting legacy cooldown/quality-anchor authority.

## Implementation Sequencing

| Phase | Status | Scope |
|---|---|---|
| P1 | Implemented | A1, A4, A7, A9, A13 |
| P2 | Implemented | A2, A3, A5, A6, A8, A10 |
| P2.5 | Implemented | A15, A16, A17 + A11 updates |
| P2.6 | Implemented | A18, A19, A20, A21, A22 |
| P3 | Agreed/Pending | A12, A14 and legacy-shadow retirement enforcement |

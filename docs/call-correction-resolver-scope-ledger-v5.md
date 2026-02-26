# Call-Correction Resolver Scope Ledger v5

Status: Superseded by v6
Date: 2026-02-26
Classification: Non-trivial
Dependency rigor: Full (shared pipeline behavior, resolver/stabilizer/suppression/config/observability contracts)
Pre-edit checklist: Scope Ledger approved? yes (`Approved v5`)
Scope: Unified resolver-only transition ledger (code + docs + replay parity)
Decision refs: ADR-0026, ADR-0028, ADR-0030, ADR-0032, ADR-0033, TSR-0004, TSR-0005, TSR-0006
Supersedes: `docs/call-correction-phase3-switchover-ledger-v2.md`, `docs/call-correction-phase3-switchover-ledger-v3.md`

## Resolver-Only Baseline (Keep)

| ID | Status | Keep/Add/Defer | Scope Item | Why / Contract Impact |
|---|---|---|---|---|
| K1 | Agreed/Pending | Keep | Resolver weighted vote state machine (`confident/probable/uncertain/split`) | Core recall/stability behavior already outperforming legacy. |
| K2 | Agreed/Pending | Keep | Resolver split detection using distance + overlap + frequency guard | Deterministic contested-signal protection; keep as primary ambiguity rail. |
| K3 | Agreed/Pending | Keep | Resolver-primary gate helper with family truncation rails | Preserves current family recall improvements in primary mode. |
| K4 | Agreed/Pending | Keep | Spotter reliability weighting and minimum reliability floor | Keeps noisy spotter suppression and deterministic weighted support. |
| K5 | Agreed/Pending | Keep | CTY invalid rails (`C`/`B`/suppress) in resolver apply | Maintains existing operator safety and invalid-call handling contract. |
| K6 | Agreed/Pending | Keep | Resolver bounded resource model (queue caps, candidate/reporter caps, fail-open enqueue) | Preserves non-blocking ingest and bounded memory guarantees. |
| K7 | Agreed/Pending | Keep | Stabilizer resolver-aware ambiguity and low-confidence delay reasons | Keeps useful safety net while resolver is authority. |

## Resolver-Forward Additions

| ID | Status | Keep/Add/Defer | Scope Item | Why / Contract Impact |
|---|---|---|---|---|
| A1 | Implemented | Add | Fix confidence semantics to reflect emitted call outcome (not winner-only snapshot) | Removes misleading dual-`V/P` behavior in contested/no-apply cases. |
| A2 | Implemented | Add | Resolver-primary adaptive min-reports integration | Restores adaptive strictness parity in resolver path without legacy dependency. |
| A3 | Implemented | Add | Resolver neighborhood competition across adjacent frequency buckets | Reduces false forks where near-identical signals land in neighboring rounded buckets. |
| A4 | Implemented | Add | Single-observation resolver evidence lifecycle (dedupe main vs stabilizer re-release enqueue) | Prevents double counting and vote inflation from delayed re-entry. |
| A5 | Implemented | Add | Resolver-native contested edit-neighbor delay policy (distance-1 substitutions) | Targets the dominant CW/RTTY error class missing from family-only rails. |
| A6 | Implemented | Add | Resolver-driven late-variant suppression for contested neighbors | Output-stage safety net after correction timing races. |
| A7 | Implemented | Add | Resolver rejection/apply reason telemetry with stable labels | Required to tune and operate resolver-only safely. |
| A8 | Implemented | Add | Config surface for resolver-native contested policies (feature-gated rollout) | Enables safe canary and rollback control. |
| A9 | Implemented | Add | Deterministic tests plus race coverage for new resolver/stabilizer/suppression behavior | Protects determinism and bounded-resource guarantees. |
| A10 | Implemented | Add | Replay and runtime metrics extensions for resolver-only acceptance gates | Evidence-based rollout guardrails. |
| A11 | Agreed/Pending | Add | TSR/ADR/log updates for durable resolver-only decisions | Decision memory and operational traceability requirement. |
| A12 | Agreed/Pending | Add | Migration switch to disable legacy shadow comparator after burn-in | Final step toward resolver-only authority path. |
| A13 | Implemented | Add | Explicit main/replay policy-path parity contract | Prevents logic drift by requiring shared decision-policy helpers and parity checks. |
| A14 | Agreed/Pending | Add | Legacy-shadow retirement gate criteria | Prevents premature comparator removal; requires clean burn-in evidence windows. |

## Explicitly Not Ported (Deferred)

| ID | Status | Keep/Add/Defer | Scope Item | Why Deferred/Not Ported |
|---|---|---|---|---|
| N1 | Deferred | Not port | Legacy confusion-model tie-break ranking | High complexity, low proven incremental value in resolver-only path. |
| N2 | Deferred | Not port | Quality-anchor reinforcement loop | Risk of self-reinforcing false winners; conflicts with resolver-first simplification. |
| N3 | Deferred | Not port | Legacy cooldown gate | Can block true corrections; resolver contested policy is cleaner. |
| N4 | Deferred | Not port | Legacy prior-bonus min-reports boost | Adds heuristic coupling; resolver support model should stay explicit. |
| N5 | Deferred | Not port | Legacy recent-band bonus as min-reports boost | Keep recent-band for stabilizer/support validation only, not score inflation. |
| N6 | Deferred | Not port | Legacy anchor plus top-K consensus attempt engine | Superseded by resolver authority in long-term architecture. |
| N7 | Deferred | Not port | `CorrectionIndex` candidate scan as correction authority | Remove once resolver-only cutover and shadow retirement complete. |
| N8 | Deferred | Not port | Pre-correction stabilizer-eval proposal | Stabilizer should gate emitted call path, not obsolete pre-correction value. |

## Implementation Sequencing (Resolver-First)

| Phase | Status | Scope |
|---|---|---|
| P1 | Implemented | A1, A4, A7, A9, A13 (correctness + observability + safety + parity guard) |
| P2 | Implemented | A2, A3, A5, A6, A8, A10 (resolver-native behavior upgrades + rollout knobs/evidence) |
| P3 | Agreed/Pending | A11, A12, A14, N-items enforcement (decision records + legacy retirement path) |

## Confirmed Policy Choices

1. Legacy shadow comparator sunset policy: keep for one burn-in window.
2. Contested edit-neighbor timeout action: `release`.
3. Deprecation mode for non-ported knobs: soft-deprecate as no-op first.
4. Delivery strategy: phased P1 -> P3.

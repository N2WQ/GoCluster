# Call-Correction Resolver Scope Ledger v6

Status: Approved
Date: 2026-02-26
Classification: Non-trivial
Dependency rigor: Full (shared pipeline behavior, resolver/stabilizer/suppression/config/observability contracts)
Pre-edit checklist: Scope Ledger approved? yes (`Approved v6`)
Scope: Single unified ledger for resolver-only transition, including conservative recent-on-band `+1` corroborator policy.
Decision refs: ADR-0026, ADR-0028, ADR-0030, ADR-0032, ADR-0033, ADR-0034, TSR-0004, TSR-0005, TSR-0006, TSR-0007
Supersedes: `docs/call-correction-resolver-scope-ledger-v5.md`

## Resolver-Only Baseline (Keep)

| ID | Status | Keep/Add/Defer | Scope Item | Why / Contract Impact |
|---|---|---|---|---|
| K1 | Implemented (Retained) | Keep | Resolver weighted vote state machine (`confident/probable/uncertain/split`) | Core resolver recall/stability behavior. |
| K2 | Implemented (Retained) | Keep | Resolver split detection using distance + overlap + frequency guard | Primary ambiguity rail. |
| K3 | Implemented (Retained) | Keep | Resolver-primary gate helper with family truncation rails | Maintains family recall parity in resolver mode. |
| K4 | Implemented (Retained) | Keep | Spotter reliability weighting and minimum reliability floor | Preserves deterministic noise suppression. |
| K5 | Implemented (Retained) | Keep | CTY invalid rails (`C`/`B`/suppress) in resolver apply | Preserves invalid-call handling contract. |
| K6 | Implemented (Retained) | Keep | Resolver bounded resource model (queue/candidate/reporter caps, fail-open enqueue) | Preserves ingest non-blocking and bounded memory. |
| K7 | Implemented (Retained) | Keep | Stabilizer resolver-aware ambiguity and low-confidence delay reasons | Maintains safety net under resolver authority. |

## Resolver-Forward Additions

| ID | Status | Keep/Add/Defer | Scope Item | Why / Contract Impact |
|---|---|---|---|---|
| A1 | Implemented | Add | Confidence semantics track emitted-call outcome | Removes overstated `V/P` on no-apply paths. |
| A2 | Implemented | Add | Resolver-primary adaptive min-reports integration | Keeps adaptive strictness parity in resolver path. |
| A3 | Implemented | Add | Resolver neighborhood competition across adjacent frequency buckets | Reduces near-bucket false forks. |
| A4 | Implemented | Add | Single-observation resolver evidence lifecycle | Prevents double-count vote inflation. |
| A5 | Implemented | Add | Resolver-contested edit-neighbor stabilizer delay | Covers substitution-class contested errors. |
| A6 | Implemented | Add | Resolver-driven late-variant suppression for contested neighbors | Output-stage safety net for timing races. |
| A7 | Implemented | Add | Stable resolver apply/reject telemetry reasons | Required for safe operations/tuning. |
| A8 | Implemented | Add | Config surface for resolver contested policies | Enables gated rollout/rollback. |
| A9 | Implemented | Add | Deterministic/race coverage for resolver/stabilizer/suppression behavior | Protects deterministic bounded contracts. |
| A10 | Implemented | Add | Replay and runtime metrics extensions for acceptance gates | Evidence-based rollout guardrails. |
| A11 | Implemented | Add | TSR + ADR + logs updates for durable decisions | Preserves decision traceability contract. |
| A12 | Agreed/Pending | Add | Migration switch to disable legacy shadow comparator after burn-in | Final resolver-only cutover switch. |
| A13 | Implemented | Add | Explicit main/replay policy-path parity contract | Prevents runtime/replay logic drift. |
| A14 | Agreed/Pending | Add | Legacy-shadow retirement gate criteria | Ensures comparator retirement is evidence-gated. |
| A15 | Implemented | Add | Conservative resolver recent-on-band `+1` corroborator rail (one-short only) | Improves recall without broad score inflation. |
| A16 | Implemented | Add | Resolver recent `+1` replay/runtime parity counters and reason taxonomy | Allows safe impact measurement and rollback detection. |
| A17 | Implemented | Add | Resolver recent `+1` defaults and rails in config (`enabled=true`, `min_unique=3`, `subject_weaker=true`, `max_distance=1`, `allow_trunc=true`) | Captures approved v6 policy choices and deterministic startup behavior. |

## Explicitly Not Ported (Deferred)

| ID | Status | Keep/Add/Defer | Scope Item | Why Deferred/Not Ported |
|---|---|---|---|---|
| N1 | Deferred | Not port | Legacy confusion-model tie-break ranking | High complexity, low incremental value in resolver-only path. |
| N2 | Deferred | Not port | Quality-anchor reinforcement loop | Self-reinforcement risk conflicts with resolver simplification. |
| N3 | Deferred | Not port | Legacy cooldown gate | Can block true corrections; resolver gates are cleaner. |
| N4 | Deferred | Not port | Legacy prior-bonus min-reports boost | Adds heuristic coupling to resolver path. |
| N5 | Superseded by A15 | Not port | Legacy recent-band min-reports boost | Replaced with bounded resolver-only `+1` corroborator rail. |
| N6 | Deferred | Not port | Legacy anchor + top-K consensus attempt engine | Superseded by resolver authority architecture. |
| N7 | Deferred | Not port | `CorrectionIndex` candidate scan as correction authority | Remove only after full resolver-only shadow retirement. |
| N8 | Deferred | Not port | Pre-correction stabilizer evaluation proposal | Stabilizer should gate emitted-call path only. |

## Implementation Sequencing (Resolver-First)

| Phase | Status | Scope |
|---|---|---|
| P1 | Implemented | A1, A4, A7, A9, A13 |
| P2 | Implemented | A2, A3, A5, A6, A8, A10 |
| P2.5 | Implemented | A15, A16, A17 + A11 updates for decision-memory closure |
| P3 | Agreed/Pending | A12, A14, deferred-item enforcement and burn-in retirement |

## Confirmed Policy Choices (v6)

1. `resolver_recent_plus1_enabled`: `true`.
2. Subject-weaker rail for recent `+1`: `true` (`winner_recent > subject_recent` required).
3. Truncation-family allowance for recent `+1`: enabled (`resolver_recent_plus1_allow_truncation_family: true`) with `max_distance: 1` for non-family pairs.

# ADR-0329: NEARBY Boolean Predicate and Generation Six

- Status: Accepted
- Date: 2026-10-29
- Decision Origin: Design

## Context

A global NEARBY switch selected geography-projected rules, applied a separate
proximity gate and temporarily chose FAST dedupe. That made an expression's
meaning depend on a second filtering mode. The owner selected ordinary Boolean
NEARBY, removal of that override and a reset without translating old filters.
Scope Ledger v2 received exact implementation approval. Deployment, production
data changes and service restart are separate operations and are not authorized.

## Decision

Use the bare, case-insensitive NEARBY predicate with no comparison or operands.
It follows ordinary NOT/AND/OR precedence, short-circuit evaluation, REJECT vetoes
and alternative PASS rules. COMMENT="NEARBY" stays literal text. Preserve limits;
each predicate occurrence consumes one atom/node and zero operand values.
One immutable program per rule replaces the full/projected variants. Compute
proximity only when reached and cache true or false once for that spot/user
evaluation. Shared programs contain no user-specific state.

Preserve the current H3 geometry helper exactly: either endpoint may match;
160m/80m/60m use coarse cells, other supported bands fine cells. Preserve band
fallback and endpoint cache precedence. Only an empty/??? band after fallback
forces false for the band; unsupported nonempty labels retain current fine-cell
behavior. Missing required location or no matching endpoint makes NEARBY false.
NOT NEARBY therefore includes unknown proximity, not proof of distance. No
geography, source, PATH or confidence condition receives an implicit exemption.

The client configuration owns GRID, explicit/derived provenance and both cached
location resolutions. Capture them coherently with rules/controls using the
existing path-before-filter lock order. Resolve missing cells independently
from that captured GRID outside locks; preserve valid captured values. The pure
resolver does not publish caches or reread mutable clients. Captures needing
neither NEARBY nor PATH introduce no mapping work. NEARBY is independent of
prediction/display enablement. Retain conditional PATH cache publication only
when live GRID still matches captured GRID. Unrelated edits preserve location
without repeating callsign-derived lookup. This intentionally retires the
independent NEARBY cell-pair preservation contract of TSR-0109; its historical
root cause and repair remain valid records of the former design.

History matching and fingerprinting use the same detached effective cells.
True rule/location changes invalidate pending pages and continuation, including
change then restoration. Equivalent lazy cache filling does not. Propagation
observations remain current and outside the fingerprint. Preserve ordinary
nil-owner rejection, live SELF admission, history's SELF-independent exception,
TOXIC, explicit history clauses, pause, queues and lifecycle ownership.

NEARBY never changes saved or effective dedupe. Admission, diagnostics,
readbacks and greeting substitutions use the ordinary resolved preference,
including DEFAULT and disabled-policy fallback. Explicit preference or preset
dedupe changes remain valid. No keys/windows, archive formats, ingest or peer
protocols change.

Use explicit native configuration version 6 and machine schema 6 for every
active resource and success/error envelope, including bounded fallback errors.
Remove nearby_enabled, nearby_active, projections, suspension and duplicate
program domains. Refuse old machine schemas and retired fields even when false.
Capabilities distinguish zero-operand predicates with no comparison operators
from comparison fields. Preserve framing, exact sentinels, revisions, bounds,
PATCH replacement and persistence-before-success behavior.

One rules-only recovery predicate applies to PUT, PATCH and VALIDATE, with or
without NEARBY: a rules list is supplied and all settings/separate controls are
unchanged. Waive only unavailable mapping for unchanged GRID; syntax and every
other settings check remain mandatory. Retain empty-GRID lookup meaning and
ordinary human GRID admission without the old both-resolutions requirement.

At login, recognize safe unversioned/version 1-4 flat records and frozen nested
v5 records/preset baselines under historical shape, semantic and decode-work
bounds. Reject malformed/future/unsafe data without granting replacement
authority. Discard retired rules, the global toggle and applied association;
retain settings, six controls and full-SSID metadata. Marked 1-5 sentinels stay
exact; unversioned historical normalization stays unchanged, including blank
dedupe becoming MED. Install the configured new-user PASS rule 0 exactly once.
Current-generation empty rules remain empty on later login and user reset.

Keep the SSID lease and base-call preset stripe. Preserve existing v6 library
entries while discarding recognized retired entries. Admit both replacement
payloads before mutation. Preserve originals in .pre-v6.bak without overwriting
a different backup or modifying .pre-v5.bak. Commit library first and profile
second, atomically per file, with precise partial outcomes and safe retries.
Only initial authoritative profile absence grants new-user creation. A valid
v6 profile remains writable after library-only cleanup failure; a newly created
SSID attempts cleanup after its profile exists. No bulk walk, converter,
compatibility engine, deployment or production reset is introduced.

## Alternatives considered

1. Retain separate Filter-owned location cells. Viable, but retains two location
   owners and restoration paths; the selected single GRID owner removes that
   duplicated responsibility while preserving geometry.
2. Keep schema 5 and infer generation from a removed field. Rejected because
   one version would have incompatible complete-resource contracts. An explicit
   generation makes protocol refusal and reset authority observable.
3. Store proximity truth in a program or global cache. Rejected because shared
   programs must remain user-independent and bounded without new registries.
4. Translate old filters or delete whole profiles. Rejected by the reset policy;
   settings, controls and metadata must survive, and malformed input stays protected.

## Consequences

### Benefits

Users can express proximity as part of one readable selection. Geography and
source conditions keep their meaning, user dedupe stays authoritative, and
location/history ownership has one coherent capture boundary.

### Risks

Retired filters and presets are deliberately discarded after protected backup.
Old automation must request schema 6. MED/SLOW may show fewer local repeats than
the retired automatic FAST override. NOT NEARBY includes missing data. Per-file
atomic replacement does not create a transaction across profile and library.
Local tests/benchmarks do not establish production p99 or geographic accuracy.

### Operational impact

Deploy only through a separately authorized operation. Use compatible binary
and data backups for rollback; do not edit version markers. Preserve original
and prior-generation backups. Explain the once-only reset and inspect canonical
rules and dedupe after upgrade. Bulletin history/startup order, admission/pacing,
propagation calculations, archive formats and peer forwarding remain outside
this change. Required implementation tests, race checks, fuzzing and comparable
benchmarks are tracked by the implementation closeout, not claimed by this ADR.

## Links

- Supersedes: ADR-0207's NEARBY FAST override; ADR-0317's toggle/projection,
  independent-cell and active-schema-5 clauses; generation-specific reset/preset
  clauses of ADR-0318, ADR-0319 and ADR-0320. Their unaffected onboarding, source,
  controls, persistence, bounds and failure contracts remain in force.
- Previous decisions: [ADR-0207](ADR-0207-nearby-effective-fast-dedupe.md),
  [ADR-0317](ADR-0317-boolean-filter-replacement-mvp.md),
  [ADR-0318](ADR-0318-old-profile-reset-upgrade.md),
  [ADR-0319](ADR-0319-boolean-onboarding-and-source-default.md),
  [ADR-0320](ADR-0320-explicit-source-filtering.md).
- Historical repair: [TSR-0109](../troubleshooting/TSR-0109-yaml-rule-edits-partial-nearby-geometry.md).
- Contracts: [domain](../domain-contract.md#nearby-predicate-and-recovery),
  [telnet](../../telnet/README.md#grid-noise-and-nearby),
  [filter package](../../filter/README.md),
  [storage](../../data/config/README.md#user-configuration-storage).
- Validation owners: filter/rule_set_test.go, filter/legacy_upgrade_test.go,
  telnet/machine_v6_schema_test.go, telnet/machine_v6_commands_failure_test.go,
  telnet/grid_command_test.go, telnet/history_filter_test.go and
  telnet/dedupe_policy_test.go. Observed results belong to the implementation closeout.

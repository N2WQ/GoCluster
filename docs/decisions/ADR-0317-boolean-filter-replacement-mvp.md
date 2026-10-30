---
title: "ADR-0317: Boolean Filter Replacement MVP"
description: Numbered Boolean rules, geography-only NEARBY projection, and isolated schema-5 validation.
---

- Status: Accepted
- Date: 2026-10-21
- Decision Origin: Design

> ADR-0329 supersedes the NEARBY toggle/projection, independent-cell and active schema-5 clauses below. One Boolean predicate and explicit generation 6 replace them; unaffected engine, controls, limits and publication contracts remain. See [ADR-0329](ADR-0329-nearby-boolean-predicate-and-generation-six.md).

> [ADR-0330](ADR-0330-native-cc-banks-and-generation-seven.md) replaces affected shared GO/CC ownership and generation/reset clauses with independent native CC banks and generation seven. This record retains its historical decision; compatible GO predicate/source and transaction contracts continue.

## Context

[ADR-0321](ADR-0321-boolean-canonical-dxcc-input.md) restores canonical DXCC
operand input in human/YAML requests while retaining numeric rules and schema 5.

[ADR-0319](ADR-0319-boolean-onboarding-and-source-default.md) refines onboarding
and exact-expression presentation.
[ADR-0320](ADR-0320-explicit-source-filtering.md) supersedes its source policy:
omitted SOURCE imposes no restriction, and shipped defaults select both sources.

[ADR-0318](ADR-0318-old-profile-reset-upgrade.md) supersedes this MVP's
refusal-only treatment of recognized old profiles with a reset-only login
upgrade. The fresh-store scope below remains the historical MVP authorization;
it is not the current existing-user transition.

Category filters combine implicit defaults, mutable allow/block selections and
special cases. Adding expressions on top would leave two engines deciding which
spots pass. The owner selected complete replacement, rather than a compatibility
layer, and authorized a test-only MVP through Scope Ledger v1.

The experiment uses a fresh private store and an owner-started local cluster.
Production rollout, automatic old-user resets, bulk preset removal and conversion
are not authorized by this MVP. The eventual production direction is to reset
personal filters and presets while preserving unrelated preferences and metadata.

## Decision

### One rule engine

Use numbered PASS/REJECT slots 0 through 9. Slot numbers identify editable rules,
not priority. Any matching REJECT vetoes; otherwise any PASS qualifies. With no
active PASS, there is no personal PASS restriction. An empty set creates no
hidden category or default restriction.

Use comparisons, NOT, AND and OR with conventional precedence in that order.
Parentheses override precedence. This is a Boolean language, not regular
expressions or SQL NULL logic. The MVP uses case-insensitive ASCII literal
COMMENT substrings, explicit missing-data behavior and exact typed comparisons.
These trial semantics are documented in the
[telnet contract](../../telnet/README.md#filters) and shared `HELP RULE`.

Compile on edit or load into bounded immutable programs. Keep matching in the
filter package; the expression child package owns syntax only. Share unchanged
programs and operands rather than maintaining a global rule cache. Capture rules,
scalar controls, NEARBY cells and path inputs coherently; evaluate comments and
predictions outside delivery locks. Prediction observations remain current.
No propagation mathematics or H3 geographic calculation changes.

Separate SELF, BEACON, TOXIC and bulletin controls retain their owners. Preserve
the existing difference between live SELF delivery and command/history
self-match exceptions. Explicit archive BAND/MODE/COMMENT selections remain
mandatory even for self-spots.

### NEARBY omits geography only

Prepare a geography-omitted expression alongside each full rule. Remove DX/DE
continent, state, zone, DXCC and grid atoms structurally. Preserve all other
conditions. For example:

```text
MODE=CW OR DXCONT=EU                 becomes MODE=CW
NOT (DXCONT=EU AND MODE=CW)          becomes NOT MODE=CW
```

Ignore emptied rules for both PASS and REJECT aggregation. Preserve the full
saved definitions. Edits while ON update both forms; OFF resumes the latest full
rules. There is no legacy geography-map restoration snapshot.

If restored NEARBY is already ON but its location is unavailable, allow rule-only
edits through human commands and YAML with every other saved selection unchanged.
Do not invent usable cells or turn NEARBY off. Spots remain blocked until a usable
grid is set or NEARBY is disabled when both user cells are unavailable. Partly
usable geometry keeps its existing band-specific behavior. YAML recovery waives
only unchanged GRID mapping availability, not syntax or other settings checks.
Existing human control/settings admission remains unchanged. Enabling NEARBY
or changing its grid retains ordinary admission checks.

### Explicit defaults and version 5

Admit configured mode/source defaults before listening. A genuinely new user gets
one ordinary editable PASS rule in slot 0. Existing empty records and resets do
not reinstall that default. RESET FILTER clears rules and NEARBY, preserving
settings and separate controls. Adding a PASS broadens selection; narrowing the
default requires editing or removing slot 0.

Use one explicit schema-5 definition and codec path for records, presets,
readbacks and machine writes. Refuse unversioned, older and future stored formats
without rewriting them. Refuse old category mutations with numbered-rule
guidance; do not translate them or add AR aliases. There is no active old engine.

Keep the existing full-SSID lease, reconnect/retired-session fences, exact captured
write bytes, atomic replacement and bounded applied preset baseline. Preserve
authoritative login metadata and the current-session-first recent-IP merge.
Human accepted edits remain live with a warning after a late save failure.
Machine writes and LOAD persist before publication; VALIDATE never writes or
publishes. SAVE retains its explicit library-only partial-success outcome.

The compact runtime configuration shares immutable programs. The public YAML
definition DTO carries an attested preparation cache; caller edits invalidate that
attestation. Neither role retains old category fields.

Keep explicit rule, byte, collection and decoding-work bounds. These are MVP
safety limits, not final capacity or latency promises. Compare the preserved
baseline workloads and obtain representative runtime profiles before production
performance conclusions.

## Alternatives considered

1. Layer expressions over category filters. Rejected because a matching PASS
   could still be hidden by a second engine or implicit default.
2. Convert every old profile and preset. Deferred in favor of a fresh isolated
   MVP store; the owner selected a production reset direction.
3. Suspend the entire filter under NEARBY, or substitute TRUE for geography.
   Rejected because either changes non-geographic OR/NOT behavior.
4. Select final numerical performance targets before a working prototype.
   Deferred until controlled measurements and owner-cluster testing.

## Consequences

### Benefits

- Users inspect the full rules and their effective NEARBY forms.
- Live delivery, history and machine writes share the same prepared truth.
- Immutable bounded state avoids mutable per-category maps on the hot path.
- The prototype can expose usability and operational problems before rollout.

### Risks

- General expression evaluation can cost more than a fixed category's early
  rejection. Allocation-free matching alone does not establish a CPU or p99 win.
- OR, NOT and missing values can surprise users; HELP and actual scenario testing
  remain essential.
- Old automation and stored profiles are incompatible with the MVP.
- Owner-cluster acceptance and representative profiles are separate evidence;
  unit tests or a build do not establish production readiness.

### Operational impact

Run only against designated disposable identities and private user/archive paths.
Verify the actual executable identity and outward-traffic isolation before
logging in or posting test DX. The owner starts and restarts the cluster.
Rollback uses compatible binaries and matching data backups, not a changed
version marker. Existing archive encodings and recorded metadata remain intact.

## Links

- Supersedes for the Boolean MVP: personal MODE delta semantics in
  [ADR-0272](ADR-0272-mode-visibility-deltas.md) and the MINSNR feature model in
  [ADR-0258](ADR-0258-per-mode-minimum-snr-filter.md).
- Replaces personal list/schema clauses of
  [ADR-0261](ADR-0261-literal-comment-filter-and-history.md) while retaining its
  literal archive searches; replaces old-version compatibility clauses of
  [ADR-0267](ADR-0267-compatible-stored-yaml-decoding.md).
- Preserves ownership, persistence and admission from
  [ADR-0244](ADR-0244-exact-configuration-persistence.md),
  [ADR-0269](ADR-0269-detached-human-preference-publication.md) and
  [ADR-0270](ADR-0270-complete-storage-admission.md), without old map snapshots.
- Related implementation: [rule sets](../../filter/rule_set.go),
  [matching snapshots](../../telnet/matching_snapshot.go),
  [v5 storage](../../filter/storage_v5.go),
  [machine transactions](../../telnet/machine_v5_commands.go).
- Related tests: [active integration](../../telnet/boolean_activation_test.go),
  [coherent publication](../../telnet/matching_snapshot_test.go),
  [rule projection](../../filter/rule_set_projection_test.go),
  [native storage](../../filter/user_record_test.go).
- Related TSRs: [TSR-0104](../troubleshooting/TSR-0104-yaml-capability-operator-quoting.md)
  records the capability-serialization interoperability fix found during the
  private experiment, without changing expression semantics.
- Superseded in part by: [ADR-0329](ADR-0329-nearby-boolean-predicate-and-generation-six.md) for the affected NEARBY/schema/generation clauses.

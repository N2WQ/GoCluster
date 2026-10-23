---
title: "ADR-0321: Boolean Canonical DXCC Input"
description: Resolve canonical DXCC labels once per human or YAML request and retain numeric rules.
---

- Status: Accepted
- Date: 2026-10-23
- Decision Origin: Troubleshooting chat

## Context

The Boolean replacement retained numeric DXCC matching but omitted canonical
input supported by [ADR-0248](ADR-0248-canonical-dxcc-input-and-human-labels.md).
The owner approved Scope Ledger v3 to restore convenient operands through
human and machine inputs while keeping numeric readback and persistence.
Historical evidence and root cause are in
[TSR-0110](../troubleshooting/TSR-0110-boolean-dxcc-prefix-input.md).

## Decision

Human GO/CC numbered rules and YAML PUT/PATCH/VALIDATE accept positive ADIF
codes and exact canonical CTY Prefix labels for DXDXCC/DEDXCC, with `=` and `<>`.
Reuse existing case/whitespace normalization, slash-label and conflict behavior;
do not add a prefix whitelist or callsign lookup fallback.

One lazy request-owned index covers both fields and every supplied slot.
Unknown/conflicting labels or unavailable CTY refuse the entire proposal.
Numeric input never invokes CTY; integer overflow cannot fall through to labels.
Resolve the full rule before NEARBY projection, then discard the callback/index.

All prepared operands, responses containing expressions, readbacks and saved
rules use numeric ADIF. Current operand order, repetitions and budgets remain.
Native storage loaders, schema 5, defaults, matching, NEARBY semantics and CTY
refresh are unchanged. Machine writes retain commit-before-publication;
human late-save failures retain active-but-unsaved warnings.

This refines the numeric-only Boolean input contract associated with
[ADR-0317](ADR-0317-boolean-filter-replacement-mvp.md). It restores ADR-0248's
entity resolution, not historical category syntax, list deduplication or label
expansion in human readbacks. Those historical decisions remain recorded.

## Alternatives considered

1. Normalize and reparse expression text: rejected because source columns,
   COMMENT spelling and independent size checks would require extra machinery.
2. Resolve in storage or matching: rejected because saved rules and delivery
   must not depend on CTY availability or later refreshes.
3. Cache a shared index: rejected because request ownership needs no retained
   state, refresh invalidation or synchronization.

## Consequences

### Benefits

- Familiar historical labels work through both human and machine inputs.
- Saved numeric entity identity remains stable across reload and CTY refresh.
- Matching incurs no label resolution.

### Risks

- Prefix-bearing edits require available, unambiguous CTY data.
- Index construction is bounded by the captured CTY snapshot; it is not cached.
- Labels select whole entities, not country subdivisions or callsign patterns.

### Operational impact

`PASS RULE 0 MODE=CW AND DXDXCC<>K` reads back as
`PASS RULE 0 MODE=CW AND DXDXCC<>291`. Numeric-only requests need no CTY.
Native prefix-bearing files are unsupported. No migration, server restart,
production rollout or new schema is authorized by this decision.

## Links

- [Troubleshooting evidence](../troubleshooting/TSR-0110-boolean-dxcc-prefix-input.md)
- [Runtime contract](../domain-contract.md#expressions-and-field-semantics)
- [Operator DXCC guidance](../../telnet/README.md#canonical-dxcc-prefixes)
- [Compiler tests](../../filter/rule_compile_test.go)
- [Human and machine tests](../../telnet/dxcc_commands_test.go)

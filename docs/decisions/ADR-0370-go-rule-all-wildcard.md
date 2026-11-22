# ADR-0370: ALL in GO Filter Rules

- Status: Accepted
- Date: 2026-11-22
- Decision Origin: Design, Approved v1

## Context

Numbered GO rules replaced the old category commands. Users want to write an
explicit comparison such as `BAND=ALL` while combining it with other conditions.
Calls and comments already accept ALL as text.

## Decision

Use bare ALL in equality and inequality lists for all GO comparison fields
except DXCALL, DECALL and COMMENT. Equality includes missing values. Inequality
matches nothing. Validate all list members and account for them under existing
raw and expanded limits. Ordered comparisons require numbers.

The shared compiler prepares wildcard comparisons as constants. Canonical
expressions retain the wildcard. Quoted literal values follow the field's
ordinary validation, and specific MODE or EVENT entries named ALL retain quotes
through readback and restore. Current telnet, YAML schemas 6 and 7, profiles and
presets use the same expression grammar.

## Alternatives considered

1. Expand ALL into catalog entries. Such lists omit missing data, may exceed rule
   limits, and cannot represent every numeric value.
2. Rewrite wildcard expressions into other comparisons for older executables.
   That would hide the requested field from readback and use current field
   domains as a substitute for a constant.

## Consequences

Users can read and copy canonical rules without losing their wildcard intent.
The compiled constant needs no field retrieval, CTY resolution for the wildcard,
or path prediction of its own. The retired v5 validator retains its operand
grammar wherever it compiles an old rule to qualify a file for reset.

This scope assumes the new cluster has no older rules using a bare custom
catalog value named ALL. Older binaries can refuse profiles or presets after
they contain a wildcard. Follow the existing rollback contract and use the
matching executable, profile backup and preset library. Preserve current data
before restoring backups.

## Links

- [GO expression contract](../domain-contract.md#expressions-and-field-semantics)
- [Telnet filters](../../telnet/README.md#filters)
- [Rule compiler](../../filter/rule_compile.go)
- [Wildcard tests](../../filter/rule_all_test.go)
- Extends [ADR-0317](ADR-0317-boolean-filter-replacement-mvp.md) operand semantics.

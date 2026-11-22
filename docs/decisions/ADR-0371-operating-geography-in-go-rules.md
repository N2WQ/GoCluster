# ADR-0371: Operating Geography in GO Rules

- Status: Accepted
- Date: 2026-11-22
- Decision Origin: Design, Approved v1

## Context

GO rules using fixed continents or ADIF entities need edits when another
operator uses them. A portable callsign can operate in a different entity from
its home license. For example, CTY resolves `N2WQ/VE3` to Canada (ADIF `1`).

## Decision

Accept bare `MYCONT` for `DXCONT`/`DECONT` and bare `MYDXCC` for
`DXDXCC`/`DEDXCC` with `=` and `<>`, alone or in lists. Each occurrence counts
toward existing operand limits. Derive immutable session values from the
portable CTY match at login, using the radio callsign without its numeric SSID.
The existing match can be reused when the callsign has no numeric SSID. For
example, `N2WQ/VE3-01` uses `N2WQ/VE3` for geography. A successful operating
match remains usable if home-call license validation is skipped. Missing
values are unknown.

Canonical rule text, YAML, saved profiles, and presets keep the operand names.
Prepared rules mark the operand without storing session geography. Live and
filtered-history evaluations use the same connection-owned value. With unknown
session geography, another known list member can satisfy `=`. Inequality
requires enough information to establish a difference from every member.
Ordinary `NOT` and `ALL` semantics continue to apply.

## Alternatives considered

- Bind operands in a separate compiled program per connection. This adds
  rebind work after edits and preset loads and would duplicate rule ownership.
- Resolve the operating geography for each spot. That adds database lookups to
  fan-out and lets CTY refresh change the meaning of a rule within a session.
- Use home-call license geography. A portable login would then select home
  entity spots even when the operator is working in a different entity.

## Consequences

### Benefits

Operators can share the same expression and reuse it after travel or a callsign
change. A bounded value copied from the connection suffices for matching.

### Risks

Old executables can refuse profiles or presets containing a new operand.
Rollback uses the matching executable, profile backups, and preset library.
CTY loss at login can make a geography-only PASS match no spots until reconnect.

### Operational impact

No schema bump or profile reset is needed. A reconnect adopts newly available
CTY data. Operators can use `HELP RULE` for the syntax and `SHOW FILTER RULE`
to inspect the saved expression.

## Links

- [Operator guide](../../telnet/README.md#your-operating-geography-in-go-rules)
- [Field contract](../domain-contract.md#expressions-and-field-semantics)
- [Portable CTY login and delivery test](../../telnet/operating_geography_test.go)
- [GO rule operands and rollback precedent](ADR-0370-go-rule-all-wildcard.md)

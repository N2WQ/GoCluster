# ADR-0273: History COMMENT Alias Boundary

- Status: Accepted
- Date: 2026-10-10
- Decision Origin: Troubleshooting chat

## Context

History supports configured mode aliases, comma-separated selections and a
literal COMMENT remainder. Two unconditional COMMENT scans made a supported
COMMENT mode alias inaccessible and enabled punctuation before the real phrase.
Approved v3 checkpoint 16 selects the same first/after-comma alias precedence
already used for BAND/MODE keywords.

## Decision

A standalone supported, filter-visible COMMENT mode alias is a MODE value at
the start of a list or after a comma. Otherwise standalone COMMENT starts the
literal phrase. Comma-embedded COMMENT remains a list value; an unsupported
value is refused. Absent or hidden aliases retain the ordinary clause rule.

Commands owns a request-local lexical transition shared by the COMMENT cut and
typed list boundary decision. Telnet calls a borrowed ASCII prefix predicate,
requiring a space after the actual marker before broadening punctuation. Edits
recompute the boundary. Retain the reader's unrelated standalone NEXT guard.
The predicate performs no semantic validation or CTY lookup and retains no input.

Keep existing raw remainder slicing, ASCII-edge trimming, detached phrases,
validation order, comma grammar, dialect and nil-filter restrictions. The
connection owner still validates before replacing its immutable query/cursor.
Invalid requests neither scan the archive nor change preferences or continuation.

## Alternatives considered

1. Always reserve COMMENT as a clause. Rejected by the selected alias policy.
2. Duplicate separate reader/parser scanners. Rejected because the same lexical
   decision would continue to drift across ingress and execution.
3. Parse the complete command on every byte. Rejected because validation and
   selector resolution are unnecessary for this bounded prefix decision.

## Consequences

### Benefits

Configured keyword aliases and literal phrases have one discoverable boundary,
with zero allocations in the tested borrowed prefix scan and no retained state.

### Risks

A comma before a configured COMMENT alias continues MODE instead of starting a
phrase. Finish the list without a comma to start a clause deliberately.

### Operational impact

With COMMENT mapped to CW, `SHOW DX MODE COMMENT COMMENT POTA` selects CW and
POTA text; `SHOW DX MODE CW COMMENT POTA` also works. Without that alias,
`SHOW DX MODE CW, COMMENT POTA` retains its ordinary clause interpretation.
Use the returned NEXT command; no stored configuration migration is required.

## Links

- Refines only COMMENT keyword boundaries in
  [ADR-0263](ADR-0263-history-list-comma-boundaries.md); other decisions remain.
- Root cause: [TSR-0063](../troubleshooting/TSR-0063-history-comment-alias-boundary.md).
- Tests: `commands/history_comment_alias_test.go`,
  `telnet/history_comment_alias_test.go` and the cumulative checkout runner.
- Contracts: [commands history](../../commands/README.md#archive-history),
  [telnet history](../../telnet/README.md#archive-history-and-continuation).

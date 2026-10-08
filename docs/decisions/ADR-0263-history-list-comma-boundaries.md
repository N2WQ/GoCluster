# ADR-0263: Commas And Keyword Aliases In History Lists

- Status: Accepted
- Date: 2026-10-08
- Decision Origin: Troubleshooting chat

## Context

Review of e132615 found that history parsing recognized every standalone BAND
and MODE as a clause before normalizing mode values. A valid startup taxonomy
with CW variants `[BAND, MODE]` therefore supported the aliases in filters but
rejected `SHOW DX MODE BAND` and `SHOW DX MODE MODE`. Appending a comma bypassed
the clause detector. With space-separated lists, repeated clauses and alias
values also had competing interpretations.

The owner selected commas as required separators for both history categories
and authorized scope v2. This supersedes only ADR-0262's delimiter and keyword
boundary rules; its filtering, paging and resource decisions remain in force.

## Decision

Require commas between BAND/MODE history list values. Allow spaces around commas
and continue ignoring empty comma fields, including leading, repeated and
trailing commas. A configured BAND/MODE mode alias is a value at the start of a
mode list or after a comma. Otherwise a standalone keyword starts a clause.
A keyword that is not a supported value in the current category starts a clause,
including after an empty comma field. Finish a list without a comma before
starting another clause to avoid a configured keyword alias continuing it.

For CW variants `[BAND, MODE]`, `MODE BAND` and `MODE MODE` select CW;
`MODE CW, MODE, FT8` selects CW/FT8; `MODE CW BAND 20` selects CW on 20m;
`MODE CW MODE FT8` remains a repeated-clause error. `MODE CW FT8` and
`BAND 20 40` are invalid. Invalid requests preserve the previous cursor and filters.
COMMENT still consumes its literal remainder, and NEXT keeps the canonical lists.

Use the existing mode normalizer and supported filter vocabulary. The taxonomy
loader, configuration schema, saved PASS/REJECT grammar and archive format retain
their current contracts. Parser state is local to the request; query ownership
and detached, deduplicated finite selections remain as specified by ADR-0262.

## Alternatives considered

1. Always prefer configured aliases in space-separated mode lists. Rejected
   because repeated clauses and BAND following MODE would have ambiguous meaning.
2. Keep space-separated history lists and add context-dependent clause lookahead.
   Rejected by the owner in favor of explicit commas for both categories.
3. Reserve BAND/MODE in the taxonomy or require a trailing comma for singleton
   aliases. Rejected because existing supported mode aliases should remain usable.

## Consequences

### Benefits

- Configured keyword aliases work in singleton and comma-separated mode lists.
- Commas distinguish list continuation from a following clause.
- Existing filtering and cursor ownership stay bounded and unchanged.

### Risks

- Space-separated multi-value history commands accepted by e132615 now fail.
- A comma immediately before a configured keyword alias continues the mode list.

### Operational impact

Use `SHOW DX K1ABC 20 BAND 20m,40m MODE CW,FT8 COMMENT POTA`.
Update history command clients and harness examples to use commas. No taxonomy
migration or saved-filter edit is required. Deterministic local tests establish
the custom taxonomy case; deployed behavior needs a separate live verification.

## Links

- Supersedes [ADR-0262](ADR-0262-history-band-mode-selections.md) delimiter and
  keyword-boundary clauses only.
- Root cause: [TSR-0047](../troubleshooting/TSR-0047-history-mode-alias-clause-collision.md).
- Tests: `commands/history_selection_taxonomy_test.go`,
  `commands/history_selection_test.go`, `telnet/history_selection_test.go`,
  `telnet/comment_input_test.go`, and Python harness fixtures.
- Contracts: [commands history](../../commands/README.md#archive-history),
  [telnet history](../../telnet/README.md#archive-history-and-continuation),
  [domain contract](../domain-contract.md).

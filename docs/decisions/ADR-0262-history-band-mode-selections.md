# ADR-0262: BAND And MODE Archive History Selections

- Status: Accepted
- Date: 2026-10-08
- Decision Origin: Design

## Context

Operators need ad hoc band/mode lists in a station-history search without editing
their saved filters. DX/MYDX already applies explicit identity and COMMENT
selections before counting and retains one bounded cursor per connection. The
grammar must preserve literal COMMENT text, current filters, aliases, paging,
input budgets and resource ownership.

## Decision

Accept BAND and MODE after the optional existing selector/count, in either order
and at most once each. Lists use comma/space separators with OR within a category
and AND between categories, selector and COMMENT. COMMENT consumes the literal
remainder and therefore comes last. Band normalization and mode taxonomy come
from the existing PASS BAND/MODE contracts; UNKNOWN selects blank spot modes.
Deduplicate canonical values; reject missing or unsupported values, repeated
categories and ALL/NONE in the new lists. Omission imposes no additional query
restriction for that category. Invalid commands do not replace a valid cursor.

Explicit selections narrow the saved-filter predicate, without changing
preferences. BAND/MODE, like explicit identity/COMMENT, remain mandatory even for
self-spots; saved-filter self exceptions retain their previous behavior. Apply
identity and cheap BAND/MODE gates before scanning comments, and all selection
gates before counting. Preserve existing count, CTY, retention, work-budget,
chronological rendering, continuation and failure contracts.

Retain only immutable, detached canonical names in the existing query. Band
cardinality is bounded by the supported band table; mode cardinality by the
bounded, startup-loaded filter taxonomy (at most 128 mode definitions).
Repeated input cannot increase retained cardinality. No raw command, taxonomy
snapshot, iterator, result slice, secondary index, matcher cache or worker is
retained. Fresh replacement, invalidation, completion and close relinquish the
query through the existing cursor owner. NEXT carries the original selections;
relevant settings changes still invalidate continuation and pending publication.

The telnet reader recognizes COMMENT after BAND/MODE lists using a borrowed,
allocation-free prefix scan bounded by the existing command byte ceiling.
Printable punctuation is still permitted only after an explicit phrase marker;
ordinary command/login whitelists, editing and machine framing remain intact.

## Alternatives considered

1. Override saved band/mode rules for a search. Rejected by the owner in favor of
   narrowing, consistent with existing COMMENT history selection.
2. Allow only one new category at a time. Rejected by the owner in favor of
   combinable AND selections with OR lists.
3. Add an archive index, schema or persisted search preference. Unnecessary for
   the existing bounded, non-realtime paged search contract.
4. Treat BAND/MODE tokens after COMMENT as clauses. Rejected because it changes
   existing literal phrase semantics and makes keyword text unsearchable.

## Consequences

### Benefits

- One history command can restrict station, bands, modes and comment text.
- Existing saved preferences and archive encodings remain compatible.
- Query state stays bounded and follows existing connection lifetime.

### Risks

- Saved blocks can exclude an explicitly requested band/mode for nonself spots.
- Sparse searches can still require NEXT under the existing candidate budget.
- Combined commands must fit the existing 128-byte default input limit.

### Operational impact

Use, for example, `SHOW DX K1ABC 20 BAND 20,40 MODE CW FT8 COMMENT POTA`, then
the returned NEXT command on the same connection. No configuration migration,
backfill, new setting or live-filter mutation is required. The extended Python
harness requires both labeled nonself stimuli to survive admission and enough
retained history to finish its bounded assertions; offline results do not prove
a deployed cluster has this grammar.

## Links

- Refines [ADR-0251](ADR-0251-exact-call-paged-history.md) and
  [ADR-0261](ADR-0261-literal-comment-filter-and-history.md); neither is superseded.
- Tests: `commands/history_selection_test.go`, `commands/history_archive_test.go`,
  `telnet/history_selection_test.go`, `telnet/comment_input_test.go`, and the
  Python telnet harness with offline oracle fixtures.
- Contracts: [commands history](../../commands/README.md#archive-history),
  [telnet history](../../telnet/README.md#archive-history-and-continuation),
  [domain contract](../domain-contract.md).
- No TSR: this is new command behavior, not incident troubleshooting.

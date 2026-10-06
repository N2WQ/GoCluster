# ADR-0248: Canonical DXCC Input And Human Labels

- Status: Accepted
- Date: 2026-10-06
- Decision Origin: Design

The exact human FULL/category presentation below is superseded by
[ADR-0249](ADR-0249-effective-human-filter-details.md). Other decisions remain
in force; the text below preserves the accepted historical decision.

## Context

Operators recognize CTY canonical prefixes more readily than ADIF numbers.
Canonical Prefix fields are not necessarily CTY lookup keys: portable lookup
can select the wrong entity for slash-bearing labels. Several canonical labels
can also identify one ADIF entity, so showing only the supplied label hides the
breadth of a filter selection.

## Decision

PASS/REJECT DXDXCC and DEDXCC accept exact trimmed, uppercased CTY canonical
prefixes alongside existing positive integer codes. Resolve a complete mixed
list before mutation, deduplicate by ADIF, and reject unknown or conflicting
labels atomically. Canonical input requires CTY; positive numeric input remains
independent of CTY membership. ALL and NEARBY contracts remain unchanged.

SHOW DX/MYDX resolve canonical labels before existing callsign normalization
and portable lookup. Preserve ADR-0011's count grammar, limits, client-filter
intersection and archive behavior. SHOW DXCC detail lookup is unchanged.

All human SHOW FILTER views use unambiguous canonical prefixes. Expand all
unambiguous labels for a selected ADIF entity, preserving entity-wide matching:
I, IG9 and IT9 select ADIF 248. Overview counts count entities. FULL/category
retain flags and every true/false stored entry, grouping labels for one entity
as one quoted key.
After complete conflict detection, omit conflicting labels from display
associations while retaining every unambiguous alternative. If none remain,
display Unknown DXCC followed by that entity's stored number. Missing CTY
associations use the same fallback, preserving distinct entity identities.
Machine YAML and persistence keep numeric ADIF keys.

Use one request-owned CTY index built from one captured database. Its storage
is bounded by that snapshot's records; nothing is cached across commands.
Human readbacks share the snapshot across counting and generation, retaining
ADR-0246/0247's width, ASCII quoting, response-size and delivery-pause contracts.

## Alternatives considered

1. Reuse portable callsign lookup directly. Rejected because canonical slash
   labels and suffixes have different meanings.
2. Require CTY membership for numbers or reinterpret numeric history arguments.
   Rejected because those are separate compatibility changes.
3. Store strings in filters. Rejected because matching, archive and saved
   configuration already use ADIF entities.
4. Retain an index globally or on CTYDatabase. A request-owned index avoids new
   refresh invalidation, retained-state and synchronization responsibility.
5. Show one representative prefix or retain numbers in exact human views.
   Rejected because users need to see all unambiguous canonical labels for an
   entity.
6. Retain conflicting labels with ADIF annotations. Rejected in favor of
   omitting those labels and using the existing fallback when none remain.

## Consequences

### Benefits

- Operators can select and inspect entities by canonical prefix.
- Slash-bearing history labels resolve without portable-call misclassification.
- Numeric persistence and machine clients need no migration.

### Risks

- Canonical labels depend on the local CTY snapshot and may change on refresh.
- Canonical labels select whole entities, not country subdivisions or callsign
  patterns. Conflicting labels fail resolution rather than choosing an entity.
- Expanded exact labels can exceed the existing response budget; the existing
  explicit error replaces partial output.

### Operational impact

- One CTY scan per canonical-input command or relevant human filter readback.
- No added background work, process-lifetime cache, matcher lookup or migration.
- Unknown saved codes remain visible, including when CTY is unavailable.

## Links

- Related decisions: [ADR-0011](ADR-0011-show-history-dxcc-selector.md),
  [ADR-0246](ADR-0246-delivery-timed-human-readbacks.md),
  [ADR-0247](ADR-0247-complete-finite-filter-selections.md).
- Related tests: cty/dxcc_test.go, commands/dxcc_history_test.go,
  telnet/dxcc_commands_test.go, telnet/configuration_human_dxcc_test.go.
- Related docs: [canonical DXCC prefixes](../../telnet/README.md#canonical-dxcc-prefixes).
- Refines canonical selection and human DXCC labels only; no prior ADR is
  superseded in full. No TSR was needed for this planned feature.

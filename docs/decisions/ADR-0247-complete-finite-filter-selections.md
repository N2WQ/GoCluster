# ADR-0247: Complete Finite Filter Selections

- Status: Accepted
- Date: 2026-10-05
- Decision Origin: Design

The exact human FULL/category presentation below is superseded by
[ADR-0249](ADR-0249-effective-human-filter-details.md). Other decisions remain
in force; the text below preserves the accepted historical decision.

## Context

[ADR-0246](ADR-0246-delivery-timed-human-readbacks.md) made the human filter
overview readable, with short selections and counts for longer selections.
That policy hides useful names for finite categories: a restrictive selection
of all 19 supported bands becomes `Only 19 bands`, and all supported modes
become `Only 12 modes` under the default taxonomy. The user selected passing
names and useful explicit exclusions for all finite categories, without
enumerating disabled choices.

The overview must continue to describe the existing matcher. Restored ordinary
maps can contain canonical nonstandard names that the matcher accepts. EVENT
families are taxonomy-owned, and one spot can carry both allowed and blocked
tags. A supported-name-only display could omit effective retained rules.

## Decision

For BAND, MODE, SOURCE, EVENT, PATH, CONFIDENCE and DX/DE continents, show passing
selections and useful exclusions by name, with continuation lines instead of
count fallback. Use `All` for unrestricted selections, `None` when no selection
passes, and `All except` for effective exclusions from an otherwise unrestricted
selection. Keep stable lexical map ordering. Do not add disabled inventories.

Keep MODE's unknown-mode qualification, EVENT's untagged qualification,
confidence exemptions, PATH's existing CLOSED/UNLIKELY interpretation and
NEARBY's suspension of geography. Ordinary-map false entries remain inactive;
EVENT continues to use key presence, normalize names and collapse aliases.
Retain explicit EVENT blocks even outside a restrictive allow list because a
mixed-tag spot can still be rejected.

Keep short previews and long-list counts for callsign, DXCC, grid and zone
selections. FULL/category output remains the exact stored-rule authority.
This changes presentation only; matcher, loading, persistence, YAML, SETTINGS,
pause and writer contracts remain governed by the existing decisions.

Stream finite tokens into rows of at most 78 printable ASCII characters plus
CRLF. Treat each stored token atomically; non-simple tokens use ASCII quoting,
and a long token uses the existing lossless quoted-piece writer. Generated
explanations can wrap between words. Never word-wrap raw stored values.

Before copying or sorting ordinary-map keys, sum eligible escaped token sizes
plus separators against the remaining 65,536-byte response budget. This lower
bound caps copied-key cardinality as well as aggregate token bytes. EVENT
canonicalization has at most 64 families from its mask. Count every final
wrapped row, header and footer before constructing the response buffer. A
successful overview contains the complete finite selection; an oversized one
returns the existing explicit size error, without truncation or partial output.

## Alternatives considered

1. Enumerate enabled and disabled vocabularies. Rejected by the user because
   passing selections and useful exclusions communicate the rule with less
   clutter.
2. Keep counts for every long finite selection. Rejected because the reader
   cannot see which named choices pass.
3. Change only BAND and MODE. The user selected all finite categories together.
4. Enumerate only built-in supported names. Rejected because canonical
   nonstandard retained rules and taxonomy-defined families can be effective.
5. Truncate, paginate or relax the response budget. Outside this presentation
   correction; the complete-or-error contract remains in force.

## Consequences

### Benefits

- Finite selections remain visible when they need multiple lines.
- Existing matching semantics and exact stored-rule inspection remain intact.
- Quoted tokens preserve bytes and predictable terminal widths.
- Preparation and final output retain executable resource bounds.

### Risks

- Unusually large retained finite selections can exceed the overview budget
  where the earlier count-only overview succeeded.
- Lexical band order preserves the existing overview order rather than
  introducing a different frequency-based ordering policy.

### Operational impact

- Human overview lines can increase, while the same delivery/reading hold applies.
- A size error is explicit. Exact category and client YAML commands retain
  their existing format and independent complete-response budgets.

## Links

- Related issues/PRs/commits: -
- Implementation: [finite overview rows](../../telnet/configuration_human_finite.go),
  [matcher-specific summaries](../../telnet/configuration_human_summary.go)
- Related tests: [finite selections and limits](../../telnet/configuration_human_finite_test.go),
  [existing exact/settings contracts](../../telnet/configuration_human_test.go),
  [restored semantics](../../telnet/configuration_human_summary_test.go)
- Related docs: [transport guide](../../telnet/README.md),
  [operator guide](../OPERATOR_GUIDE.md),
  [support guidance](../../customgpt/support-cards/configuration-readback.md)
- Related TSRs: -
- Supersedes / superseded by: Refines ADR-0246's overview count policy for finite
  categories. ADR-0246's exact output, byte budget, pause and delivery decisions
  remain accepted.

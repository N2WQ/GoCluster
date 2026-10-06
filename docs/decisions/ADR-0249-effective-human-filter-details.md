# ADR-0249: Effective Human Filter Details

- Status: Accepted
- Date: 2026-10-06
- Decision Origin: Design

## Context

The exact human FULL/category format repeats all-selection flags alongside
maps, exposes inactive false entries, and requires users to interpret stored
state. Operators selected effective selections for every filter category,
uppercase PASS/REJECT labels, and single ON/OFF values for switches.

## Decision

Human `SHOW FILTER FULL` and category responses display complete effective
PASS/REJECT selections with ALL/NONE. ALL means unrestricted before applying
listed exclusions. Ordinary false entries are inactive and omitted. Rejected
entries are removed from PASS; a nonempty false-only allow map stays restrictive
and produces PASS: NONE. REJECT ALL suppresses overridden selections.

Preserve the existing matcher, including EVENT key presence and aliases,
untagged inclusion, PATH's inherited CLOSED behavior and explicit overrides,
UNKNOWN-mode visibility, confidence exemptions, and SELF's ordinary-filter
bypass while TOXIC still applies. NEARBY suspends geography even without usable
cells; detail sections show unrestricted category selections with a suspension
note. Callsign patterns retain supplied order and duplicates; a precedence
note explains overlapping PASS/REJECT patterns without wildcard subtraction.
Switches show effective ON/OFF; NEARBY also reports availability and consequences.

DXCC uses every unambiguous canonical label for each effective ADIF entity in
numeric entity order. Conflicting labels remain omitted, valid alternatives
remain visible, and unknown associations keep their numbered fallback.

Preflight only effective content before key collection/sorting. Inactive or
overridden maps cannot reject a small effective response. Preserve the complete
65,536-byte final CRLF response budget, 78 printable ASCII columns, lossless
quoted pieces, single captured CTY snapshot, and delivery-pause ownership.
Oversize returns an explicit error without partial output. Literal ALL/NONE or
list punctuation in stored values is quoted to distinguish data from syntax.

Keep the overview layout, changing its FULL pointer to detailed selections.
Machine YAML remains the exact stored-state authority, preserving numeric ADIF
keys, flags, false entries and defaults. Persistence, command grammar, SETTINGS
and legacy formatters are unchanged.

## Alternatives considered

1. Rename exact maps while retaining flags and false entries. Rejected because
   operators selected effective-only output.
2. Limit the simplification to DXCC. The operator selected all categories.
3. Apply ordinary-map rules uniformly. Rejected because EVENT, PATH, patterns,
   exemptions and NEARBY have different observable semantics.
4. Subtract wildcard patterns. Rejected in favor of retained patterns and a
   clear rejection-precedence note.
5. Use two list fields for switches. Rejected in favor of one ON/OFF value.

## Consequences

### Benefits

- Human detail views explain active selections with less stored-state clutter.
- Canonical DXCC labels retain the breadth of entity-wide selection.
- Machine clients and saved configuration need no migration.

### Risks

- Scripts parsing the previous human exact format must use machine YAML.
- Category selections are subject to documented exemptions and other filters;
  a listed PASS is not a guarantee that a complete spot will be delivered.

### Operational impact

- Scratch remains request-owned and bounded by escaped-content admission.
- No background work, cache, new synchronization or matcher work is added.
- Human response-size admission now measures effective selections; machine
  admission continues to measure exact stored configuration.

## Links

- Related tests: `telnet/configuration_human_effective_test.go`,
  `telnet/configuration_human_dxcc_test.go`, human readback and quoting tests,
  `commands/configuration_help_test.go`.
- Related docs: [human readbacks](../../telnet/README.md#human-configuration-readbacks),
  [configuration support card](../../customgpt/support-cards/configuration-readback.md).
- Replaces only the exact-human FULL/category presentation portions of
  [ADR-0246](ADR-0246-delivery-timed-human-readbacks.md),
  [ADR-0247](ADR-0247-complete-finite-filter-selections.md), and
  [ADR-0248](ADR-0248-canonical-dxcc-input-and-human-labels.md).
  Their other decisions remain in force. No TSR is needed for this planned change.

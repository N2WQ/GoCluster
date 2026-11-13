# ADR-0348: Human Spot Comment Integrity

- Status: Accepted
- Date: 2026-11-12
- Decision Origin: Troubleshooting chat

## Context

Human prose used the same comment cleanup as skimmer output. A comment such as
`CW UP 2` could acquire a 2 dB report; meaningful times and `CWT` could be removed
before display, filtering and archive storage. Human telnet input also delegated
wire-time extraction to that scan, allowing a comment clock to take precedence.
The source findings and evidence limits are recorded in
[TSR-0132](../troubleshooting/TSR-0132-human-comment-report-and-time-parsing.md).

The owner selected a narrow report shorthand with one accepted numeric meaning,
preservation of meaningful human text, and rejection of human telnet spots
without a usable terminal wire timestamp. The existing RBN parser, taxonomy,
source classification, archive format and learning algorithms remain boundaries.

## Decision

### Human report and comment contract

Use a human-specific entry point backed by the shared tokenizer, taxonomy
scanner and recognition machinery. Route manual DX, peer PC11/PC61/PC26,
DXSummit and human telnet input through it, including diagnostic mode helpers.
Keep the legacy parser entry points and RBN/skimmer display cleanup equivalent.

Without explicit `dB`, accept a report only when the first selected normalized
mode is FT8 or FT4, the taxonomy's `bare_numeric_report` permission is enabled,
and the immediately adjacent source token contains an integer with an ASCII
`+` or `-` sign. The inclusive numeric range is -30 through +30. Preserve
configured aliases, first-mode precedence and explicit-dB recognition/priority.
Thus `FT8 +31` remains text while `FT8 +31 dB` retains explicit report handling.

Preserve human comment times, fused time text, `CWT`, unrelated numbers and
speed-like text in source order, subject to existing whitespace/punctuation
normalization. `CWT` can still select CW metadata while remaining visible in
the stored comment. Keep recognition-side numeric state separate from cleanup
decisions so preservation does not change existing explicit-dB selection.

Display, SNR filters and existing path-learning eligibility checks use the same
accepted report value. The range is a conservative product guardrail, not a
physical validity boundary or a claim of decoder provenance, receive direction,
calibration or improved prediction accuracy. Independent primary-source review
informed that limit on the claim; it did not choose product policy.

### Manual DX input

Permit ASCII `+` only after `DX`, two complete argument words and a separating
space. This supports both current argument orders. The reader borrows the
current edited prefix; erasing the boundary revokes permission. Do not broaden
login, frequency/callsign, other-command or machine-header alphabets. Retain
line limits, byte restrictions, editing, echo and physical-line recovery.
Contextual rejection guidance may name `+` only within a DX comment.

### Human telnet wire time

Require a standalone final raw uppercase five-byte `HHMMZ`, with hours 00-23
and minutes 00-59. Flexible spacing remains accepted. Remove only this terminal
wire token before human comment parsing. Missing, malformed, wrapped or fused
terminal clocks and fields following the clock reject the spot before queueing.
Never fall back to an earlier comment time.

If an upstream omits its actual timestamp and terminal prose has identical
`HHMMZ` bytes, the parser cannot distinguish the two. The owner accepted that
wire convention and ambiguity. No extra heuristic is introduced to guess intent.
Peer frames and DXSummit retain their separate date/time fields.

Retain existing today-UTC assignment and future-skew conversion after valid
wire-clock extraction. A clock more than two minutes ahead still converts to
arrival time; this decision does not redesign midnight rollover or future-time
normalization. The human comment parser itself supplies no authoritative time.

### Ownership and compatibility

Use input-bounded local scratch only; add no retained registry, worker, queue,
runtime key or persisted field. Preserve taxonomy ownership from ADR-0069,
source-marker stripping from ADR-0091, accepted-source peer publication from
ADR-0284 and RBN CW speed behavior from ADR-0332. Peer/DXSummit remain human
sources, including relayed `-#` identities. This does not infer provenance.

Only new ingested rows receive the new human comment behavior. Do not rewrite
historical reports/comments or reset the archive; stripped old text is not
recoverable. Bands, mobile callsign normalization, command aliases, source
classification and model calibration are outside this decision.

## Alternatives considered

1. Require explicit `dB` for every human report. This loses common adjacent
   signed FT8/FT4 shorthand; the owner selected the narrow form instead.
2. Change the legacy parser globally. This changes skimmer cleanup and breaks
   the compatibility boundary.
3. Duplicate a complete human parser. This duplicates taxonomy, EVENT and dB
   recognition and creates avoidable drift.
4. Restore text after destructive parsing. This requires reconstruction and can
   lose source order or report adjacency; preservation belongs in parsing.
5. Accept human telnet spots with missing wire time using arrival time. The
   owner chose rejection instead. An earlier comment clock is not a fallback.

## Consequences

### Benefits

- Human split numbers, times and `CWT` remain meaningful in new stored comments.
- Display, filtering, history and learning consume consistent accepted metadata.
- Every human ingress route receives the same report/preservation contract.
- Wire timestamp ownership is explicit and independently testable.

### Risks

- Feeds without the selected final time grammar, including feeds appending a
  grid after time, lose those spots. No format fallback is added.
- Terminal prose ambiguity and existing UTC/skew conversion remain limitations.
- Accepted syntax cannot establish receiver identity or measurement credibility.
- Old archive text remains unchanged and cannot be reconstructed by this repair.

### Operational impact

- No new configuration, schema migration or archive reset is required.
- Manual DX accepts `+` only within its comment; other input restrictions remain.
- Existing learning gates, source classes, RBN parsing and peer relay ownership
  remain in force. This decision does not authorize deployment or claim a live
  prediction or latency improvement.
- Local validation passed: the complete Windows lane, targeted race checks,
  four fuzz campaigns, static analysis and allocation envelopes. See TSR-0132
  for counts, reference-asset setup and skipped external-interoperability checks.

## Links

- Approval: owner-approved Human comment integrity Scope Ledger v1.
- Related tests: [human parser](../../spot/human_comment_parser_test.go),
  [legacy parser oracle](../../spot/comment_parser_stream_test.go),
  [manual positive report input](../../telnet/dx_plus_input_test.go),
  [human command parsing](../../commands/human_comment_test.go), and
  [consumer delivery](../../internal/cluster/human_comment_delivery_test.go).
- Related docs: [human comments](../../spot/README.md#human-spot-comments),
  [wire time](../../rbn/README.md#human-telnet-wire-time), and
  [domain contract](../domain-contract.md#human-spot-comments-and-wire-times).
- Related TSR: [TSR-0132](../troubleshooting/TSR-0132-human-comment-report-and-time-parsing.md).
- Supersession: refines human comment parsing only; no wholesale supersession
  of ADR-0069, ADR-0091, ADR-0284 or ADR-0332.

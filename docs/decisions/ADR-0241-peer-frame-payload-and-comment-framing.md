# ADR-0241: Peer Frame Payload and Comment Framing

- Status: Accepted
- Date: 2026-10-04
- Decision Origin: Troubleshooting chat

## Context

At `c3bdcd1cefedfbeaf965323a354ac2e5aa331e08`, the framing fuzzer minimizes
generic payload loss to `PC00^H0^^H0^^H0`. ParseFrame classifies four payload
fields (`H0`, empty, `H0`, empty) and transport hop 0. Encode strips the already
parsed payload again, losing two fields. It also turns zero fields into one
empty field. The generic parser's blank-field boundary is an explicitly selected
contract; weakening that boundary would conceal the encoding defect.

The earlier reader treats every tilde as a terminator. The frame parser and
original-comment validator also reject it. Pinned DXSpider revision
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d` permits comment tildes and replaces
PC11/PC61 comment carets with them. Merely relaxing the validator would still
truncate legitimate comments. The correction must also carry framing context
while discarding overflowing input, without retaining the discarded sentence.

PC61 already validates plain IP addresses with `netip.ParseAddr` and rejects
zones. Its owned relay fields already preserve accepted address text. This
surface needs falsifiable RFC 4291 IPv6 and native receiving-reader evidence,
rather than an expanded production IP policy. [TSR-0039](../troubleshooting/TSR-0039-peer-normalized-relay-and-telnet-iac.md)
preserves earlier date and framing failures and records verification separately.

## Decision

- Frame.Fields is payload after parsing. Encode serializes it exactly once,
  distinguishes zero fields from one empty field and adds the requested hop.
  A blank field stops generic suffix extraction. Existing generic, PC92/PC93
  and spot hop classifications remain unchanged; protected hop-like payload
  values remain data. Public PayloadFields retains its legacy copy/strip API.
- Preserve negative-hop formatter conventions. Manually constructed generic
  hop-like trailing payload is serialized literally; it need not survive the
  generic parser's existing contiguous-hop classification. Canonical round-trip
  guarantees apply to successfully parsed nonnegative-hop frames.
- Encode returns the complete string, even if oversized. Tests check literal
  payload and source nonmutation before expecting refusal at the existing
  parser/writer limit. PC92/PC93 queue callers do not treat an empty Encode
  result as an error. The separate original-spot per-variant size refusal is
  unchanged; no limit, reservation or queue behavior is raised.
- In PC11/PC61/PC26, protect tildes only inside the fifth payload field
  (comment). A tilde outside it and all CR/LF bytes end a sentence. ParseFrame
  admits tildes only in the comment, after the bounded field split; the
  original-comment validator removes only its tilde prohibition. Other field
  validity and genuinely malformed rejection before both dedupe paths remain.
- Use the same reader boundary rule during extraction and discard. A fixed
  continuation retains only header bytes, partial leading whitespace-rune
  bytes and saturated field position, including limits smaller than a PC
  header. It owns no payload, slices, workers or external lifecycle. Whitespace
  stays in returned spot-shaped sentences for strict rejection; a comment
  `~PCxx` cannot repair a padded sentence through resynchronization.
- Keep existing transport tolerance, scratch release before reads, allocation
  envelope, backing-buffer release, deadlines, EOF ordering and teardown.
  Complete buffered frames precede a stored read error; incomplete EOF tails
  are not emitted. Other PC families retain existing terminator behavior.
- Retain plain IPv4/IPv6 validation and original IP text. Full/compressed,
  uppercase/leading-zero, embedded IPv4 and mapped IPv6 forms pass the existing
  GoCluster rule; zones, prefixes, brackets, ports and malformed spellings do
  not. Each positive native case uses fresh state because dedupe excludes IP.
- Preserve the ADR-0239/0240 original/local ownership split and all hop,
  destination conversion, source-exclusion, dedupe, forwarding, age and queue
  gates. Local mutation must not enter relay, including comment and IP changes.
- Use actual pinned generators for comment substitution and native framing.
  Generated PC26 has no hop: it proves local timestamp/comment admission and
  no relay. A separately identified hop-bearing adaptation proves modern-only
  relay through the production writer and a receiving native reader. Pinned
  downstream cache/disk observations remain distinct from literal byte checks.

## Alternatives considered

1. Change generic parsing to consume hops across blank fields. Rejected by
   explicit policy: it would destroy the selected payload contract.
2. Correct only the fuzzer oracle. Insufficient because current Encode loses
   real parsed payload. Independent literal goldens and arity checks are needed.
3. Permit tilde only in the comment validator. Insufficient because the reader
   and frame parser would still prevent valid admission.
4. Ignore every tilde until CR/LF. Rejected because a bare terminal tilde on a
   still-open peer is an existing supported ending, including recovery.
5. Buffer discarded input or add connection workers. Unnecessary: a small
   reader-owned continuation carries all required field context.
6. Tighten IPv6 to the pinned receiver's hex/colon predicate or canonicalize
   accepted IP text. Rejected: legitimate dotted-tail IPv6 and original spelling
   are part of the selected GoCluster contract.

## Consequences

### Benefits

- Generic parsed payload and arity survive canonical encoding.
- Legitimate generated comment tildes survive native input and onward relay.
- Overflow recovery cannot turn a discarded comment tail into accepted input.
- IPv6 validity, original text and receiving-reader observations have distinct
  regressions without an unnecessary address-policy change.

### Risks

- Strict original validity still rejects malformed inputs that tolerant local
  parsing could previously repair. Literal IAC remains unsupported.
- GoCluster's broader callsign/IP envelope is not full pinned DXSpider receiver
  parity. That receiver rejects dotted IPv4 tails and some selected call forms.
- Framing follows field position before semantic admission. Malformed field
  content is still rejected by the parser/validator; framing is not admission.
- Component, race, fuzz and allocation checks do not establish deployment,
  production monitoring or a throughput/latency improvement.

### Operational impact

No configuration migration or new knob is introduced. Compare original fields,
writer bytes and actual receiver storage separately. Valid local eligibility
does not guarantee forwarding, and IP/comment differences alone do not create
a new dedupe identity. Support-agent documentation impact: Required; current
routes and the peer support card point to the authoritative admission contract.

## Links

- Related issues/PRs/commits: Approved Scope Ledger v5; baseline `c3bdcd1`.
- Related tests: [literal encoding/oracle tests](../../peer/protocol_roundtrip_test.go),
  [reader framing/fuzz tests](../../peer/reader_spot_framing_test.go),
  [native IP/comment/reference tests](../../peer/spot_relay_framing_test.go),
  [primary admission integration](../../internal/cluster/peer_spot_admission_test.go)
  and reader scratch/owner qualification.
- Related docs: [Peer contract](../../peer/README.md#original-spot-admission-and-relay),
  [Domain contract](../domain-contract.md#relay-under-overload),
  [Reference scripts](../../scripts/README.md#pinned-dxspider-spot-interoperability),
  [RFC 4291 address text forms](https://www.rfc-editor.org/rfc/rfc4291.html#section-2.2).
- Related TSRs: [TSR-0039](../troubleshooting/TSR-0039-peer-normalized-relay-and-telnet-iac.md).
- Supersedes / superseded by: Refines the encoder payload clause of
  [ADR-0050](ADR-0050-peering-hop-canonicalization-pc92-dedupe-and-overlong-diagnostics.md)
  and comment framing in [ADR-0239](ADR-0239-peer-original-validation-and-relay.md)
  and [ADR-0240](ADR-0240-peer-dxspider-date-admission.md). Their accepted history,
  original relay, date, gate and resource contracts remain effective.

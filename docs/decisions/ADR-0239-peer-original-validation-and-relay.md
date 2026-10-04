# ADR-0239: Peer Original Validation and Relay

- Status: Accepted
- Date: 2026-10-04
- Decision Origin: Troubleshooting chat

## Context

Peer sysops reported changed onward spots. Baseline PC11/PC61 relay reconstructed
sentences from the locally parsed `Spot`, so normalization, comment extraction,
frequency rounding and fallback could alter transit content. It also read the
local object after queue handoff, when the local consumer could own and mutate
it. Tolerant local parsing could conceal malformed originals before primary
dedupe. [TSR-0039](../troubleshooting/TSR-0039-peer-normalized-relay-and-telnet-iac.md)
records the source-derived causes and the limits of the evidence.

The durable requirement is to validate original peer spots first, permit local
correction of valid spots, and independently relay the original content. This
must preserve ADR-0054's local-queue gate, existing compatibility conversions
and bounded resources. Literal decoded `0xFF` is an additional hazard: native
receipt decodes doubled Telnet IAC, but native writes do not escape it.

## Decision

- Apply shared original validation to incoming PC11, PC61 and PC26 before any
  local normalization/fallback, queue handoff or primary/peer dedupe admission,
  including receive-only mode. Reject malformed originals without accepted-spot
  display, archive or onward relay. Optional rejection diagnostics stay bounded.
- Bind the complete field and framing grammar in
  [peer behavior](../../peer/README.md#original-spot-admission-and-relay): exact
  payload counts, broader original GoCluster calls, unpadded finite plain
  decimal frequency with representable existing primary rounding, real UTC
  date/time, valid original origin, plain PC61 IP and PC26's optional merge field.
  Do not use local repair or PC92's narrower identity rule for admission.
- Require nonempty comments and reject bytes `0x00-0x08`, `0x0A-0x1F`,
  `0x80-0x9F` and literal `0xFF`, including those bytes inside valid UTF-8.
  Permit tabs, whitespace-only comments and other permitted bytes. Reject IAC
  content for every transport rather than expanding escaping in this fix.
- Retain case-insensitive PC/H recognition, optional `~` and existing transport
  terminator tolerance. Validate the original sentence without outer whitespace
  or padded/malformed hops. PC11/PC61 require hops of one or two digits, 0-99;
  every stacked token must be valid and the rightmost value governs. PC26 keeps
  its legitimate no-hop form. Payload positions protect hop-like calls and the
  optional PC26 merge field; generic suffix stripping must not erase them.
- Clone at most eight original fields into compact owned storage before local
  parsing or normalization-cache access. Capture the unchanged peer key and
  original timestamp before local handoff. Relay never reads the handed-off
  mutable `Spot`; local correction semantics remain in the existing parser and
  local pipeline.
- Relay PC11 as PC11 to modern and legacy destinations. Relay original PC61 to
  modern destinations and convert it to PC11 for legacy destinations by removing
  only its IP field. Preserve PC26's modern-destination restriction. Change only
  hop decrement, required framing and that legacy conversion. Emit `^Hn^~`
  followed by writer CRLF.
- Retain forwarding enablement, local-queue acceptance, hop greater than one,
  source exclusion, peer dedupe and existing age behavior. Valid H0/H1 remain
  locally eligible. PC11/PC61 retain their second original-timestamp age check;
  PC26's age-check behavior remains unchanged. Distinct valid comments may still
  share the existing dedupe identity.
- Refuse each output variant that exceeds the existing writer sentence limit,
  without truncation. A fitting legacy conversion may still be
  sent when modern PC61 does not fit. Cache admission is not undone and no retry
  queue is added. Retain the 65,536-byte sentence maximum plus writer CRLF,
  shared 8 MiB parse pool, reader reservation, separate queue/cache charges,
  capacities, TTLs and lifecycle. Reservations require actual allocation evidence.

## Alternatives considered

1. Keep tolerant local admission while forwarding only valid originals. Rejected
   because malformed inputs would still enter primary dedupe and accepted local
   processing. The operator selected shared validity before both paths.
2. Forward generic raw frames without a destination matrix. Rejected because it
   either excludes legacy recipients or loses PC61-to-PC11 compatibility, and
   generic suffix handling can damage legitimate payload fields.
3. Normalize transit into a supposedly cleaner common form. Rejected because
   local corrections cannot become authoritative peer content; upgrading PC11
   also invents a missing PC61 IP field.
4. Escape literal IAC through the transport. Viable only with a larger transport
   and transmitted-byte-accounting scope. The operator selected rejection of
   `0xFF` for this bounded fix.
5. Tighten all spot calls to the pinned DXSpider rule or redesign dedupe keys.
   Rejected for this scope. Broader original calls are explicitly accepted;
   downstream rejection and valid duplicate suppression remain separate facts.

## Consequences

### Benefits

- Malformed originals cannot consume primary dedupe identity through local repair.
- Valid local corrections and faithful peer transit have separate owners.
- Compatibility and failure behavior remain explicit under queue/size pressure.

### Risks

- Inputs previously accepted after normalization or fallback are now rejected.
  Literal `0xFF` is unsupported comment content, even when correctly escaped
  by an incoming Telnet sender.
- GoCluster's accepted calls and IP spellings may be rejected by a downstream
  receiver. GoCluster validity and emitted-byte fidelity do not imply receiver
  acceptance.
- The native reader has already consumed transport framing. Egress framing is
  canonical; this decision does not promise exact preservation of incoming
  terminator spelling or duplicate hop markers.
- Compact ownership and two possible encodings still require allocation and
  concurrent-lifecycle verification inside the existing budgets. This ADR makes
  no performance or complete PC92 qualification claim.

### Operational impact

No configuration migration or new knob is introduced. Check `parse_rejected`
for admission refusal, then separate valid-spot age, queue, duplicate and relay
gates. Local display/archive may contain corrections; receiver observations
must compare against the decoded original and authorized conversion.

## Date-contract refinement

[ADR-0240](ADR-0240-peer-dxspider-date-admission.md) refines the original date
spelling and validated local timestamp assignment after sender review found
that the zero-padded grammar omitted DXSpider's `%2d` day. It preserves this
accepted decision's ownership, admission, relay, gate and resource contracts.
Literal comment tilde support is required and pending as a separate framing
correction; full DXSpider spot compatibility remains qualified.

## Links

- Related issues/PRs/commits: Approved Scope Ledger v3; baseline
  `dd89af2d47e0abfadf80c709fbdc24d48b7f1478`.
- Related tests: peer admission, parser/encode round trips, literal relay and
  correction isolation, native Telnet IAC, age/queue/hop/dedupe gates, primary
  admission-to-delivery, untrimmed fuzzing, parse ownership/allocation and
  qualification capacity; pinned-receiver bytes and storage are distinct checks.
- Related docs: [Peer behavior](../../peer/README.md),
  [Domain contract](../domain-contract.md), [Validation runbook](../dev-runbook.md).
- Related TSRs: [TSR-0039](../troubleshooting/TSR-0039-peer-normalized-relay-and-telnet-iac.md).
- Supersedes / superseded by: Refines spot framing/admission/ownership clauses of
  ADR-0050, ADR-0054, ADR-0091 and ADR-0234; their other contracts remain effective.
  [ADR-0054](ADR-0054-peering-control-priority-and-local-acceptance-relay.md)
  retains control priority and successful-local-queue relay gating.

## Payload and comment-framing refinement

[ADR-0241](ADR-0241-peer-frame-payload-and-comment-framing.md) refines payload
encoding and supplies the separately approved PC11/PC61/PC26 comment-tilde
correction across reader, parser and validation. This accepted history remains
unchanged; current behavior and compatibility limits are in that refinement.

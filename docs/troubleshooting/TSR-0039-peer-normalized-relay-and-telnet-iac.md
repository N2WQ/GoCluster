# TSR-0039 - Peer Normalized Relay and Telnet IAC

Status: Monitoring
Date Opened: 2026-10-04
Date Resolved: n/a
Owner: Core maintainers
Technical Area: peer admission, protocol framing, relay ownership
Trigger Source: Operator report and troubleshooting chat
Led To ADR(s): ADR-0239
Tags: PC11, PC61, PC26, original payload, primary dedupe, Telnet IAC

## RCA Summary

- What happened: Peer sysops reported changed onward spots. Source review also
  found that malformed original fields could become locally acceptable through
  normalization or timestamp/origin fallback before primary dedupe.
- Why: The PC11/PC61 relay rebuilt peer sentences from the locally parsed
  `Spot`, whose fields and comment had already changed. The original sentence
  had no shared strict admission boundary. Literal decoded `0xFF` also cannot
  survive native relay unchanged because the writer does not Telnet-escape it.
- What fixed it: The implemented remediation validates original PC11/PC61/PC26
  fields first, owns compact original copies, and separates local corrections
  from relay. It rejects unsupported comment byte `0xFF`; it does not expand
  transport escaping. Local verification passed; deployment and production
  monitoring have not been performed.
- How we know: These causes were traced in baseline source at
  `dd89af2d47e0abfadf80c709fbdc24d48b7f1478`. The implementation's separate
  malformed-admission, mutation-isolation, native-writer and pinned-receiver
  checks passed. The verification section records their boundaries; none
  establishes a production rollout.
- Operator/support answer: Local output may legitimately show corrections.
  Onward peer content must retain accepted originals except hop, framing and
  legacy conversion. Missing input can be malformed, stale, duplicate or
  ineligible for forwarding; inspect the corresponding gate before changing
  configuration. A downstream receiver may reject a valid broader GoCluster
  callsign without GoCluster having altered it.

## Triggering Request

- Request date: 2026-10-04.
- Request summary: Vet the changed-spot complaint and surgically prevent
  malformed peer spots from entering primary dedupe while preserving onward
  original payloads independently of local correction.
- Request reference: Peer-forwarding review chat, Approved Scope Ledger v3.

## Symptoms and Impact

- PC11/PC61 onward frequency spelling, comment, call suffixes, timestamp or
  origin could differ from the decoded original. PC11 could become PC61 for a
  modern destination, adding an IP field that the source did not supply.
- Local normalization, comment parsing and fallback could hide malformed
  originals, allowing them into accepted local processing and dedupe.
- Tests comparing only prefixes and decremented hops did not establish payload
  fidelity. Writer-byte equality alone also misses Telnet interpretation by the
  next receiver.
- PC26 belongs to shared malformed-peer-spot admission even though the initial
  changed-relay complaint concerned PC11/PC61. Its merge and destination rules
  remain separate.

## Timeline

1. 2026-10-04 - Current baseline source and the pinned DXSpider contract were
   reviewed; normalized reconstruction was distinguished from original relay.
2. 2026-10-04 - The operator selected shared original validity, broader original
   callsigns, strict sentence checks with existing transport tolerance, and
   canonical `^Hn^~` output within existing limits.
3. 2026-10-04 - Source review identified the literal-IAC gap. The operator chose
   rejection of `0xFF` and approved v3's bounded implementation and validation.
4. 2026-10-04 - Implemented shared original admission and owned original relay
   on `peer_forwarding`; completed local tests, full race checks, parser fuzzing,
   real receiver storage observations and allocation/lifecycle checks.

## Hypotheses and Tests

1. Forwarding the normalized local `Spot` preserves the received content.
   - Evidence: Baseline `peer/manager.go` parsed and handed off the spot, then
     called `broadcastSpot`; the recipient formatter rebuilt PC11/PC61 from
     that spot. `peer/parse.go` normalized calls, parsed/reduced the comment and
     allowed date/time and origin fallback.
   - Outcome: Rejected by source inspection. The original payload must be
     retained independently of local processing.
2. Generic raw-frame forwarding is sufficient for every destination.
   - Evidence: Modern-only forwarding excludes legacy recipients. Sending
     PC61 unchanged to legacy peers loses the existing PC61-to-PC11 conversion;
     upgrading PC11 invents a missing IP field. Generic suffix stripping can
     also consume hop-like payload fields or PC26 optional slots.
   - Outcome: Rejected. Field-position-aware framing and the explicit
     destination matrix are required.
3. The original prohibited comment ranges cover all native transport hazards.
   - Evidence: Baseline `peer/protocol.go` decodes `FF FF` as literal `FF`;
     `peer/session_transport.go` writes the decoded sentence directly. Incoming
     `A FF FF B` becomes decoded `A FF B`; retransmitting those decoded bytes
     makes the next native parser consume `FF B` as a Telnet sequence.
   - Outcome: Rejected by source inspection. Shared admission additionally
     rejects literal `0xFF`; transport escaping remains outside this fix.
4. Counting one later valid output proves malformed input did not poison dedupe.
   - Evidence: A malformed original can be repaired into a valid local object,
     which can suppress the later control spot. The count alone does not
     identify which input was accepted.
   - Outcome: Insufficient oracle. Tests must identify the valid accepted
     record and separately observe queue, cache, archive/display and relay
     boundaries.

## Findings

- Root cause: Local interpretation was used as authoritative transit data.
  Shared original validation and an owned relay payload establish different
  responsibilities without changing local correction semantics.
- Contributing factors: Tolerant original parsing, generic hop suffix handling,
  payload-insensitive relay assertions and missing Telnet receiver observations.
- Native transport tests observed escaped incoming `FF FF` becoming literal
  `FF`, then being rejected before local handoff and peer dedupe for all three
  spot types, with forwarding enabled and disabled. No historical production
  wire capture or production performance result is claimed.
- This requires a durable decision because admission, byte policy, compatibility
  and ownership are observable contracts. Existing dedupe identities, limits,
  TTLs and valid-spot suppression are not being redesigned.

## Decision Linkage

- ADR created: [ADR-0239](../decisions/ADR-0239-peer-original-validation-and-relay.md).
- Decision delta summary: Validate originals before both dedupe paths and local
  fallback; permit local corrections while relaying owned original fields;
  reject unsupported IAC content and refuse oversized output variants.
- Contract/behavior changes: Strict malformed admission, original-field PC11
  retention and PC61 legacy conversion, canonical `^Hn^~` plus writer CRLF.
  Receive-only admission and no-hop PC26 use the same original validity rule.

## Verification and Monitoring

- Local validation completed on 2026-10-04. Final `go test ./...`,
  `go test -race ./...` and `go vet ./...` passed. Staticcheck and configured
  golangci-lint passed with process-local Go 1.26.2, matching the installed
  analyzers; tests and race checks used Go 1.27.1 on Windows amd64.

| Approved requirement | Implementation and observed evidence |
|---|---|
| Reject malformed originals before accepted processing or either dedupe | [Shared guard](../../peer/spot_relay.go) and [native admission integration](../../internal/cluster/peer_spot_admission_test.go): 70 malformed/valid identity pairs across forwarding modes; no malformed queue/cache admission or accepted archive/display handoff, followed by identified valid controls. |
| Isolate local correction from original relay | [Correction isolation](../../peer/spot_relay_test.go): mutate calls, comment, mode, frequency, time, origin and IP after handoff; four PC11/PC61 destination combinations retain literal originals and the captured peer key. |
| Retain framing, compatibility and relay gates | [Contract tests](../../peer/spot_relay_contract_test.go), gate/age/size tests and [native writer/receiver tests](../../peer/spot_relay_interop_test.go): all permitted comment bytes survive native receipt; hop, forwarding, queue, age, duplicate, source and PC26 restrictions pass; only fitting output variants are sent. |
| Keep resources and lifecycle bounded | [Full-handler resource tests](../../peer/spot_relay_resources_test.go): 64 session identities, leased native receipt, compact backing ownership, actual allocation, saturated queues and cancellation/Stop release checks passed, including race runs. Qualification wire fixtures pass shared admission without weakening separate synthetic cache pressure. |

- Both `FuzzOriginalPeerSpotAdmission` and `FuzzParseFrameHopSuffix` passed
  30-second runs with two workers, respectively 146,279 and 112,403 executions.
- The configured `TestDXSpiderReference` suite passed against the pinned
  receiver with no skips. Production native writer bytes and actual receiver
  cache/disk storage were observed separately. The receiver's own frequency
  rounding, comment trimming and DE suffix behavior are not GoCluster transit
  changes. Broader `-#`/`-123` spotters and mapped IPv6 IP spellings can remain
  valid and preserved by GoCluster while the pinned receiver rejects them.
- For the maximum many-token full-handler fixture, measured total allocation
  was 9,567,912 bytes; existing destination queue clones accounted for 4,128,768
  bytes, leaving 5,439,144 bytes against its unchanged 7,923,816-byte parse
  reservation. The added conservative scratch inventory was 214,376 bytes.
  These controlled allocation/ownership checks are not a live latency profile
  or a complete runtime qualification claim.
- Archive/display observations cover the real accepted-spot queues and recent
  ring, not final archive persistence or delivery to connected users. Standard
  suites skip externally configured receiver cases; the separately configured
  receiver run supplied that evidence. Review was lead-owned with scoped
  worker reviews; no independent non-steered review occurred.
- `go run ./cmd/codemap check -all` passed. The troubleshooting-record checker
  reports two pre-existing evidence-section failures in unchanged TSR-0037,
  verified against HEAD; it reports no issue for this record or its index row.
  That unrelated record was not modified.
- Signals to monitor: Existing `parse_rejected`, ingest-queue refusal and peer
  queue/size diagnostics. Diagnostic overload can lose records; an absent log
  entry is not proof of admission. Compare receiver storage separately from
  emitted bytes and local corrected output.
- Rollback triggers: Valid original content changes beyond the permitted
  hop/framing/conversion, malformed primary admission, ownership races,
  exceeded existing resource bounds or lost relay-gate behavior.

## References

- Issue(s)/PR(s): none; local peer-forwarding review.
- Baseline commit: `dd89af2d47e0abfadf80c709fbdc24d48b7f1478`.
- Related ADR(s): [ADR-0054](../decisions/ADR-0054-peering-control-priority-and-local-acceptance-relay.md),
  [ADR-0239](../decisions/ADR-0239-peer-original-validation-and-relay.md).
- Related docs: [Peer behavior](../../peer/README.md#original-spot-admission-and-relay),
  [Domain contract](../domain-contract.md#relay-under-overload).
- Reference receiver: DXSpider revision
  `3e9b3621d94dd45c68702e4a0f896aac33f2a91d`. Its acceptance envelope is narrower
  than the selected original GoCluster callsign rule.

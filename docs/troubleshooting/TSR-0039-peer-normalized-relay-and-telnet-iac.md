# TSR-0039 - Peer Normalized Relay, Date Admission and Telnet Framing

Status: Monitoring
Date Opened: 2026-10-04
Date Resolved: n/a
Owner: Core maintainers
Technical Area: peer admission, protocol framing, relay ownership
Trigger Source: Operator report and troubleshooting chat
Led To ADR(s): ADR-0239, ADR-0240, ADR-0241
Tags: PC11, PC61, PC26, original payload, date, primary dedupe, Telnet IAC, tilde

## RCA Summary

- What happened: Peer sysops reported changed onward spots. Source review also
  found that malformed original fields could become locally acceptable through
  normalization or timestamp/origin fallback before primary dedupe. Later
  sender review found that v3's strict date grammar rejects ordinary DXSpider
  spots on days 1-9; legitimate tilde comments expose an older framing gap.
- Why: The PC11/PC61 relay rebuilt peer sentences from the locally parsed
  `Spot`, whose fields and comment had already changed. The original sentence
  had no shared strict admission boundary. Literal decoded `0xFF` also cannot
  survive native relay unchanged because the writer does not Telnet-escape it.
  The v3 date guard overlooked DXSpider's `%2d` day spelling; the reader at
  that baseline treated the first `~` anywhere as a terminator.
- What fixed it: The v3 remediation validates original PC11/PC61/PC26 fields
  first, owns compact original copies, and separates local corrections from
  relay. It rejects unsupported comment byte `0xFF`. The v4 date refinement
  admits the exact space-padded day spelling and assigns the validated instant
  before local dedupe/age checks while retaining original date bytes. Its final
  verification is recorded separately below. The approved v5 correction writes
  parsed payload once and recognizes comment tildes by field position across
  reader, parser and validation, including overflow recovery. Deployment and
  production monitoring have not been performed.
- How we know: The original relay and IAC causes were traced in source at
  `dd89af2d47e0abfadf80c709fbdc24d48b7f1478`. The implementation's separate
  malformed-admission, mutation-isolation, native-writer and pinned-receiver
  checks passed for the exercised forms. Source-generated inputs from pinned
  DXSpider later exposed the missed date spelling. The verification section
  preserves v3 evidence and records the date correction separately; none
  establishes a production rollout or full DXSpider spot compatibility.
- Operator/support answer: Local output may legitimately show corrections.
  Onward peer content must retain accepted originals except hop, framing and
  legacy conversion. Missing input can be malformed, stale, duplicate or
  ineligible for forwarding; inspect the corresponding gate before changing
  configuration. Compare early-month date spelling and the deployed version
  when diagnosing missing input. A downstream receiver may reject a valid
  broader GoCluster callsign or selected IP spelling without GoCluster having
  altered it. Literal comment tildes are supported by the separately approved
  v5 correction.

## Triggering Request

- Request date: 2026-10-04.
- Request summary: Vet the changed-spot complaint and surgically prevent
  malformed peer spots from entering primary dedupe while preserving onward
  original payloads independently of local correction.
- Request reference: Peer-forwarding review chat, Approved Scope Ledgers v3, v4 and v5.

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
- At `668ec513`, standard dates such as ` 4-Oct-2026` are rejected before local
  handoff and both dedupe paths, including when forwarding is disabled. A
  guard-only relaxation could still make the tolerant local parser use now,
  changing identity and age decisions.
- Before v5, DXSpider-generated comments such as `CQ~TEST` could not survive
  the reader. This predates the branch and remained outside the date correction.
- At `c3bdcd1`, generic re-encoding strips hop-like payload a second time across
  blank fields. `PC00^H0^^H0^^H0` loses two payload fields; a round trip could
  appear green if the parser and test oracle shared the same loss.

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
5. 2026-10-04 - Sender-format review at `668ec513` found that strict zero-padded
   date grammar omitted DXSpider's `%2d` output. Pinned formatter/generator
   execution confirmed the spelling; the earlier receiver fixtures had supplied
   dates generated with Go's zero-padded layout.
6. 2026-10-04 - The operator selected the exact two date spellings, validated
   timestamp assignment before keys and age checks, and reference-generated
   reverse-direction coverage in approved v4. Literal comment tilde handling
   was tracked as a required separate bounded framing correction.

7. 2026-10-04 - The operator selected preservation of generic payload across
   blank fields, RFC 4291 IPv6 and original IP text, and comment-tilde handling
   across reader, parser and validation, then approved v5's bounded patch.

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

5. Strict two-digit days admit all standard DXSpider sender dates.
   - Evidence: Pinned `DXUtil::cldate` uses `%2d`. Actual PC11/PC61/PC26
     generators emit ` 1-Oct-2026` and ` 9-Oct-2026`, then `10-Oct-2026` and
     `31-Oct-2026`. The v3 guard requires a digit at the first day position.
   - Outcome: Rejected. Admit the exact ASCII-space-padded single-digit form
     alongside zero padding, while validating the real calendar.
6. Relaxing only the original guard fixes the date defect.
   - Evidence: Local parsing trims the date to `4-Oct-2026`, then its fixed
     two-digit layout can fail and use now. `dxKey` and ingest age currently
     consume the local `Spot.Time`.
   - Outcome: Rejected. Assign the validated UTC instant before either consumer;
     retain the separate original field for relay. The shared tolerant parser
     is unchanged.
7. GoCluster-to-DXSpider receiver tests establish incoming date compatibility.
   - Evidence: Earlier fixtures manufacture zero-padded Go dates; successful
     downstream storage does not exercise DXSpider's actual sender formatter.
   - Outcome: Rejected. Add actual generator output entering the native reader,
     and distinguish no-hop PC26 local admission from hop-bearing relay.

## Findings

- Root cause: Local interpretation was used as authoritative transit data.
  Shared original validation and an owned relay payload establish different
  responsibilities without changing local correction semantics.
- Contributing factors: Tolerant original parsing, generic hop suffix handling,
  payload-insensitive relay assertions and missing Telnet receiver observations.
- Date root cause: The agreed original grammar and its guard overlooked the
  reference sender's day spelling. The v4 correction assigns validated time
  before identity and age, rather than changing global tolerant parsing.
- Framing root cause before v5: The shared reader used the first tilde as a
  sentence boundary. DXSpider permits comment tildes and replaces comment
  carets with them. Parser and original-comment guards also rejected tilde.
  The v5 correction addresses all three owners and overflow continuation.
- Generic encoding root cause: The parser already extracted transport hops,
  stopping at a blank field, but Encode stripped the payload again. Encoding
  also represented zero fields as one empty field. The parser's generic
  classification stays intact; Encode now writes its payload once.
- Native transport tests observed escaped incoming `FF FF` becoming literal
  `FF`, then being rejected before local handoff and peer dedupe for all three
  spot types, with forwarding enabled and disabled. No historical production
  wire capture or production performance result is claimed.
- This requires a durable decision because admission, byte policy, compatibility
  and ownership are observable contracts. Existing dedupe identities, limits,
  TTLs and valid-spot suppression are not being redesigned.

## Decision Linkage

- ADR created: [ADR-0239](../decisions/ADR-0239-peer-original-validation-and-relay.md).
- Date refinement: [ADR-0240](../decisions/ADR-0240-peer-dxspider-date-admission.md)
  accepts both established day spellings and assigns validated local time
  before keys/age; all other admission and relay policies remain effective.
- Decision delta summary: Validate originals before both dedupe paths and local
  fallback; permit local corrections while relaying owned original fields;
  reject unsupported IAC content and refuse oversized output variants.
- Contract/behavior changes: Strict malformed admission, original-field PC11
  retention and PC61 legacy conversion, canonical `^Hn^~` plus writer CRLF.
  Receive-only admission and no-hop PC26 use the same original validity rule.

- Framing refinement: [ADR-0241](../decisions/ADR-0241-peer-frame-payload-and-comment-framing.md)
  preserves generic parsed payload and supports literal spot-comment tildes
  while retaining original admission, IP spelling, gates and all limits.

## Verification and Monitoring

### Original-payload relay v3 evidence

The following results were observed for the v3 implementation before the
sender-date defect was found. They establish the exercised relay, admission,
ownership and receiver cases, not acceptance of space-padded sender dates or
literal comment tildes.

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
  These outbound cases supplied zero-padded dates; they did not establish
  DXSpider-generated spot admission into GoCluster.
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

### Date refinement v4 evidence

The date correction was verified locally on 2026-10-04. The selected checks
include a reproducible generic framing-fuzzer failure outside the v4 date
change, so the overall validation is not all green.

- `go test ./...`, `go test -race ./...` and `go vet ./...` passed with
  Go 1.27.1 on Windows amd64. Staticcheck and configured golangci-lint passed
  with process-local Go 1.26.2. Lint first found two new test-only style issues;
  after correction, the affected date/sender cases passed normally and under
  race, and the full configured lint run reported zero issues.
- Exact date/calendar vectors, handler timestamp/peer-key/age assertions and
  real native/primary integration passed. The primary test observes three
  processed records, one duplicate and two accepted outputs: both day spellings
  share the same primary identity and exact UTC timestamp. Archive/display
  evidence covers accepted queues and the recent ring, not disk persistence
  or delivery to connected users.
- The configured `go test ./peer -run '^TestDXSpiderReference' -count=1 -v`
  suite passed in 55.436 seconds with no skips against the unmodified pin.
  The new sender matrix covers 96 native-input cases for days 1, 9, 10 and 31,
  including explicitly adapted zero-padded equivalents; it also covers 18
  invalid-calendar controls and two separately adapted hop-bearing PC26
  destination cases. Actual generated PC26 has no hop and is locally admitted
  without relay. Final affected sender/date cases passed normally and under
  race after the test-only lint corrections.
- Production writer-byte comparisons, native IAC rejection and the existing
  real receiver storage suite passed. Sender generation, original byte fidelity
  and downstream storage remain separate observations. The sender script's Perl
  syntax check passed; no formatter/generator or pinned source was replaced.
- Full leased-handler ownership/allocation checks passed for zero- and
  space-padded dates, including short, maximum one-token and many-token
  comments. The final space-padded many-token sample allocated 9,567,616 bytes;
  separately owned destination clones accounted for 4,128,768 bytes, leaving
  5,438,848 bytes within the unchanged 7,923,816-byte parse reservation.
  Calendar-extreme keys remain 65 bytes. The qualification wire-fixture check
  passed; none of these component checks establishes runtime performance or
  complete qualification.
- `FuzzOriginalPeerSpotAdmission` passed 30 seconds with two workers and
  180,350 executions, including new date-shape seeds.
- `FuzzParseFrameHopSuffix` failed during its planned 30-second run, after
  about 4.76 seconds. Minimized input `PC00^H0^^H0^^H0` reencodes to
  `PC00^H0^^H0^`; the existing oracle reports a trailing hop-like payload.
  `peer/protocol.go` and `peer/protocol_fuzz_test.go` have no v4 diff. An
  overlay substituting v3 `peer/spot_relay.go` reproduced the identical failure;
  this is an existing generic framing/test-contract finding, not an introduced
  date regression. The generated corpus file was preserved outside the repo
  as `gocluster-date-v4-hop-efe1da389565ed8f.fuzz` in the local temporary
  directory, and its literal input is retained here. Further diagnosis or a
  framing/oracle change requires its own approved scope; no waiver is inferred.
- Deliberately omitting `local.Time = stamp` through a temporary Go overlay
  made both the fixed timestamp and stale-admission assertions fail. This
  confirms that the date tests detect the broken guard-only correction.
  Repository production source was not replaced by these negative controls.
- Code-map generation/freshness checks passed. The troubleshooting-record
  checker still reports exactly two pre-existing TSR-0037 failures: missing
  root-cause evidence and fix/remediation sections. That record, the generic
  framing implementation/fuzzer and shared tolerant parser are unchanged. No
  issue was reported for TSR-0039 or its index.
- The final review was lead-owned with design-aware scoped workers; no
  independent non-steered review occurred. Tilde framing remains required and
  pending, and deployment/production monitoring have not been performed.

| Required contract | Falsifiable date evidence |
|---|---|
| Exact two spellings, real calendar and strict clock | Explicit UTC timestamp vectors, leap-day/year-domain controls, invalid dates, unpadded days and arbitrary-padding rejection. |
| Validated instant governs both dedupe paths and ingest age | Handler peer-key checks plus real primary-admission integration; equivalent day spellings share identity, and stale space-padded dates cannot enter local/peer queues or dedupe caches in either forwarding mode. |
| Original date bytes survive relay | Production-writer comparisons for PC11/PC61 modern and legacy destinations, plus separate hop-bearing PC26 relay. |
| Actual sender output enters native transport | [Pinned-generator tests](../../peer/spot_relay_date_test.go): PC11, PC61 and PC26 on days 1, 9, 10 and 31, exact local timestamps and invalid-calendar controls. Generated PC26 has no hop and must not relay. |
| Existing bounds, ownership and protocol behavior remain valid | Date-shaped backing/allocation, existing receiver and baseline/race checks passed. Admission fuzzing passed; the generic framing fuzzer has the pre-existing counterexample recorded above. |

Support-agent docs impact: Required. Current peer/script documentation, domain
contract and custom GPT routes/card point to the date contract and keep incoming
sender evidence separate from outbound bytes and downstream storage.

### Payload and comment-framing v5 evidence

Approved v5 covers the generic encoding/fuzzer mismatch, literal comment tildes
at reader/parser/validator boundaries and RFC 4291 native relay evidence. It
does not change the IP validator, dedupe identities, queue or age rules, limits,
transport escaping or the other PC families' parser grammar. The date correction
and its historical fuzzer failure above are preserved as evidence.

- `go test ./...` and `go vet ./...` passed with the pinned reference runtime
  configured. `staticcheck ./...` and
  `golangci-lint run ./... --config=.golangci.yaml` passed using process-local
  Go 1.26.2, compatible with the installed analyzers. Initial unused-assignment
  and tagged-switch findings in tests were corrected; affected tests were
  rerun. No production change was needed for those findings.
- `go test -race ./...` passed with native CGO support. Configured targeted
  native/IP/comment, reference and primary-admission race checks also passed.
  Default suites may skip external references when unconfigured; the explicit
  configured `go test ./peer -run '^TestDXSpiderReference' -v -count=1` run
  passed in 70.984 seconds with no skips. It exercises actual sender output,
  native transport, a production writer/second native reader, and pinned
  receiver cache/disk observations; these remain distinct evidence.
- Sequential two-worker fuzz runs passed: `FuzzParseFrameHopSuffix` for 120
  seconds (205,523 executions), `FuzzPeerSpotReaderFraming` for 120 seconds
  (683,857), `FuzzLineReaderRetainedBound` for 60 seconds (352,461) and
  `FuzzOriginalPeerSpotAdmission` for 30 seconds (155,208). Commands used
  `go test ./peer -run '^$' -fuzz '^<target>$' -fuzztime <duration> -parallel 2`.
- Temporary Go overlays against exact `c3bdcd1` sources made the new regressions
  fail for the intended defects: encoding lost the blank-protected payload;
  the old reader returned truncated CQ and released an internal Q during
  discard; the old parser's embedded-terminator guard rejected tildes; the
  old comment validator rejected them before local handoff. No repository
  production source was replaced by these negative controls.
- Twenty literal positive IPv6 spellings passed native ingress, original
  validation, local handoff, production writer and receiving native parser,
  retaining original modern IP text or removing only IP for legacy peers.
  Twenty-two malformed/wrapped address controls rejected. The real primary
  integration exercised 51 malformed cases in each forwarding mode, with
  ACK barriers, no malformed local/cache/archive/display admission, and a
  subsequent valid same-identity spot accepted.
- Literal reader tests cover comment tildes at all two-part read boundaries,
  byte reads, bare terminal markers on an open socket, repeated endings,
  consecutive frames, whitespace-shaped rejection, tiny limits 1-5, overflow
  decoys, PC92's separate cap, completed bytes before EOF, incomplete tails,
  scratch release and teardown. The unchanged gate tests now carry tilde
  comments; correction isolation mutates all local fields, including IP,
  before verifying original output through a native receiving reader.
- `go test -tags qualification ./peer -run
  '^TestQualificationReaderOwnedBackingAndReturnedLine$' -count=1` passed.
  `TestPC92MetadataOwnerLayout` observed a 152-byte reader wrapper, a 5,632-byte
  per-owner inventory within 8,192 bytes and active inventory 3,312 within
  4,096. The new fixed continuation owns no payload references; its allocation
  test passes. `BenchmarkSpotFrameBoundaryScan` measured 823.2 ns/op, 0 B/op
  and 0 allocs/op on Windows amd64. This establishes a narrow scan-allocation
  property, not an end-to-end latency or throughput improvement.
- The modified Perl sender passes its native `-c` syntax check. Existing
  sender defaults remain explicit and unchanged. Reference-generated no-hop
  PC26 proves local timestamp/comment admission and no peer-dedupe/relay;
  separate labelled hop-bearing fixtures prove modern-only onward transport.
- The final Go-code-quality, lifecycle, retained-state and scope pass found no
  additional material defect. Review was lead-owned with design-aware scoped
  workers; no independent non-steered review occurred. All 30 changed paths
  remain inside approved v5. Support-agent documentation impact: Required;
  contract, script documentation, routes/card, ADR and this TSR are updated.
- Code-map generation/freshness, changed Markdown links/anchors and whitespace
  checks passed. The troubleshooting checker still reports only TSR-0037's
  two pre-existing omissions (root-cause and fix/remediation evidence); that
  record is unchanged and no issue is reported for this record or its index.

| Approved v5 item | Implementation and falsifiable verification |
|---|---|
| Preserve parsed payload, arity and complete oversized encoding | `peer/protocol.go`; literal `protocol_roundtrip_test.go` goldens, source snapshots, live queue/parser boundaries, baseline overlay and corrected hop fuzz oracle. |
| Admit literal comment tildes across all three owners | `peer/reader.go`, `peer/protocol.go`, comment-only relaxation in `peer/spot_relay.go`; `reader_spot_framing_test.go`, native/reference fixtures and distinct old-reader/parser/validator negative controls. |
| Retain plain IPv6 validity and original IP text | Production address guard and serializer unchanged; `spot_relay_framing_test.go` exercises literal RFC 4291 forms, malformed controls and modern/legacy native output. |
| Preserve malformed rejection, correction isolation and relay gates | `peer_spot_admission_test.go`, `spot_relay_test.go` and receiver fixtures observe real dedupe/archive/display, mutable local handoff, original output and existing forwarding/age/queue/destination gates. |
| Preserve reader bounds, ownership and lifecycle | Fixed discard continuation, scratch/EOF/release tests, owner inventory, qualification backing checks, scan allocation evidence and full race suite. |
| Record current operator/support contract and history | ADR-0241, historical ADR backlinks, this Monitoring TSR, domain/peer/script docs, custom GPT routes/card and generated runtime map; no rollout or production observation claimed. |

Full DXSpider spot compatibility remains qualified because selected broader
callsign/IP policies differ from the pinned receiver. This record remains
Monitoring; no rollout or production fix is claimed.

- Signals to monitor: Existing `parse_rejected`, ingest-queue refusal and peer
  queue/size diagnostics. Diagnostic overload can lose records; an absent log
  entry is not proof of admission. Compare receiver storage separately from
  emitted bytes and local corrected output.
- Rollback triggers: Valid original content changes beyond the permitted
  hop/framing/conversion, malformed primary admission, ownership races,
  exceeded existing resource bounds or lost relay-gate behavior.

## References

- Issue(s)/PR(s): none; local peer-forwarding review.
- Original-relay baseline: `dd89af2d47e0abfadf80c709fbdc24d48b7f1478`.
- Date-correction baseline: `668ec513292f6dbf363063d6212c99ad6a7c24b1`.
- Related ADR(s): [ADR-0054](../decisions/ADR-0054-peering-control-priority-and-local-acceptance-relay.md),
  [ADR-0239](../decisions/ADR-0239-peer-original-validation-and-relay.md),
  [ADR-0240](../decisions/ADR-0240-peer-dxspider-date-admission.md),
  [ADR-0241](../decisions/ADR-0241-peer-frame-payload-and-comment-framing.md).
- Related docs: [Peer behavior](../../peer/README.md#original-spot-admission-and-relay),
  [Domain contract](../domain-contract.md#relay-under-overload).
- Reference sender and receiver: DXSpider revision
  `3e9b3621d94dd45c68702e4a0f896aac33f2a91d`. Its acceptance envelope is narrower
  than the selected original GoCluster callsign/IP policies. Its standard day
  formatter and comment-tilde behavior are material incoming compatibility
  evidence, separate from receiver storage.

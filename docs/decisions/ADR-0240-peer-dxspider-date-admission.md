# ADR-0240: Peer DXSpider Date Admission

- Status: Accepted
- Date: 2026-10-04
- Decision Origin: Troubleshooting chat

## Context

ADR-0239 separates shared original-field admission from local processing and
owned original relay. Its approved date grammar required a two-digit day. At
`668ec513292f6dbf363063d6212c99ad6a7c24b1`, that guard rejects the ordinary
DXSpider spelling on days 1-9 of every month, before both local and transit
processing, including receive-only operation.

Pinned DXSpider revision `3e9b3621d94dd45c68702e4a0f896aac33f2a91d` formats
`cldate` with `%2d`, producing ` 4-Oct-2026`; its PC11, PC61 and PC26 generators
retain that spelling. Earlier GoCluster-to-DXSpider receiver fixtures generated
zero-padded dates and did not exercise this direction. Source review and narrow
formatter/generator execution established the defect; they did not establish
the corrected GoCluster behavior. [TSR-0039](../troubleshooting/TSR-0039-peer-normalized-relay-and-telnet-iac.md)
records the evidence and verification status.

Relaxing the guard alone is insufficient. The shared tolerant local parser
trims date input, requires a fixed two-digit day, and can fall back to now.
Original admission must supply the validated instant before identity and age
checks, while retaining the original field for relay.

## Decision

- Accept exactly 11-byte dates with either the existing two-digit decimal day
  or one ASCII space followed by a digit 1-9. Both `04-Oct-2026` and
  ` 4-Oct-2026` identify the same UTC instant. Reject unpadded days, tabs,
  arbitrary or trailing padding, and invalid calendar dates.
- Retain the existing English month-case tolerance, four-decimal-digit year
  domain and strict `HHMMZ` clock rules. No fallback or broad trimming is added
  to original-field validation.
- After local parsing succeeds, assign the validated UTC instant to
  `Spot.Time` before computing `dxKey` or checking ingest age. Clone and retain
  the original date spelling for onward serialization. The shared tolerant
  parser and its other callers remain unchanged.
- Verify sender-generated PC11, PC61 and PC26 for days 1, 9, 10 and 31 using
  the unmodified pinned generators with a controlled test clock. Check exact
  local timestamps, invalid-calendar and padding controls, dedupe identity,
  stale-date refusal and literal relay fields through native transport.
- Treat the pinned PC26 generator's no-hop output as local admission without
  transit. Verify hop-bearing PC26 relay separately, preserving its existing
  modern-destination and other gates.
- Keep all other v3 policies, dedupe identities, resource reservations,
  limits, ownership and relay gates unchanged. Recheck date-shaped ownership
  and actual allocations inside those existing bounds.
- Track literal comment `~` support as a required, pending interoperability
  correction in a separate bounded framing change. It must cover terminal
  markers, fragmented reads, consecutive frames and overflow recovery as
  well as literal comment tildes. Reader splitting and recovery are outside
  this date correction. Full DXSpider spot compatibility remains qualified.

## Alternatives considered

1. Keep only zero-padded days. Rejected because ordinary pinned DXSpider
   sender output would be rejected on days 1-9.
2. Trim arbitrary date whitespace or accept any parser-recognized spelling.
   Rejected because it weakens the agreed strict original-field contract
   beyond the two established spellings.
3. Relax only the validator. Rejected because tolerant local parsing could
   assign now, changing age eligibility and dedupe identity.
4. Change the shared tolerant parser globally. Unnecessary for this bounded
   peer correction; assigning the validated instant at the admission boundary
   fixes the affected owner without changing unrelated callers.
5. Fold tilde support into this correction. Deferred to the required separate
   framing change because it affects shared reader splitting and recovery.

## Consequences

### Benefits

- Ordinary DXSpider sender dates enter the same original-validation boundary.
- Local dedupe and age use the validated original instant, while relay retains
  the admitted date bytes.
- Reference-generated input exposes a direction that outbound receiver tests
  alone could not prove.

### Risks

- Strict original admission still rejects other date spellings previously
  repaired through fallback. The two accepted spellings do not authorize
  general padding or normalization.
- Literal comment tildes still fail incoming framing. Broader GoCluster calls
  and selected IP spellings may also be rejected by the pinned receiver;
  fidelity, local admission and downstream acceptance remain separate claims.
- No deployment, production monitoring, complete daemon parity or runtime
  performance claim follows from local component verification.

### Operational impact

No configuration or limit change is introduced. A missing early-month peer
spot can result from the date-grammar defect at the earlier branch baseline;
inspect its original date bytes and version before diagnosing dedupe. No-hop
PC26 can ingest locally without relay. A tilde-containing DXSpider comment
remains a known incoming-framing limitation with no configuration workaround.
Support-agent documentation impact: Required; route operators to the current
peer contract and TSR rather than duplicating acceptance rules.

## Links

- Related issues/PRs/commits: Approved Scope Ledger v4; correction baseline
  `668ec513292f6dbf363063d6212c99ad6a7c24b1`.
- Related tests: original-date contract, handler timestamp/peer-key/age,
  primary-admission integration, pinned sender/native input, literal production
  writer and existing receiver storage, ownership/allocation and protocol fuzzing.
- Related docs: [Peer behavior](../../peer/README.md#original-spot-admission-and-relay),
  [Domain contract](../domain-contract.md#relay-under-overload),
  [Reference scripts](../../scripts/README.md#pinned-dxspider-spot-interoperability).
- Related TSRs: [TSR-0039](../troubleshooting/TSR-0039-peer-normalized-relay-and-telnet-iac.md).
- Supersedes / superseded by: Refines only original date grammar and validated
  local timestamp assignment under
  [ADR-0239](ADR-0239-peer-original-validation-and-relay.md); its other contracts
  and accepted history remain effective.

## Payload and comment-framing refinement

[ADR-0241](ADR-0241-peer-frame-payload-and-comment-framing.md) refines payload
encoding and supplies the separately approved PC11/PC61/PC26 comment-tilde
correction across reader, parser and validation. This accepted history remains
unchanged; current behavior and compatibility limits are in that refinement.

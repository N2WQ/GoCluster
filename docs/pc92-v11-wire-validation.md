# PC18/PC92 v11 wire and mailbox correction evidence

Date: 2026-10-01. Authority: approved v11, incorporating v10 R01/R02/R08.
This is a slice record, not final-source qualification or an overall compliance
verdict. The lead owns integration, remaining lifecycle/scheduling/graph work,
final verification, and the separate overall acceptance result.

## Implementation and boundaries

- `config/peering_contract.go` applies the pinned receiver's `normalise_call`
  followed by callsign validity. The result is slash-free and stable; a short
  base such as `W1AW/P` can be unrepresentable because the receiver's optional
  prefix is greedy. Do not substitute generic slash removal. Local publication
  remains limited to 15 canonical bytes and keeps collisions local-only.
- Active `NewManager` construction validates before opening storage. A missing
  configured local identity can use the explicit constructor argument; a
  nonempty mismatch fails. The normalized active copy contains at most 64
  enabled peers and does not mutate the caller's configuration. Disabled peer
  records are not copied into protocol-owned backing. Numeric metadata,
  canonical peer/login identities, local flags 4/5, and the 300-second positive
  backoff maximum are enforced. Loader zero/empty/dormant sentinels remain.
- Inbound presented-login, callsign ACL, IP ACL, and password selection stay
  literal. Canonicalizing publication/configuration does not grant admission
  to arbitrary aliases.
- `peer/protocol.go` consumes the identified transport hop once. Numeric stacks
  collapse only outside the payload grammar. K's open-ended extensions and
  PC93's optional slots retain hop-like text and empty positions. Malformed
  terminal hops cannot fall back to older numeric tokens. Whole malformed
  membership records are still rejected before authority changes.
- Parsed authority `Frame.Fields` remain payload during encode, input mailbox,
  and candidate staging. `peerprobe` no longer invokes the legacy generic
  suffix helper on parsed fields. PC92 keys hash every validated payload
  position, including empties and hop-like content, while excluding `Frame.Hop`.
- Implicit self-subject C, including empty C, preserves learned root metadata
  and replaces the complete membership. Received external-node flags 6/7 remain
  supported although local root flags 6/7 fail validation.
- Named PC93 groups such as `LOGGER` again reach the existing local announcement
  callback. Callsign-like destinations failing canonical representation cannot
  fall through to broadcast. The legacy spot callsign classifier is reproduced
  without populating spot's normalization cache from peer labels; it grants no
  recipient authority. Actual private delivery still requires a unique current
  owner and current session/revision validation.
- `eligiblePC92Record` shares non-authoritative exclusions with the normal,
  staging, and full-mailbox paths. Current registered ownership remains a
  separate manager/controller check. An obsolete session cannot gate its
  replacement.
- `ProtocolStats.PC93InputRefused` counts input mailbox count/byte refusals under
  `queueMu`; it is distinct from dedupe `PC93Refused` and survives drain. Sampling
  emits the fixed rate-limited reason `PC93 input admission refused` only when
  the sampled cumulative counter advances. No payload log or replay backlog is
  introduced. The literal reason inventory is 19, with 32 fixed buckets.

The receiver adapter now executes unmodified DXUtil normalization/validation
before constructing the test DXProt channel, matching the daemon's ordinary
login boundary. It also exposes `route_nodes` and `route_users` separately so
same-callsign node/user relationships are observable without `Route::get`
shadowing. Existing snapshots remain available. This is receiver-component
evidence, not a deployed DXSpider or CCCluster claim.

## Post-approval contract-to-test disposition

The review had the approved design context. It is not represented as an
independent normative review. Material checker refinements were accepted before
implementation: correct invalid constructor fixtures; replace the destructive
framing fuzz invariant; use actual receiver normalization; reject private
normalization failures instead of broadcasting them; coordinate shared files.

| Failure mechanism / stimulus | Required observable | Evidence and checker | Principal false-green excluded |
| --- | --- | --- | --- |
| Portable/SSID input and greedy short-base prefix | Literal receiver identity or rejection; stable canonical result | Config unit `TestCanonicalPeeringCallDXGrammar`; actual receiver `TestDXSpiderReferenceCanonicalIdentity` | Go-only expected normalization |
| Unique -> ambiguous -> unique aliases; IP and reserved-node change | Publication excludes collisions and recovers current IP; local admission unchanged | Consumer `TestPC92PublicationCanonicalAliasLifecycle`; existing private/telnet owner tests | Testing only a normalizer |
| Invalid direct construction, including disabled-bit bypass | Error before database creation; caller slice unchanged; <=64 active backing | `TestNewManagerWireContractBeforeStorage`, `TestActivePeeringContractOwnsNormalization`, loader tests | Loader-only validation; broad test defaults hiding invalid fields |
| Configured alias versus presented login/ACL spelling | Unauthorized alias rejected before PC18; existing IP/password checks retained | Actual session `TestInboundHandshakeCanonicalIdentityDoesNotWidenAuthentication`, configured handshake/listener tests | Canonicalization accidentally broadens authentication |
| Raw configured alias in actual Go startup | Canonical wire login and receiver channel identity; truthful PC18 | `TestDXSpiderReferenceGoSessionStartup` in both directions/modes | Direct channel construction bypasses daemon normalization |
| K H123/revision, H98 extension; PC93 H-like text/onode and empties | Exact positions survive parsing, encoding, queue, staging | `TestFrameAuthorityPayloadPositions`, `TestPC92QueuedAndStagedPayloadPreserved` | Parser passes while later encoder strips payload |
| Malformed C member H9x and malformed terminal hop | Previous topology/freshness/cache/relay unchanged | `TestPC92MalformedHopPayloadNoAuthority`, decoder vectors | Silently discarded bad member creates partial C |
| Numeric stack versus ambiguous optional payload | Single transport hop only where grammar distinguishes it; preserve payload otherwise | Literal parser vectors; `FuzzParseFrameHopSuffix` | Blanket ban on final H-like payload rewards data loss |
| Implicit and empty C after explicit metadata | Complete replacement without clearing missing root metadata | `TestPC92ImplicitCMetadataAndEmptySnapshot`; actual receiver implicit empty C | Parse success without state assertions |
| Different payload empty positions or H-like extension; hop-only change | Different payload keys; equal hop-only keys; bounded key length | `TestPC92KeyPreservesCompletePayload`, valid existing key vectors | Key independently strips fields after parser repair |
| Noncanonical upstream sender through transit | Wire fields preserved except transport hop; internal identity canonical | `TestPC92TransitPreservesPayloadIdentity` | Encoder silently rewrites third-party payload |
| Other protocol families and probe ping | Original payload and valid response preserved | `TestFrameSharedProtocolRegression`, `TestPeerProbeUsesParsedPayload`, existing parser tests | Shared helper regression outside PC92 |
| LOGGER, duplicate, unrepresentable private destination | Exactly one local announcement; zero private-to-broadcast leakage | `TestPC93NamedGroupLocalDelivery`, `TestPC93UnrepresentablePrivateTargetNeverBroadcasts` | Parse-only test omits delivery callback |
| Empty/full PC92 mailbox with own origin, H0, unsupported/malformed; obsolete owner | Exclusions never gate or mutate authority; replacement remains current | `TestPC92MailboxEligibilityMatchesNormalPath`, `TestPC92MailboxStaleOwnerCannotGateReplacement` | Direct controller test misses full-mailbox fallback |
| PC93 count/byte saturation, drain and concurrent producers | Exact persistent mailbox counter, separate cache counter, PC92 still served, no repeated old diagnostic | `TestPC93MailboxRefusalsPersistAndRemainIsolated`, `TestPC93MailboxConcurrentRefusalStats`; race | Occupancy/log-only evidence loses refusals after drain |
| Changed fixed reason inventory | Every literal reason fits preallocated backing and accounting | `TestPC92BookkeepingDiagnosticReasonsFitFixedBacking`, `TestPC92BookkeepingFixedAllocationEnvelope` | New dynamic labels silently escape the bound |

## Observed development checks

All commands below use Go's actual tools, not mocked success. They are slice
development checks; the integrated final lane and required qualification remain
the lead's separate obligations.

Passed targeted config, framing, keys, codec, publication identity, PC93,
constructor, queue/staging, and peerprobe checks:

```text
go test ./config ./peer ./cmd/peerprobe -run 'Test(CanonicalPeeringCall|PeeringWire|ActivePeering|NewManagerWire|FrameAuthority|FrameShared|ParseFrame|Encode|PayloadFields|PC92Key|PC92Implicit|PC92Malformed|PC92Transit|PC92Queued|PC92PublicationCanonical|PC93|PeerProbe)' -count=1 -timeout=60s
go test ./peer -run 'Test(ManagerStartDials|AuthorizeInbound|PeerRegistry|InboundHandshakeConfigured|InboundListener|InboundHandshakeCanonical|PC93Named)' -count=1 -timeout=40s
go test ./peer -run 'Test(PC92Mailbox|PC93Mailbox|PC92Bookkeeping)' -count=1 -timeout=60s
go test -race ./peer ./telnet -run 'Test(PC93|PC92PublicationCanonical|PC92QueuedAndStaged|InboundHandshakeCanonical|PeerMembershipConcurrent|CurrentDirectMessage)' -count=1 -timeout=90s
go test -race ./peer -run 'Test(PC92Mailbox|PC93Mailbox)' -count=1 -timeout=60s
```

The actual pinned receiver checks passed (10.450 seconds test runtime):

```text
go test ./peer -run '^TestDXSpiderReference(CanonicalIdentity|PC18IdentityAndK|GoSessionStartup)$' -count=1 -v -timeout=90s
```

`DXSPIDER_ROOT` was the clean local checkout at
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`. The adapter used the existing portable
Perl runtime with its prerequisite library/DLL directories; no installation or
receiver-source changes were made. Go's WinLibs compiler was pinned before
adding Perl's DLL directory to PATH, so Perl's bundled gcc could not replace the
selected Go compiler.

Fuzzing:

- `FuzzDecodePC92Atomic`, 30 seconds with two workers: PASS, 183,173 executions.
- `FuzzParseFrameHopSuffix` initially exposed a short invalid PC92 header whose
  re-encoding moved the apparent hop boundary. The authority splitter now rejects
  payloads below the action's minimum before splitting. The regression input is
  retained at `peer/testdata/fuzz/FuzzParseFrameHopSuffix/cd11fdcdcac4845e`.
- After that correction, the targeted regression passed and framing fuzz ran
  30 seconds with two workers: PASS, 171,154 executions.
- The decoder fuzz was then repeated on the corrected parser: PASS, 186,329
  executions in 30 seconds with two workers. Mailbox race checks also passed
  after the persistent-counter implementation (1.253 seconds test runtime).

Benchmarks with correctness guards measured the following on windows/amd64,
Intel Core i9-10900, while other development builds could be active:

| Case | Time/op | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Canonical K1ABC | 450.9 ns | 0 | 0 |
| Canonical K1ABC-1 | 472.2 ns | 0 | 0 |
| Portable EA8/K1ABC/P-01 | 1,145 ns | 120 | 3 |
| K frame preserving H123/branch | 311.7 ns | 192 | 2 |

These measurements establish observed allocation behavior, not a speedup,
workload latency result, or full allocation proof. The copied active peer backing
and canonical string overlap require inclusion in the remaining metadata
inventory. The new normalizer's regexp/capture work remains bounded by the
36-byte raw identity limit; canonical received identities use the zero-allocation
common path. The input refusal counter and sampled statistics each add one
`uint64`; the diagnostic index adds one bounded entry without growing buckets.

An initial receiver build failed because C: had approximately 30 MiB free. It
did not execute any receiver tests. Subsequent builds used process-local
`GOCACHE`, `GOTMPDIR`, `TEMP`, and `TMP` under
`D:/codex-gocluster-v11-20261001`; no global Go settings or runtime cluster
configuration changed. Transient invalid fixture expectations were corrected to
explicit valid numeric metadata rather than weakening production validation.

## Remaining integration obligations

- Integration includes `PC93InputRefused` in topology, runtime, load and Q5
  no-refusal qualification guards; cache-only statistics cannot prove the
  absence of mailbox loss.
- Root/controller and graph integrations own phase authority, scheduler timing,
  typed topology, and original-age ingress behavior. The isolated staging test
  proves retained wire preservation, not establishment deadline correctness.
- Final source tests, full lint/race lane, receiver suite, allocation proof, and
  long qualification results must be reported separately. Enabled SQLite and
  other previously open ownership proofs are not closed by these wire fixes.

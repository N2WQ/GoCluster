# PC18/PC92 approved v6 execution record

The user authorized v6 on 2026-10-01. Implementation is on branch `p92`, from
baseline `0143c9ae9cb8f7709635c3812c6f4e0e574397b5`. V6 inherits D1-D8 and
S01-S14 from v4, the isolation/staging/recovery/qualification provisions of v5,
and the reduced spot-forwarding envelope of v6.

**Status: implementation in progress; acceptance remains open.** This is an
execution record, not a new scope proposal or a release approval. The user
resolved the intentional-hold and simultaneous-staging qualification issues in
[approved v7](pc18-pc92-scope-ledger-v7.md). Its focused amendments control those
qualification details; this record retains the inherited protocol scope.

## Inherited approved protocol scope
The objective is to implement and verify the agreed PC18 and PC92 **A/C/D/K compatibility profile** against DXSpider, including publication, received topology, ordering, recovery, lifecycle, and bounded resources.

The reviewed GoCluster baseline remains `0143c9ae9cb8f7709635c3812c6f4e0e574397b5`. The reference is the pinned [DXSpider revision `3e9b3621`](https://github.com/EA3CV/dxspider/tree/3e9b3621d94dd45c68702e4a0f896aac33f2a91d). Compatibility claims will identify this profile and revision.

The policy selections incorporated into this ledger are:

| Decision | Agreed behavior |
|---|---|
| **D1 — IP publication** | Broadcast available IP addresses in locally originated PC92 records, including address updates. Per-user controls and preference storage remain outside this scope. |
| **D2 — Node identity** | Use one canonical local node identity, aligned with peer login identities. Require a known, validated remote identity before advertising that peer. |
| **D3 — User identity** | Preserve local admission behavior. Publish only valid, unambiguous canonical identities. Resolve incoming private PC93 messages to the unique current session represented by that identity. Conflicting or unrepresentable identities remain local-only with diagnostics. |
| **D4 — Protocol coverage** | Implement A/C/D/K, including received external subjects. Drop F/R and unknown actions without topology or freshness changes. Preserve legacy negotiation without adding full legacy bridging. Preserve documented CCCluster startup and direct local publication; exclude CCCluster from transit PC92 broadcasts. |
| **D5 — Convergence and failure** | Permit bounded coalescing. Make overload, incomplete remote state, and recovery observable. Close affected sessions on authoritative control admission failures. Use bounded best-effort shutdown withdrawal. |
| **D6 — Freshness and dedupe** | Share PC92/PC93 origin freshness and ordering. Preserve separate peer payload and telnet bulletin dedupe. At payload-cache saturation, retain unexpired keys and refuse new untrackable work until capacity returns. Local spot ingestion continues. |
| **D7 — Numeric metadata** | Retain safe configured compatibility values, including shipped `5457` and `633`. Allow an explicitly empty `node_build` as omission. Do not describe these values as GoCluster’s product build number. |
| **D8 — Product identity** | Render actual startup build identity safely and truthfully. If it cannot be represented within the approved limits, fail whole-service startup when peering initialization is enabled. |
| **Publication overflow** | Pause PC9x peering while keeping local users connected. Resume only when the supported capacity envelope fits; never publish a truncated or arbitrary partial C snapshot. |

The selected design is a **manager-owned protocol controller**. It will own live topology, freshness, publication revisions, timestamp allocation, and publication ordering. Sessions will own transport and an ordered PC92 output stream. Telnet will own actual user membership and private-message delivery. SQLite will remain an optional asynchronous diagnostic projection.

A separate read-only comparison considered a mutex-protected reducer with a publication outbox. That approach remains technically viable, but distributes ordering and recovery obligations across more components. The controller design provides clearer ownership for the reviewed requirements. Socket writes, database work, and ordinary spot ingestion will remain outside its state-processing sequence.

The implementation scope is:

1. **S01 — Typed protocol parsing and reliable framing**

   Implement action-specific PC92 decoding and validation: origin, timestamp, subject, members, flags, metadata, counts, and extensions. Support omitted self-subject where permitted, short IP forms, comma-encoded IPv6, and meaningful empty fields.

   Correct terminal-hop handling without consuming PC93 text or metadata that merely resembles a hop field. Enforce limits consistently across accepted header forms and fragmented reads. Preserve native telnet parser state across reads and retain supported alternative transport behavior.

   **Acceptance:** malformed records cannot partially mutate topology, advance freshness, or relay. A malformed member invalidates an authoritative C operation.

2. **S02 — Origin-wide timestamps and transmission order**

   Replace per-session production sequencing with one local-origin sequence. Allocate timestamps and enqueue their publication effects in the same order. Initialization, membership changes, C snapshots, and K records must use that ordering.

   Handle sequence exhaustion, UTC rollover, clock stalls, reconnects, and ordinary process restart. Never manufacture an out-of-window future timestamp to maintain apparent progress.

   **Acceptance:** actual socket captures and DXSpider reception demonstrate ordering, including bursts above 99 records per second and a receiver retaining pre-restart state.

3. **S03 — Local users, canonical identities, IPs, and private delivery**

   Obtain bounded, versioned snapshots of admitted current sessions. Preserve replacement ownership: obsolete cleanup cannot withdraw a replacement session.

   Apply one canonical identity policy consistently to A/C/D, counts, metadata ownership, and incoming private PC93 delivery. Cover unique-to-ambiguous and ambiguous-to-unique transitions, SSID aliases, portable forms, and collisions with reserved node identities.

   Publish available IPs and address changes. Preserve dashboard callbacks and exact local session counts separately from published-user counts.

   **Acceptance:** no arbitrary private-message recipient, private-message fanout, stale withdrawal, or ambiguous IP ownership.

4. **S04 — Direct peers and establishment authority**

   Generate the correct initial A relationship between the local origin and remote child. Publish established direct attachments and withdrawals under explicit session ownership.

   Preserve first-established-wins behavior. A losing or failed handshake must not alter global topology, consume global freshness, or relay staged records. Valid startup PC92 received before PC22 must be handled exactly once after the appropriate authority boundary.

   **Acceptance:** simultaneous inbound/outbound attempts and delayed cleanup cannot replace or remove the winning session’s authority.

5. **S05 — Complete C snapshots, accurate K records, and schedules**

   Publish complete direct membership, including eligible users, direct nodes, and available metadata. Support a valid empty membership snapshot. K counts must reflect the advertised population, permit zero, and exclude the root from its child-node count.

   Run periodic C and K schedules independently. Preserve their existing zero-disable semantics. Provide a one-off complete local recovery baseline even when periodic C is disabled.

   Recovery must include metadata reassertion through A where necessary: DXSpider’s treatment of existing members means C alone cannot prove an IP update was repaired.

   **Acceptance:** a new peer’s snapshot cannot clear publication obligations still owed to existing peers; older queued deltas cannot undo a newer snapshot.

6. **S06 — Authoritative received topology**

   Maintain live topology independently of SQLite, with distinct origin, subject, and ingress ownership. Support multiple parent relationships and usable alternate ingress paths.

   Implement A as membership/metadata addition or update, D as removal of the specified relationship, C as complete subject membership replacement, and K as liveness/metadata—not membership entries manufactured from counts.

   Protect the local root and direct-session authority. Missing optional IP metadata must not silently erase a known address.

   **Acceptance:** removing one relationship or ingress does not erase a still-reachable node or user. Incomplete knowledge is explicitly distinguishable from a complete configuration.

7. **S07 — Validation, freshness, dedupe, and relay**

   Validate supported records before committing freshness, duplicate suppression, topology, or forwarding effects. Reject returning local-origin records.

   Drop H0; process a fresh H1 record locally without forwarding it. Repeated terminal-hop traffic must not extend liveness. Preserve useful alternate-ingress evidence without reapplying the same payload or refreshing its age.

   Apply shared PC92/PC93 ordering while retaining the existing separation between peer payload dedupe and telnet bulletin dedupe. Direct talk remains outside the telnet bulletin cache.

   **Acceptance:** failed admission cannot masquerade as successful receipt; an identical valid retry remains admissible when capacity returns. F/R and unknown actions consume no freshness.

8. **S08 — Bounded delivery and explicit recovery**

   Give authoritative PC92 traffic ordered, bounded delivery with observable saturation and session closure. A slow peer must not block healthy peers or ordinary spot ingestion.

   Repair local publication with complete baselines and metadata updates. Treat missed remote-origin withdrawals as incomplete remote knowledge until authoritative refresh or expiry resolves them. Reconnect or A/K reception alone does not establish completeness.

   **Acceptance:** queue saturation, lost D records, and missed IP updates are checked against resulting receiver state, including when periodic C is disabled.

9. **S09 — Startup, shutdown, and resource ownership**

   Prepare identity, callbacks, membership providers, queues, and workers before peer traffic can use them.

   Track pending handshakes as well as established sessions. Close sockets on terminal exits; cancel dialing and retry waits; fix backoff reset behavior; join writers, workers, timers, and transport owners before closing storage.

   Quiesce new publication during shutdown and make bounded withdrawal attempts while transport is still available.

   **Acceptance:** startup failures and Stop leave no late publication, retry spin, orphan socket, or surviving owned worker.

10. **S10 — Optional persistence and atomic projection**

    Keep live acceptance and routing behavior equivalent with the topology database enabled or disabled. Database delays and runtime write failures must not block socket readers or invalidate already accepted live state.

    Persist complete subject replacement atomically. Preserve compatible diagnostic columns and document the projection’s meaning. Stored rows must not become fresh reachability or freshness authority after restart.

    Preserve existing database-construction failure behavior. Any necessary schema adjustment must be nondestructive and reversible; unexpected destructive migration requirements require revised approval.

    **Acceptance:** fault injection demonstrates atomic projection, observable persistence failures, and unchanged live behavior.

11. **S11 — Hard bounds, expiry, and overload**

    Enforce the resource envelope below across primary state, reverse indexes, queued representations, snapshots, and cleanup bookkeeping. Queue accounting must include retained parsed structures, not just raw frame bytes.

    Keep live expiry independent of database retention and local K transmission settings. Do not evict unexpired freshness protection simply to admit a new origin.

    **Acceptance:** exact-limit, limit-plus-one, churn, disconnect, and expiry tests demonstrate bounded primary and secondary state.

12. **S12 — Configuration, documentation, and shared contracts**

    Validate identity relationships, numeric fields, and resource limits. Implement explicit empty-build omission through the production loader. Preserve existing defaults and sentinels except for the changes expressly listed here.

    Check exported timestamp-generator consumers in `cmd/peerpoc` and `cmd/peerprobe`, retaining package dependency direction.

    Update protocol/operator documentation, relevant ADRs, diagnostics guidance, and support routing. **Support-agent documentation impact: Required**, including the relevant `customgpt/` peer support material.

13. **S13 — Honest PC18 identity**

    Pass authoritative startup build information into peer initialization before the first banner. Separate readable product identity from numeric protocol compatibility fields.

    Safely represent delimiters, control characters, reserved product-name patterns, and metadata that could trigger false PC91 recognition—including hashes beginning with `91`.

    **Acceptance:** actual reference parsing recognizes the intended capabilities without an invented DXSpider identity, fabricated build counter, or extra frame.

14. **S14 — Handshakes and end-to-end compatibility evidence**

    Specify and test exact inbound/outbound transcripts for the selected DXSpider, legacy, and CCCluster paths.

    Cover capability preference, absent capability, family mismatch, authentication, ACLs, malformed startup records, valid pre-PC22 PC92, repeated PC18, and simultaneous attempts. Correct accidental establishment through spot traffic and unintended extra PC20 behavior.

    Preserve outbound asymmetry: this work does not add an outbound PC18 solely for branding.

    **Acceptance:** observe actual DXSpider channel, user, route, and membership state. CCCluster compatibility claims require separate CCCluster evidence.


## Controlling v5/v6 amendments

The concrete workload and resource table are recorded in
[pc92-qualification.md](pc92-qualification.md). The following replace the
corresponding v4 provisions:

- Spot forwarding: 10,000 distinct new PC11/PC61/PC26 keys per minute across
  peers, plus 20,000 excess burst keys; at most 1,000 new keys per second.
  Qualification retains 100,000 duplicate arrivals per minute. PC92 remains
  100 new records per second plus 4,000 excess records.
- Independent dedupe pools: spot 131,072 entries/64 MiB key bytes/96 MiB complete
  allocation; PC92 and PC93 each 65,536 entries/8 MiB; bulletin 8,192/2 MiB.
  The last three together have a 32 MiB complete allocation budget.
- No eviction of unexpired keys. Duplicates never renew age; exactly 600 seconds
  is unexpired. Reclaim eligible entries before refusal; cleanup lag at most
  one second under the qualified workload.
- Protocol input: PC92 192 records/3 MiB, PC93 64 records/1 MiB in shared arrival
  order. Spot forwarding is outside this mailbox. At most 4,096 freshness
  entries belong exclusively to PC93, within 16,384 shared origin entries.
- Pending handshakes: 128 candidates, each 256 records/512 KiB, globally
  8,192 records/16 MiB. Authenticate before staging; no global authority until
  establishment wins; FIFO revalidation before live input; discard every losing
  or failed candidate. Traffic never extends the configured phase deadlines.
- Each peer has 128 queued control records/1 MiB and its configured normal queue
  count/1 MiB, plus one bounded active write. Control backlog age is five seconds;
  writes have a two-second deadline. Close only the affected failed transport.
- Complete-publication overflow and unsafe clock close/gate PC9x established and
  pending sessions. Local service and established legacy links continue. Reserve
  publication space for every configured peer that can reconnect; disconnecting
  peers cannot clear their own capacity gate.
- Clock failure closes/gates within five seconds. Retain issuance/freshness
  protection; recovery requires safe UTC beyond retained issuance and one second
  of stable health. Payload expiry continues to use elapsed time.
- PC92 authoritative admission failure closes the affected session, marks its
  knowledge incomplete, and gates retry until actual required headroom remains
  available for one second. Spot/message/bulletin dedupe refusal affects only
  that class and keeps sessions open. Refused payloads are not replay queued.
- Observe valid gate recovery within the next second; then configured reconnect
  backoff (maximum 300 seconds), handshake deadlines, and complete C followed by
  metadata A within five seconds of externally observed handshake completion.
  Recovery remains mandatory when periodic C and K are disabled.
- Owned affected memory totals at most 480 MiB: spot 96, graph/freshness 96,
  queues/readers/active work 160, staging 16, other caches 32,
  snapshot/projection 48, remaining metadata 32. Include backing allocations,
  secondary indexes, active generations and transient growth. This is not an RSS
  ceiling. Shipped process GOMEMLIMIT remains 1536 MiB.
- Maximum accepted configured peer line/PC92 limit is 64 KiB; smaller values
  remain effective. Higher enabled values fail config validation. The complete
  maximum C fixture is 62,171 bytes before its terminator.
- Preserve general admission, stabilization/correction behavior, and existing
  configuration defaults unless explicitly listed above. Reserved local
  publication identities are the local node and configured direct peers.

## Implementation and evidence map

Every row remains subject to full final-state validation; source presence is
not acceptance evidence by itself.

| Scope | Implementation | Targeted evidence / outstanding qualification |
| --- | --- | --- |
| S01 | `peer/protocol.go`, `reader.go`, `parse_budget.go`, `pc92_codec*.go` | Codec/reference/framing/budget tests and fuzzing; final sustained parser pressure remains open. |
| S02 | `peer/timestamp.go`, `pc92_controller.go`, `pc92_publication.go` | Actual DXSpider receiver accepted writer-captured rate coalescing and midnight rollover. Deterministic sender-controller recreation and two actual sender OS processes passed against retained receiver state; the process fixture uses production Manager startup/TCP, not the whole cluster executable. |
| S03 | `telnet/peer_membership.go`, `peer/pc92_publication.go`, `manager.go`, `internal/cluster/main_runtime.go` | Membership replacement/collision/private-delivery tests; full client population qualification remains open. |
| S04 | `peer/session.go`, `manager.go`, `pc92_controller.go` | Startup staging/ownership tests, stale-close and canceled-source regressions; v7 resolves the reachable Q4 staging profile, whose full qualification remains open. |
| S05 | `peer/pc92_publication.go`, `session.go` | C/A revision recovery, zero counts and independent disabled timers; actual DXSpider IP-repair evidence. |
| S06 | `peer/pc92_graph.go` | Atomic graph, multi-parent, metadata, protection and capacity tests; sustained maximum graph allocation remains open. |
| S07 | `peer/pc92_controller.go`, `dedupe.go`, `pc93.go` | Malformed/unsupported/local-origin/hop/shared-freshness tests; exact ingress and PC93 byte-cap regressions. |
| S08 | `peer/session_transport.go`, `pc92_resources.go`, `pc92_publication.go` | Control queue plus active-write bounds, failure gates and stable headroom; repeated whole-runtime fault qualification remains open. |
| S09 | `peer/manager.go`, `session.go`, `session_transport.go`, runtime startup/shutdown | Joined terminal cleanup, late-enqueue refusal and gate-before-dial tests; complete five-second shutdown qualification remains open. |
| S10 | `peer/topology.go`, `pc92_projection.go` | Atomic rollback, empty C and additive migration tests; a real held SQLite connection did not block live replacement, and Stop joined within five seconds in normal and race runs. This is component evidence, not full-runtime qualification. |
| S11 | `peer/pc92_resources.go`, graph/caches/transport/staging owners | Cache retained-heap measurements and exact bound regressions; aggregate 480 MiB qualification remains open. |
| S12 | `config/peering_contract.go`, loader, both peer CLI consumers | Targeted loader/identity/limit tests; operator, ADR and support-agent documentation completion remains open. |
| S13 | `peer/pc18_identity.go`, runtime build adapter | Identity/capability/startup tests and actual DXSpider parser state. |
| S14 | Session tests and `scripts/pc92-dxspider-interop.*` | Actual pinned DXSpider receiver-component startup and membership tests passed; this is not a full deployed DXSpider daemon or CCCluster parity claim. |

Full closeout requires the approved Go test/vet/staticcheck/lint/race lane,
targeted protocol fuzzing, Q1-Q6 (after any explicitly approved qualification
revision), direct final-diff review, documentation, and complete traceability.
No commit, push, deployment, release, or partial-protocol rollout is authorized.

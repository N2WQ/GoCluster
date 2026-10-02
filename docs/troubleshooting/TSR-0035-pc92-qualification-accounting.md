# TSR-0035 - PC92 Qualification Accounting

Status: Monitoring
Date Opened: 2026-10-01
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: peer, telnet, internal/cluster
Trigger Source: Chat request
Led To ADR(s): ADR-0230, ADR-0231, ADR-0232
Tags: PC92, qualification, allocation, latency

## RCA Summary
- What happened: Initial runtime smoke could not prove the proposed latency or delivery guarantees; logical byte counters also omitted retained allocation costs.
- Why: Shipped batching/stabilization intentionally delays output; DX callsign correlation changes under correction. String rounding, retained map capacity, active staging, queued projections and channel backing require separate physical accounting.
- What fixed it: Approved v7 separates latency and shipped-hold profiles, requires stable delivery tokens and reachable pressure phases, and retains the complete allocation budgets. Accounting fixes reserve owned backing and release terminal state.
- How we know: The shipped smoke observed approximately 75.2-second p99 on surviving original-call output. Targeted allocation, lifetime and race regressions passed. Full runtime qualification remains open; the smoke did not prove the 22 unobserved original calls were lost.
- Operator/support answer: Preserve production policy while diagnosing latency. Distinguish intentional holds, renamed output, missing delivery, logical bytes, reserved bytes, heap deltas and RSS. A short smoke or isolated heap sample does not certify the full resource contract.

## Triggering Request
- Request date: 2026-10-01
- Request summary: Implement the approved PC18/PC92 compatibility and bounded-resource contract and vet the qualification evidence.
- Request reference (chat/issue/link): Approved v6 and Approved v7; repository execution records linked below.

## Symptoms and Impact
- The original smoke used original DX calls as identifiers and had no successful-enqueue observer.
- Shipped 200 ms broadcast batching and repeated 15-second stabilization checks were incompatible with treating every input as a 5/25 ms latency sample.
- A 64-peer established population cannot also authenticate a distinct staging owner among those same 64 identities.
- Logical storage estimates could undercount allocator classes, maps after deletion, active batches and snapshot generations.

## Timeline
1. 2026-10-01 - Actual socket smoke demonstrated intentional holds and the limitations of original-call correlation.
2. 2026-10-01 - User selected isolated latency profiles and two reachable capacity phases, then approved v7.
3. 2026-10-01 - Detailed checker review and targeted allocation/lifecycle tests refined the harness and ownership accounting.

## Hypotheses and Tests
1. Missing original calls prove network loss.
   - Evidence/commands: Initial runtime smoke measured 1,648 original calls out of 1,670, with correction/stabilization active and no immutable token.
   - Outcome: Inconclusive; correction or suppression was not distinguished from loss.
2. CW/SSB selection avoids intentional delays.
   - Evidence/commands: Runtime counters and socket timings recorded repeated stabilizer holds and approximately 75.2-second surviving-output p99.
   - Outcome: Rejected; qualification needs an explicitly declared eligible profile.
3. String length and current map cardinality bound owned storage.
   - Evidence/commands: Allocation-class, graph churn, staging lifetime and projection overlap regressions in `peer/allocation_charge_test.go` and related resource tests.
   - Outcome: Rejected; complete allocation accounting includes backing capacity and active ownership.

## Findings
- Root cause: The initial measurement treated policy-delayed output and mutable
  callsigns as if they defined a stable low-latency delivery oracle. Logical
  length counters also omitted retained allocation ownership. These were
  measurement and accounting gaps, not evidence that22 spots were lost.
- Full freshness occupancy is reachable through normal C admission, PC93 watermark renewal and hourly node expiry during controlled pre-load clock setup.
- Authority UTC can be controlled for setup without shortening payload TTL, I/O deadlines or required real-time qualification duration.
- The PC92 key builder hashes member fields; maximum wire size is not the size retained in its payload key cache. Wire/parse and key-byte budgets need separate evidence.
- These findings require durable measurement and ownership decisions, documented by ADR-0230, but do not establish completion of Q1-Q6.

## V9 persistence evidence (2026-10-01)

The isolated v9 experiment rejected the pinned ncruces/go-sqlite3 candidate
under the approved repair boundary. Production modernc SQLite is unchanged.
The [evidence report](../pc92-persistence-feasibility-v9.md) preserves versions,
source/binary hashes, commands, controls and the full interpretation limits.

- Eight failed opens left 64 MiB of simultaneous native virtual reservations,
  including 2.5 MiB committed. Successful open/close freed its complete extent.
  The observer held scalar extent metadata only; no wrapper references or
  process-exit cleanup were used to explain retention.
- Engine allocation exhaustion produced the exact typed OOM panic. Explicit
  test cleanup released that engine; this is not production panic containment.
- The existing Windows fallback grew an 8 MiB logical engine to 9,969,664 bytes
  of backing, with an old/new overlap lower bound of 17,940,480 bytes. This is
  a backend growth sequence, not a measured SQL projection peak.
- Selecting that existing fallback also failed the ordinary WAL path with
  IOERR_SHMMAP. Modernc and candidate-native WAL controls passed. The fault
  simulated unavailable optional APIs; no older Windows execution is claimed.
- Correcting allocator/VFS behavior or narrowing supported platforms exceeded
  v9. The conditional compatibility matrix, 1,000-cycle and 30-minute tests,
  aggregate host proof and Linux execution were therefore not run.

Durable lesson: logical engine limits do not prove backing or overlapping
ownership, and successful Close does not prove failed-initialization cleanup.
Keep experiment rejection distinct from production-driver behavior and from
the still-open 480 MiB aggregate proof. The fresh checker review used inherited
context and was not an independent non-steered review.

## V11 audit and checker corrections (2026-10-01)

The audit of `0d8a728` found protocol identity, positional framing, typed
membership, alternate ingress, deadline/scheduling and diagnostic gaps. Its
qualification findings showed why passing narrow tests did not establish the
agreed clock or final-source contract. Approved v11 preserves the earlier
negative evidence and separates correction completion from overall acceptance.

- A minimum-five-second clock assertion was the wrong oracle: five seconds is
  a maximum from independently applied fault, including scheduling and closure.
  The tagged seam now changes clock state outside the actor mailbox. A fresh
  configured identity proves a global gate; a still-retiring duplicate does not.
- Hashing only before a run, or trusting a Go report before wrapper exit, allowed
  false success after source drift or failure. Wrappers now run a retained
  once-built executable, compare before/build/after manifests and binary hash,
  and publish one initially-unqualified final result. Go reports are provisional.
- Receiver node/user relationships with equal callsigns coexist. C metadata
  selection depends on new/repeated typed relationships; member node metadata
  differs from explicit subject metadata. Parser round trips could not reveal
  these receiver state rules; the harness now queries Node and User separately.
- A controller that drains a whole staged batch or services maintenance as one
  indivisible bundle can starve deadlines. Commit, replay readiness and local
  recovery are separate milestones. Schedule C/A at commit so a replaying winner
  never receives ordinary deltas before its recovery pair.
- Full-count queues do not prove the intended byte-pressure population. In the
  v11 Q4B diagnostic, small cache-refill records overtook large pressure frames
  across different sockets. The measured 929,952 bytes exactly matched that
  mixture. The fixture must observe admission of the large frames before
  starting refill, while retaining its original deadlines and final simultaneous
  population checks. Treat the captured failure as invalid pressure evidence,
  not proof of a production quota violation or permission to lower the target.

Targeted evidence is retained in the v11 wire, graph and qualification reports.
The [v11 implementation record](../pc92-v11-closeout.md) retains current full-lane,
Q5/Q6, negative latency and Q4 sequencing results.
Full final-source workload and complete480MiB proof remain separate obligations;
passing wrapper fixtures certifies their failure handling, not protocol capacity.

## V12 re-audit findings (2026-10-01)

The re-audit of2c06079 showed that a normalization helper could repair malformed
received identities before validation, and that generic empty-field metadata
merging preserved stale K version/build values. A raw wire oracle and literal
SQLite TEXT assertions distinguish these defects from parser roundtrip success
or SQL numeric conversion. Local/login normalization and authentication remain
separate boundaries.

Admission recovery also needs the history of the actual required resources.
Sampling once per tick misses a brief loss/restoration; checking mailbox fit
before taking the final gate lock can clear using an obsolete interval. A new
pending failure needs its own generation. Duplicate alternate-ingress refusal
requires observation capacity, not full reapplication of a refused C.

The prior Q6 combined five-second wait could accept a four-second recovery.
Q5 began checking after the last key expired and used lookups that themselves
pruned the cache. Those results do not prove first-availability recovery timing.
V12 requires original stored age, actual capacity/gate facts, absolute deadline
fences through the successful predicate and an external observation. It never
uses the implementation's healthy-since as its expected result.

The CTY asset bundled in2c06079 is explicitly retained as a separate documented
refresh. [Provenance and exact hashes](../cty-refresh-2c06079.md) distinguish
earlier-asset runs from future qualification. No actor attribution or geographic
correctness follows from a matching hash.

See the [v12 validation record](../pc92-v12-validation.md) for current execution
status. Its early sustained necessary condition failed: with 63 blocked peers,
205 offered PC92 records produced 12 commits and 192 queued records before the
next valid record was refused and the healthy source closed. The same graph and
producer with zero blockers processed 398 of 399 records, peak queue three,
without closure. Both used a retained executable with matching source manifests.

The exact graph checks run serially after each meaningful transaction, before
the authority owner can dequeue its next input. Finite storage therefore did
not imply affordable service cost. Membership admission still met one second
in this run; that narrow success cannot waive the mailbox failure. CPU and
allocation profiles support the repeated decode/prepare mechanism; cumulative
allocation is not simultaneous owned memory. The explicit v12 stop condition
prevents substituting new retained machinery or relaxed limits without revised
scope. Full Q5/Q6 and complete-workload qualification were not run after this
failure.

## V14 controlled retry follow-up

Approved v14 replaces exact refused-record graph checks with bounded identity
retries. The early4-second diagnostic now reconciles scheduled/offered/committed
traffic:63blocked+one live and zero-blocker control each processed399/399/399
PC92 plus7 PC93, with peak mailbox1 and membership admission under25ms. These
are development diagnostics, not final-source long qualification. See the
[v14 matrix/evidence](../pc92-v14-validation.md) for final execution status.

Fresh review found that local Flush callbacks can acquire the manager lock in
an order different from event occurrence; healthy timing therefore takes the
later occurrence timestamp. Startup enqueue must recheck its grant/global gate
under the same manager lock used by closure, then release that lock before any
socket close. A stopped per-attempt timer can remain a runtime-heap zombie;
one timer per identity reused across attempts supplies a count bound. These
observations explain the tests and ownership comments; none closes the existing
SQLite/context/retirement aggregate gaps.

Retry qualification must classify errors by handshake phase. Permitted startup
candidate expiry cannot swallow post-handshake recovery errors. Start the
five-second recovery deadline once, check it after blocking reads, and reject
duplicate completion markers rather than renewing the deadline. A stable-peer
membership check also cannot prove membership admission during recovery: cause
the change between recovery C and A, then verify immutable A before the deltas
and retain the original one-second deadline under the combined load.

## Continued v14 workload and ownership evidence (2026-10-02)

The full sustained-cache profile passed its 45-minute load and 11-minute drain.
The first full shipped-runtime profile instead lost 15 peers at about 20
minutes and lacked eight local spots per client. The fixture responded to PC51
but omitted DXSpider's independent five-minute ping initiation; with GoCluster's
unchanged 600-second keepalive/idle settings, replies alone raced the idle
deadline. Correct the fixture's healthy-peer behavior rather than silently
changing production settings or ignoring disconnected recipients.

With the corrected fixture, full Q1 retained all 16 peers through load/drain and
delivered all 4,050,000 required PC92 relays and 6,750,000 peer spot deliveries.
Local delivery still failed. Bounded reader/child-enqueue missing-ID examples
identified the same nine omissions for all 100 clients. Actual-function pair
reproductions and mapped input timing matched one primary and eight SLOW
secondary 32-bit hash collisions. The full logical spot keys were distinct;
both caches store only truncated hashes, so an equality collision suppresses
the later spot. Different-hash and exact-expiry controls forwarded. The
[continued evidence](../pc92-v14-validation.md#missing-spot-attribution-and-current-scope-boundary)
retains all pairs, source/binary provenance and timing-reconstruction limits.
The earlier eight shipped omissions lack retained IDs and remain separate
historical evidence. Do not change the required-recipient denominator to make
accidental hash collisions pass.

The 20-second corrected preflight passed latency, but full Q1 did not: overall
client enqueue p99 was 12.1–13.8 ms and individual-minute first-byte p99 reached
36 ms. Preserve both results. An early CPU profile is not a steady-state
performance diagnosis, and successful protocol forwarding does not prove the
complete local delivery pipeline.

The blocked-logger diagnostic also refined a prior ownership hypothesis. Weak
references showed that session and original error objects were reclaimed while
160 outer wrappers waited with zero transport permits. Source inspection found
a separate retained owner: standard log.output formats each peer error into
an exclusive heap buffer before waiting for its output mutex. Bounded message
length and a bounded downstream line buffer do not bound the number of these
post-permit buffers. Report this gap rather than claiming full-session
retention or excluding protocol-derived copies as runtime stacks.

Likewise, the current application's maximum 194 live manager-context children
does not establish the backing bound of Go1.26.4's retained Swiss-map directory.
The source-only tombstone construction is a limitation of the cardinality
proof, not an observed production runaway or attacker control of runtime hash
values. Enabled SQLite, context backing and terminal diagnostic ownership
remain open; production changes outside v14 require a new scoped approval.

## Decision Linkage
- ADR created/updated: [ADR-0233](../decisions/ADR-0233-pc92-controlled-retries-and-peer-cap.md), ADR-0230, [ADR-0231](../decisions/ADR-0231-pc92-audit-corrections.md), and [ADR-0232](../decisions/ADR-0232-pc92-wire-and-recovery-evidence.md).
- Decision delta summary: Explicit compatibility, failure/recovery, allocation and qualification contracts.
- Contract/behavior changes: V7 changes only the declared qualification profiles and reachable Q4 phases; production batching and correction defaults remain unchanged.

## Verification and Monitoring
- Validation steps run: Targeted protocol/reference checks, tagged fixture oracle tests, allocation/lifetime regressions and targeted race tests. See the qualification record for current execution status.
- Signals to monitor (metrics/logs): Required versus observed deliveries, observer failures, per-minute latency, queue/candidate charges, graph/caches, gates and terminal cleanup.
- Rollback triggers: Unexpected policy changes, missing required deliveries, resource overruns or incomplete recovery invalidate acceptance; do not release on a diagnostic-only result.

## References
- Issue(s): none.
- PR(s): none.
- Commit(s): working branch `p92`; no release claim.
- Related ADR(s): [ADR-0230](../decisions/ADR-0230-pc18-pc92-authority-and-bounds.md).
- Related docs: [v7 execution record](../pc18-pc92-scope-ledger-v7.md), [qualification status](../pc92-qualification.md).

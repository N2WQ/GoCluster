# PC18/PC92 approved v14 execution ledger

The user authorized **Approved v14** on 2026-10-02. This authorizes the
controlled-retry scope proposed in v13 and the configurable peer-cap amendment,
not a cap-only change. Baseline: branch `p92`, commit
`2c0607986f3d3d9dc1921eb5b7c5ae00595143d2`, with the existing uncommitted v12
corrections retained. DXSpider reference:
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`.

## Objective and inherited contract

Replace repeated refused-record headroom simulation with bounded controlled
retries. The v12 experiment demonstrated starvation with 63 blocked identities
and one live ingress; its zero-blocker control continued serving traffic.
Retain the v12 raw identity, K omission, CTY provenance and valid oracle fixes.
Supersede only the admission-recovery clauses of ADR-0230/0231/0232 and v12:
saved refused-wire witnesses, repeated decoding/graph preparation after graph
mutations, mailbox/cache headroom interval tracking, and headroom-based one-to-two
second admission reopening. Preserve historical evidence of that failed design.

All other inherited PC18/PC92 requirements remain: truthful identity, literal
authentication, atomic authority admission, available-IP publication, supported
A/C/D/K behavior, selected CCCluster boundaries, complete C then metadata A,
unsupported-action exclusion, current-owner routing, and separate local service
when peering fails. Membership queue admission remains due within one second
for healthy established peers, including during recovery; complete recovery is
due within five seconds after handshake even with periodic C/K disabled.
Queue admission and local Flush are not remote acknowledgements.

The 480 MiB ceiling includes owned protocol backing and overlapping generations,
including SQLite. Runtime stacks/GC and unchanged configuration are reported
separately. Existing resource partitions, cache capacities, original admission
ages, strict expiry after 600 seconds, no unexpired eviction, and no duplicate
age renewal remain unchanged. The enabled-SQLite, context-child backing and
outer-retirement proofs remain open: correction closeout is distinct from overall
acceptance.

## Agreed implementation items

| Item | Boundary and behavior | Required evidence |
| --- | --- | --- |
| V14-01 | Required integer `peering.max_peers`, shipped value 64, supported 1–64. Reject absent, null, fractions, strings, booleans, negative/zero, overflow and >64 before lossy typed YAML conversion. No fallback or unlimited sentinel. | Raw YAML loader and direct-constructor boundary tests; malformed types, exact boundaries and absence; errors precede storage/workers. |
| V14-02 | Active enabled identities must fit N; disabled rows do not count and `both` counts once. Globally disabled peering retains dormant row-count behavior, but key presence/type/range always apply. Direct NewManager is active. Bound sessions/retry identities/replays by N; pending remains128 and transport owners N+128. Publication indexes are 1000+N, N+1 and conservative1000+2N, reserving actual enabled identities only. | Consumer tests N=1,2,8,63,64; N+1, constructor/loader, complete publication counts, owner cancellation and ordinary128-candidate Q4B. Unrelated64/192 constants remain unchanged. |
| V14-03 | Fixed manager-owned retry coordinator under existing Manager.mu; controller remains sole topology/freshness owner and writer sole socket owner. Scalar per-identity history, delay/due, candidate/attempt generation and fair order. No saved refused wires, graph simulations, graph clones/indexes, per-record channels, new goroutine or persisted retry history. | Ownership/lock review, bounded state tests, profiles and changed-owner allocation proof before long qualification. No manager lock across I/O/dial/auth/waits/graph preparation. |
| V14-04 | Authority refusal immediately fences identity and closes affected session; controller invalidates ingress before any replacement startup grant. Rejected record commits no partial topology/freshness/cache authority. One terminal outcome per actual attempt; stale callbacks cannot affect replacements. | Reader/controller/writer/Run races, pending invalidation barrier, old-generation callbacks, same rejected timestamp/key later admitted by a valid owner. |
| V14-05 | During overload, shared configured exponential backoff across both directions (effective defaults2,4,8…300 seconds; preserve constructor normalization). One recovery candidate per identity. Real outbound dial failure advances history but consumes no startup grant. Ordinary non-overload and legacy reconnect behavior remains unchanged, without double private/shared delay. | Actual concurrent inbound/outbound candidates, failed dial, denied arrivals, stop/wait versus actual failure, legacy and ordinary controls. |
| V14-06 | Authenticate inbound callsign/IP/password before eligibility. Startup grant immediately before outbound login/startup or inbound PC18; current attempt, absolute original handshake deadline, cancellation and global gates must still permit it. Global grants at least one second apart without catch-up credit; circular identity fairness, at most N−1 intervening grants for continuously eligible ready waiters. Ineligible/absent/expired candidates reserve no grant. | Auth flood, timeout/cancel/race tests, factual grant trace. A64-way wave requires at least63 seconds, so default60-second init candidates may time out and retry; no deadline extension or all64-in-one-window assumption. |
| V14-07 | Reset history only after current successful PC9x establishment/replay and matching post-establishment recovery C/A have both locally flushed, followed by60 uninterrupted healthy seconds. Receipt is compact scalar FIFO progress/target captured atomically with enqueue; never initial A/K or sendRecord return. Quiet peers qualify; legacy/remote spots/C do not substitute. Observe reset on next service within one second. | Fast writer, stalled/failed A, stale/duplicate/wrong-generation completion, replay order and60-second boundaries; remote completeness remains false until authoritative remote C. |
| V14-08 | Pure global clock/publication closure interrupts healthy interval, preserves history and does not increase/restart cooldown. A genuine failure already recorded still counts once. Existing global close/gate/resume behavior and mandatory complete membership/metadata recovery remain. | Both orders of failure/global-gate race, established and waiting candidates, unchanged clock/publication qualification and periodic-disabled recovery. |
| V14-09 | Incremental fixed retry/receipt bookkeeping at most32KiB within metadata32MiB partition. Account session layout across provisional641 retained identities at N64 (general2N+513), not just live192; retain transport160MiB, current360320-byte slack and one5MiB graph scratch owner. | Actual Go sizes/allocation classes, overlapping generations and ownership inventory; timers/waiters/callback cleanup and churn under race. No freed witness-memory credit against open SQLite proof. |
| V14-10 | Replace admission-specific Q5/Q6 oracle with bounded factual identity/session/attempt/direction/failure/grant/flush/reset/retirement events and independently derived expected schedules. Normal builds retain no event history. Preserve cache original-age Q5 and global-gate timing. | Missing/premature/late/stale/wrong-generation/duplicate/cancel/overflow negative controls; absolute deadline checks after blocking operations; no production due/healthySince oracle, observer prune, shortened TTL or authority injection. |
| V14-11 | Early63-blocked+one-live and zero-blocker comparison; actual63-recovering+healthy and64-recovering+maintenance at near-capacity graph,8000-member near64KiB C and1000 local users. At least30-minute persistent overload, release/recovery/reset and another failure, exercising real capped delays and disabled periodic C/K. | Scheduled versus actual offered counts and lateness must fail on underproduction. Approved mix:10000 distinct PC11/61/26 spot-forwarding keys/minute,100000 duplicates/minute,6000 PC92/minute (45%A,45%D,2%C,8%K; C8000 users), inherited PC93/bulletins. Reconcile mandatory delivery and deadlines; profiles and cleanup. |
| V14-12 | Narrow repair of ten known tagged qualification lint findings in q6 fixture, reader allocation, q5/q6 and qualification_topology: errors.As/Is, context-aware dial/listen with existing deadlines, static switch, time.Until and constant renewal count. | Tagged lint/vet/race tests; no unrelated cleanup or runtime semantics change. |
| V14-13 | Update runtime/config/support/allocation/qualification documentation, item-to-code/evidence mapping, ADR supersession, TSR history, indexes/maps and final closeout. | Final direct diff and fresh code-quality review; complete final-state selected lanes, pinned actual receiver, retained binaries/manifests and exact CTY hash. |

## Review and execution order

Requirements ambiguity, design-space, retained-state, lifecycle/leak, hot-path,
config, pre-approval falsifiability and scope-challenge reviews were performed.
Material findings were included above: invalidation acknowledgement, outcome
deduplication, effective sentinel normalization, writer receipt race,641-owner
accounting, offered-load integrity,63-second wave timing, gate/failure precedence,
fractional YAML rejection and dormant row-count behavior. Engineering reviews
were design-aware; no independent scientific review occurred or is claimed.

Before the first implementation slice, complete and disposition the detailed
test-strategy-adversary contract-to-test matrix. Then implement coordinator,
config/consumers and writer receipt with disjoint ownership; run allocation and
lifecycle checks; run the service-cost gate before long qualification. Review
the final diff and run the final selected lane once on the relevant final state.

Validation includes targeted unit/consumer/integration/race, normal full tests,
vet, staticcheck, golangci-lint, full race, qualification-tagged peer tests/race/
vet/lint, inherited raw-identity/K receiver regressions and parser fuzz. Final
evidence uses before/build/after source manifests, retained binary, reference and
CTY hashes; only the wrapper's final authoritative verdict counts. Eventual
overall acceptance also needs full affected Q5/Q6/cache and Q1–Q4 plus the open
allocation proofs. A short gate alone cannot establish compliance.

## Boundaries and stop conditions

Support-agent documentation impact: **Required**. The peer support card and
troubleshooting index cover the new YAML migration and retry phase diagnostics.

No IP-preference command, SQLite/Pebble migration, persistence schema or allocator
redesign, changed resource ceilings, relaxed deadlines, new authority owner,
ordinary admission redesign, deployment, restart, commit or push. No hot reload
for max_peers. Existing private configurations require the explicit key and are
not automatically edited. If allocation or service gates require changes outside
these boundaries, stop and request revised exact approval with the evidence.

Status: authorized; implementation and final qualification not yet complete.

# PC18/PC92 approved v15 execution ledger

The user authorized **Approved v15** on 2026-10-02. Baseline: branch `p92`,
`2413beb47551d428d96d06a3f9178e2577d8ec9d`, clean before v15 edits. This records
the approved consolidated scope; approval does not establish implementation or
acceptance. DXSpider remains pinned to
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`.

## Objective and inherited contract

Fix the demonstrated local spot loss and sustained latency failure, close the
context, diagnostic-retirement and enabled-SQLite allocation gaps, and measure
completion against the original PC18/PC92 acceptance criteria. Earlier passing
component runs remain historical evidence, not final-source acceptance.

Preserve D1-D8 and S01-S14 through the [v6](pc18-pc92-scope-ledger-v6.md),
[v7](pc18-pc92-scope-ledger-v7.md), [v8](pc18-pc92-scope-ledger-v8.md),
[v9](pc18-pc92-scope-ledger-v9.md), [v11](pc18-pc92-scope-ledger-v11.md),
[v12](pc18-pc92-scope-ledger-v12.md) and
[v14](pc18-pc92-scope-ledger-v14.md) amendments. Preserve literal authentication,
truthful PC18 identity, atomic establishment authority, supported A/C/D/K,
available-IP publication, selected CCCluster boundaries, complete immutable C/A
recovery, and controlled retries. `peering.max_peers` remains required, 1-64,
without hot reload. Healthy established peers, including those recovering, must
admit membership changes to their queues within one second; complete recovery
is due within five seconds after handshake even with periodic C/K disabled.
Neither queue admission nor local Flush is a remote acknowledgement.

The 480 MiB ceiling includes all owned protocol backing, native allocations,
temporary and overlapping generations. Runtime stacks/GC and unchanged
configuration remain separately reported. Existing partitions, traffic-class
isolation, no unexpired cache eviction, original admission ages, strict expiry
after 600 seconds and no duplicate age renewal remain unchanged.

## Selected operational decisions

- Diagnostic overload may drop/coalesce diagnostic records, count losses and
  summarize recovery. This grants no permission to drop protocol traffic.
- Detailed peer diagnostics move to the dedicated peer log. Peer status and
  known lost/unconfirmed-write counts remain available without that sink.
- A configured topology database that cannot open within its allocation budget
  causes clear startup failure while preserving its file and committed data.
  No silent disable, destructive reset or budget increase is permitted. This
  supersedes v9 P05's accepted-file requirement only for resource exhaustion.

## Agreed implementation items

| Item | Approved behavior and boundary | Required evidence |
| --- | --- | --- |
| V15-01 | Preserve all inherited contracts and one consolidated requirement-to-code/evidence record. | Traceability; inherited regressions; no dropped requirement or relaxed denominator. |
| V15-02 | Replace hash-only spot cache equality with exact equality of the existing 42-byte primary and 32-byte secondary encodings. Hashing selects the same 64 shards. Preserve normalization, truncation, windows, source classes, upgrades, consumer policies and other callers' Hash32 values. | Nine retained collision pairs; literal key/hash vectors; repeats, boundaries, out-of-order and report upgrades; FAST/MED/SLOW consumer checks. |
| V15-03 | Primary cleanup evaluates current expiry and deletes under the same shard lock, removing stale deletion and proportional deletion scratch. Preserve strict expiry and compaction. | Deterministic same-key refresh through processSpot, expired control, mutation control, race and populated cleanup costs. |
| V15-04 | Add a diagnostic-only Q1 workload: 15-minute load plus 11-minute drain, unchanged population/mix/settings; CPU windows 0-20s, 60-180s and 660-780s; bounded one-second runtime/cache/queue observations. Optional stage instrumentation has a 32 MiB total fixture-owned backing cap. | Actual windows and all sample sequences; no forced GC during load, no blocking profiling control in generator; missing/duplicate/unknown/overflow/clock/incomplete evidence invalidates diagnosis. Diagnostics never qualify acceptance. |
| V15-05 | Only measured repeated normalization, cached-format lookup, newline and intermediate-copy removal in existing dedup/output-pipeline/telnet owners. Direct normalized append into the existing telnet batch is permitted. Shared Spot formatter APIs and byte semantics remain unchanged. | Retained comparable warm baseline before each patch; named measured cost and predicted reduction; matched profiles/benchmarks; frozen independent byte oracle; writer priority, atomic records, batching threshold overshoot, read pause, close-after-control, client state, prediction age, errors and shutdown. |
| V15-06 | Fixed manager-owned context parent per N+128 transport slots and two optional projection workers; one operation child each, fully canceled before reuse. Explicit outbound root even when parent deadline is earlier. | Child/backing inventory including Dialer and parse waiters; values/deadlines/cause; all partial-start, dial, auth, replay, EOF, timeout and Stop paths; no custom Context or cancellation bridge workers. |
| V15-07 | Replace arbitrary peer diagnostic callbacks and direct logger I/O with a concrete bounded typed mailbox. Reserve before copying/formatting; retain no session, raw wire, error interface or Stringer. Nonpeer logging stays unchanged. | All callback/log escape paths audited; blocked general sink, overload, concurrent closure, bounded field backing; status distinguishes disabled, known drops and unconfirmed writes. |
| V15-08 | Minimal cmd/peerdiag companion at an absolute sibling path, hidden Windows launch. Sole owner of the existing peer_connections daily file and overlong sample/rotation. Parent stops constructing that peer sink. Fixed 256 x 2 KiB queue and 512 x 2 KiB exact-key deduper with existing window/oldest semantics. | Bounded IPC, ACK/deadline checks; one generation, kill and join before replacement; missing helper visibly degrades logging only. Normal shutdown closes producer admission, discards/counts pending records, terminates/joins. Failed OS termination leaves charged ownership, gates replacement and reports cleanup failure. Actual Windows/Linux process evidence, bounded directory/rotation work, both-binary packaging/provenance. |
| V15-09 | Topology-only repaired local fork of ncruces/go-sqlite3 v0.35.6 and generated engine v6.3.35304. Other modernc users unchanged. One serialized connection; deadline includes waiting; streaming rows/bindings. Own resources before partial initialization, close wrapper with zero SQL handle, convert only recognized OOM, poison/retire before reuse and retain failed cleanup ownership. | Pinned provenance/MIT notices; module-graph isolation; per-layer fault/retirement gates; actual engine and host backing bounds; no dynamic extensions or unbounded callback registry; no destructive schema/file migration. |
| V15-10 | Bounded Windows fallback adapted from upstream v0.30.0 native file mapping/byte locks and private-shadow coherence, corrected for 64 KiB allocation granularity. Keep native Windows and Linux paths. No dotlk or new unsupported OS baseline. | Separate native/candidate/modernc process readers/writers, pinned reader, checkpoint, crash/reopen, 32/64 KiB boundaries, mapping cap and external growth. Exact committed data; acknowledged commits must survive. |
| V15-11 | Resource-exhausted database startup fails clearly, preserving committed data/file; runtime persistence failures preserve live authority. | Actual manager/startup refusal; independent database content/integrity control; no reset, silent disable or ceiling increase. |
| V15-12 | Complete ownership proof includes protocol/native/helper/temporary/retired backing under unchanged partitions. | Enabled SQLite and helper under simultaneous Q4 pressure, source inventory and synchronized charge evidence; no separate-peak sum or RSS substitution. Ordinary shared ingestion dedupe retains its previous separately reported owner classification; report its larger exact-key backing. |
| V15-13 | Final-source original qualification and authoritative final verdict, with both binaries, manifests, CTY and reference identity. | All required outputs reconciled; no missing, stale, shortened, partial, diagnostic or failed evidence may qualify. Correction completion and overall acceptance remain separate. |
| V15-14 | Update operator/config/support documentation, code maps, durable ADR/TSR, packaging and implementation/evidence mapping. | Direct final diff review; selected normal/static/race/tagged/fuzz/benchmark/profile/package/workflow checks. Actual Linux execution; cross-compilation is insufficient. |

## Resource allocations to prove

Within the existing 32 MiB metadata partition, reserve 16 MiB for SQLite
(8 MiB engine, 6 MiB fallback WAL views/shadows, 2 MiB host), 3 MiB for all
parent/helper diagnostic protocol data and 13 MiB for the remaining metadata,
including contexts and retired identities. These are proposed allocations to
prove, not established bounds. No aggregate or other partition increases.

The fallback has 64 globally shared region slots across attached files. Each
slot allows a 64 KiB native view and 32 KiB shadow; logical WAL-index coverage is
2 MiB. Engine-private copies stay in its 8 MiB allocation. Helper accounting
includes startup, exec/argv/environment/paths, IPC, active writes, dedupe
metadata, rotation work and any retained failed generation. Successful cleanup
assumes responsive supported OS termination; an unkillable process is reported
and charged, never described as successfully cleaned up.

## Review, validation and execution

Pre-approval ambiguity, design, falsifiability, scope, blast-radius, config,
lifecycle, retained-state and hot-path reviews were performed. The detailed
postapproval test-strategy review was completed before the first implementation
slice. Findings were dispositioned as covered or checker-only refinements:
literal oracles, deterministic cleanup interleaving, explicit warm duration,
real subprocess evidence, complete descendant inventory, all diagnostic escape
paths, loaded-config semantics, lower-level failed release ownership, dependency
isolation, DSN compatibility, acknowledged-commit crash semantics and Linux
execution. Reviews were design-aware; no independent certification is claimed.

Detailed matrices and evidence are maintained in the dedup, ownership, SQLite
and consolidated v15 validation records. Optional stage instrumentation is
initially omitted; add it only if needed for the approved warm diagnosis and
prove its full backing bound before use. The release workflow checker currently
rejects every create-release.ps1 change. Approved companion packaging remains
authorized; report that narrow checker limitation rather than silently changing
or bypassing the checker.

Execution order: correctness and warm diagnosis/permitted repairs; context and
diagnostic ownership; isolated SQLite repair gates before production integration;
complete allocation proof, final-source qualification and documentation.

Original mandatory acceptance remains:

- Q1: 100 clients, 16 peers, 45-minute load plus 11-minute drain.
- Q2: 100 clients, 64 peers, full graph, 30-minute load plus 11-minute drain.
- Q3: the full population, 30 minutes/six burst-and-slow-peer cycles plus drain.
- Every healthy client's every minute and overall enqueue p99 <=5 ms and
  first-byte p99 <=25 ms, using every required token, including missing,
  duplicate and unknown-token failures.
- Shipped Q1: full 45+11 minutes with intentional holding enabled; delivery
  requirements remain, without the 5/25 ms thresholds.
- Q4 A/B: each full 30-minute pressure/refill workload with 1,000 locals,
  64 established plus 128 prelogin candidates, or 63 established plus 128
  authenticated contenders; enabled SQLite and helper included.
- Q5: original saturation/isolation/no-eviction/real-600-second-expiry cases.
- Q6: original fault/recovery, periodic-disabled, actual pinned receiver and
  20-minute receive-only cases; full 3,750-second controlled-retry workload.
- Full cache-memory and 45+11-minute sustained-cache profiles, complete
  allocation proof, final normal test/vet/staticcheck/lint/full race, applicable
  tagged/fuzz/benchmark/package/platform checks and provenance reconciliation.

Traffic remains 10,000 distinct new PC11/61/26 forwarding keys/minute across
peers, 100,000 duplicates/minute, 6,000 PC92/minute (45% A, 45% D, 2% C, 8% K,
8,000-member C), and inherited PC93/bulletin rates. See the original
[qualification contract](pc92-qualification.md) for workload details.

## Boundaries and completion

No relaxed rates, deadlines or denominators; runtime tuning; correction/model
policy; new production workers beyond approved diagnostics; new spot/protocol
queues, drops, batching defaults, shared caches or scheduling; destructive DB
migration; deployment, user-cluster restart, commit or push. No IP-preference
command or peer cap above 64. A measured performance cause outside V15-05's
owners/mechanisms remains open and requires a focused scope amendment. Likewise,
an unavoidable material shared dependency upgrade must be dispositioned before
integration. Do not label partial correction as acceptance.

Support-agent docs impact: **Required**. Status: **implementation in progress;
overall acceptance incomplete**. Planned checks are not executed evidence.

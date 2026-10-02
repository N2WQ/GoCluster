# PC18/PC92 approved v12 execution ledger

The user authorized **Approved v12** on 2026-10-01, after the proposed ledger
and a review of its workflow compliance. Approval preceded mutation. Baseline:
branch `p92`, commit `2c0607986f3d3d9dc1921eb5b7c5ae00595143d2`.
DXSpider reference: `3e9b3621d94dd45c68702e4a0f896aac33f2a91d`.

## Objective and current state

Correct re-audit findings RA01 (normalizing malformed received PC92 identities
into validity), RA02 (admission-recovery qualification accepts late recovery),
and RA03 (K numeric omissions preserve stale metadata). Include the related
source-supported recovery continuity, mailbox-clear race, cause-specific
capacity and Q5 deadline-checker defects found during verification. Retain and
document the CTY refresh, as explicitly selected by the user.

The [v11 ledger](pc18-pc92-scope-ledger-v11.md) and inherited v6/v7/v8/v9
decisions remain controlling. Correction completion and overall acceptance are
separate: the enabled-SQLite, context-backing and outer-retirement allocation
proof, failed runtime latency preflights and missing final-source long profiles
remain open. This ledger does not claim runtime or interoperability success.

## Unchanged authority

- Truthful PC18 identity; literal authentication; stable valid canonical local
  identity, collision handling and current private-message ownership.
- Available-IP publication, A/C/D/K, unsupported-action exclusion, selected
  CCCluster startup/direct publication and transit boundaries.
- Complete atomic C; mandatory C then metadata A recovery with periodic C/K
  disabled. One-second membership queue admission includes healthy established
  peers during recovery. Five-second complete C/A recovery never extends it.
- All resource partitions and the 480 MiB protocol-owned backing/overlap ceiling,
  including SQLite. Exactly 600 seconds remains unexpired; no unexpired eviction
  or duplicate age renewal.
- 10,000 new PC11/PC61/PC26 spot-forwarding keys/minute, 100,000 duplicates/minute,
  and the inherited separate PC92, PC93, bulletin and Q1-Q6 workload.

## Approved slices

| Item | Implementation boundary | Required distinguishing evidence |
| --- | --- | --- |
| V12-01 / RA01 | Separate pure raw PC92 grammar from unchanged local/login normalization. Received origin is not trimmed, uppercased or repaired. Entry call portions permit receiver-compatible trailing ASCII spaces within the existing printable envelope, then raw validation before stable canonicalization. Local encoding canonicalizes origin without mutating the input. Transit preserves original accepted payload except transport hop. | Literal origin/subject/member vectors including `K1ABC/`, `/K1ABC`, `K1ABC-000`; seeded whole-operation non-mutation; corrected same-timestamp retry; tracked startup metadata/staging; count/byte mailbox and current/stale-owner controls; portable/SSID, private routing, authentication and transit regressions; actual receiver, fuzz and decoder allocation/timing including large C. |
| V12-02 / RA03 | One pure effective subject-node view maps empty decoded K version/build to explicit `"0"`. Use it for new subject preparation, existing subject commit, final charge and metadata overlap. Preserve A/C/D omissions, implicit subjects, member defaults, absent IP, external relationship-edge metadata and distinct-origin metadata. Parsed record/transit remain unchanged. No SQL schema or driver change. | Independently seed5457/633: K build omitted gives5457/0; both omitted or zeros give0/0; version omitted/build634 gives0/634. Actual receiver, populated membership and nonzero restoration controls; production receive/project/write and exact SQLite TEXT comparisons; old active projection and peak/final ownership tests. |
| V12-03 / RA02 dependencies | Sole-controller transition-driven recovery, bounded failure generations/causes, fixed mailbox charge-class interval history, exact post-transaction graph predicates, logical cache expiry and coherent final gate clearing as detailed below. | Sub-tick loss/restoration, unchanged counts with changed reference credits, healthy unrelated churn, pending/new failure races, mailbox handoff and final-lock races, cache expiry/refill, independent gates, cause-specific count/byte boundaries and ordinary post-reconnect eligibility. |
| V12-04 / RA02 and Q5 | Bounded qualification-only events distinguish actual availability, interruption, matching gate removal and external observation. Absolute deadlines before/after blocking observation, predicate and success; cancellation/missing/stale/overflow/late evidence fails. Observe Q5 recovery around first applicable real expiry during the hold. | Four-second hold, premature recovery, interrupted headroom, late successful predicate/observation, cancellation and missing-generation negative controls using actual helpers. No production healthy-since oracle; no TTL shortening, authority injection or authentication bypass. Fresh full Q5/Q6. |
| V12-05 | Early changed-owner allocation and complete scheduler service-cost gate before expensive qualification. At most32KiB added fixed bookkeeping inside existing metadata partition; preserve one graph scratch owner and bounded refused wire. | Reachable63 blocked+one live ingress;64 blocked+queued work/maintenance; large C and near-capacity graph/metadata, PC92/PC93, replay, lifecycle, publication and expiry. Whole deadline outcomes plus allocation/CPU/mutex/block profiles; cancellation, shutdown and churn under race. |
| V12-06 | Retain CTY refresh; document source/status provenance, semantic counts, Git-normalized and working-byte hashes, and attribution uncertainty. Exact consumed asset belongs in future qualification manifests. | Parse/hash/provenance reconciliation; isolated qualification inputs; retain prior-asset runs as historical. No geographic-correctness claim from hashes or parsing. |
| V12-07 | Keep item-to-code/evidence mapping; update affected operator/protocol, support, allocation, qualification, comment, ADR/TSR and closeout records. | Detailed post-approval matrix before implementation; final selected lanes, actual pinned receiver and affected qualification; retained binaries and source/reference/asset manifests; fresh final verification and separate correction/overall verdicts. |

## Recovery architecture and timing

Each configured peer has at most one current failure episode. Retain the
existing bounded refused wire as a capacity witness, never as automatic replay.
Capture the failure cause and a generation that distinguishes a newer pending
manager-side failure from the episode evaluated by the controller.

| Cause | Required recovery resources |
| --- | --- |
| Mailbox | Applicable count and byte headroom only. |
| New authority | Applicable mailbox, cache, freshness, ingress and transactional graph capacity. |
| Duplicate alternate ingress | Mailbox plus current missing origin/external-subject observations and graph backing bytes, after ingress loss on closure. No full graph reapplication, new cache slot or freshness reservation. |

Expiry of a duplicate witness's original key neither resolves ingress shortage
nor creates new authority requirements or grants stale ingress. Newly received
traffic always takes ordinary current-owner/freshness/dedupe/admission checks.

Maintain mailbox health and continuous healthy-since for the finite allocation
charges of supported <=64KiB records, at most72 classes. Update under queueMu
on every PC92 count/byte transition, including transitions before pending
failure handoff. Preserve health on true-to-true transitions.

After complete controller transactions retire their ordinary decoded/plan
scratch, evaluate affected blocked records sequentially using the existing
5MiB scratch allowance. Cover receive, replay, alternate ingress, PC93
freshness, ingress loss, expiry/compaction and direct-peer changes. Equal counts
do not prove unchanged resource requirements. Do not retain decoded records,
desired sets or plans for all blocked peers.

Cache availability follows original stored elapsed admission ages and strict
age>600s expiry, including availability before delayed physical cleanup and
intervening consumption. Cleanup grants no additional recovery grace.

At final gate clearing acquire manager.mu then queueMu, read the decision time
after locking, verify current generation including undrained failures, reread
mailbox fit AND continuous healthy-since, and enforce the full one-second
interval again. No expensive graph scan occurs under either final-clear lock.
Subsequent traffic may consume space; no reservation policy is introduced.

Track per-peer reasons even while a global clock/publication gate remains.
Effective PC9x admission requires every applicable reason to clear. Required
headroom must be continuously available for one second, then recovery observed
within the next second. Handoff, scheduling and observation consume this
allowance. Reconnect backoff, handshake and complete C/A deadlines are separate.

New fixed bookkeeping is limited to32KiB charged within the existing32MiB
metadata partition. Existing refused-wire bounds and retirement remain. There
is no new worker, unbounded history, graph clone, reverse index or reservation.

## Material risk and stop condition

Exact blocked-record graph evaluation is bounded, but maximum-population CPU
affordability is unmeasured. Require the early V12-05 service-cost gate before
expensive qualification. If existing bounds require new graph summaries,
certificates, retained plans, owners, reservations, limits or changed deadlines,
stop and present revised design/scope. These alternatives are not implicitly
authorized. A microbenchmark alone cannot establish service qualification.

## Validation and sequencing

1. Complete and disposition the detailed test-strategy matrix before the first
   implementation slice; record findings without claiming independence.
2. Add distinguishing regressions and implement identity and K as separate
   bounded slices. Correct recovery oracles and tracking together.
3. Pass early allocation and complete service-cost gates.
4. Run targeted checks and one final relevant-state lane: `go test ./...`,
   `go vet ./...`, `staticcheck ./...`,
   `golangci-lint run ./... --config=.golangci.yaml`, `go test -race ./...`.
5. Include explicit qualification-tagged tests/race, affected parser fuzz,
   benchmarks/profiles and the actual pinned receiver. Missing dependencies or
   skipped reference cases cannot count as interoperability success.
6. Regenerate full Q5/Q6 with corrected checkers; run affected cache qualification
   and fresh runtime/Q4 diagnostics. Freeze relevant source/reference/assets,
   retain one built executable and raw evidence, and use wrapper final verdicts.
   Run resource-sensitive profile families separately.

The detailed validation record will retain exact commands and material results.
Fresh review reruns affected checks; broader invalidation requires the complete
selected lane again. Short diagnostics never replace long qualification.

## Scope challenge and decisions

Three read-only specialist reviews used inherited context: design-aware, not
independent normative certification. Lead dispositions incorporated raw-wire
role differences, local encoder compatibility, K node/edge distinction, exact
SQLite TEXT and old projection ownership, tagged-test coverage, actual-expiry
observations, locked continuity recheck, newer pending failures and
duplicate-ingress resource causes. Poll-only recovery and resetting on unrelated
mutations were rejected; larger incremental graph machinery is not authorized.

Support-agent docs impact: **Required**. Identity rejection, cause-specific gate
diagnosis, K metadata and corrected evidence interpretation affect support.
Update the peer support card/routing and operator docs. Preserve accepted ADR
history through a linked refinement of ADR-0231, and retain durable findings in
TSR-0035. Update indexes and generated maps where their inputs change.

## Boundaries and closeout

No SQLite/Pebble migration, persistence allocator redesign, unrelated latency
refactor, feature/configuration relaxation, deployment, running-cluster restart,
commit or push is authorized.

Report separately:

- V12 corrective work: each approved item implemented, documented, reviewed and
  supported by its required evidence; failures remain explicit.
- Overall acceptance: complete480MiB proof, resolved latency failures and all
  mandatory final-source profiles, including Q1-Q3, shipped-Q1, full Q4A/B and
  sustained-cache evidence. Missing proof remains incomplete.

Execution status: **stopped at the approved service-cost boundary**. All detailed
post-approval findings were dispositioned before implementation. The sustained
63-blocked/one-live necessary condition filled the 192-record mailbox and closed
the healthy source; the identical zero-blocker control passed. See the
[execution evidence](pc92-v12-validation.md). No broader recovery redesign is
authorized by v12. Corrective closeout and overall acceptance both remain
incomplete; the uncommitted implementation is not a qualified release.

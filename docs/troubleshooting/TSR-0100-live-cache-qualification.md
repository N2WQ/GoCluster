# TSR-0100 - Live Cache Qualification

Status: Open
Date Opened: 2026-10-15
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: VOACAP live forecast and delay retention
Trigger Source: Chat request
Led To ADR(s): ADR-0310
Tags: cache, expiry, heap, latency, windows, qualification

## RCA Summary

V3 update (2026-10-16): the owner selected the two correctness fixes only and
authorized extraction with `Approved v3`. Original value maps and full scans
replace the indexed implementation. V2 source is hash-verified under
`.tmp/voacap-live-cache-v3/v2-source/`; all earlier artifacts remain unchanged.
V3 functional tests, race checks and analyzers passed. Timing qualification
blocked on two candidate refresh batches below the frozen 1 ms duration floor:
N=64 measured 662,800 ns and N=10,000 measured 949,000 ns. All 116 pilot
observations had been valid; the pilot's 2 ms projected duration margin did
not guarantee the scored floor. The campaign was stopped early after this
blocking validity failure, without retries, threshold changes or a performance
verdict. This is a measurement limitation, not evidence of a code regression.
The patch remains uncommitted and unaccepted. This does not resolve the indexed
performance failures below; the optimization remains excluded and this record
remains Open.

- What happened: v1 and v2 passed correctness, race and retained-memory checks
  but failed performance acceptance. V2 has 45 passing, 12 failing and 16
  inconclusive timing guards, with no invalid observations.
- Why: a linear dense-sweep bound did not ensure elapsed-time parity with the
  previous scan. V2 removed eager forward delay repairs and the destructive
  sparse prefix; dense expiry still fails. Deferred insertion maintenance also
  makes its trigger and blocked-reader tail slower. Exact CPU attribution for
  the remaining constant costs is not established by these timing results.
- What fixed it: v2 fixes the large-cache delay-update guard and the invalid
  tail observer, but no implementation is accepted. Original evidence and
  unchanged thresholds are retained; failed results were not discarded.
- How we know: 12 paired comparisons, symmetric AA/BB controls, raw interval and
  state witnesses, 12 retained-heap pairs per payload shape, and paired controlled
  worker profiles. See the execution record for commands, hashes and results.
- Operator/support answer: both requested policies are implemented locally, but
  the candidate is not accepted for deployment. The measured regressions concern
  local workloads and do not establish production client latency.

## Triggering Request

- Request date: 2026-10-15.
- Request summary: investigate live eviction and expiry together, select
  replacement-only refresh and deterministic tied victims, then implement v1.
- Request reference: owner `Approved v1`; live-cache execution record.

## Symptoms and Impact

In v1, at 50,000 entries, rotating delay updates had candidate/baseline geometric-mean
ratio 2.02524. Expiring 10%, 50% and 100% of each map had ratios 2.65727, 2.29051
and 2.31444. Their AA/BB controls passed; the approved limit was 1.10. Dense
expiry still occurs under the fallback mutex. Faster capacity-victim selection
cannot compensate for these failed protected-workload guards.

Capacity insertion and no-expiry sweep observations were much faster, but their
candidate same-binary controls failed the symmetric bounds. Their required 0.50
gates therefore remain inconclusive, not passed. All three overall lookup p99
comparisons include unresolved zero p99 observations. Both separately witnessed
expiry-overlap p99 comparisons have failed repeatability controls.

## Timeline

1. 2026-10-15 - exact v1 approval; coupled indexed heaps and regression fixtures
   implemented against baseline `260cc7437e863874894a956de4feb78b3ddbb71c`.
2. 2026-10-15 - full ordinary/race lanes and analyzers pass; final harness review
   strengthens actual sweep execution and malformed-report isolation.
3. 2026-10-15 - frozen 72-process timing and 73-process heap campaigns finish;
   all workload witnesses pass, but performance acceptance fails.
4. 2026-10-15 - controlled worker/caller-lookup profiles finish successfully;
   v1 remains unaccepted and ADR-0310 remains Proposed.
5. 2026-10-15 - owner authorizes v2; deferred delay repair, read-only expiry
   classification and revised overlap observer are implemented. Full ordinary
   and race lanes pass 70 packages and 10,773 cases each; analyzers pass.
6. 2026-10-16 - all 504 v2 timing and 73 heap processes complete successfully.
   Timing acceptance fails; separate serial and heap arithmetic checks agree
   with the saved measurements. ADR-0310 remains Proposed.
7. 2026-10-16 - matched worker and retention profiles pass their workload
   checks; final source/diff/documentation checks complete. V2 remains
   uncommitted and unaccepted.
8. 2026-10-16 - owner authorizes correctness-only V3; original maps and scans
   are restored with the two policy fixes. Functional validation passes, but
   scored timing is invalid after short refresh batches. Campaign stopped;
   no commit, merge or deployment. Any new qualification must preserve this
   failed attempt and establish a separately authorized protocol.

## Hypotheses and Tests

The four hypotheses below record v1; the authorized v2 evidence follows under
Decision Linkage.

1. One bounded age heap per map can improve capacity selection while preserving
   all protected costs within 10%.
   - Evidence/commands: frozen `qualify.py --mode timing`; 47 workloads and
     52 timing metrics, including symmetric controls and preserved raw samples.
   - Outcome: Rejected for this candidate. Fifteen protected timing guards fail.
2. Dense fallback's O(N) bound is sufficient to preserve dense cleanup speed.
   - Evidence: crossover work-count tests pass, yet the 50,000-entry dense
     workload ratios are 2.29 to 2.66 with passing same-binary controls.
   - Outcome: Rejected. An asymptotic bound does not establish constant costs.
3. The new bookkeeping exceeds its 20 MiB retained-heap budget.
   - Evidence/commands: `qualify.py --mode heap`; 12 paired processes per
     1/8/24-row shape with both maps at 50,000 and compact identical payloads.
   - Outcome: Rejected in this fixture. Largest net increase is 6,819,648 bytes;
     an intentionally retained-payload fault is detected at 166,387,572 bytes.
4. A monotonic `time.Since` observer can resolve individual fast lookup tails.
   - Evidence: unchanged raw intervals include equal start/acquired/end readings
     and zero p99. Installed Go 1.27.1 Windows amd64 time source reads shared
     interrupt time; `testing.B.Elapsed` uses a separate precision path.
   - Outcome: Rejected for these observations. No new clock campaign or retry
     was substituted. TSR-0097 documents related supported-timer constraints.

## Root cause or best current explanation

- Best current explanation: v1's eager delay repairs were a material cost; v2
  meets that update guard. Removing the sparse prefix is insufficient to meet
  dense-expiry timing. Deferred maintenance also transfers cost to the next
  insertion. Further design and attribution are needed before another revision.
- Contributing factors: v2 resolves v1's invalid clock observations, while
  same-binary variability still prevents certification of 16 additional guards.
- A v1 paired 256-completion worker profile improved from a reported 0.2514487 s to
  0.0137678 s. Baseline `addCacheLocked` consumed 190 ms inclusive sampled CPU;
  the candidate had only 10 ms total samples. This demonstrates the narrower
  workload benefit and cannot waive other failures or prove production gains.
- No concrete production correctness defect was found in the v1 final review.
  Its full ordinary and race lanes each passed 70 packages and 10,718 cases.

## Decision Linkage

### Authorized v2 follow-up

The owner authorized `Approved v2` to replace eager forward delay repairs with
one conservative timestamp per existing node and to classify sparse versus dense
expiry before destructive removals. Capacity eviction reconciles up to 32 stale
roots before rebuilding once; bulk expiry checks actual times and reconciles
survivors. The selected policies, ownership bounds and acceptance limits remain
unchanged. V1 evidence is preserved; it does not qualify the revised code.

The v2 focused Windows tests passed 85 cases with two opt-in skips. Vet and
Staticcheck passed; naming and parameter-order lint findings in the fixture were
corrected, and the targeted tests and lint then passed again. New coverage
includes the all-dirty but actually fresh population, a fresh actual parent over
an expired child, work bounds, deferred repayment allocation and reader cleanup
on panic, Goexit and cancellation.

The revised observer uses the existing shared live benchmark clock. Original
coarse-clock reports could count more overlapping lookups than the physical
reader/lock population permitted. V2 checks that bound and excludes ambiguous
clock-edge observations from the overlap subset while retaining every raw
sample. All ten fixed 64-pass pilot processes exited successfully with valid
witnesses and 502 to 512 actual overlaps each. The predeclared rule selected 256
scored passes, with a required 1,000 overlaps per scored process. Pilot ratios
are not acceptance results.

Serial cases run as one matrix; each concurrent shape runs in its own process.
Lossless gzip preserves all raw intervals while bounding decoded report size.
The controller binds scored launch to successful pilot receipts and matching
source, observer and binary identities. Only the selected replication count,
overlap floor and selection hash may differ from the pilot protocol.

The full ordinary and race lanes each passed 70 packages, with 10,773 passed
cases and 94 skips. Full vet, Staticcheck and lint passed. The 504-process timing
campaign reports 45 pass, 12 fail, 16 inconclusive and zero invalid guards.
A separate arithmetic cross-check reproduced all 44 serial statuses and 132
compare/control geometric means exactly. It used inherited review context,
not a non-steered independent design review.

At N=50,000, rotating delay updates pass at 0.706891 times baseline, and all
three required insertion/no-expiry targets pass. Dense expiry still fails at
1.654071 / 1.792782 / 2.080563 for 10% / 50% / 100% expiry. Half/full expiry
actual-overlap lookup p99 fails at 1.961744 / 2.173012. Touch-all then insert
has a faster complete cycle (0.816885) but a slower triggering-request total
(1.302333) and actual-overlap lookup p99 (1.320740). Those failed metrics all
have passing same-binary controls; faster unrelated cases cannot waive them.

All 73 heap-process receipts passed a separate identity/hash/workload check.
Maximum additional retained heap is 7,619,912 bytes (7.266914 MiB), below the
20 MiB limit. The retained-payload fault exceeds its baseline reference by
167,987,240 bytes and is detected. Warm bookkeeping allocation tests also pass.
The complete evidence and matched-profile disposition are maintained in the
execution record. V2 remains unaccepted.

V2 retention profiles include seeding and coarse CPU samples. Inclusive prune
CPU was 40/70 ms for baseline/candidate half expiry and 40/150 ms for full
expiry. Full expiry has no survivors to rebuild; its candidate samples remain
in dense filtering and deletion. This narrows the investigation but does not
isolate an exact low-level cause. The 256-completion worker profile improved
from a reported 0.2330424 to 0.022622 seconds, with only 10 ms of candidate CPU
samples; that narrower benefit cannot waive the failed guards. All profile
artifacts and interpretation limits are retained in the execution record.

- ADR created/updated: ADR-0310 remains Proposed.
- Decision delta summary: the selected live policies retain their authorization;
  this indexed implementation has not earned performance acceptance.
- Contract/behavior changes: replacement-only refresh and deterministic greatest
  existing typed-key ties are present in the local candidate. Configuration,
  persistence policy and forecast calculations are unchanged.

## Verification and Monitoring

- Validation steps run: full ordinary/race, targeted final tests, vet,
  Staticcheck, lint, support checks, frozen timing/memory qualification and
  paired actual-worker profiles. Full results are in the execution record.
- Signals to monitor: rotating delay update cost, dense-expiry lock duration,
  real overlap-tail observations, and live map/index cardinality agreement.
- Rollback triggers: the candidate is uncommitted and has not been deployed.
  Do not merge or deploy it as an accepted performance change. A later design
  must satisfy the unchanged guards under a newly reviewed frozen protocol.

## References

- Issue(s): owner review and approval conversation.
- PR(s): none.
- Commit(s): baseline `260cc7437e863874894a956de4feb78b3ddbb71c`.
- Related ADR(s): [ADR-0310](../decisions/ADR-0310-live-forecast-cache-retention.md).
- Related docs: [execution record](../voacap-live-cache-execution.md),
  [TSR-0097](TSR-0097-path-sharding-measurement-clock.md),
  [TSR-0099](TSR-0099-global-heap-counter-observers.md).

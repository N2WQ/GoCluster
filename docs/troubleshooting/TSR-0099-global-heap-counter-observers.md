# TSR-0099 - Global Heap Counter Observers

Status: Monitoring
Date Opened: 2026-10-15
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: Path reliability resource and allocation qualification
Trigger Source: Chat request
Led To ADR(s): ADR-0309
Tags: memory, MemStats, TotalAlloc, allocations, runtime, observer

## RCA Summary

- What happened: two resource campaigns stopped on the accepted S6 baseline.
  A separate S7 enforce update batch reported one allocation in 100,000 calls.
- Why: disabling automatic GC does not make process-global memory counters
  silent. Captured heaps contain asynchronous runtime timer allocations. The
  source of the update batch's single allocation is not established.
- What fixed it: Approved v6 uses a conservative cumulative-allocation bound
  and exact AllocsPerRun(1) API controls. All replacement gates pass. The old
  single allocation remains unattributed and is preserved, not relabeled.
- How we know: native receipts, exact allocator-class deltas, retained heap
  stacks, frozen source/binaries and read-only raw revalidation. No candidate
  resource observation was reached in either earlier campaign. V6 completed
  the fixed 40 processes and all 20 paired guards without native retries.
- Operator/support answer: the replacement development gates pass. The older
  failures do not establish a production memory regression or delivery delay.
  No deployment, firewall change or full-cluster runtime capture occurred.

## Triggering Request

- Request date: 2026-10-15.
- Request summary: complete S7 after owner-approved S4 and S7 timing exceptions,
  preserving allocation, retained-memory and profile gates.
- Request reference: owner tokens Approved v4, Approved v5 and Approved v6;
  execution ledger.

## Symptoms and Impact

- Original resource observer expected only bucket/sidecar allocations and no
  frees. Its baseline observed an additional live 16-byte allocation.
- V5 permitted only zero or exactly one 16-byte runtime event, identical in both
  epochs, with its complete captured stack and no baseline budget credit.
- V5 baseline then observed a 32-byte allocation and a later 16-byte free.
  Expected new bucket bytes were 29,360,128. Immediate HeapAlloc delta was
  29,360,160; post-GC delta was 29,360,144. Bucket class 448 contained exactly
  65,536 new allocations. Class 32 added one malloc; class 16 added one free.
- Separately, same-reference off/shadow update allocation batches passed;
  enforce failed with allocation_delta=1 and operations=100000. Three half-life
  cases remained unrun. All 54 matched v5 profile processes completed.

## Timeline

1. 2026-10-15 - Original baseline resource observer rejected 16 extra bytes.
2. 2026-10-15 - Approved v5 authorized the exact bounded timer-event observer.
3. 2026-10-15 - New baseline rejected 32-byte allocation plus 16-byte free.
4. 2026-10-15 - All v5 profiles completed; the third update-allocation control
   stopped on one allocation. No unchanged retry was performed.
5. 2026-10-15 - Approved v6 replaced observer boundaries; its first baseline
   passed natively but the report regex rejected Windows CRLF output.
6. 2026-10-15 - A frozen transport-only adapter adopted that exact saved process,
   then ran only the remaining 39. All memory and replacement allocation gates pass.

## Hypotheses and Tests

1. Disabled automatic GC guarantees no unrelated process allocation.
   - Evidence: both native resource receipts and post-only runtime timer stacks.
   - Outcome: Rejected. Explicit GCs, timers and background runtime work remain.
2. Allowing only the first identified timer event makes the observer robust.
   - Evidence: new baseline's distinct class-32 malloc/class-16 free counters.
   - Outcome: Rejected. The approved narrow rule still correctly stops it.
3. The enforce batch's one allocation came from the runtime.
   - Evidence: no allocation profile for the failed unprofiled process.
   - Outcome: Inconclusive. Source/escape review does not locate its cause.
4. A monotonic allocation counter can conservatively bound new retained bytes.
   - Evidence: installed Go 1.27.1 runtime/mstats.go defines TotalAlloc as
     cumulative allocation bytes that do not decrease on frees.
   - Outcome: Implemented under Approved v6 and verified in all 20 paired
     guards; bounds 32.000000 to 32.317871 bytes per bucket, below 40.
     Candidate TotalAlloc minus verified newly allocated baseline owners gives
     the bound under the controlled ownership boundary. Warm owners, baseline
     noise and candidate frees give no credit.

## Findings

- Root cause: the resource observer assumed a quiet global runtime
  that the actual baseline does not provide. This is not a bucket-size failure.
- Contributing factors: a final profile identifies owners and sizes, not the
  time of allocation/free events. HeapAlloc can decrease when old objects die.
- The updater bracket includes benchmark timer operations as well as public API
  calls. Go initializes ResetTimer's metric map before the benchmark function;
  that map is not a demonstrated cause of this failure.
- Go's AllocsPerRun(1) provides one warm callback and one measured callback,
  sets GOMAXPROCS=1 for callbacks, restores the prior setting and cannot round
  away a single allocation. It still reads global Mallocs and does not attribute
  background work. The replacement establishes a controlled single-processor
  API gate; the old GOMAXPROCS=4 observation remains unexplained, and concurrent
  production allocation behavior remains unproved.

## Decision Linkage

- ADR created/updated: ADR-0309 retains the approved S7 support design and v5
  timing exception, with these unwaived qualification failures recorded.
- Decision delta summary: Approved v6 authorizes the observer replacements;
  no support/receiver/model change or memory-budget relaxation.
- Contract/behavior changes: none to production; no resource limit relaxation.

## Verification and Monitoring

- V5 completed: full S7 functional/race evidence, affected observer ordinary/race
  checks, analyzers, 54 raw-revalidated profile processes and two update batches.
- V6 completed: 40 resource processes/20 paired guards, six exact zero-allocation
  API controls plus six one-allocation witnesses in ordinary/race, 87 targeted
  cases per lane, 85 Linux/cgo cases, final analyzers and S7 local checkpoint.
- Not completed: whole-cluster runtime profiling. The old four-processor single
  allocation remains unattributed; serial replacement results do not explain it.
- Implemented falsifiers: 40*N versus 40*N+1, temporary allocations then frees,
  baseline/warm-owner credit, missing/decreasing counters, wrong/dead owners,
  incomplete work and one deliberately injected measured allocation.
- Rollback/stop trigger: any failed nonwaived gate. Preserve failures and use a
  separately reviewed/versioned observer; never silently retry or relabel them.

## References

- Commit(s): accepted S6 da1ab804095f1d79f1a961b2e09788a8df2a1420.
- Related ADR(s): ADR-0309.
- Related docs: docs/path-reliability-first-wave-execution.md;
  docs/path-reliability-runtime-profiling-plan.md.
- Local evidence: .tmp/path-reliability-first-wave/s7/resource/;
  s7/v5/resource/; s7/v5/resource-stop-analysis.json;
  s7/v5/profile-partial-audit.json and profiles/stopped-run.json;
  s7/v6/transport/accepted.json; s7/v6/profile-reuse-audit.json;
  s7/v6/v6-targeted-final-audit.json.
- V6 transport root cause: the decoded Output contained CRLF while the receipt
  regex accepted LF only. Normalizing decoded CRLF preserves strict lifecycle,
  counters and ownership checks; original native/JSON/profile files are unchanged.

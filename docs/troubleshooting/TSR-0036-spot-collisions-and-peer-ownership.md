# TSR-0036 - Spot Collisions and Peer Ownership

Status: Monitoring
Date Opened: 2026-10-02
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: dedup, spot, peer, internal/peerdiag, qualification
Trigger Source: Chat request
Led To ADR(s): ADR-0234, ADR-0235
Tags: pc92, dedupe, latency, allocation, sqlite

## RCA Summary

- What happened: Full runtime qualification missed local spot deliveries and
  exceeded latency limits. Enabled SQLite, context and diagnostic retirement
  still lacked complete ownership evidence.
- Why: Nine retained delivery failures were reproduced as 32-bit hash
  collisions. A later warm baseline found another primary collision. CPU
  profiles also identify expensive history maintenance; its contribution to
  tail latency has not yet been isolated.
- What fixed it: V15 replaces hash equality with exact existing key bytes and
  makes primary expiry/delete atomic. Context, helper-process and bounded
  SQLite ownership repairs are under implementation and validation.
- How we know: Literal collision tests, deliberately broken controls and
  targeted race tests pass. Original final-source qualification remains open.
- Operator/support answer: Passing protocol tests alone does not establish
  complete local delivery, sustained latency or the 480 MiB ownership ceiling.

## Triggering Request

- Request date: 2026-10-02
- Request summary: Consolidate remaining work, fix demonstrated failures and
  measure completion against the original acceptance criteria.
- Request reference: Approved v15 execution ledger.

## Symptoms and Impact

The retained v14 Q1 run lost nine tokens at every local recipient while peer
relay succeeded. The v15 diagnostic baseline offered 150,000 new keys over
15 minutes, then drained for 11 minutes. Each of 100 local clients missed IDs
19367 and 90004; peer recipients missed none. Its enqueue p99 upper bounds
were 31-35 ms and first-byte bounds 40-47 ms. These profiled diagnostic timings
cannot substitute for unprofiled acceptance.

## Timeline

1. 2026-10-02 - Original full Q1 failure retained and nine collision pairs
   reproduced from immutable input evidence.
2. 2026-10-02 - V15 approved; exact-key and cleanup regressions plus mutation
   controls passed on their identified source.
3. 2026-10-02 20:26 UTC - Warm baseline completed with all three CPU windows and
   900 one-second samples; failed delivery/latency verdict retained.

## Hypotheses and Tests

1. Local missing spots were peer topology losses.
   - Evidence: Exact peer-recipient delivery, zero drained cache occupancy,
     and literal local dedupe collision pairs.
   - Outcome: Rejected for the retained collision failures.
2. Hash equality was sufficient spot identity.
   - Evidence: Distinct encoded keys sharing each retained 32-bit hash;
     restoring hash-only equality makes regressions fail.
   - Outcome: Rejected.
3. Repeated formatting is the dominant warm CPU cost.
   - Evidence: The 660-780 second profile attributes 16.72 of 125.47 sampled CPU
     seconds to WhoSpotsMe.Record (15.92 to bucket scrub), and 9.55 to harmonic
     cleanup. Formatting/normalization functions individually contribute much
     less.
   - Outcome: Not supported by this profile; tail-latency causality is still
     inconclusive. Matched v16 runtime comparisons subsequently reduced those
     maintenance costs and delivered every token. The combined run passed all
     first-byte cohorts but still failed enqueue latency (worst minute8.6ms).
     Maintenance improvements alone did not establish overall acceptance.
4. A one-minute persistence diagnostic proves sustained queue stability.
   - Evidence: The prescribed 30-minute test filled the 64-request legacy queue
     after 142 seconds. A 60-second profile attributed 48.33% of sampled CPU to
     statement preparation; its 60 completed requests included the later drain.
   - Outcome: Rejected. Transaction-local statement reuse reduced the full
     projection from about1.38seconds to0.47seconds; a matched short rerun
     completed60 projections and60legacy requests without backlog. The Windows
     30-minute rerun subsequently committed all1,800 requests of each class;
     final-source native evidence remains required. The same Linux capacity
     binary failed its five-second deadline under TCG but passed in515.336ms
     with verified WHPX acceleration. Apparatus identity must accompany timing
     conclusions; this short pass does not replace sustained qualification.
5. Restoring an absent environment variable with PowerShell `$null` preserves
   its original state.
   - Evidence: On the observed PowerShell7.6.6/.NET10.0.12 host, the frozen
     qualification wrapper left14 previously absent controls present-empty.
     A completed warm run therefore failed the external caller-environment
     check before its paired run could start.
   - Outcome: Rejected. Explicit `[NullString]::Value` preserves absence.
     Six same-process success/failure cases now compare the entire caller
     environment and directory; all110 wrapper fixtures passed. Persist full
     presence/value hashes before launching a diagnostic. A missing original
     snapshot cannot be reconstructed from a later reproduction; retain the
     failed comparison and run a fresh pair.
6. Go's resolved path spelling necessarily preserves the previous SQLite
   driver's saved-file target.
   - Evidence: Actual Windows GUID and drive junctions exposed a different
     database selection for `junction\\..\\kept.db`, and refusals of accepted
     trailing-dot/space or literal extended-target names. The unchanged
     modernc observer and distinct saved sentinels established the targets.
   - Outcome: Rejected. The pinned prior Windows VFS uses lexical native
     normalization and lets the OS open follow reparse points. Restoring that
     behavior passed the saved-target, exact-dot and three-process WAL cases.
     A private reparse-reader draft was removed without an execution claim.
     Match the prior driver's actual file and sidecar semantics before choosing
     a path-resolution algorithm.
7. Matching ordinary Unicode paths establishes filename conversion parity.
   - Evidence: Actual URI byte sequences ED A0 80, E2 82 and F4 90 80 80 selected
     different empty candidate files instead of modernc's saved database.
     Valid Unicode, FF and C0 AF controls passed.
   - Outcome: Rejected. Windows native CP_UTF8 replacement grouping differs
     from Go's conversions for these sequences. The bounded native conversion
     correction passed the unchanged saved-file vectors and native admission,
     race/checkptr controls. Retain malformed-byte file-target oracles as well
     as printable-path tests; this pass does not close other DSN differences.
8. Guarding public SQL entry points is sufficient after a failed native release.
   - Evidence: Source review found deferred arena-overflow frees, fallback WAL
     private-memory frees and successful ROW/DONE dispatch that bypassed the
     new cleanup-failure checks. Windows temporary-lock release errors also
     disappeared before the callback boundary. A permitted pragma exceeding
     the 4 KiB arena reaches the deferred overflow path.
   - Outcome: Rejected by source inspection. The correction must cover
     in-flight unwind and successful return codes, keep skipped suballocations
     owned by the fixed engine extent, and propagate failed native release
     through every affected callback. A positive file close can establish
     release of its locks; an unconfirmed close keeps the owner charged.
     The corrected Windows owner/VFS/peer gates passed normal and race, with
     explicit checkptr and vet where applicable, including actual mid-operation
     close failure and constructor subprocesses. Source and file-set witnesses
     matched in `sqlite/windows-native-owners-20261003-b`. Tests that start
     with an already-failed connection alone cannot establish safe handling
     of a failure arising during an admitted operation. This component result
     does not close the separate DSN or final qualification gaps.

9. A bounded DSN also bounds every string copied by its SQLite callbacks.
   - Evidence: Short SQL expressions generated 600,000-byte URI fields and a
     pragma virtual-table argument inside the 8 MiB engine. Isolated child
     runs succeeded while allocation profiles attributed megabytes of host
     copies to `Memory.ReadString`. The retained baseline is in
     `sqlite/callback-backing`; these allocation totals are not simultaneous
     live-byte measurements. A later 1,100,000-byte child reached the private
     wrapper's one-million-byte NUL-scan limit within the engine allowance,
     panicked, and exposed an attached-file cleanup gap. The retained
     `before-large` artifacts distinguish this correctness failure from the
     earlier allocation-only falsifiers.
   - Outcome: Rejected. An input-byte limit cannot prove the bound of values
     produced by the engine. The callback inventory must follow generated
     values through dispatch, including unrecognized keys, before copying.
     A borrowed view still needs an engine-sized scan bound; removing its
     allocation alone does not make an artificial shorter scan safe.
     The bounded repair and native qualification remain tracked separately
     in the SQLite validation record.

10. Preserving the replacement wrapper's boolean parser preserves the previous
    topology driver's URI behavior.
    - Evidence: An isolated native file-control comparison against pinned
      modernc found six `psow` mismatches among 23 cases, including `01`, `256`,
      `512`, hexadecimal and overflowing integers. The observed flag controls
      SQLite's powersafe-overwrite assumptions. Saved sentinels remained intact;
      this test did not demonstrate database corruption.
    - Outcome: Rejected. Compatibility must use the previous driver as the
      oracle, not just the replacement wrapper's own parser. The private OS
      callback can reuse the pinned engine's URI-boolean implementation on its
      existing key pointer without copying the generated value or changing
      generic/public parser APIs. Exact before/after evidence remains in the
      SQLite validation record.

11. Killing a SQLite writer and successfully reading its WAL database proves
    that recovery writes can begin.
    - Evidence: Native05 returned `database is locked` on the recovery UPDATE.
      The fixture called Process.Kill but delayed Cmd.Wait until test cleanup.
      A live-writer control reproduced successful reads and integrity checks
      while a conflicting UPDATE still returned BUSY. After joining the killed
      PID, the same observer wrote immediately without a sleep, retry or
      extended timeout. Corrected Windows normal/race/checkptr and native06
      Linux normal/race runs passed all three commit phases.
    - Outcome: Rejected. Kill requests termination; Wait establishes its
      completion. Recovery tests must join the exact writer once before
      testing released locks. Keep the pending COMMIT response unread so an
      unacknowledged commit remains an old-or-new assertion. This confirms a
      fixture ordering defect and passing corrected recovery; it does not
      reconstruct every scheduling event in the historical failed process.

## Findings

- Root cause: Hash-only equality suppresses distinct encoded spot identities;
  each retained pair reproduces that failure through the actual dedupe owner.
- Root cause: History maintenance repeatedly scanned bucket populations or
  every harmonic callsign even when only one owner needed removal. Clean
  retained-binary benchmarks measured substantial reductions after the approved
  v16 algorithms, with unchanged allocations and no reproduced sparse-case
  regression. This does not establish the original end-to-end latency limits.

The original 42-byte primary and 32-byte secondary identities must remain the
equality keys; their hashes only choose shards. Cleanup must evaluate current
expiry and delete under the same lock so a refreshed key is not removed using
a stale decision. Preserving all bytes of the old encoding preserves its
intentional normalization/truncation policy; it does not invent a new spot
identity contract.

The additional warm primary pair is IDs 16953/19367, old hash `2ad54793`, with
different calls, frequencies and encoded minute timestamps. The retained
input ledger places their arrival gap at 14.3094923 seconds and encoded age at
60 seconds, within the existing 120-second primary window. UTC reconstruction
has a 16.919 ms interval plus wire uncertainty and assumes no unobserved wall
clock jump; there is no direct per-drop trace. The literal collision regression
and complete-token rerun are separate evidence obligations.

Resource reservations are not allocation proofs. A helper process does not
release ownership until it is joined, and failed native SQLite cleanup must
remain referenced and charged. Diagnostics must not call a potentially blocked
general logger on protocol or shutdown paths.

## Decision Linkage

- ADR created: ADR-0234 and ADR-0235.
- Decision delta: Exact cache equality, fixed context owners, bounded diagnostic
  companion, topology-only bounded SQLite ownership and behavior-preserving
  history maintenance with an explicitly bounded expiry index.
- Contract changes: Selected diagnostic loss/degradation and resource-exhausted
  topology startup refusal; inherited protocol behavior remains binding.

## Verification and Monitoring

- Slice evidence and final acceptance are tracked separately in the v15
  validation record. No complete allocation or production-readiness verdict is
  claimed here.
- Monitor complete recipient tokens, per-minute latency, helper dropped and
  unconfirmed counts, failed cleanup and SQLite ownership/refusal.
- A regression in identity, live topology authority, committed data, resource
  ownership or original acceptance prevents closeout.

## References

- [Approved v15](../pc18-pc92-scope-ledger-v15.md)
- [V15 validation](../pc92-v15-validation.md)
- [Dedup evidence](../pc92-v15-dedup-validation.md)
- [V16 history evidence](../pc92-v16-performance-validation.md)
- [SQLite evidence](../pc92-v15-sqlite-validation.md)
- [ADR-0234](../decisions/ADR-0234-peer-owned-resources-and-exact-spot-keys.md)
- [ADR-0235](../decisions/ADR-0235-spot-history-maintenance.md)
- Baseline commit: `2413beb47551d428d96d06a3f9178e2577d8ec9d`.
- External evidence: `D:\codex-gocluster-v15-20261002`.

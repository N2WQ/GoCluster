# Flood-control v2 execution record

Approval: owner supplied exact `Approved v2` on 2026-11-15 (America/New_York).
Workspace: `C:\src\gocluster-flood-config`, branch `codex/flood-config`.
Starting revision: `dedefb5e449755306a7f9424eab9ece2b56e0fa1` plus the completed
v1 owned diff. The other agent's `C:\src\gocluster` checkout is read-only to
this execution. No merge, push, deployment, restart or external connection is
authorized by this ledger.

## Approved queue

1. Checkpoint the completed configuration repair.
2. Native RBN/FT4/FT8/PSKReporter bypass the reporting-station (DECALL) quota
   only. Prove MANUAL/PEER/UPSTREAM separation and automatic rolling-window
   recovery, preserving local reputation checks and all other rails.
3. One eligible peer flood charge/verdict governs local admission and original
   onward relay. A peer-owned FIFO has at most 64 outstanding jobs, including
   builders and active work, and at most 8 MiB conservative backing charge.
   Preserve original fields, exact source identity, admitted work after source
   disconnect, and existing completed CTY/license-rejected transit without a
   flood charge. Cancellation/closure, queue/primary pressure and flood
   suppress/drop prevent relay. Cancel CTY waits and join validation before its
   dependencies retire. No per-job goroutine, generic Spot callback or side map.
4. Compare actual rolling behavior with a test-only replenishing allowance on
   identical ordered synthetic arrivals and independent state. Prepare bounded
   private capture conversion/replay and verify the procedure on loopback.
   Reuse a hash-checked ignored snapshot of the other agent's recorder without
   editing its source or adding a second tracked recorder. Event/input/count
   limits are 256 KiB / 256 MiB / 100,000; summaries remain bounded. Preserve
   timing uncertainty, unsent tails, partial evidence and unknown labels.
5. Complete integrated review, ordinary/race Windows lanes, analyzers, triggered
   fuzz/resource/profile checks, docs, code maps and production data handoff.

Production rolling policy, shipped numbers/actions, all required configuration
tables and current duplicate charging remain unchanged. Native DECALL table
values remain accepted but inactive. Other automated-source rails remain
available. Real-traffic threshold calibration, algorithm selection, duplicate
equivalence selection, transport rate limiting and activation remain deferred.

## Validation and stop conditions

Targeted checks are provisional until the complete selected final lane passes.
Use `scripts/test-windows.ps1` and stable native binaries. Full ordinary and race
lanes, vet, Staticcheck and lint are required; include distinct recipient socket
checks, exact counters, maximum-frame allocations, churn, cancellation, partial
startup and missing CTY. Inspect final source completeness and final diff.
Update operator/support docs and durable decisions as applicable. Generate and
check code maps after final source/document writes.

If a bound fails, redesign within it or return for revised scope. Do not enlarge
budgets, weaken checks, infer copy truth from equal fields or select operational
thresholds from generated traffic. Preserve failed evidence. Stop overlapping
work if unattributed edits appear. Checkpoint only coherent attributable units.

Pre-approval discovery and challenge used the lead and two read-only reviewers
with shared context; these are not independent non-steered reviews. Postapproval
falsifiability gates precede each affected implementation slice.

## Progress

- Slice 1: checkpoint `cc903987` contains the v1 configuration repair and its
  recorded validation. No v2 runtime implementation was included in that commit.
- Slice 2: detailed shared-context falsifiability review completed. Positive
  native quotas/all actions, retained other rails, conflicting human/mode
  flags, pairwise human isolation, actual resumed output without cleanup,
  unchanged duplicate charging, real runtime activity and local reputation
  checks cover the findings. No material scope gap. Checker refinements are
  implemented in `human_quota_test.go` and the actual runtime fixture. The new
  native tests failed against the old implementation before the bypass edit.
- Slice 2 targeted evidence: config/flood ordinary 958 cases; final flood
  owner rerun 57; runtime activity/local reputation 10; command reputation 13;
  focused flood/runtime race 67; all passed with zero skips. Targeted vet and
  YAML comment/value rigor passed. Shared-context actual-diff review found an
  accidental positive entry in the disabled test helper; restored zero and
  reran the affected owner checks. Final integrated checks remain pending.
- Before peer changes, the saved native peer binary ran the complete leased
  handler benchmark three times per short/max-one-token/max-many-token shape,
  100 ms each. Raw baseline is `peer-baseline-bench.log`; it includes actual
  full handler work, not just enqueue. No production latency claim follows.
- Slices 3-5: pending prerequisites and validation.

Raw evidence is private/ignored under `.tmp/flood-v2-validation/`. Prior v1
evidence remains under `.tmp/flood-config-validation/`.

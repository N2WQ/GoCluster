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
- Slice 3 implemented; lead actual-source review and shared-context reviewers
  dispositioned typed cancellation, charged builders, source tokens, FIFO wakeup,
  actual recipients, complete-path measurement and retirement-before-primary.
  Deferred relay workspace is charged inside the same 8 MiB budget. Selected
  peer/flood ordinary: 1,736 passed / 60 optional reference skips; focused race:
  496 passed / zero skips; final affected race: 39 passed / zero skips; final
  ordinary units: 20 passed. Final integrated checks remain pending.
- Plain full-path benchmark medians (baseline 100 ms / final one second, three
  samples each): short 19.0 / 30.9 us; one-token 4.82 / 3.94 ms; many-token
  13.78 / 12.73 ms. Short cost includes serial consumer completion. Large-shape
  timings overlap baseline variability; allocations essentially unchanged.
  The first final run included mutex/block profiling and is not comparable to
  unprofiled timings. Separate profiles show runtime/channel/test-completion
  waits; no production throughput or p99 verdict follows.
- Recorder relocated to `C:\src\gocluster-cc-user\scripts\telnet-recording-proxy.py`,
  same approved SHA-256. Concurrent work owns ADR-0352, so this branch's native
  record is now ADR-0353 to avoid an eventual merge collision; no behavior change.
- Slice 4 implemented in `internal/floodcorpus` and `cmd/flood_replay`; see the
  [private evidence runbook](runbooks/flood-evidence-capture.md). Import uses the
  existing native peer reader/parser/original-field admission without starting
  networking. A minimal read-only `LineReader.PendingInput` reports incomplete
  framing/Telnet tails. Capture and corpus bytes are bounded, privately retained,
  hash-bound and rechecked before manifest-last publication. UTC/offset interval,
  sent-prefix uncertainty, build revision/nullable dirty state and eligibility
  exclusions remain explicit. Reports bind the exact event hash.
- Actual loopback recorder-to-import-to-comparison ordinary/race checks initially
  passed 46 cases each, zero skips. Final interval/generated-phase refinements
  are included in the integrated rerun. The owned lab used no real credentials
  or remote endpoint; 269 server-direction bytes produced four PC11/61/26
  events, retained exact original byte copies and left duplicate harm unknown.
- The generated 30-arrival example and independent arithmetic vectors cover
  bursts, continued excess, exact cutoff and idle/slower recovery. Its labelled
  counterfactual recovers one logical post lost through copy charging, while
  distinct genuine equal-field posts demonstrate ambiguity. These fixtures
  establish the tool's behavior, not production harm or algorithm suitability.
- Shared-context Slice 4 source review found no blocking issue; its report-hash
  reproducibility observation was implemented. Lead reviewed final native byte,
  source partition, cap-oracle, timing, truth and arithmetic paths. This was
  shared-context review, not independent review.
- Qualification-tag compilation exposed a legacy test fixture's removed ingest
  field; it now uses the same test-only typed FIFO consumer as ordinary peer
  fixtures. No production fallback was added. Full lint identified an unused
  private return value/fixture parameter and direct EOF comparison; removed the
  unused values and used `errors.Is`, preserving behavior.
- Slice 5: final integrated qualification in progress.

Support documentation impact: operator and support behavior changed. Updated
`data/config/README.md`, `peer/README.md`, `docs/domain-contract.md`, durable
ADR-0353/0354, the private evidence runbook and `customgpt/source-map.md`.
No support Worker/API or workflow policy changes are required.

Raw evidence is private/ignored under `.tmp/flood-v2-validation/`. Prior v1
evidence remains under `.tmp/flood-config-validation/`.

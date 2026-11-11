# Overnight correctness work — Scope Ledger v1

## Authority and execution identity

The owner approved the chat's complete Scope Ledger v1 with the exact token
`Approved v1` before implementation. This file preserves that authority and
records execution; it does not authorize additional work.

- Planning baseline: `7417a99fb971`.
- Subsequent owner instruction: create and switch to `Nov-11`.
- Execution baseline: `b21528357ba7` on `Nov-11`. Its only additional commit is
  the completed HELP update; the approved production repair targets are unchanged.
- Isolated worktree: `C:\src\gocluster-overnight`. The original checkout remains
  at the same clean HELP commit on `main`; no owner changes were discarded.
- Window: 2026-11-10 23:20 through 2026-11-11 07:20 America/New_York
  (2026-11-11 04:20–12:20 UTC). Stop starting slices by 10:20 UTC or earlier
  when validation needs a larger reserve.
- Deliver an ordered validated subset, with deferred and unsuccessful items
  explicit. Correctness and evidence take priority over slice count.
- Keep results local. No merge, push, release, deployment, production service or
  database operation, firewall change, or unrelated HELP/workflow edit is authorized.

## Approved queue

| Slice | Bounded behavior | Status |
|---|---|---|
| S1 | Grid pending merge keeps locator/provenance coherent; actual overrides derived; preserve CTY, observation, timestamp and expiry rules | Targeted validated; final lane pending |
| S2 | Flood cleanup preserves threshold-plus-one live history and existing configured actions, modes, partitions, expiry and overflow behavior | Targeted validated; final lane pending |
| S3 | Cymru pending reservations follow admitted queue/worker ownership; failed admission retains no new identity; old completion cannot remove newer ownership | Queued |
| S4 | Documented propagation retention zero inherits the parent through the existing loader/normalizer and consumer | Queued |
| S5 | Periodic server keepalive emits exact CRLF through bounded raw control, outside membership locks; generic whitespace and blank-command behavior unchanged | Queued |
| S6 | Following S1, terminal grid-hook admission, lookup-before-writer joins, grid-only background cancellation/completion and store closure after all grid owners retire | Queued |
| S7 | Stable session worker affinity preserves ordinary broadcast order across membership churn; direct SELF remains separate and immediate | Queued |
| S8 | Optional measured DX normalization once per worker job and SELF match once per recipient using existing exact helpers, without new retained state | Queued, conditional |
| S9 | Optional temporal no-due return before allocation, with unchanged deadlines, force, order and model decisions | Queued, conditional |

S8–S9 require all feasible correctness work to pass and sufficient final-validation
reserve. S6 requires S1 to pass. One implementation slice writes at a time;
read-only investigation/review can proceed concurrently.

## Implementation and validation gates

For each slice: verify source and dependencies; derive observable failing cases;
add a red regression where feasible; implement the smallest complete repair;
run owner package ordinary/race and distinct integration/performance evidence;
review actual normal, failure, overload and retirement paths; then checkpoint.
Targeted validation is provisional until the final whole-branch lane passes.

Failed work is diagnosed within scope. If a new policy or material expansion is
required, preserve investigation, restore only the current slice's owned edits
to the last passing checkpoint, defer that slice/dependents, and continue safe
independent approved work. Do not waive checks or weaken tests.

Final relevant state requires full native Windows ordinary/race lanes,
`go vet ./...`, Staticcheck, golangci-lint, relevant transport/lifecycle/fuzz seed
checks, connected CustomSCP ordinary/race when runtime/delivery changes warrant
it, and before/after benchmarks/profiles for retained performance changes.
Update affected operator/support documentation and appropriate decision records;
generate/check code maps after final source/docs, review the actual diff, and
perform fresh final verification. Raw evidence lives in `.tmp/overnight-20261110/`.

## Exclusions and limits

Do not change reputation cache-cap meaning, enrichment wait policy, history
decoding/concurrency, batching/GC/worker defaults, CW caching, envelope ownership,
peer parsing, model/scientific semantics, or previously persisted grid records.
Small private helpers and deterministic operation seams are permitted; public
API redesign, broad reorganization and general testing frameworks are excluded.

Fixed queues and ownership-coupled deletion do not certify a process memory
ceiling. Grid hooks gain synchronization; stable affinity can leave skew after
departures; preserving flood history retains the intended live window. Measure
relevant costs. Existing synchronous storage and arbitrary callbacks remain
owned until return under ADR-0275; the work window is not a hung-disk shutdown
deadline. Blocked OS execution is a reported gap, not authority to modify policy.

## Review and falsifiability record

Three fresh read-only design investigations and a separate scope challenge were
completed before approval. Material findings were dispositioned: periodic sends
must occur outside membership locks; writer provenance regression must force
one pending batch; all grid maintenance/recovery owners must join; exact DX
normalization helpers must be reused; ordered subset replaces an inconsistent
prefix promise. The periodic-only raw-control refinement was re-challenged.

The lead owns implementation, source completeness, validation, disposition and
final claims. Review evidence is not transferred authority. No scientific/model
validity change or claim is part of this work.

### S1 detailed contract-to-test gate

| Contract | Distinguishing stimulus | Observable oracle | False-green risk | Checker/owner |
|---|---|---|---|---|
| Locator and provenance move together | Derived then actual; CTY-only then derived; actual then derived | Literal expected grid and flag, not a production-derived expectation | Direct store calls bypass broken pending merge | Cluster owner tests of pending merge and real writer/reopened store |
| Actual precedence survives batching | Conflicting records in one pending window | Reopened actual grid is unchanged and non-derived | Timer flush accidentally separates records | Long flush interval plus controlled producer completion and final drain |
| Other merge fields remain intact | CTY-only records, observations, out-of-order times and expiry | Explicit CTY/count/min-first/max-update/expiry values | Assertions cover only locator | Cohesive owner semantic cases |
| Existing lookup identity remains intact | Base calls and CTY-derived miss | Existing lookup results remain identical | New tests alone omit callers | Existing grid lookup and gridstore owner packages, ordinary/race |

## Execution evidence

- 04:20 UTC: verified clean `Nov-11`, current AGENTS.md and unchanged target
  source relative to the planning baseline. No production changes made yet.
- Native prerequisites: Go 1.27.1 windows/amd64, CGO enabled, Staticcheck 2026.2.1,
  golangci-lint 2.14.0. Isolated stable-path build preparation started.
- Initial preparation invocation passed a comma-list as one external PowerShell
  argument; discovery correctly refused it. Corrected to an explicit array in
  native PowerShell `-Command`; no test or source failure was involved.
- Baseline owner suites: `gridstore`, `floodcontrol`, `reputation`, `config`;
  661 passed cases, zero skips. Evidence: `baseline-owner-tests.log`.
- S1 red: unchanged extracted coalescer failed all three tuple orderings in
  helper and same-batch persistence tests (`s1-red.log`).
- S1 ordinary: `-Packages ./internal/cluster -Run
  '^Test(MergePendingGridRecord|GridWriterPendingProvenancePersistence|GridLookup)'`;
  15 passed cases, zero skips (`s1-green.log`).
- S1 race: corresponding cluster/gridstore selection plus existing durable
  precedence/merge cases; 17 passed cases, zero skips (`s1-race.log`).
- S1 source completeness: lead comparison and a separate read-only reviewer
  found no material issue. Extracted production writer retains batching, TTL,
  error/drop, metrics and final-flush behavior. TSR-0119 and support routing
  explain the repaired boundary and exclude old-data repair. Troubleshooting
  index validation and `git diff --check` passed. Final broad lane remains open.
- S2 detailed falsifiability: fixed-clock controller process/cleanup tests cover
  thresholds above one, all three actions, repeated cleanup, separate source
  thresholds, inclusive cutoff/expiry deletion and each selected DX mode.
  Existing full package tests retain forwarding/overflow/dedup coverage. Tests
  synchronously inspect actual decisions and output, not a copied prune algorithm.
- S2 red: all new cleanup regressions exposed live-history loss (`s2-red.log`).
  Full owner ordinary/race lanes each passed 17 cases, zero skips after repair
  (`s2-green.log`, `s2-race.log`). Lead source review verified the sole behavior
  change is cleanup's partition-specific retained count; existing mutex, action,
  cutoff, actor cap and arrival paths are unchanged. TSR-0120 preserves ADR-0062.

## Closeout

Pending. No implementation or validation pass is claimed by this initial record.
Support-agent documentation impact: Required for implemented operator/config/
connection behavior; exact routing changes and ADR/TSR disposition follow slices.

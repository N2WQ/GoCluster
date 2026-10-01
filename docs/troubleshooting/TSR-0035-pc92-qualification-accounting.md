# TSR-0035 - PC92 Qualification Accounting

Status: Monitoring
Date Opened: 2026-10-01
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: peer, telnet, internal/cluster
Trigger Source: Chat request
Led To ADR(s): ADR-0230
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

## Decision Linkage
- ADR created/updated: ADR-0230.
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

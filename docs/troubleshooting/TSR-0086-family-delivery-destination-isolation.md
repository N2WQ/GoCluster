# TSR-0086 - Family Delivery Destination Isolation

Status: Monitoring
Date Opened: 2026-10-11
Date Resolved: n/a
Owner: Codex
Technical Area: final output gates and historical admission
Trigger Source: Chat request
Led To ADR(s): none
Tags: family, telnet, archive, peer, history

## RCA Summary

- What happened: a telnet family-hiding decision also cleared normal FAST,
  MED and SLOW fanout permissions, losing otherwise eligible ring, archive and
  local peer output.
- Why: the display-only decision overwrote shared secondary gates. The main
  stabilizer path also treated hidden telnet output as still-held ownership.
- What fixed it: keep shared destination gates independent of family display
  hiding, and use normal fanout eligibility after stabilizer ownership resolves
  when admitting history.
- How we know: the loaded-config production-constructor regression separately
  checks actual TCP, original peer tuples, raw archive bytes, reopened archive
  and historical storage. Baseline and checkpoint evidence are recorded in
  the execution record.
- Operator/support answer: family hiding does not remove eligible spots from
  saved history or peers. Secondary MED refusal and actual stabilization holds
  still govern those destinations.

## Triggering Request

- Request date: 2026-10-11.
- Request: approved delivery scope v1, F1 telnet-only destination isolation.
- Reference: [scope ledger](../call-correction-delivery-scope-ledger-v1.md).

## Symptoms and Impact

After a slash-family seed, the old display heuristic could hide a bare member
from all normal destinations. With the stabilizer enabled, that immediate or
overflow release also skipped eligible recent-band, CustomSCP and WHOSPOTSME
admission. A true pending hold must still wait for its release owner.

## Timeline

1. Inspection found shared flags cleared in `buildFinalDeliveryPlan` and
   `!telnetDeliverNow` used as a proxy for unresolved stabilizer ownership.
2. F1 reproduction, repair and complete checkpoint follow the ordered
   [execution record](../call-correction-delivery-execution.md).

## Hypotheses and Tests

1. Family hiding is telnet-only already: rejected by the shared flags used by
   ring, archive and peer fanout.
2. Every hidden spot is still held: rejected; immediate and queue-overflow
   releases can be hidden after ownership has already resolved.
3. Removing all destination gates is necessary: rejected. Keep the shared
   secondary gates, source/format admission, real holds and risky timeout policy.

## Findings

- Root cause: one output-only display decision erased shared destination
  permissions, and one history predicate conflated hiding with pending ownership.
- Legacy history and CustomSCP require separate recording oracles because
  production selects one concrete historical owner.
- Peer checks compare original input fields; local display decisions do not
  change the source tuple forwarded to peers.

## Decision Linkage

- ADR created/updated: none; restore the telnet-only contract in
  [ADR-0014](../decisions/ADR-0014-call-correction-family-policy.md).
- Contract changes: repair destination isolation while preserving the existing
  stabilizer timing and MED gates in
  [ADR-0148](../decisions/ADR-0148-post-stabilizer-shared-secondary-fanout.md).
- Fresh percentage arbitration is the separate F2 checkpoint.

## Verification and Monitoring

See the execution record for baseline failures, final commands and results.
Monitor stabilizer holds, overflow releases and destination/history coverage.
These checks establish fanout and admission integrity, not RF identity or
calibrated accuracy.

## References

- Tests: `internal/cluster/family_delivery_destinations_test.go`.
- Code: `internal/cluster/output_pipeline_delivery.go`.
- Peer contract: [ADR-0284](../decisions/ADR-0284-original-content-peer-publication.md).

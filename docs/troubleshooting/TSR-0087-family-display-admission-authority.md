# TSR-0087 - Family Display Admission Authority

Status: Monitoring
Date Opened: 2026-10-11
Date Resolved: n/a
Owner: Codex
Technical Area: family arbitration and telnet broadcast admission
Trigger Source: Chat request
Led To ADR(s): ADR-0295
Tags: family, fresh-evidence, broadcast, per-policy-history

## RCA Summary

- What happened: family display history could hide a later spot after the
  purported earlier rival was refused by the broadcast queue. Specificity and
  cached support could also hide a freshly stronger base/shorter identity.
- Why: comparison mutated history before queue admission, skipped rival checks
  on repeats, and used one cached support/time/frequency across delivery lanes.
- What fixed it: the approved F2 decision uses fresh shared-population evidence
  and commits compact lane witnesses only after nonblocking queue admission.
- How we know: baseline reproduction and final test/benchmark evidence are
  recorded separately in the delivery execution record.
- Operator/support answer: an earlier queue offer and usable fresh evidence
  must justify hiding in each delivery policy. Missing or protected evidence
  shows the call; local display hiding preserves saved and original peer output.

## Triggering Request

- Request date: 2026-10-11.
- Request: implement the approved F2 display policy autonomously after F1.
- Reference: [scope ledger](../call-correction-delivery-scope-ledger-v1.md).

## Symptoms and Impact

An attempted stronger slash spot can be dropped before any ordinary offer,
yet its saved entry can suppress a later base spot. FAST-only offers can also
hide MED/SLOW output, and refresh or frequency changes in one lane can alter
another lane's protection. No queue admission result was available to the
caller. These are display/accounting failures, not evidence of RF accuracy.

## Timeline

1. Read-only source and test review identified comparison-time mutation,
   same-key bypass and attempted-broadcast history.
2. F1 independently repaired shared destination/history isolation.
3. F2 reproduction and complete validation follow the ordered
   [execution record](../call-correction-delivery-execution.md).

## Hypotheses and Tests

1. An attempted broadcast proves an ordinary offer: rejected by the bounded
   queue's nonblocking refusal path; test actual refusal followed by TCP output.
2. Specificity or cached support proves a current winner: rejected; use one
   eligible population with dissent and global split/uncertainty/loss protection.
3. One entry timestamp proves all delivery policies: rejected; expiry,
   frequency and literal identity belong to each admitted lane separately.

## Findings

- Root cause: attempted publication mutated cached family state before actual
  queue admission; cached support and one cross-lane witness supplied authority.

History admission and current scoring are different evidence owners. Keep the
earlier compact offer witness, but calculate both current shares afresh.
Hidden/refused attempts cannot refresh history or remove rivals; a changed
population can legitimately restore an older admitted rival's authority.

## Decision Linkage

- ADR: [ADR-0295](../decisions/ADR-0295-fresh-admitted-family-display.md).
- Decision delta: greater fresh whole percentage wins, slash wins only a
  usable slash/base tie, and successful ordinary policy offers own protection.
- Preserve relation/configuration switches, shared destination gates, SELF,
  source-original peer content, existing percentage arithmetic and formats.

## Verification and Monitoring

See the execution record for actual baseline results, targeted ordinary/race
checks, deterministic concurrency checks, benchmarks and the complete lane.
Monitor existing broadcast drops and delivery behavior; queue admission is
not per-client receipt. Production latency, wider association and calibrated
call accuracy remain unverified.

## References

- Code: `internal/cluster/telnet_family_suppressor.go`,
  `internal/cluster/output_pipeline_delivery.go`, `telnet/server.go`.
- Tests: `internal/cluster/family_admission_delivery_test.go`,
  `internal/cluster/family_admission_qualification_test.go`,
  `internal/cluster/telnet_family_suppressor_test.go`.
- Related: [TSR-0086](TSR-0086-family-delivery-destination-isolation.md),
  [ADR-0284](../decisions/ADR-0284-original-content-peer-publication.md).

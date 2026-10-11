# TSR-0082 - FT Expired Burst Admission

Status: Monitoring
Date Opened: 2026-10-11
Date Resolved: n/a
Owner: Codex
Technical Area: FT confidence, arrival expiry, delivered glyphs
Trigger Source: Chat request
Led To ADR(s): ADR-0283
Tags: FT8, FT4, FT2, expiry, confidence, bounded state

## RCA Summary

- What happened: a late receiver upgraded expired two-receiver P spots to V.
- Why: Observe admitted reports without retiring groups already due; the timer
  and drain boundary did not constrain admission.
- What fixed it: all-due retirement before admission, with exclusive expiry and
  synchronous shared release on the existing output owner.
- How we know: compiled baseline boundary probes and normal/DIAG TCP, sealed
  ring and reopened archive failures; passing checkpoint evidence is recorded
  in the linked execution record.
- Operator/support answer: reports at or after quiet/hard expiry belong to a
  new burst. A recent-band S floor may remain; old receiver counts do not.

## Triggering Request

- Request date: 2026-10-10
- Request summary: five bounded method patches, Approved v1; FT expiry exclusive.
- Request reference: approved method execution record.

## Symptoms and Impact

At baseline `3b828d5`, two held FT8 receivers followed by a third arriving exactly
at quiet expiry delivered V for all three. Old output should remain P, with the
new singleton ? or its existing recent-band S floor. PC61 preserves call/receiver
and order but has no confidence-glyph field, so it is not a glyph oracle.

## Timeline

1. Existing-API probes reproduced equality and strictly late admission defects.
2. The first TCP fixture excluded FT8 in its default filter; a RESUME reply also
   differed because spots were not paused. Those fixture failures are excluded
   from wire evidence. Real command replies and subsequent DX receipts resolved
   the fixture before production changes.
3. P4 implemented the shared all-due retirement/release path and boundary tests.
4. Final checkpoint results are recorded in the execution record.

## Hypotheses and Tests

1. Delayed drain lets late support change old output: confirmed by the admitted
   baseline TCP/ring/reopened-archive glyph mismatches.
2. Retiring only the current key is sufficient: rejected; all-due, unrelated-head
   and capacity-reclamation tests cover the whole bounded pending population.
3. The capped benchmark's initial failure proved missing release: rejected;
   GetCount measures total writes, not retained slots. The corrected oracle checks
   30000 writes and 16 retained spots separately.

## Findings

- Root cause: arrival admission and timer retirement enforced different expiry
  boundaries, permitting expired membership to grow.
- Review also found scratch capacity discarded on an empty heap; preserving it
  avoids extra bounded allocations introduced by admission-side draining.

## Decision Linkage

- ADR created/updated: ADR-0283; ADR-0060 records the accepted refinement.
- Decision delta summary: enforce exclusive expiry at admission as well as drain.
- Contract/behavior changes: late reports start a new generation; due groups
  release once before current output continues. Timing/defaults/caps are unchanged.

## Verification and Monitoring

- Validation steps run: exact commands/results in the execution record.
- Monitor FT burst delivery, live pending limits and measured output-loop latency.
- Local checks establish eligibility and count integrity, not signal association,
  calibrated probability, field accuracy or production capacity.

## References

- Related ADR(s): [ADR-0283](../decisions/ADR-0283-ft-admission-expiry.md)
- Related docs: [execution record](../call-correction-method-execution.md)
- Related tests: FT admission, delivery, capacity and churn regressions.

# TSR-0016 - FT Confidence Arrival Window Misses Live Corroboration

- Status: Superseded
- Date opened: 2026-04-08
- Status date: 2026-04-08

## RCA Summary

- What happened: FT output still showed essentially no `P` or `V` glyphs after
  PSKReporter frequency canonicalization made dial frequencies consistent.
- Why: The fixed two-second arrival hold was too short for real multi-spotter
  FT decode-arrival dispersion, and RBN digital timestamps lacked second-level
  slot precision.
- What fixed it: ADR-0059 introduced slot-aware grouping with mode-specific
  slot grace windows, but that approach was later superseded by TSR-0017 and
  ADR-0060 when live evidence showed slot anchors still fragmented events.
- How we know: Live samples showed large exact FT groups spread over 15-20
  seconds, source inspection found the fixed two-second due time, and tests
  covered the slot-aware timing change before supersession.
- Operator/support answer: Treat this record as historical context for the
  rejected slot-aware FT timing step; use TSR-0017/ADR-0060 for the current FT
  burst-clustering model.

## Trigger
Operators reported that FT `P`/`V` confidence tags were still absent after PSKReporter FT frequency canonicalization had been implemented.

## Symptoms and impact
- FT spots showed canonical dial frequencies, confirming the frequency-semantics fix was active.
- Live output still produced `?` and `S`, but essentially no `P` or `V`.
- This made the new FT corroboration rail operationally incomplete even though the glyph feature was present.

## Hypotheses tested
1. Client filters or dedupe were hiding valid FT `P`/`V` glyphs.
2. Frequency fragmentation was still splitting corroborators across buckets.
3. The fixed arrival-based corroboration hold was too short for real FT decode-arrival dispersion.

## Evidence
- Local live probe on `localhost:8300` showed canonical FT dial frequencies in output, so ADR-0058 was active.
- A 25 second live sample produced `?=1499`, `S=508`, `P=0`, `V=0`.
- Exact canonical-frequency FT groups were present but spread far beyond the current 2 second hold:
  - `MW6CCG|FT8|14074.00` = 24 spotters over 17.4 seconds
  - `JR1CBC|FT8|21074.00` = 22 spotters over 15.6 seconds
  - `IT9MRM|FT8|18100.00` = 21 spotters over 17.4 seconds
  - `JE1VOL|FT4|14080.00` = 13 spotters over 20.0 seconds
- Code inspection in `ft_confidence_runtime.go` confirmed the live controller still used a fixed `now + 2s` due time per group.
- Code inspection in `rbn/client.go` confirmed RBN digital FT timestamps only preserve `HHMM`, not second-level timing.

## Root cause or best current explanation
The original FT corroboration controller grouped by arrival-time bursts instead of transmission slots. Once frequency semantics were corrected, the remaining fragmentation came from real multi-spotter decode-arrival jitter exceeding the fixed 2 second hold. RBN FT8/FT4 required an additional arrival-time fallback because their parsed source timestamps lack second precision.

## Fix or mitigation
- Replace the fixed arrival-based hold with slot-aware FT timing:
  - FT8 slot end + 6 seconds grace
  - FT4 slot end + 3 seconds grace
  - FT2 slot end + 2 seconds grace
- Key corroboration by DX call, exact FT mode, canonical dial frequency, and transmission slot.
- Use arrival-time slotting for RBN FT8/FT4 and observed-time slotting for PSKReporter/local FT sources.
- Raise pending-state bounds while preserving fail-open behavior.

## Why an ADR was or was not required
- ADR required because:
  - this changed durable FT confidence timing semantics, user-visible latency, and the grouping key used by the main output pipeline.

## Links
- Related ADRs:
  - `docs/decisions/ADR-0057-ft-confidence-corroboration.md`
  - `docs/decisions/ADR-0058-pskreporter-ft-frequency-canonicalization.md`
  - `docs/decisions/ADR-0059-slot-aware-ft-confidence-timing.md`
- Related issues/PRs/commits:
  - pending
- Related tests:
  - `ft_confidence_runtime_test.go`
- Related docs:
  - `README.md`

# TSR-0017 - FT Confidence Slot Anchors Still Split Live Corroboration

- Status: Resolved
- Date opened: 2026-04-08
- Status date: 2026-04-08

## RCA Summary

- What happened: FT `P` and `V` tags were still absent after the slot-aware
  timing model from ADR-0059 was implemented.
- Why: PSKReporter observed times and RBN minute-only timestamps did not form a
  stable shared slot identity, so same-event corroborators still fragmented
  across slot keys; FT spots also reached resolver placeholder confidence before
  the FT burst rail.
- What fixed it: ADR-0060 replaced slot identity with bounded arrival-burst
  clustering, tuned mode-specific quiet gaps and hard caps, bypassed
  resolver/temporal placeholder confidence for FT modes, enabled cross-source
  corroboration, and added FT burst observability.
- How we know: Live samples still had `P=0`/`V=0`, exact FT groups were present,
  and replay-style simulation showed a bounded arrival-burst model would
  produce practical corroboration.
- Operator/support answer: For current FT corroboration issues, inspect burst
  clustering counters and source timing behavior, not the superseded slot-anchor
  model.

## Trigger
Operators reported that FT `P` and `V` tags were still absent after the slot-aware corroboration model from ADR-0059 had been implemented.

## Symptoms and impact
- FT spots already showed canonical dial frequencies, confirming the ADR-0058 frequency fix was active.
- Live output still produced only `?` and `S`, so the advertised FT corroboration feature remained operationally incomplete.
- The slot-aware timing change increased hold latency without delivering the intended live promotion quality.

## Hypotheses tested
1. The running server had not actually been rebuilt or restarted with the new FT code.
2. Client filtering or dedupe was hiding valid FT `P`/`V` glyphs.
3. The remaining defect was the slot-anchor model itself, not the burst timing constants.

## Evidence
- The running `gocluster.exe` on `localhost:8300` had been rebuilt and restarted after the slot-aware source edits, so this was not just a stale binary.
- An 8 second live sample produced:
  - `? = 387`
  - `S = 124`
  - `P = 0`
  - `V = 0`
- Large exact `DX + mode + canonical dial` FT groups were present in live traffic, including:
  - `JA4LXY|FT8|21074.00 count=26 span=14.6s`
  - `4X1NX|FT8|21074.00 count=23 span=7.6s`
  - `SV1JMC|FT8|18100.00 count=18 span=13.6s`
  - `JR4HCQ|FT8|21074.00 count=15 span=7.6s`
- A replay-style simulation over a captured 30 second telnet sample showed that a bounded arrival-burst model would have produced practical corroboration on the same traffic:
  - `? = 248`
  - `P = 38`
  - `V = 16`

## Root cause or best current explanation
Slot identity was the wrong primary abstraction for live FT corroboration in this pipeline. PSKReporter observed times and RBN-digital minute-only timestamps did not provide a stable shared slot anchor, so same-event corroborators still fragmented across slot keys even after dial-frequency canonicalization. After the burst model replaced slot identity, live tracing also showed one implementation bug and one tuning gap:
- FT modes were still reaching the resolver rejection path, which stamped placeholder confidence before the FT corroboration stage and caused the burst rail to skip those spots entirely.
- The first burst timing guess (`FT8 2s/18s`, `FT4 1s/9s`, `FT2 750ms/6s`) was still too short for the observed live arrival dispersion.

## Fix or mitigation
- Replace slot-aware grouping with bounded arrival-burst clustering keyed by normalized DX call, exact FT mode, and canonical dial frequency.
- Extend bursts while corroborators continue arriving within a mode-specific quiet gap and force-release them at a hard cap.
- Tune those bounds from live arrival dispersion instead of the initial guess:
  - FT8 `6s` quiet gap / `12s` hard cap
  - FT4 `5s` / `10s`
  - FT2 `3s` / `6s`
- Explicitly bypass resolver/temporal placeholder confidence assignment for FT modes before the burst rail runs.
- Allow cross-source corroboration between PSKReporter and RBN-digital when the burst key matches and reporter calls differ.
- Add FT burst observability so live tuning can be measured directly.

## Why an ADR was or was not required
- ADR required because:
  - this changed the durable FT grouping model, user-visible latency semantics, and overview observability contract.

## Links
- Related ADRs:
  - `docs/decisions/ADR-0058-pskreporter-ft-frequency-canonicalization.md`
  - `docs/decisions/ADR-0059-slot-aware-ft-confidence-timing.md`
  - `docs/decisions/ADR-0060-source-aware-ft-burst-clustering.md`
- Related issues/PRs/commits:
  - pending
- Related tests:
  - `ft_confidence_runtime_test.go`
  - `stats/tracker_ft_test.go`
- Related docs:
  - `README.md`

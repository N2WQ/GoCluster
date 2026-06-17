# TSR-0015 - PSKReporter FT Frequency Semantic Mismatch

- Status: Resolved
- Date opened: 2026-04-08
- Status date: 2026-04-08

## RCA Summary

- What happened: FT `P`/`V` corroboration did not appear in live traffic even
  after the FT confidence rail was enabled.
- Why: PSKReporter FT spots used observed RF frequencies, while RBN digital and
  the mode-seed table used canonical dial frequencies; this split identical FT
  events across different keys.
- What fixed it: ADR-0058 added a shared FT dial-frequency registry,
  canonicalized PSKReporter FT2/FT4/FT8 operational frequencies at ingest, and
  preserved raw observed RF separately for archive use.
- How we know: Live telnet probing ruled out client filters, source inspection
  found direct use of the PSKReporter frequency field, and tests covered FT
  frequency mapping, ingest, and archive preservation.
- Operator/support answer: For missing FT corroboration, first check whether
  PSKReporter FT frequencies are canonical dial frequencies; nearby observed RF
  values fragment corroboration.

## Trigger
Operators reported that FT `P`/`V` corroboration glyphs were not appearing in live traffic even after the bounded FT-confidence rail was enabled.

## Symptoms and impact
- Live FT spots from PSKReporter showed varying RF frequencies instead of stable dial frequencies.
- FT corroboration groups split across nearby reported frequencies, reducing `P`/`V` promotions.
- The digital mode-frequency map risked learning many unnecessary FT buckets from PSKReporter traffic, weakening the intended dial-frequency semantics.

## Hypotheses tested
1. The client filter or dedupe policy was hiding `P`/`V` output.
2. The FT hold window alone was too short.
3. PSKReporter FT frequency semantics were incompatible with the rest of the pipeline.

## Evidence
- Live telnet probing confirmed `W1UE` was not filtering `P`/`V`.
- Code inspection showed PSKReporter ingest used the single payload frequency field directly in `pskreporter/client.go`, while RBN digital ingest parsed dial frequency from the telnet line.
- Live samples showed identical FT bursts arriving over several seconds with nearby-but-different PSKReporter frequencies.
- `spot/mode_infer.go` and `ft_confidence_runtime.go` both key off the operational `Spot.Frequency`, so the ingest semantic mismatch propagated across the pipeline.

## Root cause or best current explanation
PSKReporter FT spots were entering the system with observed RF frequency semantics, while RBN digital spots and the configured mode-seed table assumed canonical dial frequencies. That semantic mismatch fragmented FT corroboration and polluted dial-based inference buckets.

## Fix or mitigation
- Add a shared FT dial-frequency registry derived from `mode_inference.digital_seeds`.
- Canonicalize PSKReporter FT2/FT4/FT8 operational frequency at ingest using that registry.
- Preserve the raw observed PSKReporter RF frequency separately on the spot and in archive records.

## Why an ADR was or was not required
- ADR required because the fix changes operator-visible FT frequency semantics, archive schema/versioning, and shared ingest behavior across multiple packages.

## Links
- Related ADRs:
  - `docs/decisions/ADR-0057-ft-confidence-corroboration.md`
  - `docs/decisions/ADR-0058-pskreporter-ft-frequency-canonicalization.md`
- Related issues/PRs/commits:
  - pending
- Related tests:
  - `spot/ft_frequency_test.go`
  - `pskreporter/client_test.go`
  - `archive/record_test.go`
- Related docs:
  - `README.md`

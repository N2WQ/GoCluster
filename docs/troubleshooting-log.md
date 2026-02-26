# Troubleshooting Log

This index tracks troubleshooting records (`TSR-XXXX`) that can lead to ADRs.

## How To Use
- Create new troubleshooting records in `docs/troubleshooting/` using `TSR-TEMPLATE.md`.
- Add one row per TSR to the table below.
- If a TSR results in a durable decision, add/create the ADR and cross-link both records.
- Do not delete old TSR rows; mark status updates in place.

## TSR Index
| TSR | Title | Status | Date | Area | Led To ADR | Links |
|---|---|---|---|---|---|---|
| TSR-0001 | <title> | Open | YYYY-MM-DD | <area> | - | `docs/troubleshooting/TSR-0001-<slug>.md` |
| TSR-0002 | Per-Spot Call Correction Split-Evidence Ambiguity and Quality Penalty Reinforcement | Resolved | 2026-02-23 | spot/correction, main/telnet fan-out | ADR-0021 | `docs/troubleshooting/TSR-0002-call-correction-ambiguity-and-quality-penalty.md` |
| TSR-0003 | Phase 2 Signal Resolver Shadow-Mode Design and Validation Plan | Open | 2026-02-23 | spot/correction, main output pipeline | ADR-0022 | `docs/troubleshooting/TSR-0003-phase2-signal-resolver-shadow-design.md` |
| TSR-0004 | Resolver Primary Switchover Mode and Rollback Contract | Resolved | 2026-02-25 | main output pipeline, spot/correction, config | ADR-0026 | `docs/troubleshooting/TSR-0004-resolver-primary-switchover-mode.md` |
| TSR-0005 | Resolver-Primary Recall Gap on Truncation Families and Overstated V Glyph in Contested States | Resolved | 2026-02-26 | main output pipeline, spot/correction | ADR-0032 | `docs/troubleshooting/TSR-0005-resolver-primary-family-recall-and-contested-glyph.md` |
| TSR-0006 | Resolver Neighborhood Competition and Contested Edit-Neighbor Rails | Resolved | 2026-02-26 | main output pipeline, correctionflow, telnet suppression, replay, config | ADR-0033 | `docs/troubleshooting/TSR-0006-resolver-neighborhood-and-edit-neighbor-contested-rails.md` |
| TSR-0007 | Resolver-Primary One-Short Misses and Conservative Recent +1 Corroborator Rail | Resolved | 2026-02-26 | main output pipeline, spot/correction, correctionflow, replay, config | ADR-0034 | `docs/troubleshooting/TSR-0007-resolver-recent-plus1-corroborator-rails.md` |
| TSR-0008 | Resolver Neighborhood Forced-Split Regression From Unrelated Adjacent Buckets | Resolved | 2026-02-26 | internal/correctionflow, main output pipeline, replay, config | ADR-0035 | `docs/troubleshooting/TSR-0008-resolver-neighborhood-unrelated-adjacent-split-regression.md` |
| TSR-0009 | Resolver Reliability/Confusion Parity Gap and Winner Tie-Break Integration | Resolved | 2026-02-26 | main output pipeline, cmd/rbn_replay, cmd/callcorr_reveng_rebuilt, internal/correctionflow, spot/signal_resolver | ADR-0036 | `docs/troubleshooting/TSR-0009-resolver-reliability-confusion-parity-gap.md` |

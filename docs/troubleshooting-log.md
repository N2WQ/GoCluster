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

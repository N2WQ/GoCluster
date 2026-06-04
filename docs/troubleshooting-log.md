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
| TSR-0025 | P50 Merge CPU And Heap Pressure | Resolved | 2026-06-04 | pathreliability, retained state, hot path, profiling | ADR-0139 | `docs/troubleshooting/TSR-0025-p50-merge-cpu-and-heap-pressure.md` |
| TSR-0024 | Custom SCP Test Time Horizon | Resolved | 2026-05-23 | spot, custom_scp, tests | ADR-0136 | `docs/troubleshooting/TSR-0024-custom-scp-test-time-horizon.md` |
| TSR-0023 | Path Capped Count Gate Overload | Resolved | 2026-05-11 | pathreliability, config, telnet diagnostics, propagation logs | ADR-0134 | `docs/troubleshooting/TSR-0023-path-capped-count-gate-overload.md` |
| TSR-0022 | Path Receiver Cap Lifetime Count | Resolved | 2026-05-10 | pathreliability, retained state | ADR-0130 | `docs/troubleshooting/TSR-0022-path-receiver-cap-lifetime-count.md` |
| TSR-0021 | Command-Like Login CTY Admission | Resolved | 2026-05-04 | spot, CTY, telnet, ingest, peer, replay | ADR-0110 | `docs/troubleshooting/TSR-0021-command-like-login-cty-admission.md` |
| TSR-0002 | Per-Spot Call Correction Split-Evidence Ambiguity and Quality Penalty Reinforcement | Resolved | 2026-02-23 | spot/correction, main/telnet fan-out | ADR-0021 | `docs/troubleshooting/TSR-0002-call-correction-ambiguity-and-quality-penalty.md` |
| TSR-0003 | Phase 2 Signal Resolver Shadow-Mode Design and Validation Plan | Open | 2026-02-23 | spot/correction, main output pipeline | ADR-0022 | `docs/troubleshooting/TSR-0003-phase2-signal-resolver-shadow-design.md` |
| TSR-0004 | Resolver Primary Switchover Mode and Rollback Contract | Resolved | 2026-02-25 | main output pipeline, spot/correction, config | ADR-0026 | `docs/troubleshooting/TSR-0004-resolver-primary-switchover-mode.md` |
| TSR-0005 | Resolver-Primary Recall Gap on Truncation Families and Overstated V Glyph in Contested States | Resolved | 2026-02-26 | main output pipeline, spot/correction | ADR-0032 | `docs/troubleshooting/TSR-0005-resolver-primary-family-recall-and-contested-glyph.md` |
| TSR-0006 | Resolver Neighborhood Competition and Contested Edit-Neighbor Rails | Resolved | 2026-02-26 | main output pipeline, correctionflow, telnet suppression, replay, config | ADR-0033 | `docs/troubleshooting/TSR-0006-resolver-neighborhood-and-edit-neighbor-contested-rails.md` |
| TSR-0007 | Resolver-Primary One-Short Misses and Conservative Recent +1 Corroborator Rail | Resolved | 2026-02-26 | main output pipeline, spot/correction, correctionflow, replay, config | ADR-0034 | `docs/troubleshooting/TSR-0007-resolver-recent-plus1-corroborator-rails.md` |
| TSR-0008 | Resolver Neighborhood Forced-Split Regression From Unrelated Adjacent Buckets | Resolved | 2026-02-26 | internal/correctionflow, main output pipeline, replay, config | ADR-0035 | `docs/troubleshooting/TSR-0008-resolver-neighborhood-unrelated-adjacent-split-regression.md` |
| TSR-0009 | Resolver Reliability/Confusion Parity Gap and Winner Tie-Break Integration | Resolved | 2026-02-26 | main output pipeline, cmd/rbn_replay, cmd/callcorr_reveng_rebuilt, internal/correctionflow, spot/signal_resolver | ADR-0036 | `docs/troubleshooting/TSR-0009-resolver-reliability-confusion-parity-gap.md` |
| TSR-0010 | Stabilizer Delaying V Glyph Spots Against Intended Delay Eligibility | Resolved | 2026-03-01 | internal/correctionflow stabilizer policy, main/replay parity, docs | ADR-0046 | `docs/troubleshooting/TSR-0010-stabilizer-v-glyph-delay-eligibility.md` |
| TSR-0011 | Peering Enabled Flag Overridden by Host/Port Auto-Enable | Resolved | 2026-03-03 | config, peer | ADR-0047 | `docs/troubleshooting/TSR-0011-peering-enabled-flag-override.md` |
| TSR-0012 | PC92 Hop Suffix Amplification and Overlong Diagnostics Gap | Resolved | 2026-03-05 | peer/protocol, peer/manager, peer/reader | ADR-0050 | `docs/troubleshooting/TSR-0012-pc92-hop-suffix-amplification-and-overlong-gap.md` |
| TSR-0013 | FT2 Mode Token and Operator-Surface Gap | Resolved | 2026-03-27 | spot, pskreporter, filter, telnet, commands, docs | ADR-0055 | `docs/troubleshooting/TSR-0013-ft2-mode-token-and-operator-surface-gap.md` |
| TSR-0014 | Local Self-Spot Delay and Confidence Semantics | Resolved | 2026-03-27 | commands, main output pipeline, telnet, custom_scp, docs | ADR-0056 | `docs/troubleshooting/TSR-0014-local-self-spot-delay-and-confidence.md` |
| TSR-0015 | PSKReporter FT Frequency Semantic Mismatch | Resolved | 2026-04-08 | pskreporter ingest, spot, archive, mode inference, FT confidence | ADR-0058 | `docs/troubleshooting/TSR-0015-pskreporter-ft-frequency-semantic-mismatch.md` |
| TSR-0016 | FT Confidence Arrival Window Misses Live Corroboration | Superseded | 2026-04-08 | main output pipeline, FT confidence, docs | ADR-0059 | `docs/troubleshooting/TSR-0016-ft-confidence-arrival-window-misses-live-corroboration.md` |
| TSR-0017 | FT Confidence Slot Anchors Still Split Live Corroboration | Resolved | 2026-04-08 | main output pipeline, FT confidence, stats, docs | ADR-0060 | `docs/troubleshooting/TSR-0017-ft-confidence-slot-anchor-fragmentation.md` |
| TSR-0018 | Peer Bulletin Duplicate Fanout | Resolved | 2026-04-19 | peer, telnet fan-out, config | ADR-0063 | `docs/troubleshooting/TSR-0018-peer-bulletin-duplicate-fanout.md` |
| TSR-0019 | MODE UNKNOWN Filter Feedback | Resolved | 2026-04-22 | filter, telnet, docs | - | `docs/troubleshooting/TSR-0019-mode-unknown-filter-feedback.md` |
| TSR-0020 | RBN Mode Column Confusion | Resolved | 2026-04-26 | rbn, parser, replay, call correction | ADR-0087 | `docs/troubleshooting/TSR-0020-rbn-mode-column-confusion.md` |

# Decision Log

This index tracks all architecture and workflow decisions recorded as ADRs.

## How To Use
- Create new ADRs in `docs/decisions/` using `ADR-TEMPLATE.md`.
- Add one row per ADR to the table below.
- Do not delete old ADR rows; mark status as `Superseded`/`Deprecated` when needed.

## ADR Index
| ADR | Title | Status | Date | Area | Supersedes | Superseded By | Links |
|---|---|---|---|---|---|---|---|
| ADR-0001 | <title> | Proposed | YYYY-MM-DD | <area> | - | - | `docs/decisions/ADR-0001-<slug>.md` |
| ADR-0002 | UI v2 Render Pipeline Optimization | Accepted | 2026-02-07 | ui/tview-v2 | - | - | `docs/decisions/ADR-0002-ui-v2-render-pipeline.md` |
| ADR-0003 | Call Correction Anchor Gating and Config-Driven Determinism | Accepted | 2026-02-07 | spot/correction | - | - | `docs/decisions/ADR-0003-call-correction-anchor-gating.md` |
| ADR-0004 | Slash-Variant Precedence and Canonical Grouping in Call Correction | Accepted | 2026-02-08 | spot/correction | - | - | `docs/decisions/ADR-0004-call-correction-slash-precedence.md` |
| ADR-0005 | Mode-Specific Spotter Reliability Selection for Call Correction | Accepted | 2026-02-13 | spot/correction | - | - | `docs/decisions/ADR-0005-mode-specific-spotter-reliability.md` |
| ADR-0006 | Confusion-Model Tie-Break Ranking for Call Correction | Accepted | 2026-02-13 | spot/correction | - | - | `docs/decisions/ADR-0006-confusion-model-tie-break-ranking.md` |
| ADR-0007 | Call-Correction Top-K Evaluation, Strict Prior Bonus, and Decision Counters | Accepted | 2026-02-13 | spot/correction | - | - | `docs/decisions/ADR-0007-call-correction-topk-prior-bonus-observability.md` |
| ADR-0008 | Call Correction Recent-On-Band Bonus (Min-Reports Only) | Accepted | 2026-02-14 | spot/correction | - | - | `docs/decisions/ADR-0008-call-correction-recent-band-bonus.md` |
| ADR-0009 | Call Correction Stacked Prior + Recent Bonus for Min-Reports | Accepted | 2026-02-14 | spot/correction | ADR-0007 (prior-bonus scope) | - | `docs/decisions/ADR-0009-call-correction-stacked-prior-recent-bonus.md` |
| ADR-0010 | S Glyph Confidence Floor Includes Recent-On-Band Admission | Accepted | 2026-02-16 | main/spot confidence | - | - | `docs/decisions/ADR-0010-s-glyph-recent-on-band-floor.md` |
| ADR-0011 | SHOW DX / SHOW MYDX Optional DXCC Selector | Accepted | 2026-02-17 | commands/processor | - | - | `docs/decisions/ADR-0011-show-history-dxcc-selector.md` |
| ADR-0012 | CC Dialect Accepts SHOW DX / SH DX History Aliases | Accepted | 2026-02-17 | commands/processor | - | - | `docs/decisions/ADR-0012-cc-show-dx-alias.md` |
| ADR-0013 | Telnet Stabilizer for Risky Call-Correction Output | Accepted | 2026-02-17 | main/telnet fan-out | - | - | `docs/decisions/ADR-0013-call-correction-telnet-stabilizer.md` |
| ADR-0014 | Call-Correction Family Policy: Slash Threshold, Truncation Advantage Rail, and Telnet Family Suppression | Accepted | 2026-02-22 | spot/correction, main/telnet fan-out | - | - | `docs/decisions/ADR-0014-call-correction-family-policy.md` |
| ADR-0015 | YAML-Driven Call-Correction Family Policy Knobs | Accepted | 2026-02-23 | config, spot/correction, main/telnet fan-out | ADR-0014 (config surface) | - | `docs/decisions/ADR-0015-call-correction-family-policy-yamlization.md` |
| ADR-0016 | Family-Aware Recent/Stabilizer Keys, Suppressor Edge Guard, and Truncation Bonus Rails | Accepted | 2026-02-23 | main/telnet fan-out, spot/correction, config | - | - | `docs/decisions/ADR-0016-call-correction-family-aware-recent-and-delta2-rails.md` |
| ADR-0017 | Shared Daily Scheduler Parsing and Rate-Counter Semantics | Accepted | 2026-02-23 | internal/schedule, internal/ratelimit, main, uls, reputation, pskreporter, ingest | - | - | `docs/decisions/ADR-0017-scheduler-ratelimit-unification.md` |
| ADR-0018 | Filter Allow/Block Mutation Helper and Telnet Domain Handler Cleanup | Accepted | 2026-02-23 | filter, telnet/filter command engine | - | - | `docs/decisions/ADR-0018-filter-mutation-and-telnet-handler-consolidation.md` |
| ADR-0019 | Shared Spot Distance Core and Periodic Cleanup Runner | Accepted | 2026-02-23 | spot/correction, spot cleanup lifecycle | - | - | `docs/decisions/ADR-0019-spot-distance-core-and-cleanup-runner.md` |
| ADR-0020 | Multi-Check Telnet Stabilizer Retries With Bounded Re-Delay | Accepted | 2026-02-23 | main/telnet fan-out, config, stabilizer scheduler | ADR-0013 (single delayed check behavior) | - | `docs/decisions/ADR-0020-stabilizer-max-checks-retries.md` |
| ADR-0021 | Call-Correction Split-Evidence Ambiguity Guard and Validated Non-Winner Quality Penalty Rail | Accepted | 2026-02-23 | spot/correction | - | - | `docs/decisions/ADR-0021-call-correction-ambiguity-guard-and-quality-rail.md` |
| ADR-0022 | Phase 2 Signal Resolver Shadow Mode | Proposed | 2026-02-23 | spot/correction, main output pipeline, stats | - | - | `docs/decisions/ADR-0022-phase2-signal-resolver-shadow-mode.md` |
| ADR-0023 | Consolidate Historical Analysis into Replay Tool | Accepted | 2026-02-24 | cmd/rbn_replay, analysis workflow, docs | - | - | `docs/decisions/ADR-0023-replay-analysis-tool-consolidation.md` |
| ADR-0024 | Replay Dual-Method Recall/Stability Metrics | Accepted | 2026-02-24 | cmd/rbn_replay, replay evidence artifacts | - | - | `docs/decisions/ADR-0024-replay-dual-method-stability-metrics.md` |
| ADR-0025 | Replay Run History and Local Comparison Ledger | Accepted | 2026-02-24 | cmd/rbn_replay, replay operations | - | - | `docs/decisions/ADR-0025-replay-run-history-and-local-comparison.md` |

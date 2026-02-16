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

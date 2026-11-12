# TSR-0128: Shared Geography Startup

Status: Monitoring
Date Opened: 2026-11-11
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: startup, H3, GRID, NEARBY, CustomSCP, replay
Trigger Source: Troubleshooting chat
Led To ADR(s): ADR-0344

## RCA Summary

- What happened: prediction-off live startup and replay CustomSCP could lack H3 mappings.
- Why: initialization belonged to the predictor switch rather than shared consumers.
- What fixed it: required bounded load-only startup independent of prediction.
- How we know: new cold-process checks failed against the original live/replay
  startup sources and passed after shared initialization; exact evidence is in
  the [execution record](../cache-geography-repair-v6.md).
- Operator/support answer: repair both named configured files; runtime does not
  generate them, and disabling prediction does not remove this requirement.

## Root cause or best current explanation

Live startup initialized shared H3 maps only when path prediction was enabled.
Replay never initialized them. Valid grids therefore converted to invalid cells
in cold processes; CustomSCP observations could retain zero geographic cells.
Manually initializing global maps in consumer tests concealed startup omissions.

## Fix or mitigation

Every live startup now requires configured res1/res2 inputs; enabled replay
CustomSCP has the same requirement before downloads or scratch mutation. Search
the startup error for `h3_table_path`, `res1.bin` and `res2.bin`. Resolve relative
paths from the launch working directory. Restore both packaged files; startup
does not generate or rewrite them. See [ADR-0344](../decisions/ADR-0344-required-shared-geography.md).

Prediction remains disabled when configured off. This repairs future geography
availability, not existing persisted zero-cell observations or historical results.

## Decision Linkage

ADR-0344 replaces ADR-0153's predictor-dependent H3 requirement only. The owner
selected startup failure for invalid shared geography. H3 geometry, IDs, table
meaning and existing persisted records retain their contracts.

## Checker diagnosis

The exact original live and replay startup sources failed the new cold child
checks with missing usable geography. Private prediction-off YAML also needs
both fallback flags disabled to satisfy its existing config contract. The live
display fixture must select FT8 explicitly because shipped new-user modes omit
it, then wait for the rule publication response rather than server input echo
before broadcasting. A filtered or prematurely offered line cannot prove glyph
behavior. These are fixture corrections, not changes to product filtering.

## Verification boundary

Fresh joined children exercise actual startup owners without prewarming. They
check GRID/NEARBY, distinct-versus-same-cell accepted CustomSCP evidence and
disabled prediction. Loader fixtures cover bounded oversized/growing readers,
read/open/close failures, aggregate errors, captured bytes after removal and
once-owned initialization sequences. The execution record owns actual commands
and observed outcomes; these local tests do not establish deployed accuracy,
whole-process memory ceilings or production latency.

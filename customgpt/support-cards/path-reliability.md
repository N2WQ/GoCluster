# Support Card: Path Reliability Troubleshooting

## Match

Use when a telnet user or node operator asks about blank or weak path glyphs,
path thresholds, `SET PATHSAMPLES`, `SET DIAG PATH`, receiver diversity, H3
tables, grid handling, or noise penalties.

## First Safe Check

Treat threshold edits as a last step. First identify the observed symptom and
inspect user-visible diagnostics:

- confirm the user's grid with `SET GRID`
- inspect `SET PATHSAMPLES`
- use `SET DIAG PATH` when detailed path evidence is needed
- check effective path YAML and H3 table availability for operator-side issues

## Must Include

- A blank path glyph can mean insufficient, low-count, low-weight, stale, or
  capped evidence, not necessarily a bad path.
- Thresholds mix operator policy and algorithm calibration; do not retune them
  as the first normal troubleshooting step.
- Effective YAML matters before suggesting config edits.

## Must Avoid

- Do not call a blank glyph a bad path.
- Do not recommend changing thresholds before confirming symptom, diagnostics,
  and effective YAML.

## Sources

- `customgpt/troubleshooting-index.md`
- `README.md`
- `docs/OPERATOR_GUIDE.md`
- `pathreliability/README.md`
- `data/config/PATH_PREDICTIONS.md`
- `data/config/README.md`

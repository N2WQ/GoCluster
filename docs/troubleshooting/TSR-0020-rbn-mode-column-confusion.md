# TSR-0020: RBN Mode Column Confusion

- Status: Resolved
- Date opened: 2026-04-26
- Status date: 2026-04-26

## RCA Summary

- What happened: RBN history rows with nonblank RF mode were admitted even when
  the RBN spot-class field represented classes such as `DX` rather than the
  intended skimmer observation classes.
- Why: The implementation modeled `tx_mode` as RF mode but ignored the separate
  RBN `mode` spot-class field, so admission used RF-mode presence instead of
  class acceptance.
- What fixed it: ADR-0087 added an RBN spot-class model, admitting `CQ`,
  `BEACON`, and `NCDXF B`, dropping `DX`/blank/unknown, and tagging accepted
  beacon classes.
- How we know: CSV field inspection, observed class values, parser review, and
  RBN/replay/rebuilt-replay tests validated the class contract.
- Operator/support answer: When RBN mode looks wrong, distinguish RF
  `tx_mode` from RBN spot class `mode`; do not treat the two fields as
  interchangeable.

## Trigger
RBN history data showed two fields containing the word "mode". The CSV `tx_mode` field carries the RF/transmission mode such as `CW` or `RTTY`, while the CSV `mode` field carries spot classes such as `CQ`, `DX`, `BEACON`, and `NCDXF B`.

## Symptoms and impact
The ingest path was filtering on nonblank RF mode, not on RBN spot class. That allowed `DX` class rows to enter RBN-derived processing and did not consistently tag `BEACON` / `NCDXF B` rows as beacons from the class field.

Operationally, this could pollute call-correction evidence with RBN history rows that are not the intended skimmer observation class.

## Hypotheses tested
1. `mode` is the RF mode.
   - Rejected. The values are spot classes (`CQ`, `DX`, `BEACON`, `NCDXF B`).
2. `tx_mode` is the RF mode.
   - Accepted. It is the field that specifies `CW` / `RTTY`.
3. Nonblank RF mode is sufficient RBN admission.
   - Rejected. Spot class must also be admitted.

## Evidence
- RBN history CSV fields include both `mode` and `tx_mode`.
- Observed `mode` values are `CQ`, `DX`, `BEACON`, `NCDXF B`, and blank.
- Code inspection showed CSV parsers read `tx_mode` into row mode and ignored the spot-class `mode` field.
- Live RBN parsing accepted any non-minimal spot with explicit RF mode and nonzero report.

## Root cause or best current explanation
The implementation correctly used `tx_mode` for RF mode in CSV replay, but it did not model the separate RBN spot-class field. Admission was therefore based on RF-mode presence instead of the RBN class contract.

## Fix or mitigation
Add an RBN spot-class model and require accepted classes before ingest:

- accept `CQ`, `BEACON`, and `NCDXF B`
- drop `DX`, blank, and unknown classes
- preserve `tx_mode` / explicit RF mode as `Spot.Mode`
- tag accepted `BEACON` and `NCDXF B` as beacons

## Why an ADR was or was not required
- ADR required because RBN spot-class admission is a durable ingest contract with operator-visible spot and replay-count consequences.

## Links
- Related ADRs: `docs/decisions/ADR-0087-rbn-spot-class-admission.md`
- Related issues/PRs/commits:
- Related tests: `rbn/parse_spot_test.go`, `cmd/rbn_replay/rbn_history_test.go`, `cmd/callcorr_reveng_rebuilt/main_test.go`
- Related docs: `rbn/README.md`, `docs/rbn_replay.md`

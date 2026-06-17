# TSR-0013 - FT2 Mode Token and Operator-Surface Gap

- Status: Resolved
- Date opened: 2026-03-27
- Status date: 2026-03-27

## RCA Summary

- What happened: Spots containing `FT2` could enter some paths but were not
  consistently parsed, filtered, or documented as an explicit supported mode.
- Why: The shared human-style comment parser and operator-facing mode/filter
  surfaces lacked `FT2`, while PSKReporter could already preserve explicit
  configured mode strings.
- What fixed it: ADR-0055 added `FT2` to the shared parser token table, filter
  supported modes, CC mode shortcuts, help/usage text, and PSKReporter
  regression coverage.
- How we know: Source inspection showed parser/operator-surface gaps, and
  tests covered manual DX, human telnet parsing, PSKReporter, filters, commands,
  and telnet usage behavior.
- Operator/support answer: If `FT2` behaves inconsistently, check both the
  shared parser and MODE/filter/help surfaces; PSKReporter config support alone
  is not enough.

## Trigger
A deployed node began surfacing spots containing `FT2`, prompting investigation into where the mode could enter the system and whether the cluster should support it explicitly.

## Symptoms and impact
- Human/manual spot comments containing `FT2` did not parse as an explicit mode.
- Operators could configure PSKReporter to admit `FT2`, but filter/help surfaces did not expose `FT2` as a supported mode.
- This created inconsistent behavior across ingest, filtering, and operator documentation.

## Hypotheses tested
1. `FT2` was being synthesized by the shared mode-inference pipeline.
2. `FT2` was entering only through PSKReporter or historical archive records.
3. `FT2` was present in human-style comments and the shared parser/operator surfaces simply lacked first-class support.

## Evidence
- Shared comment parsing in `spot/comment_parser.go` recognized `FT8`/`FT4` but not `FT2`.
- Manual DX, peer PC11/PC61, and human telnet ingest all depend on that shared comment parser for explicit mode extraction.
- PSKReporter already preserved explicit mode strings when allowed by config, so the larger gap was parser/operator-plane parity rather than MQTT transport logic.
- `filter.SupportedModes`, CC `SET/<MODE>` shortcuts, and help/usage strings omitted `FT2`.

## Root cause or best current explanation
`FT2` was missing from the shared explicit-mode token table and from operator-facing mode/filter surfaces. As a result, human-style comments carrying `FT2` could not emit an explicit `FT2` mode, and operator tooling lagged behind explicit-mode ingest support.

## Fix or mitigation
- Added `FT2` to the shared comment parser token table.
- Added regression tests for manual DX and minimal human telnet parsing of `FT2`.
- Added `FT2` to filter supported modes, CC mode shortcuts, help text, and telnet usage strings.
- Added PSKReporter regression coverage for explicit `FT2` when config allows it.

## Why an ADR was or was not required
- ADR required because this changes shared parser behavior and the operator-visible mode/filter contract across multiple packages.

## Links
- Related ADRs: `ADR-0055`
- Related issues/PRs/commits: pending
- Related tests:
  - `spot/comment_parser_test.go`
  - `commands/processor_test.go`
  - `rbn/minimal_parser_test.go`
  - `pskreporter/client_test.go`
  - `filter/filter_test.go`
  - `telnet/server_filter_test.go`
- Related docs:
  - `README.md`

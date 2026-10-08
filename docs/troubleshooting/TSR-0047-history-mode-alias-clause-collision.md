# TSR-0047 - History Mode Alias Clause Collision

Status: Monitoring
Date Opened: 2026-10-08
Date Resolved: n/a
Owner: repository maintainer
Technical Area: commands, telnet, history parser
Trigger Source: Chat request
Led To ADR(s): ADR-0263
Tags: mode aliases, taxonomy, clause precedence, comma lists

## RCA Summary

- What happened: valid CW variants BAND/MODE were rejected as singleton history
  selections, while adding a trailing comma made them work.
- Why: history clause detection ran before mode normalization and treated every
  standalone keyword as a boundary, irrespective of its value position.
- What fixed it: require commas between history values and interpret supported
  keyword aliases as values first or after a comma; otherwise retain clause
  precedence and repeated-category rejection.
- How we know: the real-loader regression failed on the original parser at
  `SHOW DX MODE BAND`, then passed with exact CW/FT8 row and boundary assertions.
- Operator/support answer: use comma-separated BAND/MODE history lists. With CW
  variants BAND/MODE, `SHOW DX MODE BAND` and `SHOW DX MODE MODE` select CW.
  `MODE CW MODE FT8` is still a repeated clause; use `MODE CW,FT8` for a list.

## Triggering Request

- Request date: 2026-10-08
- Request summary: fix the configured mode alias collision and add a regression;
  the owner subsequently selected comma-required history lists and Approved v2.
- Request reference: review of
  [e132615](https://github.com/N2WQ/GoCluster/commit/e132615c77f6325a87972d6a35d891c08bd5f3b3).

## Symptoms and Impact

- The shipped taxonomy has no BAND/MODE aliases; ordinary shipped searches were
  unaffected by the collision.
- Custom supported aliases failed only in the history grammar, including DX/MYDX
  command aliases. A trailing comma bypassed exact-token boundary recognition.
- Requiring commas intentionally removes space-separated multi-value history
  syntax; saved PASS/REJECT syntax and taxonomy configuration are unchanged.

## Timeline

1. 2026-10-08 - Review reported singleton alias rejection on e132615.
2. 2026-10-08 - Current source confirmed clause detection preceded normalization.
3. 2026-10-08 - Owner selected comma-required lists and approved revised scope v2.
4. 2026-10-08 - Real-loader regression reproduced the old failure and passed after
   the parser correction; targeted command/telnet tests passed.

## Hypotheses and Tests

1. Taxonomy rejects the aliases.
   - Evidence: `spot.LoadTaxonomyFile` accepted the temporary CW variants
     `[BAND, MODE]`; its variant mapping does not reserve clause keywords.
   - Outcome: Rejected.
2. Exact-token clause detection masks valid mode values.
   - Evidence: the old parser failed `TestHistoryBandModeConfiguredAliases` at
     `SHOW DX MODE BAND` with a missing MODE selection; its trailing-comma token
     did not match the keyword detector.
   - Outcome: Supported.
3. Accepting aliases alone adequately defines list/clause boundaries.
   - Evidence: `MODE CW MODE FT8` can denote a repeated clause or an alias-bearing
     space-separated list. The owner required commas to remove that ambiguity.
   - Outcome: Rejected.

## Findings

- Root cause: unconditional keyword boundary recognition before normalization.
- Contributing factor: original tests used shipped mode names and aliases and
  did not load a taxonomy containing clause-keyword aliases.
- The delimiter compatibility change is a durable parser decision recorded in
  ADR-0263, rather than an implementation-only bug fix.

## Decision Linkage

- ADR created: [ADR-0263](../decisions/ADR-0263-history-list-comma-boundaries.md).
- Decision delta: comma-required lists with position-dependent keyword aliases.
- Contract changes: history space-separated multi-value lists now reject. Query
  matching, saved-filter narrowing, COMMENT ownership and NEXT remain intact.

## Verification and Monitoring

- Executed local evidence: original regression failure, corrected regression
  pass, and targeted command/telnet history and COMMENT input tests.
- On native Windows Go 1.27.1, `go test ./... -count=1`, `go vet ./...`,
  `staticcheck ./...` and `golangci-lint run ./... --config=.golangci.yaml`
  passed. `go test -race ./commands ./telnet -count=1` passed with CGO enabled.
- Three 15-second, four-worker fuzz runs passed: `FuzzHistoryCommand`,
  `FuzzHistoryBandModeSelectionResults` and `FuzzCommentInput`. All 29 Python
  fixtures passed using `.tmp/telnet-test-venv/Scripts/python.exe` (the default
  interpreter lacked PyYAML). Python parser, local Markdown link,
  troubleshooting-record, code-map freshness and diff whitespace checks passed.
  Logs are in `.tmp/history-selection-v2-validation/`.
- Monitor custom-taxonomy searches after deployment; no live-server test is
  implied by the deterministic regression.
- Rollback trigger: deployed comma-delimited searches select incorrect modes or
  valid clause combinations fail; rollback restores the prior grammar and its
  documented singleton keyword-alias defect.

## References

- Commit reviewed: e132615c77f6325a87972d6a35d891c08bd5f3b3.
- Related ADRs: [ADR-0262](../decisions/ADR-0262-history-band-mode-selections.md), ADR-0263.
- Related docs: [history commands](../../commands/README.md#archive-history),
  [telnet validation](../telnet-command-validation.md).

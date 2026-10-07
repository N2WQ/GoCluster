# TSR-0041 - Exact-Call History And Archive Scan Cap

Status: Monitoring
Date Opened: 2026-10-06
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: commands, telnet, archive history
Trigger Source: Chat request
Led To ADR(s): ADR-0251
Tags: exact-call, archive-history, pagination, bounded-work, continuation

## RCA Summary

- What happened: A full-call history selector could select an entire CTY entity,
  and a narrow search could silently omit matches older than 200,000 newer
  archive records.
- Why: The selector resolved through CTY to ADIF, while `RecentFiltered` bounded
  a global timestamp scan without exposing budget exhaustion or continuation.
- What fixed it: Exact normalized DX identity classification after canonical
  labels, plus bounded pages over the existing primary timestamp range. Each
  page reports its boundary and offers a connection-owned older continuation.
- How we know: Current source separates selector classification, archive paging
  and connection publication. The production-cap archive regression persists a
  raw legacy `K1ABC-1` record behind more than 200,000 unrelated newer records;
  continuation reaches its materialized `K1ABC` identity. Local archive tests
  and race checks passed; production behavior has not been confirmed.
- Operator/support answer: Check the selector, filters and retention first. Use
  the latest returned `SHOW DX NEXT H1...` command on the same connection when
  older search remains. A work-limit page is incomplete, not proof of no matches.

## Triggering Request

- Request date: 2026-10-06.
- Request summary: Close the exact-call history gap without requiring realtime
  archive search or adding an index and migration lifecycle.
- Request reference: Approved implementation scope from the troubleshooting chat.

## Symptoms and Impact

- A call selector could include other stations sharing its ADIF entity.
- Sparse exact-call or filtered history could appear empty despite deeper
  retained matches, because the legacy reader scanned only a capped recent range.
- Archive errors could be rendered as ordinary empty history.
- The affected surface is SHOW DX/MYDX and their supported aliases; live spot
  ingestion, archive writes and cleanup do not require a new storage format.

## Timeline

1. 2026-10-06 - Source inspection distinguished entity resolution from stored
   station identity, and identified the silent global scan cap.
2. 2026-10-06 - The user selected explicit incomplete pages and continuation,
   then removed the realtime/selective-index requirement.
3. 2026-10-06 - Bounded primary-range paging and connection-owned continuation
   were implemented with local archive regression and race validation.

## Hypotheses and Tests

1. Full valid calls already selected only that station.
   - Evidence: The former selector path composed client filters with resolved
     ADIF, rather than exact materialized DX identity.
   - Outcome: Rejected; entity selection was broader than an exact station.
2. Increasing the ordinary result count ensured deeper complete searches.
   - Evidence: The legacy scan had an independent 200,000-record ceiling. Its
     existing deep-scan test contained only 12,000 records.
   - Outcome: Rejected; result count did not remove the scan ceiling.
3. Closing the gap required a callsign index and backfill.
   - Evidence: A bounded page returns an exclusive timestamp/sequence position;
     subsequent pages can reach deeper retained rows without new persisted state.
   - Outcome: Rejected under the selected non-realtime requirement. Repeated
     pages still scan unrelated records; no selective-query performance claim is
     made.

## Findings

- Root cause: Entity-wide selector resolution and an unreported scan boundary
  were two separate correctness problems.
- Contributing factors: Empty-result presentation concealed read errors and
  budget exhaustion; older archive identities normalize when materialized.
- Durable decision: Exact selection, continuation ownership, retention and
  failure reporting require ADR-0251. ADR-0151's range cleanup remains intact.

## Decision Linkage

- ADR created: [ADR-0251](../decisions/ADR-0251-exact-call-paged-history.md).
- Decision delta summary: Resolve canonical entities before exact calls; scan
  retained primary rows in bounded live pages with one cursor per connection.
- Contract changes: Current request-time retention applies to every UI history
  form; budget and unreadable warnings are explicit. Relevant settings changes,
  including changing and restoring them, invalidate continuation.

## Verification and Monitoring

- Validation steps run: `go test ./archive`, `go test -race ./archive` and
  `go vet ./archive` passed locally. Command and telnet contract cases are in
  `commands/history_test.go` and `telnet/history_test.go`; full integration
  closeout is recorded separately from this local archive evidence.
- Signals to monitor: Exact command/selector, returned work-limit or unreadable
  warning, current continuation token, settings-change restart response, and
  `History search ... failed` archive diagnostics. Do not share private records
  or an entire filter configuration when one redacted command suffices.
- Rollback triggers: Cross-call results, skipped or duplicated continuation
  rows, cursor resurrection after close/settings change, or shutdown failing to
  release readers. Reassess the contract before restoring silent partial output.

## References

- Related ADRs: [ADR-0011](../decisions/ADR-0011-show-history-dxcc-selector.md),
  [ADR-0248](../decisions/ADR-0248-canonical-dxcc-input-and-human-labels.md),
  [ADR-0251](../decisions/ADR-0251-exact-call-paged-history.md),
  [ADR-0151](../decisions/ADR-0151-archive-range-deletion-cleanup.md).
- Cleanup diagnostic: [TSR-0026](TSR-0026-archive-cleanup-scan-cpu-spikes.md)
  concerns cleanup CPU, not the history query scan budget.
- Authoritative docs: [commands history](../../commands/README.md#archive-history),
  [telnet history](../../telnet/README.md#archive-history-and-continuation).
- Source and tests: `commands/history.go`, `telnet/history.go`,
  `telnet/history_filter.go`, `archive/history.go`, `archive/history_test.go`.

# ADR-0251: Exact-Call Paged Archive History

- Status: Accepted
- Date: 2026-10-06
- Decision Origin: Troubleshooting chat

## Context

ADR-0011 introduced optional entity selectors for history, and ADR-0248 gave
canonical CTY labels precedence. A full station call still resolved to an entity,
which did not answer an exact station-history request. The global timestamp
reader also silently stopped after 200,000 records, so sparse matches could be
hidden. Archive read failures could look like empty history.

The selected requirement permits non-realtime searches across multiple bounded
pages. A secondary index, migration, backfill and changed cleanup ownership are
unnecessary for this contract. TSR-0041 records the diagnosis.

## Decision

Classify supplied selectors in this order:

1. An unambiguous exact CTY canonical label selects its ADIF entity. Conflicts
   fail explicitly; slash-bearing canonical labels are not normalized as calls.
2. A full valid DX-normalized call selects exactly that materialized station
   identity. The same DX normalizer applies to queries and decoded older records,
   including numeric SSIDs. This never widens to its country after no matches.
3. Other valid supported CTY prefixes select the resolved ADIF entity.

Supplied selectors retain explicit unavailable/unloaded CTY errors. With loaded
CTY, a valid full call whose country is unresolved remains a valid exact query.
Numeric arguments remain counts, default 50 and range 1-250; either selector/count
order remains supported. Existing client-filter and self-spot rules still apply.

Use a new paged reader over the existing timestamp/sequence primary keys. Every
UI history form applies its page's captured `now - retention_seconds` cutoff,
including the exact cutoff. Each request owns one fresh iterator; subsequent
pages observe current retained history rather than a frozen cross-page snapshot.
Select newest matching rows, then render that page chronologically. Label
continued pages as older history.

Limit each page to 200,000 candidate visits, including rejected, malformed and
decode-failed consumed records and one non-consuming lookahead. Reserve one
visit for lookahead, leaving at most 199,999 consumed records. Save the full last
consumed timestamp/sequence key and resume strictly below it, even if it has
since been deleted. Lookahead remains eligible on the next page. Distinguish
range exhaustion, count reached with older search, and work-budget exhaustion.

Each connection retains at most one small typed cursor with selector, count,
position, generation and cumulative warning/result flags. Its current handle is
`H1` followed by 32 uppercase hexadecimal characters from 128 random bits.
`SHOW DX NEXT <token>` and supported history aliases continue it. Tokens are
connection-owned and rotate on successful publication; the previous token is
then invalid. A new valid search replaces the old one immediately, including
when the new read fails. Invalid fresh commands do not replace it.

Capture a detached coherent filter/path-settings snapshot per page. Relevant
settings publication invalidates the cursor, including change followed by
restoration; presentation-only preferences and changing propagation observations
do not. Propagation observations remain live during matching. Connection close
invalidates pending and stored search state. No configuration or history guard
is held during the archive scan; publication rechecks generation and closed
state before accepting output and advancing the cursor.

Cursor advancement linearizes with acceptance by the bounded control queue,
not socket delivery. Read/cancellation or token-generation failure does not
advance a continuation, so the same position is retryable unless independently
invalidated by close, relevant settings changes or a new search. Queue overflow
retains existing disconnect behavior. Successful enqueue does not promise that
the network delivered the page.

Skip unreadable records with an explicit cumulative warning. Exhausting keys
after such skips is not proof of complete matching history. A malformed saved
boundary that cannot support safe continuation fails explicitly. Archive errors
are errors, not empty history. Active legacy and paged readers guard DB lifetime;
Stop cancels readers and waits for their iterators to close before closing Pebble.

## Alternatives considered

1. Keep entity-wide callsign selectors and enlarge the silent scan cap. Rejected
   because it preserves both identity confusion and undisclosed truncation.
2. Scan the whole archive synchronously without a cap. Rejected because a narrow
   or absent match lacks a dependable per-request work and shutdown bound.
3. Add a callsign index with migration/backfill. Rejected for this requirement
   because paging suffices and avoids new write, coverage and retention state.
4. Use authenticated stateless cursors. A single connection-owned typed cursor
   avoids token serialization, authentication-key and command-size complexity,
   while providing direct replacement and single-use ownership.
5. Retain iterators or snapshots between commands. Rejected because abandoned
   searches could pin archive resources and complicate shutdown and retention.

## Consequences

### Benefits

- Exact station queries remain distinct from country queries.
- Explicit continuation reaches deeper retained history within bounded pages.
- Cursor state follows existing connection lifetime; no registry, expiry worker
  or new goroutine is introduced.
- Archive encoding, writes and range-deletion cleanup remain unchanged.

### Risks

- Pages scan unrelated records; no realtime or selective-lookup latency promise
  is made. A rare call may require several continuation requests.
- Live inserts ahead of the saved position are not revisited, and expiration
  between pages can remove older candidates. Filters and propagation can affect
  later pages; relevant settings changes require restarting.
- Queue acceptance can advance state even if subsequent network delivery fails.
  Reconnecting requires a new search.

### Operational impact

- Read the latest returned NEXT command on the same connection. Treat a work
  limit as incomplete search and unreadable warnings as reduced completeness.
- All UI history reads apply current retention without waiting for cleanup.
- No new YAML setting, secondary index, backfill or storage migration is needed.
- Legacy `Recent`/`RecentFiltered` APIs retain their read semantics with reader
  lifetime protection; UI history uses the new paged reader.

## Links

- Refines [ADR-0011](ADR-0011-show-history-dxcc-selector.md) for station selection,
  pagination, retention and failure reporting, and
  [ADR-0248](ADR-0248-canonical-dxcc-input-and-human-labels.md) for the selector
  branches after canonical-label precedence. Neither is superseded in full.
- Preserves [ADR-0151](ADR-0151-archive-range-deletion-cleanup.md) and
  [ADR-0149](ADR-0149-single-window-archive-retention.md) storage cleanup decisions.
- Related TSR: [TSR-0041](../troubleshooting/TSR-0041-exact-call-history-and-scan-cap.md).
- Related tests: `archive/history_test.go`, `commands/history_test.go`,
  `telnet/history_test.go`, `telnet/history_filter_test.go` and
  `telnet/history_fuzz_test.go` (stateful cursor sequences).
- Related docs: [commands history](../../commands/README.md#archive-history),
  [telnet history](../../telnet/README.md#archive-history-and-continuation).

- State metadata and related schema version rules are refined by
  [ADR-0253](ADR-0253-fcc-state-enrichment-and-filtering.md).

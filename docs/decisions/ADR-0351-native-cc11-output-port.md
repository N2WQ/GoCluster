---
title: "ADR-0351: Native CC11 Output Port"
description: Session-only CC11 presentation, canonical raw comments and shared bounded history-response ownership.
---

## Status

- Status: Accepted
- Date: 2026-11-13
- Decision Origin: User approval of Scope Ledger v2

## Context

Experimental checkpoint `43b51440` preserved native CC11 live/history output
alongside separate CC conversation/profile and testing experiments. Current
main `4045d435` has newer independent GO/CC banks, schema-seven persistence,
response reservations, writer cleanup and connection safeguards. A wholesale
checkpoint merge would import obsolete integration rather than preserve those
contracts.

The owner selected canonical raw native comments rather than CWSPEED/DIAG
display projection, and shared native-history/configuration admission rather
than an additional memory pool. Native output remains presentation, not a
claim of full CC User or DXSpider equivalence.

## Decision

- Recognize exact, argument-free, case-insensitive post-login `SET/VE7CC`
  and `UNSET/VE7CC` in GO and CC. Start every new/replacement connection in
  text. Never persist/export native selection or select a dialect implicitly.
  Preserve selection through current DIALECT, preset and YAML publication.
- Port the bounded pure encoder, not old conversation/profile machinery.
  Keep the 20-field/19-caret structure, one-decimal kHz, padded UTC date,
  `HHMMZ` and exactly one CRLF per spot record.
- Combine canonical mode, a present report including zero, and canonical
  stored comment with the existing beacon fallback. Native output ignores
  CWSPEED/DIAG projections and glyphs. It still sanitizes carets to tildes and
  nonprintable/non-ASCII runes to spaces without mutating stored data.
- Emit stored GoCluster ADIF directly; missing positive ADIF stays blank.
  Supplement only missing Country/ITU when current positive CTY ADIF matches
  positive stored ADIF. Preserve other metadata and first-four valid grids,
  uppercase reported/lowercase derived. Use known peer origin or the current
  node for live manual spots; do not invent old manual-history/feed origins.
- Queue a typed switch acknowledgement in the existing control FIFO. Publish
  requested selection only after accepted enqueue; the writer alone applies
  effective mode at that position. Do not flush/drop backlog or add workers,
  queues, timers, caches or a new transport-wide ownership interface.
- Preserve current history grammar, matching, generation and continuation
  rules. Choose native versus text under the configuration lease at
  publication, after archive I/O. Render outside close/history guards and
  revalidate before enqueue. Accepted whole-frame enqueue alone commits the
  next cursor; failed continuation retains its prior cursor, while a valid
  fresh query has already replaced the previous search.
- Reuse generic automatic history pauses, not mandatory human-readback holds.
  Admit bounded footer headroom before pause mutation. Conservative headroom
  may reject a large page without promising maximal frame utilization.
- Cap complete live records and complete native history frames at 65,536
  final bytes including CRLF and all statuses/footers. Explicitly omit invalid/
  oversized live records with bounded diagnostics; refuse history as a whole.
  Never replace failed native spots with ordinary-text spots.
- Native history uses the existing process-wide 8 MiB output accounting
  budget, a 1 MiB reservation and one outstanding large owner per client.
  History and configuration operations may return busy while owners remain.
  No new pool or operator setting is introduced.
- Reserve during publication before native encoding, not across the unlocked
  archive scan. Final acceptance follows configuration lease, close fence,
  then history lock. Release unqueued backing/owners before dropping the
  lease; drop guards before release, queue-full close and optional reporting.
  Transfer one immutable frame and existing owner to current writer cleanup.
  Preserve flush/discard, enlarged-batch retirement, composed queue drain and
  cleanup before `writerStopped`.

## Alternatives considered

1. Merge/cherry-pick the complete checkpoint: rejected because unrelated old
   profile, prompt, configuration and harness changes conflict with current
   ownership and behavior.
2. Apply CWSPEED/DIAG display projection to native comments: rejected by the
   owner. Their current ordinary-text behavior remains unchanged.
3. Add a dedicated native-history budget: rejected by the owner in favor of
   existing shared capacity and cleanup. Sharing accepts busy contention
   with configuration reads/preparation/saves.
4. Send unreserved pages, fragments or direct socket writes: rejected because
   queue limits alone do not account for preparation/backing, and fragments
   undermine whole-page cursor publication and writer ordering.

## Consequences

### Benefits

Native output fits current connection, configuration and writer ownership
without migrating persistent schemas or changing filtering/admission. A single
bounded producer backing holds the native page/status/footer; current writer
cleanup owns transferred output.

### Risks

GoCluster ADIF differs from DXSpider WPXLOC, including US 291 versus 226 and
Canada 1 versus 197. A client expecting WPXLOC may misidentify countries.
Derived grids may be interpreted as actual. Missing/conflicting descriptions
remain blank with diagnostics rather than invented identity.

Slow native-history replies can occupy capacity needed by configuration
operations. Existing explicit save-failure/unsaved-state behavior remains;
sharing does not guarantee every save attempt obtains capacity. A reservation
charge is not an exact heap/RSS bound. Decoded pages (up to 250 spots), detached
filters and existing writer baselines are separate from the 64 KiB wire limit.
Backing retirement must be checked as well as counter balance.

### Operational impact

Scripts wanting CC syntax and native spots may select `DIALECT CC`, then
`SET/VE7CC` after every login. Ordinary control replies, announcements and
weather bulletins retain current formats. CC12, native settings readbacks,
automatic detection and full CC User/HRD qualification remain excluded.

Retry busy history/configuration operations and request fewer rows after
native history size refusal. Monitor bounded per-connection sanitization,
unavailable/conflict and refusal diagnostics. Read pauses still suppress
without replay.

No profile/archive reset or migration is required. Native state is connection
local, so reverting this port needs no native-state data conversion. Deployment,
service restarts and production data operations require separate authorization.
Tests and benchmarks establish only their observed scope, not deployment,
production p99 compliance or installed-client interoperability.

## Links

- [Native output guide](../../telnet/README.md#ve7cc-native-spot-output)
- [Domain contract](../domain-contract.md#session-only-cc11-spot-output)
- [CC bank/schema contract](../cc-dialect.md)
- [CWSPEED decision](ADR-0332-per-user-rbn-cw-speed-display.md): refined only
  by the explicit native-output exception; ordinary display remains unchanged
- [Configuration support card](../../customgpt/support-cards/configuration-readback.md)
- Related implementation: `telnet/ve7cc_format.go`, `telnet/ve7cc_session.go`,
  `telnet/ve7cc_history.go`, `telnet/history.go`, `commands/history.go`

# ADR-0261: Literal Comment Filters And Archive Searches

- Status: Accepted
- Date: 2026-10-08
- Decision Origin: Design

## Context

Operators need literal comment filtering and archive searches beyond EVENT's
fixed activation-family taxonomy. Preferences must have bounded matching/state,
survive reconnects and presets, preserve old machine-client shapes, and compose
with the existing filtered/paged history contract.

## Decision

Use case-insensitive ASCII literal substring matching on stored comment text.
PASS is an allowlist: any PASS phrase qualifies, any REJECT match wins, and other
normal filter categories still apply. With no PASS phrases, no allow restriction
is imposed. Empty comments fail an active allowlist. Existing self-spot filter
exceptions are preserved; an explicit archive query phrase is always mandatory.

Retain at most 32 entries in each list, each 1-64 printable ASCII bytes and not
space-only. These are algorithm/admission constants, not deployment settings.
Human commands consume a literal remainder, trim surrounding spaces and retain
interior spacing/punctuation. Additions are case-insensitive/idempotent and move
equivalent phrases out of the opposite list. REMOVE deletes equivalent selected
entries; category/list/global resets clear corresponding rules. Failed admission
leaves both lists unchanged. Keep existing human save-failure behavior.

Shared filter-owned lists have no derived matcher cache. Configurations and
presets detach lists and include them in equality, revisions, size preflight,
modified status and history invalidation. Removal/replacement relinquishes
discarded values. Matching allocates no scratch normalization per candidate.

Disk version 4 stores comment rules. Older versions acquire empty lists without
changing prior values; MINSNR introduction is pinned to 3. Validate effective
stored comment nodes before typed decoding, and reject malformed/oversize state
without pruning or rewriting protected records. No bulk migration is required.

Explicit machine schema 4 exposes `comments`/`block_comments`. Freeze schemas
1-3 and preserve hidden rules during their writes. Full PUT/VALIDATE requires
both lists; PATCH omission preserves and supplied lists replace, including empty
lists. Exact YAML preserves case/order/duplicates/overlap; all entries count
toward caps and matching REJECT wins. Existing machine persistence/publication
transactions remain atomic. Final response/preset budgets remain independent.

Extend DX/MYDX archive forms with COMMENT after optional existing selector/count
arguments, preserving aliases and NEXT semantics. Count rows after phrase and
filter predicates. Retain only the bounded query phrase with the existing cursor;
do not add an archive index, encoding version, cache, iterator lifetime or worker.

## Alternatives considered

1. EVENT-only filtering cannot select arbitrary literal references or phrases.
2. Regex/wildcard semantics add interpretation and work beyond the selected
   requirement. Punctuation is literal instead.
3. Telnet-only filtering separates live/archive matching and misses shared
   configuration ownership; use the existing filter owner.
4. An archive text index adds migration/write/retention state. Existing bounded
   paged scanning satisfies the selected non-realtime history contract.
5. Extending schema 3 in place breaks existing exact client shapes. Use explicit
   schema 4 while preserving hidden preferences in older writes.

## Consequences

### Benefits

- Filtering and history share literal semantics and bounded preferences.
- Existing clients, saved filters and archive encodings retain their contracts.
- Operators can inspect counts, exact phrases and independent limits.

### Risks

- Matching adds scan work proportional to comment length and bounded phrases;
  local benchmarks establish cost, not production latency guarantees.
- ASCII-only phrases cannot search non-ASCII characters literally.
- Ingestion may remove tokens; display diagnostics/fallback text are not stored
  comments and cannot be searched through COMMENT.
- Older binaries may reject version 4 records; downgrade requires compatible
  backups with writers stopped.

### Operational impact

Use `SHOW FILTER COMMENT`, schema 4 readbacks and returned NEXT commands. Rare
matches can require several bounded pages. Backup/restore ownership, command
ceilings, response/preset budgets and archive lifetime remain unchanged.

## Links

- Combined BAND/MODE/COMMENT history grammar is refined by
  [ADR-0262](ADR-0262-history-band-mode-selections.md); COMMENT still consumes
  the remaining literal text.

- Refines [ADR-0251](ADR-0251-exact-call-paged-history.md) and
  [ADR-0258](ADR-0258-per-mode-minimum-snr-filter.md); preserves EVENT's
  [ADR-0070](ADR-0070-event-filters-preserve-untagged-spots.md).
- Contract: [telnet comments](../../telnet/README.md#comment-filters-and-searches).
- Evidence: `filter/comment*_test.go`, `telnet/comment*_test.go`,
  `commands/comment_history_test.go`, `telnet/history_comment_test.go`.
- Support routing: `customgpt/source-map.md` and configuration support card.
- No TSR: this records a new design, not durable incident diagnosis.

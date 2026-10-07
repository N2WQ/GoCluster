# ADR-0253: FCC State Enrichment and Filtering

- Status: Accepted
- Date: 2026-10-07
- Decision Origin: Design

## Context

Spots previously held DXCC, zone, continent and grid metadata but no US state.
The FCC amateur ULS archive contains licensee mailing addresses; the import
kept HD and AM records and discarded EN. The user accepted the mailing address
as the available source, selected all 60 observed FCC address codes, and
approved Scope Ledger v2. Canadian province enrichment needs another source
and is outside this change.

## Decision

1. Keep the existing HD/AM projection and add `AM.state`. Read EN licensee
   records using unique system ID and canonical callsign, retain only the
   normalized two-letter state, and discard other address fields. Accept 50
   states, DC, AS/GU/MP/PR/VI/UM and AA/AE/AP. Blank, unsupported, malformed or
   conflicting evidence leaves state empty while retaining the license.
   Evidence must be unambiguous across the final active callsign.
2. Mark the state-capable SQLite schema with `user_version=1`. Rebuild legacy
   databases even when HTTP download metadata is unchanged. Publish only a
   completed replacement and retain the last good database on failure.
   Replacement retries are finite and cancellable. A refresh exposes unknown
   lookup results rather than blocking spot handling on the long build.
3. Separate factual `LookupUS` results (available, found, state) from license
   rejection. `fcc_uls.enabled` controls enforcement only: startup, upgrades,
   refreshes and state enrichment remain active when false. Offline replay
   reads its configured local database without starting a downloader; a missing
   database remains optional when enforcement is off.
4. Share the existing bounded 200,000-entry TTL cache for license and state.
   Couple cache resets to the database generation so a query from an old
   generation cannot populate the new cache. Do not cache unavailable answers
   and do not create per-client SQL lookups or a second state cache.
5. Use the base callsign's CTY entity for FCC applicability. Spot admission and
   state enrichment cover ADIF 6, 9, 20, 43, 103, 110, 123, 138, 166, 174, 182,
   197, 202, 285, 291, 297 and 515. Guantanamo (105) and deleted Kingman (134)
   are excluded. Existing beacon, TEST, allowlist and SELF exceptions remain;
   login admission retains its previous ADIF coverage.
6. Store state in each role's `CallMetadata.State`: DE at central ingest and DX
   after the final corrected identity, including delayed output. Clear stale
   state when the identity or lookup no longer supports it. Preserve DE state
   through same-identity metadata refresh and secondary fanout.
7. Add PASS/REJECT DESTATE/DXSTATE with existing location-filter semantics.
   An unknown state fails an explicit PASS list and passes a named REJECT list.
   Rules use category AND, selection OR and rejection precedence. NEARBY
   suspends state filtering, locks edits and restores saved rules when off.
8. Write archive record version 6. Decode versions 2–5 with empty state and
   preserve their other fields. History filters use stored metadata; no old
   archive hydration or backfill is performed. State rule changes invalidate
   the active history cursor.
9. Write saved preferences/presets version 2. Migrate legacy/version 1 records
   by adding unrestricted state categories only, including nested restoration
   baselines. Preserve existing flags and false-valued map entries exactly;
   reject unknown future versions without overwriting them. Bound each state
   map to 60 canonical keys before copying it.
10. Keep machine YAML version 1's structure, order and default GET behavior.
    Version 1 writes preserve hidden state rules, while revisions describe the
    complete preferences. Expose state rules only through explicit
    `GET YAML <resource> SCHEMA 2 [ID <id>]` and version 2 bodies. Version 2 full
    PUT/VALIDATE requires both state categories and all their rule members;
    PATCH preserves omitted categories and replaces supplied collections.
    Version 1 uses its existing 64 KiB visible projection limit plus finite
    hidden state maps; version 2 applies 64 KiB to the full configuration.

## Alternatives considered

1. Persist EN or full addresses. Rejected: state enrichment does not need
   names, streets, cities or ZIP codes.
2. Maintain a separate state cache. Rejected: license and state share one
   database generation, ownership and expiry contract.
3. Expand every YAML response in place. Rejected: existing strict clients must
   keep their document format; explicit schema selection provides migration.
4. Disable reference data together with enforcement. Rejected: the accepted
   requirement needs state in delivered and archived spots when checks are off.
5. Hydrate historical spots from today's FCC database. Rejected: this would
   rewrite the meaning of old records and require another history contract.

## Consequences

### Benefits

- State filters work for delivered spots and newly archived history.
- Existing machine clients and saved preferences retain their contracts.
- One bounded lookup owner handles state and license facts.

### Risks

- Mailing state may differ from operating location; the user accepted this
  limitation. Unknown and conflicting evidence remains unknown.
- Enforcement-disabled installations now perform FCC refreshes and may need a
  first-start schema rebuild. Operators should retain database/profile/archive
  backups before upgrading; old binaries cannot read new archive/profile
  formats safely.
- The broader FCC spot admission coverage can reject previously unchecked
  entities when enforcement is enabled. Login policy does not change.

### Operational impact

- Existing configuration values remain valid; no second enable flag is added.
- Refresh logs and lookup diagnostics expose schema readiness, generation,
  refresh status, cache cardinality and import state counts.
- Shutdown cancels and joins the refresh worker before releasing its owners.
- Canada, extra spot wire columns, historical rewrite and unrelated HD date
  mapping corrections remain outside this decision.

## Links

- Authorization: user `Approved v2` in the implementation conversation.
- Related tests: `uls/state_test.go`, `uls/replace_windows_test.go`,
  `internal/cluster/fcc_state_test.go`, `archive/state_test.go`,
  `filter/state_test.go`, `telnet/state_commands_test.go`,
  `telnet/state_machine_test.go`, `cmd/rbn_replay/state_test.go`.
- Related docs: [operator guide](../OPERATOR_GUIDE.md),
  [telnet interface](../../telnet/README.md),
  [configuration](../../data/config/README.md).
- Related TSRs: none; this is a feature design decision.
- Refines [ADR-0244](ADR-0244-exact-configuration-persistence.md) for disk
  version 2, [ADR-0245](ADR-0245-machine-yaml-configuration.md) for explicit
  schema 2, and [ADR-0251](ADR-0251-exact-call-paged-history.md) for stored state.
  These decisions otherwise remain accepted.

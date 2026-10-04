# ADR-0238: Named Presets Shared Across Numeric SSIDs

- Status: Accepted
- Date: 2026-10-03
- Decision Origin: Design

## Context

Telnet preferences are automatically stored per full login callsign, including
its numeric SSID. Users need named snapshots that can be reused across their
SSIDs without merging those sessions' active settings or login metadata.

## Decision

- Add dialect-independent `SAVE PRESET <name>`, `LIST PRESET`,
  `LOAD PRESET <name>` and `DELETE PRESET <name>` commands.
- Use the existing baseline own-call normalization for collection ownership.
  Names are case-insensitive uppercase ASCII identifiers, 1-32 characters,
  starting with a letter or digit and otherwise allowing digits, letters,
  underscores and hyphens.
- Store a separate YAML collection below `filter.UserDataDir/presets`,
  using the hex-encoded owner as its filename. Keep ordinary per-SSID user
  records and autosaving in place. No automatic preset selection or live binding
  is introduced.
- Limit each owner to 20 presets and each standalone serialized preset to 256 KiB.
  Bound collection reads/writes to 8 MiB. Reject malformed or oversized data
  without resetting it. Replacement remains allowed when the count is full.
- Retain only 64 process-lifetime striped locks; decoded collections are local
  to each operation. SAVE/DELETE serialize the full read/modify/replace
  operation across SSIDs and log the resulting cardinality.
- Snapshot all persistent filters/toggles and dialect, dedupe, grid, noise,
  path sample minimum and solar cadence. Exclude login/IP history, temporary
  session controls and derived runtime caches.
- LOAD rebuilds derived state and applies existing normalization/server rules.
  Atomically persist the current SSID's preferences, preserving its login
  metadata, before installing live state. Keep the existing Filter pointer
  stable for readers. Do disk work outside client write locks.
- Commit synced, closed temporary files with platform-specific replacement,
  following the existing VOACAP replacement pattern. Handled failures clean up
  temporary files. No fallible operation follows the successful replacement.

## Alternatives considered

1. Embed presets in an ordinary per-SSID record. Rejected because autosaves from
   distinct SSIDs would not own one coherent shared collection.
2. Use one file per preset. Viable, but introduces directory enumeration and
   additional count/temporary-file handling for an already small collection.
3. Cache all owners' collections or retain an owner-keyed lock map. Rejected
   because neither is needed and historical callsign churn would retain state.

## Consequences

### Benefits

- Users can reuse complete preference snapshots across numeric SSIDs.
- Ordinary session defaults remain separate from the shared snapshot library.
- Failed LOAD commits preserve live state and the prior saved default.

### Risks

- A malformed owner collection blocks its preset operations until repaired.
- Updates serialize per lock stripe and rewrite the bounded collection.
- Process-local locks do not coordinate multiple cluster processes. A user-data
  directory has one writer process; external edits need operator coordination.
- Existing unrelated per-SSID autosave/session-replacement semantics remain.

### Operational impact

Back up the `presets` subdirectory with user records. Presets are never
implicitly evicted; users remove them with DELETE. Logs expose the count after
successful SAVE/DELETE and report temporary-file cleanup failures. HELP and
support routing document names, limits, contents and restoration behavior.

## Links

- Related issues/PRs/commits: Approved Scope Ledger v2
- Related tests: `filter/presets_test.go`, `telnet/preset_commands_test.go`, `commands/preset_help_test.go`
- Related docs: `telnet/README.md`, `commands/README.md`, `README.md`, `docs/OPERATOR_GUIDE.md`
- Related TSRs: none
- Supersedes / superseded by: none; existing per-SSID persistence remains

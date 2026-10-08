# ADR-0244: Exact Configuration Persistence and Preset Continuity

- Status: Accepted
- Date: 2026-10-05
- Decision Origin: Design

## Context

Human readbacks and client YAML writes need to describe the user's configured
preferences exactly. Legacy persistence normalized empty selections and defaults
on every read, ordinary saves were not atomic, and session replacement could
race an old session's save. A preset name alone cannot explain the configuration
after the shared named snapshot is overwritten or deleted.

This decision refines [ADR-0238](ADR-0238-named-presets.md). Shared named preset
ownership and budgets remain; exact storage, applied preset references and the
complete per-SSID persistence handoff become explicit contracts.

## Decision

### Exact values and disk versions

- Use one writable configuration model for filters and the six preferences:
  dialect, GRID, noise class, dedupe choice, path sample floor and solar cadence.
  Keep effective behavior, lookup-derived GRID, diagnostics, pause state, login
  metadata and preset status separate from writable preferences.
- Write `configuration_version: 1` on user records and named snapshots. Only
  an absent marker selects legacy migration. Explicit zero, null, malformed or
  unsupported versions fail instead of being treated as legacy.
- Read current-version values without legacy pruning or normalization. Preserve
  false map entries, empty selections, explicit false toggles, zero values and
  configured defaults. An absent optional toggle represents DEFAULT; it is
  distinct from an explicit false. Current nonnullable fields reject explicit
  null and incompatible YAML types.
- Preserve supplied callsign-pattern order for storage and readback. Compare
  rules without regard to order for preset modification status and revision
  changes, while retaining duplicate multiplicity and false map entries.
- Migrate legacy values when read and mark the next successful ordinary write.
  No separate migration job or migration-history store is introduced.

### Atomic records and session ownership

- Replace all user-record writes atomically: write a temporary file, sync it,
  close it, then perform platform-specific replacement. Handled failures retain
  the old target bytes and clean up the temporary file. Preferences, the applied
  preset name and its baseline commit as one record; ordinary preference saves
  preserve that reference and existing login metadata.
- Use 64 fixed, cancellable transaction stripes keyed by the full callsign,
  including its SSID. Retain ownership from the authoritative reconnect read
  through login metadata persistence and replacement registration. All per-SSID
  save paths, including saves already in progress and teardown, use this same
  ownership contract. A retired session cannot overwrite its replacement.
- Terminal machine rejection skips the final preference autosave while retaining
  the final owner lease and membership cleanup. An earlier failed human save must
  not change the disk record as a side effect of rejection. Ordinary disconnects
  retain final autosave, subject to the protected-record and retired-owner fences.
- Acquire transaction ownership before registry, filter, path or writer locks.
  Never wait for a stripe while holding those locks. Disk I/O runs outside client
  locks while the stripe protects the transaction; keep the Filter pointer stable
  when publishing committed values.
- If a record is unreadable, malformed or uses an unsupported format, preserve
  its bytes and run that session with temporary defaults and a warning. Human
  preference changes remain temporary. Reject LOAD, PUT and PATCH without
  altering the protected record, and reject SAVE PRESET before any library write.
  Login metadata and teardown saves also preserve the protected record.
- If the existing record was read successfully but the login timestamp/IP write
  fails, continue with its restored preferences, association and baseline, and
  warn the user. The failed atomic write retains the previous disk record.

### Applied preset snapshots

- Successful LOAD associates the name with the successfully applied preferences.
  Apply server adjustments separately from the named snapshot, report them in
  the acknowledgement, and establish the applied values as the baseline. A
  lookup-derived GRID remains effective state; configured empty GRID stays empty.
- Successful SAVE captures preferences once. The named snapshot, persisted
  current preferences and new baseline refer to that same capture. Persist the
  reference before changing the live association.
- If the named save succeeds but its per-SSID association cannot commit, report
  partial success. Preserve the previous live reference and its old disk name
  and baseline so reconnect and later ordinary saves retain them. The successful
  named library write remains available.
- Failed LOAD preserves the previous live and durable preferences and reference.
  Library overwrite or deletion does not change the already applied baseline.
  Current preferences differing from that baseline show `(modified)`; reversing
  changes clears it. Pause, diagnostics and temporary NEARBY dedupe behavior do
  not alter modification status.
- Retain one detached, bounded baseline per associated user record. Do not keep
  historical baselines or bind sessions to later named-library updates.

### Machine writes and resource boundaries

- PUT replaces a complete resource; PATCH changes supplied fields. Validate the
  complete resulting configuration and reject unavailable choices. A validation
  or persistence failure leaves both live and durable configuration unchanged.
  Require the revision returned by a fresh GET; a new connection or server
  restart establishes a new revision epoch. Pause, diagnostics and login metadata
  do not advance the configuration revision.
- An unchanged PUT must still persist the proposed configuration before reporting
  success. This repairs a failed earlier human preference save. Retain the live
  solar clock, lookup-derived GRID, NEARBY restoration state, diagnostics and pause
  controls when their configuration is unchanged. LOAD keeps its existing fresh
  solar schedule from successful publication, even at the same cadence.
  Exactly identical PUT/PATCH repairs disk without publishing runtime state.
  Pattern-order-only changes still publish and persist the supplied order.
- Keep the preset budgets at 20 names per owner, 256 KiB per standalone encoded
  snapshot and 8 MiB per collection. Existing fixed collection locks remain
  separate from the per-SSID transaction stripes.
- Bound every new readback to 65,536 final bytes, including CRLF conversion,
  document markers and human footers. Finish bounded generation before queueing
  one complete response; oversized output returns an explicit error, never a
  truncated success. PUT, PATCH and VALIDATE require the resulting complete CONFIG
  readback to fit, reserving the largest supported response metadata.
- LOAD uses the independent preset budget and checks its acknowledgement, rather
  than imposing machine-write admission. A valid large preset may LOAD while its
  FULL or YAML readback fails the 64 KiB response budget.

## Alternatives considered

1. Continue normalizing every disk read. Rejected because an exact GET/edit/PUT
   cycle would change explicit false, empty and default preferences.
2. Keep only the applied name and look up its baseline in the shared library.
   Rejected because deletion or overwrite would change modification status and
   erase the snapshot that the session actually applied.
3. Skip only retired teardown saves or use an owner-keyed lock map. Rejected
   because already-running saves and reconnect reads require one handoff, while
   retaining a lock per historical callsign would grow process state.

## Consequences

### Benefits

- Readbacks explain configured preferences and effective behavior consistently.
- Failed writes retain previous disk state; reconnect restores a coherent
  configuration and preset reference.
- Resource use is bounded without silently losing rules or snapshot history.

### Risks

- A protected record or malformed preset collection needs coordinated operator
  repair; the server does not overwrite unknown formats to recover automatically.
- Transaction stripe collisions can delay unrelated callsigns. Process-local
  locks require one writer process per user-data directory.
- Older binaries can discard new record fields or normalize exact values, while
  older strict preset decoders reject the new snapshot marker. Downgrade requires
  restoring data compatible with the chosen binary.
- Valid large presets may exceed the smaller readback budget. A broken connection
  cannot guarantee complete terminal delivery of an otherwise complete response.

### Operational impact

Back up the entire `filter.UserDataDir` (normally `data/users`), including full-SSID
records and the `presets` subdirectory. Stop server writers and external editors
before copying, repairing or restoring it. Preserve a matching binary and data
backup before upgrading; for rollback, stop writers and restore that matched pair.
Do not run an older writer against the only copy of current-version records.
See [environment operations](../ENVIRONMENT.md#user-configuration-backups-and-downgrades)
and [runtime configuration notes](../../data/config/README.md#user-configuration-storage).

## Links

- Related issues/PRs/commits: Approved Scope Ledger v3.
- Related implementation: [exact model](../../filter/configuration.go),
  [comparison](../../filter/configuration_compare.go),
  [disk validation](../../filter/configuration_storage.go),
  [user records](../../filter/user_record.go),
  [atomic writes](../../filter/atomic_user_file.go),
  [preferences](../../filter/user_preferences.go),
  [presets](../../filter/presets.go),
  [transaction ownership](../../telnet/configuration_transaction.go),
  [reconnect](../../telnet/configuration_session.go),
  [publication](../../telnet/configuration_publish.go),
  [preset commands](../../telnet/preset_commands.go),
  [machine writes](../../telnet/machine_commands.go).
- Related tests: [exact disk contracts](../../filter/configuration_test.go),
  [preset storage](../../filter/presets_test.go),
  [session handoff](../../telnet/configuration_session_test.go),
  [ownership-check competition](../../telnet/configuration_handoff_test.go),
  [runtime continuity](../../telnet/configuration_publish_test.go),
  [preset transactions](../../telnet/preset_transaction_test.go),
  [machine writes](../../telnet/machine_commands_test.go),
  [terminal rejection disk/reconnect](../../telnet/machine_failure_persistence_test.go).
- Related docs: [telnet](../../telnet/README.md),
  [commands](../../commands/README.md), [environment](../ENVIRONMENT.md),
  [runtime config](../../data/config/README.md).
- Related TSRs: none; this is a new feature decision.
- Supersedes / superseded by: refines [ADR-0238](ADR-0238-named-presets.md)'s
  normalization and per-SSID lifecycle clauses; no whole-record supersession.

- State metadata and related schema version rules are refined by
  [ADR-0253](ADR-0253-fcc-state-enrichment-and-filtering.md).

## Minimum SNR Refinement

[ADR-0258](ADR-0258-per-mode-minimum-snr-filter.md) refines this decision for
MINSNR and disk/machine version 3. Other clauses remain in force; state
migration remains pinned to version 2 and machine schemas 1/2 keep their shapes.

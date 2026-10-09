# ADR-0268: Exact-Byte User Record Preparation

- Status: Accepted
- Date: 2026-10-09
- Decision Origin: Design

## Context

Configuration admission must measure the exact record that will be written,
including authoritative login metadata, recent IPs and the applied baseline.
Reloading or serializing after admission could change those bytes. The existing
save functions already encode once immediately before atomic replacement; they
need an explicit preparation boundary for the later admission checkpoint.

## Decision

The filter package owns an opaque `PreparedUserRecord`: a captured absolute
target path and private final YAML bytes. `PrepareUserRecord`,
`PrepareConfiguration` and `PrepareUserPreferences` validate and serialize using
their existing boundaries. Configuration/preference preparation loads the
authoritative SSID record and merges metadata/IP history before encoding.
Preference preparation retains the association; configuration preparation
explicitly replaces it, including nil to clear it.

`ByteLen` reports the captured payload's length. `Commit` passes only those bytes
to the existing atomic writer. Commit does not reload, merge, normalize, validate
or serialize again. No caller maps, lists, toggles, baseline, record pointer or
preparation closure remain in the prepared payload. During preparation, callers
still own synchronized or detached input just as for the existing save APIs.

Existing save APIs remain preparation-then-commit wrappers, with private per-call
writer seams for failure tests. Preserve their validation differences; ordinary
record preparation retains its validation boundaries. The later complete
admission checkpoint adds the independently measured configuration/baseline limits.
Resolve a relative target location during preparation so a later working-directory
change cannot redirect the captured commit.

A prepared payload grants no transaction authority. Telnet retains its existing
full-callsign lease through authoritative loading, preparation and commit. A
retired owner must discard its payload. Named collection preparation stays private
and under the existing collection stripe through commit; an exported unlocked
snapshot would permit lost updates between SSIDs sharing the station library.

Checkpoint 04b adds no new quota and preserves publication/SAVE ordering. Human
edits still publish before saving; machine and LOAD operations persist before
publishing. SAVE still commits its library before preparing the association and
reports later association I/O failure as partial success. Checkpoint 04d will
prepare and admit both before either write, while retaining library-then-association
commit order for admitted operations.

## Alternatives considered

1. Measure one serialization and call an existing save function afterward.
   Rejected because the later load/merge/serialization could differ from the
   measured payload.
2. Retain a record/configuration pointer or preparation closure for commit.
   Rejected because later input mutations could alter the committed result.
3. Export a prepared collection without its ownership boundary. Rejected because
   another SSID could modify the collection before the stale snapshot commits.

## Consequences

Preparation has no durable effects and successful preparation fixes the final
bytes and target location. Atomic write/sync/close/replace failures retain the
previous target and clean up temporary files using the existing implementation.
This does not establish cross-process locking or stronger power-loss durability.

Payload lifetime belongs to the operation; no process-wide registry, retained
cache, new worker or synchronization layer is introduced. Preparation does not
make borrowed mutable input safe to encode concurrently.

## Links

- Admission refinement: [ADR-0270](ADR-0270-complete-storage-admission.md)

- Refines: [ADR-0244](ADR-0244-exact-configuration-persistence.md)
- Related scope: [Scope Ledger v3](../command-fix-scope-ledger-v3.md)
- Related evidence: [checkpoint 04b](../command-fix-execution.md#checkpoint-04b-exact-byte-preparation-and-commit)
- Implementation: `filter/prepared_user_record.go`, `filter/user_preferences.go`, `filter/presets.go`
- Supersedes / superseded by: none; existing ownership and atomic-write contracts remain.

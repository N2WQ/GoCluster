# ADR-0269: Detached Human Preference Publication

- Status: Accepted
- Date: 2026-10-09
- Decision Origin: Design

## Context

Human commands currently mutate live preferences inside their handlers and then
attempt persistence. Resource admission needs a preparation boundary before
publication, while preserving the deliberate human behavior: accepted changes
remain live if saving fails. Reconstructing runtime state from serialized
configuration alone could lose NEARBY restoration or reset the solar clock.

## Decision

Prepare human changes on a fresh, operation-owned minimal Client adapter.
Detach writable maps, lists and toggle pointers. Capture configured settings and
current effective path, dialect and dedupe state. Retain NEARBY cells and its
immutable restoration snapshot, plus the legacy scalar omitted from the writable
configuration model. No live Client struct, lock, atomic, socket, queue, cursor,
worker or lifecycle state is copied. Direct construction does not initialize or
otherwise mutate the live client's metadata.

The existing domain parsers and handlers operate on this candidate. Staging
contains no persistence. An explicit outcome owns handled/refused status,
publication/save intent, command-specific acknowledgement/warning formatting and
deliberate SOLAR rescheduling. Do not infer save intent from response strings or
configuration equality. Repeated ordinary PASS commands retain their save attempt;
unchanged DIALECT and idempotent COMMENT commands retain their no-save behavior.

Read-only filter summaries, DIALECT current/LIST and SHOW DEDUPE use live readbacks
before constructing a candidate. Retain runPreferenceCommand's generic lease
callback contract for existing history callers and fixtures.

The full-SSID transaction lease spans staging, publication, saving and response.
Rejected staging is discarded without publication or persistence. Publish the
accepted runtime candidate directly with stable Filter identity and the existing
path-then-filter lock order. Do not use machine configuration preparation to
recreate human runtime state. Keep filterless lightweight settings callers
supported.

NEARBY snapshots are shared only as immutable state. DisableNearby restores copied
maps; changing GRID updates cells; resets retire the snapshot. Any future code
that writes snapshot maps must instead detach them before mutation.

Publish accepted human changes before attempting their save. A save error retains
the live change and the existing truthful warning. Human SET SOLAR deliberately
starts a fresh schedule at publication, even for unchanged cadence. Unrelated
edits retain broadcaster progress between staging and publication. Machine
unchanged cadence and LOAD keep their existing different scheduling contracts.

Checkpoint 04c leaves new admission thresholds inactive. Checkpoint 04d will
admit the detached candidate before publication and prepare exact destination
bytes before persistence, including the separate protected-temporary contract.

## Alternatives considered

1. Validate after mutating live state and roll back on refusal. Rejected because
   readers can observe the temporary state and rollback must reconstruct runtime
   ownership correctly.
2. Suppress saving through staging flags, mutable hooks or fake persistence.
   Rejected because handlers would retain external side effects and staging
   correctness would depend on mode-dependent suppression.
3. Rebuild the candidate through the machine update path. Rejected because that
   path intentionally resolves different defaults, scheduling and restoration
   contracts.
4. Rewrite every domain handler to use a new standalone state interface.
   Rejected for this checkpoint because the minimal detached adapter reuses the
   current domain operations without transferring live lifecycle ownership.

## Consequences

Candidate lifetime belongs to one command; publication transfers its writable
collections to the live client, after which the adapter must not mutate them.
No retained cache, worker, queue or new synchronization layer is introduced.
Staging needs a detached snapshot per
mutating command, and must preserve runtime fields absent from Configuration.
There is no performance or production-capacity claim.

Publication has no fallible work. Persistence remains outside broadcast locks,
under the existing full-callsign lease. This does not change cross-process or
power-loss durability. Source review must verify structural separation; output
tests alone cannot prove that live locks or side effects were not copied.

## Links

- Admission refinement: [ADR-0270](ADR-0270-complete-storage-admission.md)

- Refines: [ADR-0244](ADR-0244-exact-configuration-persistence.md)
- Related preparation: [ADR-0268](ADR-0268-exact-byte-user-record-preparation.md)
- Related scope: [Scope Ledger v3](../command-fix-scope-ledger-v3.md)
- Related evidence: [checkpoint 04c](../command-fix-execution.md#checkpoint-04c-detached-human-preparation-and-publication)
- Implementation: `telnet/configuration_transaction.go`, `telnet/configuration_publish.go`, `telnet/filter_commands.go`
- Supersedes / superseded by: none; existing persistence and session ownership contracts remain.

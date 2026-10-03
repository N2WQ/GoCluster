# ADR-0234: Peer Owned Resources and Exact Spot Keys

- Status: Accepted
- Date: 2026-10-02
- Decision Origin: Troubleshooting chat

## Context

Full PC18/PC92 qualification demonstrated local hash-collision losses and
latency failures, while enabled SQLite, context backing and diagnostic
retirement still lacked complete bounds. The owner approved v15 and selected
dedicated peer diagnostics and startup refusal for a topology database that
cannot fit its budget. Acceptance of this decision does not establish that
implementation or qualification is complete.

## Decision

Use the existing full 42-byte primary and 32-byte secondary spot encodings for
cache equality. Preserve their normalization, truncation, source-class,
window, upgrade and timestamp semantics; preserve Hash32 for other callers and
64-shard selection. Evaluate primary cleanup expiry and deletion together under
the shard lock.

Give each of the N+128 transport credits one permanent manager context parent,
and each optional projection worker one parent. Each parent has at most one
active operation child, canceled before reuse. Preserve values, deadlines and
causes without a custom Context or cancellation bridge.

Replace arbitrary peer diagnostic callbacks/general log I/O with bounded typed
records and a dedicated `peerdiag` companion. It alone owns peer connection and
overlong files. Count known dropped records separately from writes whose
outcome is unconfirmed; acknowledge a recovery summary before marking its losses
reported. Missing or resource-refused diagnostics visibly degrade logging.
The parent kills and joins a failed generation before replacement; a failed
termination retains its charge and prevents replacement.

Use the narrowly repaired, pinned ncruces v0.35.6 / engine v6.3.35304 fork only
for peer topology persistence. Other SQLite users retain modernc. One
serialized connection and one process-wide reservation cover opening, active
work and failed retirement; a second owner cannot overlap it. Deadlines include
admission waiting. Retain failed partial-open/native cleanup ownership and
convert only the identified SQLite OOM failure. Preserve committed data and
live routing authority; persisted topology is diagnostic, not restart authority.
Fail startup clearly when the configured topology database cannot fit, leaving
its committed contents intact.

The 32 MiB metadata partition reserves 16 MiB for SQLite, 3 MiB for all parent
and helper diagnostic data, and 13 MiB for other metadata. SQLite's allocation
is 8 MiB engine, 6 MiB bounded WAL views/shadows, 2 MiB host. These allocations
must be proven, not asserted by counters. The 480 MiB aggregate remains fixed;
ordinary shared ingestion caches retain their separately reported ownership.

## Alternatives considered

1. Hash-only equality was rejected by literal distinct-key collisions.
2. An in-process asynchronous file writer cannot guarantee retirement when its
   file operation blocks; a joined helper gives the selected ownership boundary.
3. Raising the memory limit or excluding SQLite conflicts with the selected
   ceiling. Unmodified candidate and modernc topology paths did not establish
   that complete bound.
4. Silently disabling persistence or resetting an existing database conflicts
   with the selected startup/data-preservation policy.

## Consequences

### Benefits

- Distinct existing spot identities no longer collide merely because their
  shard hash matches.
- Blocked diagnostics have a bounded, observable process retirement path.
- Persistence ownership extends through failure and partial initialization.

### Risks

- Full keys increase shared ingestion cache backing; report this separately.
- The local SQLite fork needs pinned provenance, repair inventory and real
  native/fallback/Linux validation.
- An OS failure to terminate a helper or release native resources remains an
  observable retained owner, not a successful cleanup claim.
- Approved v16 and ADR-0235 cover the two measured history maintenance costs.
  Their end-to-end effect still requires the original qualification evidence.

### Operational impact

Ship both `gocluster` and sibling `peerdiag` executables. Detailed peer records
move to the dedicated log; status continues to expose diagnostic degradation,
known drops and unconfirmed writes. A demanding topology database can now refuse
startup to preserve the agreed budget and data. No peering YAML default,
protocol deadline, retry policy or topology authority changes here.

## Links

- [ADR-0237](ADR-0237-remove-console-peer-log-status.md) supersedes only the
  console peer-log status display; diagnostic ownership and accounting remain.
- [Approved v15](../pc18-pc92-scope-ledger-v15.md)
- [Validation and acceptance status](../pc92-v15-validation.md)
- [TSR-0036](../troubleshooting/TSR-0036-spot-collisions-and-peer-ownership.md)
- [ADR-0235](ADR-0235-spot-history-maintenance.md) records the approved v16
  behavior-preserving history algorithms and expiry-index ownership.
- Refines [ADR-0230](ADR-0230-pc18-pc92-authority-and-bounds.md) diagnostic and
  persistence ownership clauses; [ADR-0233](ADR-0233-pc92-controlled-retries-and-peer-cap.md)
  retry/admission contracts remain binding.

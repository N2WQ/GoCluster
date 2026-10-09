# ADR-0270: Complete Storage Admission

- Status: Accepted
- Date: 2026-10-09
- Decision Origin: Design

## Context

Raw YAML length, decoded preferences and the final saved record are different
measurements. A small alias encoding can expand into a large record. A valid
256 KiB preset can become larger when embedded in Configuration or accompanied
by login metadata and an applied baseline. Measuring a different representation
or checking SAVE after writing its library cannot guarantee refusal without effects.

## Decision

Measure each configuration and applied baseline using the standalone current
SavedPreset serializer, capped at 262,144 bytes. Cap the physical user-record
read, normalized current UserRecord encoding and exact prepared replacement
at 1,048,576 bytes independently. Read no more than one extra byte to distinguish
an oversized file. Configuration serialization is not the preset byte oracle.
Canonical loading uses pure encoding without resolving a destination path.

Use one physical-plus-logical processing budget per document: 2,097,152 units
for a user record including its nested baseline, and 16,777,216 for a preset
collection. Charge physical Content-tree node visits and decoded scalar UTF-8
bytes, plus typed logical visits and scalar bytes including repeated alias and
merge expansion. Retain yaml.v3's existing independent alias protections and
custom-owner boundaries. These units describe processing work, not file size
or a promised heap limit.

Production schema validation borrows immutable name/type metadata from the
existing operation-owned decoding state. Key it by exact Go type and cache only
schema information, never node acceptance, versions, errors or work visits.
Keep the free validators' fresh legacy schema construction for the frozen oracle.
Every repeated owner/value still receives its original checks and finalization.

Keep the existing 20-name, 262,144-byte snapshot and 8 MiB collection limits.
Observe the collection's resulting typed map keys during preflight with at most
21 keys retained. An otherwise valid over-count collection skips bulk entry
materialization, but later type/schema/alias errors and extra-document framing
retain their existing precedence. Repeated keys and merged/shadowed keys follow
yaml.v3's map insertion rules. This does not replace repeated value decoding
with winner-only materialization or introduce another limit.

Under the existing full-SSID lease, admit human candidates before publication.
Independently admit the temporary record using detached session metadata (at
most five recent IPs and the cached login timestamp) and its bounded baseline.
Then prepare the authoritative destination. An unreadable current record allows
a fitting temporary change with a warning and no write. A successfully loaded
record whose proposed replacement exceeds a limit rejects the change entirely.
Ordinary later I/O failures retain accepted human changes with a warning.

A record refused during login selects protected temporary defaults for that
session, preserving the original file even if later repaired. A warm-session
load failure does not permanently protect the session: subsequent save-intent
commands and ordinary logout retry the authoritative record and preserve fresh
metadata after repair. Cached session metadata never grants write authority.
If only the prospective login timestamp/IP update fails admission, retain the
restored preferences and existing warning; do not protect the fitting record.

Machine and LOAD commands admit and commit before publishing. VALIDATE remains
nonpersistent. Preserve human, machine and LOAD solar scheduling differences.
Preserve the existing partial human validation boundaries; later grammar fixes
remain separate checkpoints.

SAVE retains the SSID lease and the baseline-call collection stripe while
privately preparing both exact library and record payloads, before either write.
Reject failed, nil or unusable preparation without durable effects. Commit the
admitted library first, then the admitted record. Only later record I/O failure
can report partial success, retaining the previous association and baseline.
The prepared record and collection retain no caller references or reload work.

## Alternatives considered

1. Apply only a raw byte limit. Rejected because aliases and serialization can
   expand after the physical read.
2. Measure Configuration YAML or total work as heap bytes. Rejected because
   these representations do not establish the selected byte or memory claims.
3. Write SAVE's library before preparing its association. Rejected because
   admission failure would leave durable preset effects.
4. Make warm failures permanently protected. Rejected because a repaired file
   should allow a fresh authoritative save without requiring reconnection.

## Consequences

Refused files remain available for operator backup and repair. Warnings
distinguish temporary live settings from saved defaults. Limits bound selected
representations and processing work; they do not establish constant allocation,
production capacity or cross-process locking. The selected collection limits
still permit repeated-key value work, which must be measured separately.

## Links

- Refines: [ADR-0267](ADR-0267-compatible-stored-yaml-decoding.md),
  [ADR-0268](ADR-0268-exact-byte-user-record-preparation.md),
  [ADR-0269](ADR-0269-detached-human-preference-publication.md)
- Authority: [Scope Ledger v3](../command-fix-scope-ledger-v3.md), checkpoint 04d
- Evidence: [execution record](../command-fix-execution.md#checkpoint-04d-complete-storage-admission-and-save-ordering)
- Troubleshooting: [TSR-0052](../troubleshooting/TSR-0052-stored-yaml-schema-allocation.md)
- Supersedes / superseded by: none; refines the existing operation ownership.
- Implementation: `filter/storage_admission.go`, `filter/storage_decode.go`,
  `filter/presets.go`, `telnet/configuration_admission.go`

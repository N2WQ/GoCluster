# ADR-0357: IPinfo Verification And Download Integrity

- Status: Accepted
- Date: 2026-11-17
- Decision Origin: Troubleshooting chat and exact scope approval

## Context

A stale database could bypass fallback, invalid metadata could appear fresh, and
a recent HTTP check could describe a damaged local file or an archive whose import
failed. Delete-then-rename publication could also lose the previous file.

## Decision

Use the recorded build time or later verified upstream check for the active
database's freshness. Verification requires the exact source SHA-256 stored
with the Pebble generation. Preserve the build timestamp as result provenance.
After the configured age limit, signed-in reputation tries Cymru and the enabled
API, then uses a valid stale local match if available. Missing or invalid
required metadata refuses the store. Pre-login starts no network lookups.

A valid version-one database remains readable. The first successful automated
refresh builds a verified generation if the serving store lacks a source hash.
Require at least one ordered IP range with an ASN or country. Both CSV readers
skip mixed native address families, reversed bounds, and rows without either
enrichment field. A bare and mapped IPv4 pair keeps the start address's storage
representation. IPv4 preload and both direct readers validate the complete
version-one Pebble record, including the family key, ordered bounds, exact value
encoding, and enrichment. Invalid records never supply metadata. A direct
lookup inspects its selected predecessor and treats an invalid one as a miss.
Use one trimmed, clean absolute path for validation and file operations, and
refuse collisions with managed source, metadata, or database files before refresh. Build directly from
the private, verified extraction, then publish the diagnostic CSV. Prepare the
new store and optional IPv4 index before replacing `CURRENT_DB`. Close each candidate handle exactly once,
and close the previous store before directory cleanup. An unchanged response is
sufficient to retry an import if the verified archive differs from the active
database. Keep one small verification record tied to the active generation and
one fixed IPinfo epoch per existing cache slot. Negative DNS classifications
stay in the Cymru namespace.

The shared downloader verifies retained bytes before reporting them unchanged,
requires a complete HTTP 200 for replacement, and can make one unconditional
recovery request following HTTP 304. Ordinary conditional validators are scoped
to the effective resource that issued them. A redirect only carries a validator
for its own target. Ordinary checks use a single valid entity tag or valid HTTP
modification date. Older sidecars without resource identity require a full
transfer before ordinary conditional checks resume. The original request deadline and size limits apply. For IPinfo, a 304
requires a matching strong ETag bound to a full response from the same direct
resource. The old sidecar format and weak or missing validators require a full
transfer to establish byte identity. A redirected full response can verify
bytes directly. Its tag does not establish proof for the original URL.
FCC's explicit reuse of a published database without an archive remains
available. Publish data files and their sidecars by same-directory replacement,
with the previous file available on each failed replace. A failed sidecar write
after data publication leaves the old metadata available and logs the failure. These operations do not constitute a transaction across files or
guarantee power-loss durability.

## Alternatives Considered

- Reimport on each unchanged response. This simplifies the date rule but
  repeatedly reads the full archive and rebuilds the database and index.
- Use the download sidecar's last check time directly. The sidecar can advance
  ahead of database publication and therefore cannot establish which snapshot
  is serving.

## Consequences

Unchanged checks add a sequential read of the retained archive when integrity
needs verification. Work stays outside admission. Failed or missing
verification leaves the database on its previous freshness evidence.
Operators can back up the existing version-one database and pointer with the
verification record. An older executable can read the database and pointer.
It uses its original freshness behavior.

## Links

- [Troubleshooting record](../troubleshooting/TSR-0136-ipinfo-and-download-integrity.md)
- [Shared lookup cache retention](ADR-0355-shared-reputation-lookup-retention.md)
- [Operator guide](../OPERATOR_GUIDE.md#ipinfo-refresh-and-recovery)
- Regression owners are `download`, `reputation`, and `internal/fsutil`.

# TSR-0136 - IPinfo And Download Integrity

Status: Monitoring
Date Opened: 2026-11-17
Date Resolved: 2026-11-17
Owner: GoCluster maintainers
Technical Area: reputation, download, filesystem publication
Trigger Source: Chat request and VS Code audit
Led To ADR(s): ADR-0357

## RCA Summary

- Root cause: Signed-in lookups rejected stale cache hits but reused the same old data on a
  miss. The Pebble reader also invented a build timestamp for invalid metadata.
- The downloader trusted a sidecar hash as evidence about local bytes. Both
  the downloader and CSV extractor deleted the previous file before replacement.
- The downloader accepted partial HTTP 206 payloads. Weak HTTP validators could
  also advance IPinfo freshness without establishing byte identity.
- An empty import could replace the serving database, and a failed candidate
  close could panic when cleanup attempted to close it a second time.
- Mixed-family range endpoints could panic during import. Reversed ranges and
  unenriched rows could pass the import minimum. Unchecked stored field lengths
  could panic during preload or direct lookup.
- IPinfo skipped import when a verified archive was already on disk, even if
  the serving database still represented different bytes.

## Fix Or Mitigation

Bind verification to the serving database, verify retained download bytes, and
publish replacements without deleting the old file first.

## Verification And Monitoring

The audit was reconciled against published main at `3fd37c6c`. Regressions
exercise same-size corruption under HTTP 200 and 304, failed replacement after
staging, missing or malformed Pebble metadata, and signed-in source selection
across cache, in-memory IPv4, and Pebble paths. A local HTTP fixture also
exercises verified renewal across restart and failed import followed by HTTP 304.
Additional failure fixtures exercise partial responses, weak and redirected 304
responses, empty imports, and a close error after a real candidate handle closes.
Shared downloader regressions exercise redirect retargeting with identical ETag
text, non-200 successes, legacy sidecar migration, and failed sidecar replacement.
IPinfo fixtures cover the exact version-one key and value encoding, private
extraction, configured path consistency, mixed valid and invalid CSV rows, and
malformed legacy Pebble records across preload, signed-in fallback, and pre-login
admission.

A successful upstream check renews freshness for the exact serving source.
See [ADR-0357](../decisions/ADR-0357-ipinfo-verification-and-download-integrity.md)
for the active contract and
[IPinfo refresh and recovery](../OPERATOR_GUIDE.md#ipinfo-refresh-and-recovery)
for operator steps. Monitor database-open and refresh warnings after rollout.
A successful archive download and a successful database import are separate
events. Repeated import warnings warrant checking disk space, write permissions,
and the configured source paths.

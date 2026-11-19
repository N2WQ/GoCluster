# TSR-0140 - Interrupted Filter Upgrade Backups

Status: Monitoring
Date Opened: 2026-11-19
Date Resolved: 2026-11-19
Owner: GoCluster maintainers
Technical Area: filter storage, login and shared presets
Trigger Source: Local resilience review
Led To ADR(s): ADR-0363

## RCA Summary

- Root cause: The backup helper created the final name before writing the original bytes.
Process exit bypassed deferred cleanup. A subsequent upgrade correctly refused
the mismatched final backup on every retry. An affected profile could fall back
to temporary settings. An affected library could block preset access across
SSIDs while an independently safe profile reset completed.

## Fix Or Mitigation

Publish and verify backups according to
[ADR-0363](../decisions/ADR-0363-filter-upgrade-backup-publication.md).
For existing files, follow the [backup recovery guidance](../cc-dialect.md#recovering-an-interrupted-upgrade-backup).

## Verification And Monitoring

The regression suite exits a child process during writing and immediately after
publication, then retries the schema-seven upgrade. Boundary tests exercise
a destination collision and directory-sync failure. The review used local
fixtures. Production rollout and the incidence of older partial backups are unknown.

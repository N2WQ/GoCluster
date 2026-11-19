# ADR-0363: Filter Upgrade Backup Publication

- Status: Accepted
- Date: 2026-11-19
- Decision Origin: Local resilience review and approved scope

## Context

[TSR-0140](../troubleshooting/TSR-0140-interrupted-filter-upgrade-backups.md)
reproduced a failed retry when a process stopped after creating a final backup
name and before writing the original bytes.

## Decision

Under the existing base-call stripe and single-writer directory contract, write,
sync, close and verify the original bytes in a private same-directory staging
file. Use one reserved staging name per final backup. After interruption,
discard an abandoned regular staging entry under the same ownership. Refuse
unexpected staging entry types.

Publish without replacement. Windows uses a same-directory write-through move.
Linux uses a hard link and syncs the parent directory. An existing or competing
final backup must be a regular file with exactly the original bytes and must
pass the durability checks before reset can proceed. Filesystem errors leave
the affected original protected and permit a later retry.

## Alternatives And Consequences

The profile replacement writer can overwrite its target. A separate existence
check followed by replacement would leave a collision window. The existing
backup suffixes and independent profile and library outcomes still apply. The
filesystem must support the publication operation. Older divergent final
backups require operator review.

This refines the backup mechanism in
[ADR-0330](ADR-0330-native-cc-banks-and-generation-seven.md).

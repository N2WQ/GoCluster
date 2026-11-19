# ADR-0362: Versioned Public Config and Upgrade Reports

- Status: Accepted
- Date: 2026-11-19
- Decision Origin: Approved release report and config bundle Scope Ledger v1

## Context

The release ZIP includes public YAML. Operators deploy private copies, and
some releases require edits before the new executable can start. A source diff
also misses the meaning of some changes, such as a new interpretation of an
existing value or a reset of saved client filters.

## Decision

Prepare one reviewed upgrade report for an explicit published baseline and
captured candidate commit. Use the same staged Markdown for the ZIP and the
GitHub Release. Explain required actions, behavior changes, and relevant
rollback implications. Link to the exact Git comparison and the relevant
decision and troubleshooting records. The maintainer prepares the prose;
the release script verifies its inputs and creates the package.

The YAML appendix compares public baseline YAML with the staged public YAML
using the pinned maintainer-side dyff CLI. The manifest beside `data/config`
records the release identity and hashes of the YAML bytes shipped. The
manifest format version covers the manifest itself. It introduces no config
schema version or runtime interpretation.

Prepare these files before output promotion or publication. Keep the existing
single publication path, output ownership, and partial-publish recovery
procedure. Package-only builds can produce a bundle offline. A clean
package-only build can also rehearse the report with a local baseline.

## Consequences

Operators get the required actions beside the download and can identify the
original public YAML after customizing it. Maintaining the report requires
human review of the source range and a pinned tool on the release machine.
The bundle describes shipped public files and has no access to private
operator settings.

## Links

- [Release preparation and output safety](ADR-0242-release-preparation-safety.md)
- [Release tag identity](ADR-0252-automatic-commit-suffix-release-tags.md)
- [Release maintainer instructions](../../scripts/README.md#release-preparation)
- [Operator upgrade instructions](../OPERATOR_GUIDE.md#upgrade-a-node)

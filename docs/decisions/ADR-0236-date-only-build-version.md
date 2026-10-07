# ADR-0236: Date-Only Build Version

- Status: Accepted
- Date: 2026-10-03
- Decision Origin: Design

## Context

The owner selected exactly `YYMMDD` for generated, displayed, and PC18 build
identity, replacing ADR-0077's prefix, separators, commit suffix, and dirty
suffix. Numeric peer compatibility metadata must remain independent.

## Decision

Use the UTC date as six digits, for example `261003`, in release and PGO
builds and the runtime date fallback. Preserve the existing embedded commit-date
fallback for plain builds and missing-metadata fallbacks. Explicit linker
versions retain their existing precedence.

The console, startup log, `--version`, `SHOW BUILD`, and PC18 consume the
startup-resolved version. Commit, build timestamp, and Go toolchain remain
separate metadata; `SHOW BUILD` remains compact. The owner's follow-up decision
removes the dirty/modified flag from runtime identity, `--version`, and PC18.
Go may still embed `vcs.modified`, but GoCluster neither reads nor exposes it.
PC18 retains its encoding, bounds, and direction-specific handshake.
Compatibility defaults remain PC18/PC92 version `5457`, PC92 build `633`, and
legacy PC19 version `5401`.

The owner also selected explicitly numbered release tags, independent of the
product version: `261003r2`. Every release-script build requires a positive
`-ReleaseNumber`, including package-only builds. Stamp this tag separately and
display it in `--version`, `SHOW BUILD`, and PC18. Plain and PGO builds leave it
empty and omit it from output. Package-only metadata denotes an intended tag,
not completed publication. Release tags identify the UTC build date; retries
after UTC midnight receive a different date.

Git tags, GitHub Release names, duplicate checks, and notes use the release tag.
Different release numbers allow multiple releases on the same day; existing
tags are never overwritten. The clean-source gate and package-only dirty-build
exception remain unchanged. ADR-0076's tag/version coupling and ADR-0076/0078's
dirty-version display requirements are replaced.

## Alternatives considered

1. Retain commit and dirty suffixes: rejected by the selected exact format.
2. Change numeric compatibility metadata: excluded by the owner's selection.
3. Automatically choose a release number: rejected in favor of explicit input
   and predictable same-day retries.

## Consequences

### Benefits

- Consistent compact date across build scripts, operator displays, and PC18.
- Compatibility metadata remains independent of product build identity.

### Risks

- Same-day builds have identical versions; source traceability requires separate
  commit and timestamp metadata, which may be unavailable in unstamped binaries.
- Plain builds expose the commit date, not the actual compile date.
- Reusing a release number/date fails the duplicate check; a midnight rebuild
  changes the date component even if the number is unchanged.

### Operational impact

- Use release-tag, commit, and timestamp metadata to distinguish same-day binaries.
- Build identity does not indicate dirty source; there is no dirty/modified flag.
- Dirty source remains forbidden for publication.

## Links

- Release-number and numbered-tag clauses superseded by [ADR-0252](ADR-0252-automatic-commit-suffix-release-tags.md). Date-only version and runtime identity contracts remain accepted.

- Related tests: `main_version_test.go`, `commands/processor_test.go`, `peer/pc18_identity_test.go`, `scripts/test-release-identity.ps1`
- Related docs: [build notes](../../README.md#build-and-service-notes), [peer profile](../../peer/README.md), [scripts](../../scripts/README.md)
- Supersedes: [ADR-0077](ADR-0077-compile-date-binary-version.md); refines release identity and parameter clauses in [ADR-0076](ADR-0076-github-release-package.md) and dirty-version display clauses in [ADR-0076](ADR-0076-github-release-package.md)/[ADR-0078](ADR-0078-release-package-clean-source-gate.md).

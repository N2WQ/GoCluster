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
startup-resolved version. Commit, build timestamp, working-tree modified flag,
and Go toolchain remain separate metadata; `SHOW BUILD` remains compact.
PC18 retains its encoding, bounds, and direction-specific handshake.
Compatibility defaults remain PC18/PC92 version `5457`, PC92 build `633`, and
legacy PC19 version `5401`.

Release tags and names continue to equal the generated binary version. Existing
duplicate checks reject a second publication for the same UTC day. The clean
source gate and package-only dirty-build exception remain unchanged; only the
`+dirty` display requirement in ADR-0076/0078 is replaced.

## Alternatives considered

1. Retain commit and dirty suffixes: rejected by the selected exact format.
2. Change numeric compatibility metadata: excluded by the owner's selection.
3. Introduce same-day release suffixes: outside the selected date-only identity.

## Consequences

### Benefits

- Consistent compact date across build scripts, operator displays, and PC18.
- Compatibility metadata remains independent of product build identity.

### Risks

- Same-day builds have identical versions; source traceability requires separate
  commit and modified metadata, which may be unavailable in unstamped binaries.
- Plain builds expose the commit date, not the actual compile date.
- At most one release can be published per UTC date through the existing script.

### Operational impact

- Use `--version` or PC18 metadata to distinguish same-day binaries.
- A version no longer indicates dirty source; inspect the separate modified flag.
- Dirty source remains forbidden for publication.

## Links

- Related tests: `main_version_test.go`, `commands/processor_test.go`, `peer/pc18_identity_test.go`
- Related docs: [build notes](../../README.md#build-and-service-notes), [peer profile](../../peer/README.md), [scripts](../../scripts/README.md)
- Supersedes: [ADR-0077](ADR-0077-compile-date-binary-version.md); refines only dirty-version display clauses in [ADR-0076](ADR-0076-github-release-package.md) and [ADR-0078](ADR-0078-release-package-clean-source-gate.md).

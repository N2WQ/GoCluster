# ADR-0077: Compile-Date Binary Version

- Status: Superseded
- Date: 2026-04-24
- Decision Origin: Design

## Context

The console header, startup log, telnet `SHOW BUILD`, and `--version` command
expose the binary identity operators use during deployment and troubleshooting.
Existing build paths could show different values: release packages could show a
Git tag, PGO builds could show `git describe --dirty`, and unflagged builds
could fall back to Go module pseudo-version metadata such as
`v0.0.0-20260424022627-78b3cd19baac+dirty`.

Operators need a compact version that identifies when the binary was built and
which source commit it came from, without requiring semantic release tags for
every operational build.

## Decision

Use `vYY.DD.MM-<12-char-commit>[+dirty]` as the operator-visible binary
version. The date is the UTC compile date for scripted builds, and `+dirty` is
included when the Git working tree had changes at build time.

The release script uses the same generated value for the binary's runtime
`Version` field, the Git tag, and the GitHub Release name.

The runtime resolver keeps a no-ldflags fallback for plain `go build .`: when
linker-stamped compile time is unavailable, it derives the same display shape
from Go VCS metadata rather than exposing the module pseudo-version directly.

Telnet `SHOW BUILD` reports the startup-resolved short version and Go toolchain
version when available. It does not probe VCS or rebuild metadata at command
time.

## Alternatives considered

1. Continue showing Git tags or module pseudo-versions.
2. Use full commit SHA values in the console header.
3. Use local workstation date instead of UTC date.

## Consequences

### Benefits

- All supported build scripts produce the same concise operator-visible
  identity format.
- UTC date semantics match existing UTC build timestamps and scripted release
  builds.
- The short commit reference keeps the console header readable while preserving
  practical traceability.

### Risks

- Release tags and runtime binary versions are intentionally aligned for
  scripted releases.
- Plain `go build .` cannot embed true compile time without linker flags, so
  its fallback date is derived from Go VCS metadata.

### Operational impact

- Operators should cite the console `Version` and, when needed, the
  `commit`/`built` fields from `--version` when reporting deployment state.
  Telnet `SHOW BUILD` is intentionally compact and omits commit/build time.
- Dirty builds are visibly marked in the version string and still expose the
  separate Go VCS modified flag when available.
- Normal release packages reject dirty source before build; a `+dirty` release
  package version indicates an explicit local `-AllowDirty` test package.

## Links

- Related issues/PRs/commits:
- Related tests: `go test .`, `go test ./commands`
- Related docs: `README.md`, `commands/README.md`, `scripts/create-release.ps1`, `scripts/consolidate-and-build-pgo.ps1`, `docs/decisions/ADR-0078-release-package-clean-source-gate.md`
- Related TSRs:
- Supersedes / superseded by: [ADR-0236](ADR-0236-date-only-build-version.md)

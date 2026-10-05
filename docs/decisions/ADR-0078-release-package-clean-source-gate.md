# ADR-0078: Release Package Clean Source Gate

The dirty-version suffix requirement below is superseded by
[ADR-0236](ADR-0236-date-only-build-version.md); the clean-source gate remains.

[ADR-0242](ADR-0242-release-preparation-safety.md) preserves the initial gate,
adds LF module attributes and diagnostics, and permits only verified outputs
created by the current invocation in the final source-status check.

- Status: Accepted
- Date: 2026-04-24
- Decision Origin: Design

## Context

The GitHub Release package is intended to be reproducible from committed
source-controlled inputs. A local release package built from uncommitted files
can contain binary behavior, rendered README content, or config examples that
do not correspond to any commit. Separately, stale `go.mod` or `go.sum`
metadata can make a release build depend on implicit local cleanup.

## Decision

The release script fails by default when `git status --porcelain` reports any
uncommitted worktree changes. It also runs `go mod tidy -diff` before staging
or compiling; any required module-file change fails the package build without
modifying files.

For local validation only, the script accepts `-PackageOnly -AllowDirty`. Dirty
local test packages keep the existing `+dirty` version suffix so they are
visibly distinct from clean release packages. Publishing never allows dirty
source.

Generated release outputs stay out of Git history. The repository provides a
tracked `download/README.md` pointer to GitHub Releases instead of committing
`ready_to_run/` or `gocluster-windows-amd64.zip`.

## Alternatives considered

1. Warn on dirty source and continue.
2. Automatically run `go mod tidy` and package the resulting local files.
3. Prompt interactively when the worktree is dirty.

## Consequences

### Benefits

- Normal release packages correspond to committed source.
- Release publishing and local package-only builds use the same
  non-interactive cleanliness policy.
- Module hygiene problems are reported before a zip is created.
- Local test packages remain possible, but require an explicit switch and are
  marked in the binary version.
- Publishing is centralized in one script rather than split between local
  packaging and a tag-triggered workflow.
- Git visitors can find the compiled binary from a tracked download pointer
  without adding generated artifacts to commits.

### Risks

- Developers must commit or stash unrelated local edits before creating a
  normal package.
- `go mod tidy -diff` requires a Go toolchain version that supports the flag.

### Operational impact

- Operators should not receive dirty release packages from the normal release
  path.
- A binary version ending in `+dirty` means the package came from an explicit
  local test build, not the default release path.
- `download/README.md` is the Git-visible download entry point; Releases remain
  the binary distribution surface.

## Links

- Related issues/PRs/commits:
- Related tests: `go mod tidy -diff`, `scripts/create-release.ps1 -PackageOnly -AllowDirty`, `scripts/create-release.ps1` dirty-worktree failure check
- Related docs: `README.md`, `download/README.md`, `scripts/create-release.ps1`, `docs/decisions/ADR-0076-github-release-package.md`, `docs/decisions/ADR-0077-compile-date-binary-version.md`
- Related TSRs:
- Supersedes / superseded by:

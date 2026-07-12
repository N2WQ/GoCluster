# ADR-0229: Full Build-Tagged Code Map Coverage

- Status: Accepted
- Date: 2026-07-12
- Decision Origin: Troubleshooting chat

## Context

ADR-0147 established deterministic checked-in code maps generated from Go
package metadata. The implementation used the `GoFiles` and `Imports` selected
by `go list -json` on the invoking host. That made the maps deterministic only
within one build configuration.

The Windows-generated checked-in maps selected Windows socket, atomic-replace,
and non-CGO H3 files. Ubuntu CI selected the corresponding non-Windows and CGO
files and an additional repository dependency, so the same commit was fresh on
Windows and stale in CI. Regenerating on either host would only reverse which
host failed.

Code maps are support and blast-radius artifacts. They must expose relevant
checked-in platform variants without implying that every variant coexists in a
single binary.

## Decision

For every package selected by `docs/code-maps/manifest.json`, code-map
generation inventories all Go-recognized files directly in that package
directory regardless of active build constraints.

Non-test source imports are parsed and unioned across those files. Test files
remain listed, while test-only imports remain excluded from production package
dependency edges. File lists, imports, edges, and fingerprints are sorted and
deduplicated so the rendered artifact is identical across host platforms.

`go list -json` remains responsible for resolving the manifest's explicit
package paths, package names, and directories. Package-directory access and
source parsing fail closed with a file-specific diagnostic rather than
silently producing a partial map.

Generated documentation states that build-tagged files and dependency edges
are possible-build coverage and may be mutually exclusive.

This decision selectively supersedes ADR-0147's host-selected source-file and
import discovery. ADR-0147's manifest, ADR matching, generated Markdown,
freshness comparison, CI, release, and wrapper decisions remain accepted.

## Alternatives considered

1. Keep maps dependent on the invoking host.
   - Rejected because one checked-in artifact cannot pass deterministic
     freshness checks on both Windows development and Linux CI.
2. Always generate for one canonical Linux configuration.
   - Rejected because the resulting support artifact would omit Windows-only
     source paths used by the published runtime.
3. Union a fixed matrix of Windows/Linux and CGO configurations through
   repeated `go list` calls.
   - Rejected because future architecture, custom-tag, or platform constraints
     could escape the matrix and silently recreate incomplete coverage.

## Consequences

### Benefits

- Windows and Linux produce the same checked-in maps and fingerprints.
- Support and blast-radius review can see every checked-in platform variant.
- An inactive platform file can add a repository dependency without escaping
  map freshness.

### Risks

- A union edge can be impossible in the currently running binary; the map's
  limits must remain explicit.
- Syntax or import errors in an inactive non-test source file now block map
  generation instead of being hidden by the current host selection.
- Test-only dependency edges remain outside the graph and require direct test
  inspection when material.

### Operational impact

- No production GoCluster runtime, protocol, configuration, parser, queue,
  lifecycle, VOACAP, H3, telnet, or replay behavior changes.
- CI and local freshness checks converge on one platform-independent artifact.

## Links

- Related issues/PRs/commits: GitHub Actions run `29190757076`
- Related tests: `go test ./cmd/codemap`, isolated-cache
  `go run ./cmd/codemap check -all`
- Related docs: `docs/code-maps/README.md`,
  `docs/code-maps/manifest.json`
- Related TSRs: TSR-0034
- Supersedes / superseded by: selectively supersedes ADR-0147

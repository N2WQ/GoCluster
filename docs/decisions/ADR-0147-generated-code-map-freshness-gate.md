# ADR-0147: Generated Code Map Freshness Gate

- Status: Accepted
- Date: 2026-06-05
- Decision Origin: Design
- Selective supersession: ADR-0229 replaces host-selected source-file and
  import discovery with full build-tagged source coverage. All other ADR-0147
  decisions remain accepted.

## Context
The repository already routes dependency-visualization and support-agent code
map questions to `docs/code-maps/`. The custom GPT support agent can retrieve
Markdown, source, YAML, JSON, and first-party PowerShell, but it cannot run
local graph tools such as `goda`, Graphviz `dot`, `gopls`, or `callgraph`.

The missing piece was a maintained artifact. A directory with only a README
defined the convention, but it did not give agents current graph evidence. At
the same time, hand-maintained architecture summaries can drift and can be
over-trusted by future agents.

## Decision
Maintain code maps as generated Markdown artifacts based on current code
metadata and ADR records.

Add a deterministic `cmd/codemap` tool that:

- reads `docs/code-maps/manifest.json`
- uses `go list -json` to discover scoped packages, imports, source files, and
  test files
- reads `docs/decision-log.md` and `docs/decisions/*.md` to attach related ADRs
- writes generated Markdown maps under `docs/code-maps/`
- checks freshness by regenerating in memory and comparing checked-in files

Add PowerShell wrappers:

- `scripts/update-code-maps.ps1` for intentional regeneration
- `scripts/check-code-maps.ps1` for non-mutating freshness checks

CI and the release script run the non-mutating check. The release script does
not regenerate maps during packaging because release artifacts should correspond
to committed source. A `-SkipCodeMapCheck` escape hatch is allowed only with
`-PackageOnly` for local package testing.

Rendered graph images remain optional local scratch artifacts under `tmp/`.
They are not the support agent's authoritative evidence.

## Alternatives considered
1. Maintain human-authored code-map summaries.
   - Rejected because the user explicitly wants agent-generated maps based on
     code and ADRs only, and hand-authored content can drift.
2. Generate or update maps inside the release script.
   - Rejected because release packaging must not create uncommitted source docs
     behind the release tag.
3. Require `goda` and Graphviz for freshness.
   - Rejected because package metadata from Go is enough for the baseline map
     and optional graph tools should not block ordinary release validation.
4. Leave maps as ad hoc `tmp/` SVGs.
   - Rejected because the support agent cannot retrieve or regenerate those
     local artifacts.

## Consequences
### Benefits
- Support agents get current, retrievable graph evidence.
- Humans can update maps with one command and review deterministic diffs.
- CI and release catch stale generated maps before they become trusted stale
  documentation.
- The release path remains check-only and aligned with the clean-source gate.

### Risks
- ADR matching can be noisy because it is deterministic text and metadata
  matching, not semantic reasoning.
- Maps can still be over-trusted if agents ignore the generated limits section.
- The manifest can omit an important package scope until a new map is added.

### Operational impact
- No runtime, telnet, config, parser, protocol, queue, archive, peer, replay,
  or user-visible behavior changes.
- CI gains a generated-code-map freshness check.
- `scripts/create-release.ps1` fails on stale checked-in maps unless
  `-PackageOnly -SkipCodeMapCheck` is explicitly used for local testing.

## Links
- Related issues/PRs/commits:
- Related tests:
  - `go test ./cmd/codemap`
  - `go run ./cmd/codemap check -all`
  - `scripts/check-code-maps.ps1 -All`
  - package-only release smoke test
- Related docs: `docs/code-maps/README.md`,
  `docs/code-maps/manifest.json`, `docs/code-maps/runtime-ingest-fanout.md`,
  `scripts/README.md`, `.github/workflows/ci.yml`,
  `scripts/create-release.ps1`
- Related TSRs:
- Supersedes / superseded by: selectively superseded by ADR-0229

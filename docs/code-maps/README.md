# Code Maps

This directory is for support-agent-readable code maps generated from current
repository evidence. Maps are generated artifacts, not hand-authored narrative
content.

The custom GPT support agent can retrieve Markdown source files, but it cannot
run local tools such as `gopls`, `goda`, `callgraph`, or Graphviz `dot`.
Therefore durable graph evidence belongs in checked-in Markdown generated from
code, tests, `docs/decision-log.md`, and `docs/decisions/*.md`. Rendered
SVG/PNG files may be useful locally, but they are not the support agent's
authoritative input.

## When To Add A Code Map

Add a code map only when one of these is true:
- a subsystem dependency shape is repeatedly useful for support or developer
  routing
- a Non-trivial blast-radius audit found package impact that is hard to explain
  from source paths alone
- a generated graph materially clarifies a durable architecture decision

Do not check in broad whole-repo graphs by default. They are noisy, drift
quickly, and can make the support agent over-trust stale topology.

Generated map files must start with a generated-file warning and should not be
edited by hand. Change the manifest or generator, then regenerate the map.

## Generated Map Shape

Each checked-in code map is generated from `docs/code-maps/manifest.json` and
should include:
- scope: packages or subsystem covered
- source fingerprint
- generation command
- package dependency edges
- repository dependencies outside the explicit scope
- all Go-recognized source and test files in each selected package directory,
  including files excluded from the current host by build constraints
- package imports unioned from every non-test source file across those build
  constraints
- related ADRs discovered from decision-log metadata and ADR text
- limits: mutually exclusive build-tagged files and edges do not coexist in
  every binary, test-only imports are excluded, and the graph does not prove
  interface dispatch, runtime feature flags, or concrete traffic paths

## Local Commands

Examples:

```powershell
.\scripts\update-code-maps.ps1 -All
.\scripts\check-code-maps.ps1 -All
go run ./cmd/codemap generate -all
go run ./cmd/codemap check -all
```

Optional local visuals can still be rendered to `tmp/` when useful:

```powershell
goda graph ./internal/cluster ./telnet | dot -Tsvg -o .\tmp\cluster-telnet.svg
```

Use generated maps to guide source review, not to replace it. For concrete
behavior, verify the relevant source, tests, config, and decision records.

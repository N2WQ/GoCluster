# ADR-0198: 160m Merge Readiness Cleanup

- Status: Accepted
- Date: 2026-06-19
- Decision Origin: Follow-up implementation cleanup

## Context

The 160m native fallback experiment branch diverged from `origin/main` after
main accepted ADR-0194 and ADR-0195 for unrelated workflow and telnet
read-pause decisions. The branch also had stale generated code-map output,
local Phase 1 comparison artifacts under `tmp/`, a recurring Custom SCP test
fixture drift, and lint failures in solar-weather test helpers.

## Decision

Keep the native 160m runtime behavior from ADR-0197 unchanged and make only
merge-readiness cleanup changes:

- preserve mainline ADR-0194 and ADR-0195
- renumber native 160m ADRs to ADR-0196 and ADR-0197
- regenerate generated code maps from current source and ADR metadata
- remove local Phase 1 comparison artifacts from the merge diff
- repair the Custom SCP test fixture with current-relative time data
- fix lint in test and benchmark helpers without changing runtime contracts

## Alternatives considered

1. Keep duplicate ADR-0194/0195 records. Rejected because duplicate ADR IDs
   would make decision-memory references ambiguous after merge.
2. Drop the native 160m ADRs and rely only on code comments. Rejected because
   the native fallback is an operator-visible behavior change and needs durable
   decision memory.
3. Change Custom SCP runtime horizon behavior to satisfy the test. Rejected
   because the failure is fixture age drift, not a production-retention defect.

## Consequences

### Benefits

- Keeps the branch mergeable with current `origin/main`.
- Preserves unambiguous ADR numbering and links.
- Keeps generated code maps aligned with source and decision metadata.
- Removes experiment scratch artifacts from the production merge surface.

### Risks

- Main can advance again and consume later ADR numbers before merge; if that
  happens, repeat the renumbering against the new main.
- Code-map diffs may be noisy because they reflect both this branch and
  mainline changes.

### Operational impact

- No runtime, config, protocol, parser, queue, prediction, telnet, or operator
  behavior changes.

## Links

- Related issues/PRs/commits: experiment branch `experiment/160m-native-heuristic`
- Related tests: `internal/cluster/main_test.go`; `solarweather/archive_compare_test.go`; `solarweather/gate_bench_test.go`
- Related docs: `docs/decision-log.md`; `docs/code-maps/manifest.json`; `docs/code-maps/path-reliability-voacap.md`; `docs/code-maps/runtime-ingest-fanout.md`
- Related TSRs: TSR-0024
- Supersedes / superseded by: -

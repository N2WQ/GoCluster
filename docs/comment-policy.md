# Comment Policy for Go Source

This policy defines comment quality gates for `gocluster` with strict alignment between automation and human review.

## Goals

- Keep exported and package-level contracts documented and discoverable.
- Capture non-obvious operational behavior: queue/drop/disconnect semantics, timeout rationale, and concurrency ownership.
- Avoid low-signal comments that restate code.

## CI-Enforced Checks

The following checks are enforced by `golangci-lint` in CI:

- Exported declarations must have doc comments (`revive: exported`).
- Packages must have package comments (`revive: package-comments`).
- Comment formatting must use standard spacing conventions (`revive: comment-spacings`).
- `nolint` directives must include rationale and must not be unused (`nolintlint`).
- Basic spelling hygiene in comments (`misspell` with US locale).

Run locally:

```bash
golangci-lint run ./...
```

## Reviewer-Enforced Checks (Required)

These are required but intentionally human-reviewed because tool-only checks are not reliable enough:

- Comments explain `why`, invariants, and failure modes, not only `what`.
- Concurrency comments identify ownership/lifetime of goroutines, timers, channels, and locks where non-obvious.
- Backpressure comments define exact behavior for queue full conditions, drop policy, and disconnect thresholds.
- Deadline/timeout comments explain rationale and interaction with cancellation/shutdown.
- Operator-facing metrics/log comments describe interpretation and intended operational action.
- Public API comments describe behavior contracts, including ordering, drops, and error semantics when relevant.

## Where to Comment

- Exported functions/types/consts/vars: always.
- Package comment: one canonical package-level comment per package.
- Complex internal code paths: add concise invariant and ownership comments.
- Hot paths: keep comments short and stable; avoid stale algorithm narration.

## Disallowed Patterns

- Redundant comments that merely restate code.
- Ambiguous `TODO` notes with no context.
- Comments that contradict runtime behavior or tests.

## PR Gate Usage

The PR template includes a comment policy section. A PR is review-complete only when:

- CI passes comment-related lint checks.
- Reviewer confirms required human-only checks for impacted code paths.

If a policy exception is required, document it in PR notes and justify the bounded risk.

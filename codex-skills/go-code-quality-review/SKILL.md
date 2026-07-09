---
name: go-code-quality-review
description: "Use for independent read-only review of newly written gocluster Go implementation code before final closeout. Trigger after Go code changes in Non-trivial tasks when the environment supports independent agents unless the user explicitly prohibits independent-agent use."
---

# Go Code Quality Review

## Overview

Use this skill after Go implementation work and before final closeout to review
the diff against GoCluster's code-quality, workflow, validation, and
operational standards. The review is independent evidence from a separate agent
context; the lead agent still owns fixes, validation claims, traceability, and
the final response.

## Workflow

1. Confirm the phase boundary.
   - Use after code is written for Non-trivial Go implementation work.
   - Do not edit files, propose diffs, run formatters, create generated
     artifacts, or run broad/full validation suites.
   - If independent agents are supported and not explicitly prohibited, use an
     independent explorer for this review.
   - Do not trigger this skill for documentation-only Markdown changes unless
     the diff also changes Go code or a runtime/code contract.

2. Inspect the approved scope and current diff.
   - Compare the diff to the approved Scope Ledger version and slice plan.
   - Check for hidden scope expansion, speculative cleanup, compatibility
     shims, fallback paths, future-proof hooks, or broad refactors.
   - Name every changed Go file and any touched tests/docs that are material to
     the review.

3. Review GoCluster code-quality standards.
   - Apply `docs/code-quality.md`, `docs/change-workflow.md`,
     `docs/review-checklist.md`, and `docs/dev-runbook.md`.
   - Check correctness, reviewability, missing error paths, no placeholders,
     and comment intent.
   - Check bounded retained state, eviction or deletion coupling, and
     cardinality proof for maps, caches, interners, pools, indexes, or side
     tables.
   - Check goroutine, timer, channel, socket, queue, cancellation, deadline,
     shutdown, and file-handle ownership where relevant.
   - Check parser, protocol, config, YAML, operator-visible, and
     support-agent impacts when relevant.
   - Check hot-path allocation, CPU, lock, and p99 claims only against actual
     benchmark/profile evidence.

4. Review validation evidence.
   - Verify the selected validation lane matches the touched surface.
   - Check whether targeted tests, `go test`, `go vet`, `staticcheck`,
     `golangci-lint`, race checks, fuzzing, benchmarks, or pprof were required
     and reported.
   - Do not run final validation for the lead agent; identify missing or stale
     evidence instead.

5. Report findings first.
   - Order findings by severity.
   - Include file paths and line references when inspected.
   - Separate material findings, non-blocking observations, and remaining
     unknowns.
   - State whether the independent review used a separate context window, was
     prohibited, was unsupported, failed, or timed out.

## Output Expectations

- Include a compact `Go code quality review` result.
- If there are no material findings, say `Go code quality review findings:
  none material`.
- The lead agent must disposition findings, make any fixes within approved
  scope, rerun required checks, and preserve lead-owned closeout.

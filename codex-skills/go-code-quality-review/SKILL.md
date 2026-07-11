---
name: go-code-quality-review
description: "Use for read-only review of newly written gocluster Go implementation when the work is High-risk or substantial: shared/exported interfaces, material algorithms or state machines, substantial rewrites, meaningful residual uncertainty, or multiple production packages with shared impact or cross-package uncertainty. Do not trigger for every Non-trivial Go edit."
---

# Go Code Quality Review

## Overview

Use this skill after Go implementation work and before final closeout to review
the diff against GoCluster's code-quality, workflow, validation, and
operational standards. Use a separate non-steered reviewer when credible review
evidence depends on reasoning outside the implementation context. Otherwise the
method may remain lead-owned. The lead still owns fixes, validation claims,
traceability, and the final response.

## Workflow

1. Confirm the phase boundary.
   - Use after code is written for High-risk or substantial Go implementation.
   - If a separate reviewer is used, keep it read-only and findings-only.
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
   - For command-backed concurrency, lifecycle, queue, timer, shutdown,
     shared-state, or leak-detection claims, apply `docs/review-checklist.md`
     `Command Evidence`: check the command, scope, observed result, and the
     rationale or minimal excerpt required for a high-risk claim. A bare
     PASS/FAIL line is not sufficient for these claims.
   - Do not generate or run transcript evidence for the lead agent. Report
     missing, stale, skipped, failed, timed-out, partial, or waived evidence
     accurately.
   - Do not run final validation for the lead agent; identify missing or stale
     evidence instead.

5. Report findings first.
   - Order findings by severity.
   - Include file paths and line references when inspected.
   - Separate material findings, non-blocking observations, and remaining
     unknowns.

## Output Expectations

- Report material findings and inspected evidence without a mandatory heading,
  taxonomy, or result envelope.
- The lead agent must disposition findings, make any fixes within approved
  scope, rerun required checks, and preserve lead-owned closeout.

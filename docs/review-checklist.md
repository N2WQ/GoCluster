# Codex Review Checklist

This checklist owns final-diff review outcomes. Specialist skills own their
unique methods, and `docs/dev-runbook.md` owns validation commands.

## Read-only Review

Ground findings in inspected current evidence. Separate confirmed behavior,
inference, and unknowns. Read-only findings do not authorize mutation; a later
change enters the applicable gate first.

## Final-diff Review

Review the actual final diff and touched files for:

- match to approved scope and absence of hidden expansion;
- correctness, edge cases, error handling, compatibility, and determinism;
- smallest-change discipline and absence of speculative abstractions,
  fallbacks, compatibility paths, or unrelated cleanup;
- resource bounds, ownership, cleanup, lifecycle, and concurrency when
  applicable;
- config, parser, protocol, operator, scientific/model, or performance
  contracts when applicable;
- tests and checks capable of falsifying the changed behavior;
- stale or missing authoritative, operator, support, or decision documentation;
- the required support-agent documentation impact decision is present, correctly
  classified, and reflected in relevant `customgpt/` updates;
- placeholders, ignored errors, misleading comments, and unsupported claims.

Use only the applicable sections of `docs/code-quality.md` and triggered skills
for deeper domain review.

Reuse this source-review method for meaningful implementation units before
dependent work, as directed by `docs/change-workflow.md`. Follow each approved
obligation into actual implementation, including refusals, error paths and
relevant performance/resource contracts. Challenge whether the evidence could
pass with that behavior missing or wrong. A test pass or item-to-file mapping
alone is not a completeness verdict. Review expected behavior as well as tests,
and keep permanent protection proportionate to distinct failure mechanisms.

## Go Review Method Trigger

The Go code-quality review method is required only for High-risk or substantial
Go implementation. Multiple production packages trigger the method when shared
behavior, ownership, interfaces, contracts, or meaningful cross-package
uncertainty are affected. High-risk classification, shared or exported
interface changes, material algorithm or state-machine changes, substantial
production-file rewrites, and meaningful residual uncertainty are method
triggers. Line count alone does not determine substantiality.

Standard Non-trivial Go work receives a disciplined lead review. When an
independent context is required because same-context reasoning would compromise
evidence credibility, the reviewer is read-only and findings-only. The lead
owns evidence verification, fixes, reruns, integration, and final claims.

## Substantial Go Review Method

Use after Go implementation and before final closeout when the Go Review
Method Trigger above applies. The repo-managed `go-code-quality-review` skill
is a Codex discovery entry point to this complete method. Documentation-only
changes do not trigger it unless they also change Go code or a runtime/code
contract.

1. Compare the approved Scope Ledger and meaningful implementation units to the
   actual diff. Check for hidden expansion, speculative cleanup, compatibility
   shims, fallbacks, future-proof hooks and broad refactors. Name every changed
   Go file and materially touched test or document.
2. Apply the final-diff review above and applicable `docs/code-quality.md`
   sections. Check correctness, reviewability, error paths, placeholders and
   comment intent. For retained state, check cardinality and retained-byte
   bounds plus deletion/eviction coupling. For lifecycle work, check ownership
   of goroutines, timers, channels, sockets, queues, cancellation, deadlines,
   shutdown and file handles. Check parser, protocol, config/YAML, operator and
   support-agent contracts when relevant.
3. Verify the lane from `docs/dev-runbook.md` matches the touched surface.
   Review required targeted tests, Go tests, vet, Staticcheck, lint, race,
   fuzzing, benchmarks and profiles as applicable. Evaluate allocation, CPU,
   lock and p99 claims against actual measurements. Apply Command Evidence
   below; a bare PASS/FAIL does not establish a high-risk command-backed claim.
4. Report findings by severity with inspected file references and evidence.
   Separate material findings, non-blocking observations and unknowns; report
   missing, stale, skipped, failed, timed-out, partial or waived evidence
   accurately. No fixed heading, taxonomy or result envelope is required.

Use a separate non-steered reviewer when credible review evidence depends on
reasoning outside the implementation context; otherwise the method may remain
lead-owned. A separate reviewer is read-only and findings-only: do not generate
transcript evidence or run final validation for the lead. The lead retains its
own validation responsibility, dispositions findings, makes approved fixes,
reruns affected checks and owns integration, traceability and final claims.
Apply Fresh Verification And Invalidation below; lead-owned review is not
independent review.

## Fresh Verification And Invalidation

High-risk work receives a fresh final pass over the approved scope, final diff,
selected validation, claim wording, and decision disposition. The fresh pass
may be lead-owned, but it must not be described as independent review.

When review causes changes:

- rerun the affected targeted checks;
- rerun the complete lane only when the fix can invalidate broader results,
  including shared behavior, build configuration, interfaces, concurrency, or
  cross-package contracts;
- do not reuse evidence from a state that the fix invalidated.

## Command Evidence

For ordinary successful checks, command, scope, and observed result are enough.
Add rationale or a minimal excerpt for failures, skips, waivers, surprising
results, benchmarks, profiles, runtime evidence, or high-risk command-backed
claims. Label static reasoning as static; do not present it as test, profile, or
runtime confirmation. Avoid secrets and unnecessary logs.

## Traceability And Closeout

Map each approved Non-trivial item to changed locations and validation. Include
the required support-agent documentation impact decision and relevant updates.
Add documentation and ADR/TSR references only when they apply. Report material
findings, gaps, waivers, and residual risks; do not enumerate irrelevant audit
categories or repeat evidence already stated.

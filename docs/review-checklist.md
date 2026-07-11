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
- placeholders, ignored errors, misleading comments, and unsupported claims.

Use only the applicable sections of `docs/code-quality.md` and triggered skills
for deeper domain review.

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

Map each approved Non-trivial item to changed locations and validation. Add
documentation and ADR/TSR references only when they apply. Report material
findings, gaps, waivers, and residual risks; do not enumerate irrelevant audit
categories or repeat evidence already stated.

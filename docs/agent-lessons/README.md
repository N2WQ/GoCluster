# Agent Lesson Memory

This directory records recurring model and workflow lessons for agents working
in this repository.

Agent lessons are not ADRs, TSRs, runtime contracts, validation proof, or
support-agent answers by themselves. They are a routing and memory layer for
process corrections that should survive model changes and future worktree
sessions.

Use this surface only when an approved scope includes workflow or agent-memory
maintenance. Runtime behavior, protocol/config/parser changes, scientific/model
decisions, operational contracts, incidents, and troubleshooting outcomes still
belong in ADRs, TSRs, tests, source comments, operator docs, and support-agent
routing.

## Lesson Standard

Each lesson must be short and evidence-linked:

- trigger: the repeated agent behavior or failure mode
- rule: the future behavior expected from agents
- evidence: source, workflow doc, test, command output, ADR, TSR, or support
  route that proves why the lesson exists
- limit: what the lesson does not prove

Do not use lessons to bypass `AGENTS.md`, Scope Ledgers, validation lanes,
decision-memory handling, or current-source inspection.

## Current Lessons

### Evidence Before Claims

- trigger: progress, validation, benchmark, runtime, or science/model claims can
  sound stronger than the inspected evidence supports
- rule: tie each material claim to current-session source, command output,
  tests, benchmark/profile data, runtime captures, or ADR/TSR records; label
  inferred, skipped, failed, stale, or unknown evidence explicitly
- evidence: `AGENTS.md`, `docs/change-workflow.md`, `docs/review-checklist.md`,
  `VALIDATION.md`
- limit: this lesson does not validate any specific runtime behavior

### Fresh Verification For High-Risk Work

- trigger: long or high-risk implementation work can carry forward stale
  assumptions after code changes
- rule: perform a fresh verifier pass before closeout for high-risk
  Non-trivial slices; use an independent verifier only when the active
  environment and user authorization support it, otherwise perform fresh
  self-verification
- evidence: `docs/change-workflow.md`, `docs/review-checklist.md`,
  `docs/templates/non-trivial-change-template.md`
- limit: verifier notes do not replace tests, benchmarks, pprof, race checks, or
  ADR/TSR handling

### No Speculative Implementation

- trigger: high-capability models can add abstractions, fallback paths,
  compatibility shims, cleanup, or future-proof hooks that were not approved
- rule: implement the smallest approved behavior and stop for a revised Scope
  Ledger if new structure or fallback behavior becomes necessary
- evidence: `docs/code-quality.md`, `docs/change-workflow.md`,
  `docs/review-checklist.md`
- limit: this lesson does not prohibit abstractions that are required,
  scope-approved, bounded, and validated

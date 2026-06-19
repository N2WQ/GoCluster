# ADR-0194: LLM Workflow Verification And Claim Evidence

- Status: Accepted
- Date: 2026-06-18
- Decision Origin: Design

## Context

The repository already has a strict Codex execution contract, slice-shaped Scope
Ledgers, current-state discovery, workflow-drift audits, decision-memory
handling, code-walk/blast-radius/leak workflows, documentation-only validation,
and support-agent routing.

A review of Anthropic's Claude Fable 5 prompting guidance identified four
model-evolution recommendations that fit GoCluster's priorities: fresh
verification, evidence-grounded progress claims, less speculative scaffolding,
and lightweight model/workflow lesson memory. The useful parts are not the
speed-oriented prompting advice. The useful parts are the controls that reduce
false progress claims, stale assumptions, over-engineered diffs, and unsupported
performance or scientific conclusions.

The user clarified that correctness, performance, low latency, and
scientifically grounded call/path/VOACAP behavior matter more than planning
speed.

## Decision

Evolve the repo workflow with four model-neutral controls:

1. High-risk Non-trivial work gets a fresh verifier pass before closeout.
   Independent verifier agents may be used only when the active environment and
   user authorization support them; otherwise the implementing agent performs a
   fresh self-verification pass.
2. Progress, implementation, validation, performance, and science/model claims
   must be tied to current-session evidence or explicitly labeled as inferred,
   stale, skipped, failed, blocked, or unknown.
3. Code-quality and review guidance rejects speculative abstractions, fallback
   paths, compatibility shims, feature flags, broad cleanup, and future-proof
   hooks unless they are approved, necessary, bounded, and validated.
4. Add `docs/agent-lessons/README.md` as a small repo-local memory surface for
   recurring model/workflow lessons. Agent lessons are not ADRs, TSRs, runtime
   contracts, validation evidence, or support-agent answer sources by
   themselves.

Keep existing validation rigor. Do not import generic model-provider advice in a
way that weakens GoCluster's parser, queue, lifecycle, hot-path, config,
operator-output, path-reliability, VOACAP, p50, propagation, or call-correction
validation boundaries.

## Alternatives considered

1. Adopt the Fable guide broadly.
   - Rejected because generic prompt guidance can weaken repo-specific
     validation and approval discipline if imported literally.
2. Ignore the Fable guidance.
   - Rejected because fresh verification and claim-evidence discipline directly
     improve the odds of correct, measured, scientifically grounded work.
3. Make independent subagents mandatory.
   - Rejected because tool availability and user authorization vary. The
     workflow needs a single-agent fallback that preserves the verification
     obligation.
4. Put recurring lessons into ADRs.
   - Rejected because ADRs record durable architecture and workflow decisions,
     while agent lessons are lightweight process memory and must not be treated
     as runtime truth.

## Consequences

### Benefits

- Future high-risk changes get a fresh closeout check against scope, evidence,
  validation, and decision records.
- False-green validation and overstrong progress claims become easier to catch.
- Hot-path and retained-state code is less likely to accumulate unapproved
  helper layers or fallback behavior that hurts latency, allocation shape, or
  reviewability.
- Path, VOACAP, p50, propagation, and call-correction claims have a clearer
  evidence standard.
- Recurring agent mistakes can be recorded without polluting ADR/TSR history.

### Risks

- The workflow can become heavier if fresh verification is applied mechanically
  to low-risk work.
- Agent lesson memory can drift if future updates do not point back to
  authoritative source, tests, workflow docs, ADRs, or TSRs.
- Independent verifier guidance can be misread as permission to spawn agents in
  environments where the user or tool contract does not allow it.

### Operational impact

- No runtime, config, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command behavior
  changes.
- Future workflow closeouts for high-risk Non-trivial work should report fresh
  verifier outcome and claim evidence.
- Support-agent developer routing can point to the agent lesson surface for
  recurring process lessons, but implementation claims still require current
  source, tests, docs, and decision records.

## Links

- Related issues/PRs/commits:
- Related tests: documentation-only validation, targeted workflow text checks,
  support-agent routing text checks, `git diff --check`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/templates/non-trivial-change-template.md`,
  `docs/review-checklist.md`, `docs/code-quality.md`,
  `docs/dev-runbook.md`, `docs/WORKING_WITH_CODEX.md`,
  `docs/agent-lessons/README.md`, `customgpt/source-map.md`,
  `customgpt/developer-guide-index.md`, `customgpt/common-questions.md`
- Related external reference:
  `https://platform.claude.com/docs/en/build-with-claude/prompt-engineering/prompting-claude-fable-5`
- Related TSRs:
- Supersedes / superseded by:

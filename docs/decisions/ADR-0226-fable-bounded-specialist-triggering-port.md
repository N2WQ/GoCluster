# ADR-0226: Fable Bounded Specialist Triggering Port

- Status: Accepted
- Date: 2026-07-11
- Decision Origin: Design

## Context

`ADR-0206` mapped Codex's three canonical independent-review roles onto real
Claude subagents — `fable-scope-adversary`, `fable-code-reviewer`,
`fable-fresh-verifier` — using Codex's *then-current* model, in which
`scope-ledger-adversarial-review` and `go-code-quality-review` were
effectively default-on for their task category. `ADR-0217` later added
Fable's four pre-code independent-evidence roles but treated
`fable-scope-adversary`'s mandate as pre-existing fact rather than
establishing it.

Codex has since moved past that model. `ADR-0221` → `ADR-0222` → `ADR-0223`
(2026-07-10/11) explicitly rejected "default specialist stage" behavior:
Non-trivial or High-risk status alone no longer creates a default subagent
stage on the Codex side; specialists — including the scope-adversary and
Go-review methods — trigger only on concrete risk criteria. `ADR-0223`'s
rejected alternative states the reasoning plainly: requiring independent
agents for every Non-trivial or High-risk task "restores default
orchestration without an evidentiary need."

An audit of the two contracts (this session) found Fable had not carried
that refinement forward. `fable-scope-adversary` was still described as
"Use for every Non-trivial plan," and `fable-code-reviewer` as "Use after Go
code changes in Non-trivial tasks," with no equivalent to Codex's explicit
Substantial-Go definition. Every other Fable independent-review role
(`fable-fresh-verifier`, `fable-scientific-oracle`,
`fable-requirements-adversary`, `fable-design-challenger`,
`fable-test-strategy-adversary`) already carried explicit trigger
conditions matching Codex's current model — these two were the exception,
not the rule.

The user confirmed they want both roles ported to risk-triggered behavior.

## Decision

1. `fable-scope-adversary` triggers when a Plan Mode plan is High-risk,
   uncertain, disputed, difficult to reverse, leaves material residual
   uncertainty, or changes a workflow-contract file (`CLAUDE.md`,
   `docs/fable-workflow.md`, `docs/fable-review-checklist.md`,
   `docs/fable-validation.md`,
   `docs/templates/fable-non-trivial-change-template.md`,
   `.claude/agents/*.md`, `.claude/skills/**/SKILL.md`) — not merely because
   a Non-trivial plan exists. The workflow-contract-file clause is a
   deliberate addition beyond Codex's wording: it is the one category
   inherently high-blast-radius enough to warrant a standing trigger, since
   these files govern all future agent behavior across all tasks.
2. `fable-code-reviewer` triggers when Go work is Substantial: High-risk
   classification; a shared or exported interface changes; an algorithm or
   state machine changes materially; a production file is substantially
   rewritten; meaningful uncertainty remains after implementation; or
   multiple production packages change with shared behavior, ownership,
   interfaces, contracts, or meaningful cross-package uncertainty. Line
   count alone does not determine substantiality — mirrors
   `docs/review-checklist.md`'s Go Review Method Trigger almost verbatim.
3. Both triggers reuse Fable's single existing "High-risk" definition
   (`docs/fable-workflow.md`'s Fresh-verifier explorer section), now marked
   as a shared anchor referenced by three roles, rather than introducing a
   second, divergent definition.
4. When a trigger does not apply, the plan or closeout states `N/A - not
   triggered, <reason>` rather than silently omitting the review — this
   keeps the omission visible and auditable instead of ambiguous.
5. `CLAUDE.md`'s Non-Trivial Approval Gate bullet, `docs/fable-workflow.md`,
   `docs/fable-review-checklist.md`, `docs/fable-validation.md` (including
   automatic-fail conditions #13, #14, #19), and
   `docs/templates/fable-non-trivial-change-template.md` are updated
   consistently so no document contradicts another about whether either
   review is mandatory.
6. The literal backtick-quoted marker `` `SCOPE ADVERSARIAL REVIEW` `` and
   its required heading/count in
   `scripts/check-fable-workflow-contract.ps1` are preserved verbatim; only
   the surrounding mandate language becomes conditional.
7. Fable's numeric `Validation Score: X/6` and SA1-15 SELF-AUDIT taxonomy
   are explicitly *not* touched by this decision. Codex dropped its
   equivalent scorecard as unenforceable self-reported narration (`ADR-0221`
   Decision 4); Fable's scorecard is a deliberately different design
   (`ADR-0206`) cross-referenced against actually-spawned subagents with
   tool grants enforced at the agent-definition level, not self-reported
   prose — Codex's rationale for removing its version does not transfer.
8. All five already-trigger-gated Fable roles
   (`fable-fresh-verifier`/`fable-scientific-oracle`/
   `fable-requirements-adversary`/`fable-design-challenger`/
   `fable-test-strategy-adversary`) and `.claude/skills/go-blast-radius-audit`
   are unaffected.

This decision selectively refines `ADR-0206` Decision 3's default-on framing
for these two roles and `ADR-0217`'s treatment of `fable-scope-adversary`'s
mandate as pre-existing. All other `ADR-0206` and `ADR-0217` decisions
remain accepted.

## Alternatives considered

1. Leave Fable's two roles default-on and let the contracts diverge on this
   point.
   - Rejected because the divergence is not evidence-based: Codex's
     evidentiary-need argument (`ADR-0223`) applies equally to Fable's
     subagents, and the two contracts otherwise share the same 15-category
     risk taxonomy specifically so a human operator can read them side by
     side.
2. Set a more liberal Fable-specific trigger bar than Codex, reasoning that
   Fable's subagents are mechanically enforced (real tool-grant boundaries)
   and cheaper to run than Codex's self-reported markers.
   - Rejected (by explicit user choice in this session) in favor of mirroring
     Codex's trigger bar directly — the mechanism difference between the two
     contracts is about *how* compliance is proven, not about how much
     independent evidence a given risk level warrants.
3. Drop Fable's numeric scorecard and SA1-15 taxonomy to match Codex's
   `ADR-0221` Decision 4.
   - Rejected because that scorecard's evidentiary basis (real subagents with
     enforced tool grants) differs from what Codex removed (self-reported,
     unenforceable prose markers); the reasoning does not transfer.

## Consequences

### Benefits

- Fable and Codex now share the same risk-triggered specialist philosophy,
  not just the same taxonomy labels.
- Ordinary low-risk Non-trivial plans and mechanical Go edits no longer pay
  for a review with no evidentiary need.
- The two contracts stay legible side by side, as `ADR-0206` intended.

### Risks

- The new scope-adversary trigger ("High-risk, uncertain, disputed,
  difficult to reverse, material residual uncertainty") is the least
  mechanically checkable of Fable's five triggered roles and becomes the
  sole remaining pre-approval safety net for plans that don't hit it — the
  same tradeoff Codex already accepted in `ADR-0223`.
- If a future Fable workflow-contract file is added without updating the
  trigger's file list, scope-adversary could silently stop firing for it —
  the same file-list-maintenance risk already accepted elsewhere in these
  docs (e.g., the Workflow-drift audit's own file list).

### Operational Impact

- No Go runtime, config, protocol, parser, queue, lifecycle, deployment, or
  operator-command behavior changes.
- No Codex-side file changed.
- This is a workflow-contract-only change; validated via the
  workflow-contract lane (`docs/dev-runbook.md`), including
  `scripts/check-fable-workflow-contract.ps1` and
  `scripts/test-fable-workflow-contract.ps1`.

## Links

- Related docs: `CLAUDE.md`, `docs/fable-workflow.md`,
  `docs/fable-review-checklist.md`, `docs/fable-validation.md`,
  `docs/templates/fable-non-trivial-change-template.md`,
  `.claude/agents/fable-scope-adversary.md`,
  `.claude/agents/fable-code-reviewer.md`
- Related checks: `scripts/check-fable-workflow-contract.ps1`,
  `scripts/test-fable-workflow-contract.ps1`
- Related TSRs: none
- Supersedes / superseded by: selectively refines `ADR-0206` Decision 3 and
  `ADR-0217`'s treatment of the scope-adversary mandate; ported from Codex's
  `ADR-0223` (source of the risk-triggered model); not superseded

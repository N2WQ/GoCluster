# ADR-0206: Fable-Native Workflow Contract Architecture

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

gocluster has a mature Codex-executor workflow contract rooted in
`AGENTS.md`, with a Scope Ledger approval gate, a `codex-skills/` bundle,
and independent-review roles established by ADR-0202 and refined by
ADR-0203 (independent SELF-AUDIT scoring), ADR-0204 (captured validation
evidence for high-risk claims), and ADR-0205 (subagent authorization
state). `CLAUDE.md` previously only redirected Claude-based agents ("Fable")
to treat `AGENTS.md` as authoritative — but `AGENTS.md` names Codex as its
primary audience, no `.claude/` directory existed in the repository, and
`codex-skills/*/agents/openai.yaml` is Codex-CLI-specific metadata with no
Claude equivalent. Fable had no way to reliably reach the same rigor.

The user's intent was to raise Fable's output quality to Codex's contract
level without changing the underlying model — a scaffolding problem, not a
capability problem. The user explicitly chose a Claude-native redesign over
a 1:1 mirror of `AGENTS.md`'s prose-marker shape, and explicitly chose to
invest in mechanical enforcement where Claude Code's actual primitives make
that possible, rather than relying solely on self-reported compliance the
way Codex's contract historically did before ADR-0203/0204/0205 retrofitted
those gaps.

## Decision

Build a parallel Fable contract using Claude Code's actual primitives in
place of Codex's self-reported prose markers, while keeping the same
underlying discipline and the same 15-row SELF-AUDIT-equivalent taxonomy
`ADR-0203` locked for Codex, so the two contracts stay legible side by
side:

1. `CLAUDE.md` becomes the compact, always-loaded Fable contract directly
   (role, always-on rules, task gates, Document Map) — not a pointer to a
   separate file, since `CLAUDE.md` is already Claude-scoped by convention
   and needs no redirect-trick the way `AGENTS.md`/`CLAUDE.md` needed one
   for Codex.
2. The Scope-Ledger approval gate becomes `EnterPlanMode` → plan →
   `ExitPlanMode`, a harness-level approval action instead of a string
   token match. This applies uniformly to code and to workflow-contract
   Markdown changes — an explicit user decision, not a default assumption.
3. Codex's three canonical independent-review roles (`scope-ledger-
   adversarial-review`, `go-code-quality-review`, fresh-verifier) map to
   three real Claude subagents — `fable-scope-adversary`, `fable-code-
   reviewer`, `fable-fresh-verifier` — defined in `.claude/agents/*.md`
   with read-only tool grants enforced at the agent-definition level, not
   by instruction alone.
4. Narrow technical audits (leak detection, connection lifecycle, retained
   state, config contracts, hot-path design, blast radius, code walking)
   become Claude Skills under `.claude/skills/*` — in-context knowledge
   with no independent context window. This is a structural split
   `codex-skills/*/SKILL.md` does not make: it conflates in-context
   knowledge and independent-review roles into one file shape. Two
   codex-skills (`decision-memory-audit`, `workflow-contract-audit`) were
   found, on inspection, to be saturated with Codex-marker vocabulary
   rather than narrow technical content; their procedural content was
   absorbed into `docs/fable-workflow.md` directly rather than ported as
   skills.
5. Codebase-level standards — `docs/code-quality.md`, `docs/domain-
   contract.md`, `docs/decision-memory.md`, `docs/dev-runbook.md`,
   `docs/agent-lessons/README.md` — stay shared, unforked. They describe
   the codebase, not the executor.
6. The subagent-authorization model from `ADR-0205` is incorporated from
   day one: independent agents are default-on given the user's explicitly
   stated standing intent (a live statement, not repository policy text
   alone, which cannot self-authorize spawning); `not authorized/not
   requested` is reserved specifically for headless/unattended execution
   with no live user present, not for ordinary interactive sessions.
7. The captured-evidence requirement from `ADR-0204` is incorporated from
   day one: command-backed Concurrency and Leak-detection SELF-AUDIT rows
   require a pasted excerpt, not a bare self-graded `PASS`.

## Alternatives considered

1. Faithful 1:1 mirror of `AGENTS.md`'s marker/prose shape, retargeted at
   Fable.
   - Rejected because it would import self-reported compliance as the
     proof mechanism when Claude Code has stronger native primitives
     available — a real approval gate and tool-grant-enforced read-only
     agents. Mirroring would waste that.
2. Hybrid: keep every `AGENTS.md` rule and marker name unchanged, back as
   many as possible with native mechanisms.
   - Partially adopted (marker names and the 15-row taxonomy were kept for
     cross-contract legibility) without treating every Codex-specific rule
     as mandatory to preserve verbatim.
3. Fork the codebase-level standards docs for Fable's own copy.
   - Rejected; those documents describe the codebase, not the executor,
     and forking them risks drift between two copies of the same rule.
4. Build mechanical enforcement (hooks, checker scripts) as part of this
   same decision.
   - Deferred to a later ledger. Hooks and scripts are tooling/code, not
     workflow-contract documentation, and depend on the contract existing
     first.

## Consequences

### Benefits

- Fable can reach Codex-equivalent rigor without a model change — a
  scaffolding fix, not a capability fix.
- The approval gate is harness-enforced (`ExitPlanMode`) rather than
  string-matched, removing an entire class of typo/ambiguity risk Codex's
  `Approved vN` token carries.
- Independent-review roles get `Edit`/`Write` withheld at the tool-grant
  level instead of only being asked not to edit files. This is partial, not
  complete: their `Bash` grant is unrestricted, so mutating-command
  avoidance is still instruction-level for that one tool, same as Codex's
  subagents today.
- The Skills-vs-Agents split is structurally correct against this
  environment's actual tool semantics (independently verified during
  adversarial review, not just asserted).
- The authorization-state (`ADR-0205`) and captured-evidence (`ADR-0204`)
  lessons are built in from day one instead of requiring the multiple
  retrofit rounds Codex's contract needed to reach the same place.

### Risks

- The contract is architecturally sound but undogfooded at closeout — no
  real Non-trivial task has exercised it yet. A follow-up dogfood ledger is
  recommended before full reliance.
- A post-closeout systematic section-by-section audit against
  `docs/change-workflow.md` (prompted by two discovery gaps found via direct
  user challenge: an ungitignored `.claude/` tree and an unread `data/
  config/README.md`) found 8 further real gaps — content silently dropped
  during porting rather than deliberately scoped out — and fixed them:
  Skill Check discipline, Go Comment Intent Rigor's trigger/checker linkage,
  Config Contract Audit's dependency-rigor trigger, `go-leak-detection`'s
  captured-excerpt cross-reference, the domain science-claims paragraph and
  high-risk trigger list, the "missing evidence is a workflow failure"
  framing, support-agent sync's itemized trigger list, and the
  additive-not-repetitive compression discipline. Three minor items (IDE
  context discipline, Requirements & Edge Cases Note, Implementation
  Plan/slicing milestone rules) were identified and explicitly deferred
  rather than silently dropped. This pattern — confident claims built on
  unread or half-remembered source files — is the specific risk the
  recommended dogfood ledger should stress-test directly, not assume fixed.
- `.claude/agents/*.md` frontmatter (`name`/`description`/`tools`/`model`)
  reflects this session's best understanding of Claude Code subagent
  conventions; it has not been externally verified against authoritative
  Claude Code documentation. Treat as a residual gap until confirmed.
- The three subagents' `Bash` grant is unrestricted, so tool-grant
  enforcement only covers `Edit`/`Write`. Mutating-command avoidance via
  `Bash` remains instruction-level, identical to Codex's current gap. A
  follow-up ledger should investigate whether finer-grained Bash
  allowlisting is possible for these agent definitions.
- Mechanical enforcement (hooks, checker scripts) is deferred — until that
  lands, Fable's rigor still depends on self-discipline for the same
  reasons Codex's did before its own hooks/scripts existed.

### Operational impact

- No runtime Go, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command
  behavior changes.
- New files: `docs/fable-workflow.md`, `docs/fable-review-checklist.md`,
  `docs/fable-validation.md`,
  `docs/templates/fable-non-trivial-change-template.md`,
  `.claude/agents/{fable-scope-adversary,fable-code-reviewer,fable-fresh-
  verifier}.md`, `.claude/skills/{go-leak-detection,go-connection-
  lifecycle-audit,go-retained-state-audit,go-config-contract-audit,go-
  hotpath-design,go-blast-radius-audit,go-code-walk}/SKILL.md`.
- `CLAUDE.md` rewritten from pointer to full contract; `docs/WORKING_WITH_
  CODEX.md` corrected to describe both contracts as parallel, not
  competing.
- `customgpt/*` Fable-routing updates and `.claude/settings.json`
  mechanical-enforcement hooks are explicitly deferred to follow-up
  ledgers, not silently out of scope.

## Links

- Related issues/PRs/commits:
- Related tests: reviewer diff pass, workflow-drift audit, `git diff
  --check`
- Related docs: `CLAUDE.md`, `docs/fable-workflow.md`,
  `docs/fable-review-checklist.md`, `docs/fable-validation.md`,
  `docs/templates/fable-non-trivial-change-template.md`,
  `docs/WORKING_WITH_CODEX.md`, `.claude/agents/*.md`,
  `.claude/skills/**/SKILL.md`
- Related ADRs: ADR-0194, ADR-0199, ADR-0202, ADR-0203, ADR-0204, ADR-0205
- Related TSRs: none
- Supersedes / superseded by: none — parallel to ADR-0202's model, not a
  supersession of it

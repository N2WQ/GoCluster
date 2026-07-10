# ADR-0217: Fable Pre-Code Independent Evidence

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context
`ADR-0216` added four Codex-native pre-code independent-evidence skills
(`requirements-ambiguity-review`, `scientific-model-oracle`,
`design-challenger`, `test-strategy-adversary`) plus a bounded
`parallel-discovery` wave, run through fresh separate read-only `explorer`
contexts before the Scope Ledger and, for `test-strategy-adversary`, before
the first implementation slice. It explicitly kept the Fable workflow
separate and requested Codex-native controls first.

The same underlying gaps apply to Fable: a Plan Mode plan can harden
unstated semantics, code and tests can agree on the same scientific/model
error, an anchored first design can go unchallenged, and final validation
can discover its fixtures never exercised the changed behavior. The user
requested a genuinely Fable-native port — not a mechanical copy — using
Claude Code's own primitives, while preserving Fable's existing tool-grant
and `EnterPlanMode`/`ExitPlanMode` approval architecture.

## Decision
Add four new Fable custom subagents under `.claude/agents/*.md`
(`fable-scientific-oracle`, `fable-requirements-adversary`,
`fable-design-challenger`, `fable-test-strategy-adversary`), each read-only
via tool grant (`Read, Grep, Glob, Bash`, no `Edit`/`Write`), matching the
architecture of the three existing roles rather than being ported as
`codex-skills`-style in-context skills — these roles require independent
judgment in a separate context window, not routine in-context knowledge
lookup, so `ADR-0206`'s Skills-vs-Agents split places them with the Agents.

Sequencing is mapped onto Fable's actual Plan Mode Phase A/B structure,
not Codex's flat marker sequence: in Phase A (before `ExitPlanMode`),
Current-State Discovery is followed by an optional bounded parallel-
discovery wave, then `fable-scientific-oracle` (if triggered), then
`fable-requirements-adversary` (if triggered), then
`fable-design-challenger` (if triggered) — all before the plan's scope is
drafted — followed by the existing `fable-scope-adversary` review of the
drafted plan and `ExitPlanMode`. In Phase B (after approval),
`fable-test-strategy-adversary` runs after detailed `DESIGN` and before the
first `IMPLEMENTATION` slice.

Bounded parallel discovery deliberately does not get a new agent
definition. Claude Code's built-in `Explore` agent type already exists in
this environment for exactly this narrow use — "locating code... where is
X defined... which files reference Y" — and is explicitly documented as
unsuitable for "code review, design-doc auditing, cross-file consistency
checks, or open-ended analysis." For Full-rigor discovery with at least two
separable evidence domains, spawn 2-3 `Explore` agents in one message (same
on-disk state), each with a distinct bounded question, evidence contract,
and stop condition; the lead synthesizes conflicts. This is the one place
this design materially diverges from `ADR-0216`'s shape, because Fable has
a native primitive Codex's `explorer` type does not expose in the same
form — using it is more Fable-native than minting a fifth analytical agent
for pure fact-finding.

The design-challenger's neutral-evidence-packet requirement is documented
as a self-reported, instruction-level constraint, not a mechanically
enforced one — the same honest limitation Codex's own skill carries
(`inconclusive - context contaminated` is self-reported, not tool-enforced).

`docs/fable-review-checklist.md` gained a "Pre-Code Independent Evidence"
section mapping the five capabilities to `SA1-SA3, SA9-SA12, SA13` (same
IDs as Codex's mapping, since Fable's SA numbering already matches
Codex's). `docs/fable-validation.md` scorecard items 1/3/4 and its
auto-fail list (new items 23-25) now name the four new roles and the
parallel-discovery wave, mirroring `VALIDATION.md`'s equivalent additions.
`docs/templates/fable-non-trivial-change-template.md` gained new
`DISCOVERY` fields (parallel discovery, scientific/model oracle,
requirements ambiguity, design challenge) and a new `DESIGN` field
(`fable-test-strategy-adversary`), without adding a 13th marker header —
the existing GATE/DISCOVERY/SCOPE/DESIGN markers absorb the new evidence,
keeping `scripts/check-fable-workflow-contract.ps1`'s exact-count marker
checks unchanged.

No `AGENTS.md`, `docs/change-workflow.md`, or `codex-skills/*` file was
touched — a concrete-conflict check against `ADR-0216`, the four Codex
skills, and Codex's `ADR-0210` dev-runbook update found no conflict; the
latter is complementary (it already made the shared "Workflow/skill-doc
change" lane executor-aware for Fable's exact files).

## Alternatives considered
1. Mechanically port Codex's four skills as `.claude/skills/*` entries.
   - Rejected: skills are in-context, no separate context window, and
     cannot provide independent adversarial or normative evidence per
     `ADR-0206`'s existing Skills-vs-Agents split.
2. Fold the four new roles into `fable-scope-adversary`.
   - Rejected for the same reason `ADR-0216` rejected the equivalent
     Codex-side folding: model authority, unresolved requirements,
     alternative design, and test falsifiability occur at different phases
     and require different evidence products.
3. Build a dedicated custom agent for bounded parallel discovery instead of
   using `Explore`.
   - Rejected: `Explore` already exists, is scoped correctly for bounded
     fact-finding, and a dedicated agent would duplicate a capability the
     platform already provides — inconsistent with using Claude-native
     primitives instead of mechanically porting Codex's `explorer`-based
     shape.

## Consequences
### Benefits
- Independent evidence is available before semantics and architecture
  harden, matching Codex's guarantee without copying its mechanism.
- Scientific/model tests gain a normative source outside the implementation
  under test.
- Test plans must identify how a broken design would be observed before
  coding.
- Bounded parallel discovery uses an existing Claude-native primitive
  instead of adding agent-definition surface area.

### Risks
- Four additional agent definitions increase spawn/token cost and lead
  synthesis work; each has explicit positive and non-trigger conditions to
  limit over-triggering.
- `Explore`'s suitability for parallel discovery rests on its documented
  scope, not a mechanical guarantee that every sub-question stays within
  that scope — a lead spawning an `Explore` agent with an analytical rather
  than locate-oriented question would get degraded results, not a hard
  refusal.
- The design-challenger's neutral-packet constraint remains
  instruction-level, not tool-enforced, matching Codex's own residual gap.

### Operational impact
- No Go runtime, config, protocol, parser, telnet, queue, persistence,
  model, or operator-command behavior changes.
- Fable workflow behavior, validation evidence, mechanical workflow checks,
  and `.claude/agents/*.md` inventory change.
- This decision makes no measured development-speed, code-quality, or
  token-efficiency claim.

## Links
- Related issues/PRs/commits:
- Related tests: `scripts/test-fable-workflow-contract.ps1`
- Related docs: `CLAUDE.md`, `docs/fable-workflow.md`,
  `docs/fable-review-checklist.md`, `docs/fable-validation.md`,
  `docs/templates/fable-non-trivial-change-template.md`,
  `.claude/agents/fable-scientific-oracle.md`,
  `.claude/agents/fable-requirements-adversary.md`,
  `.claude/agents/fable-design-challenger.md`,
  `.claude/agents/fable-test-strategy-adversary.md`
- Related ADRs: ADR-0206, ADR-0209, ADR-0210, ADR-0215, ADR-0216
- Related TSRs: none
- Supersedes / superseded by:

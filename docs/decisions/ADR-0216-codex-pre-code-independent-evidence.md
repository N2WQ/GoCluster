# ADR-0216: Codex Pre-Code Independent Evidence

- Status: Superseded
- Date: 2026-07-09
- Decision Origin: Design

## Context
The Codex workflow already used independent scope, Go-quality, and fresh-
verification reviewers. Current-State Discovery could use read-only agents, but
it did not define bounded parallel fan-out, an independent ambiguity check, a
normative scientific/model oracle, an alternative-design challenge before lead
anchoring, or a pre-code review of whether planned tests could falsify the
selected design.

Those gaps allow late rework: a Scope Ledger can harden unstated semantics, code
and tests can agree on the same model error, and final validation can discover
that its fixtures or assertions never exercised the changed behavior. The
repository owner requested Codex-native controls first and explicitly kept the
Fable workflow separate.

## Decision
Add four repo-managed Codex skills used through fresh separate read-only
independent-agent contexts:

- `requirements-ambiguity-review` actively searches semantic-risk surfaces for
  competing interpretations before the Scope Ledger. Unresolved material
  product, operator, or model semantics block scope publication.
- `scientific-model-oracle` establishes normative sources, units, domains,
  boundaries, tolerances, provenance-independent golden vectors, uncertainty,
  and supportable claim limits before design and scope.
- `design-challenger` receives a neutral, semantically resolved evidence packet
  and compares viable architectures before the lead records a preferred design.
- `test-strategy-adversary` reviews falsifiability after approved detailed
  `DESIGN` and before the first implementation slice.

Use a bounded initial `parallel-discovery` wave of two or three read-only agents
when Full-rigor discovery has at least two separable evidence domains. Every
assignment shares the same revision/worktree snapshot and has a distinct
question, evidence contract, and stop condition. The lead verifies conflicts,
dispositions findings, and retains every workflow gate.

When typed subagents are available, use `explorer`; otherwise use the platform's
supported independent context with explicit read-only/findings-only constraints.
A skill supplies specialist instructions but does not itself establish
independence. A design challenger exposed to the lead's preferred solution is
inconclusive, not independent evidence.

Keep the existing marker set, SA1-SA15 categories, `Approved vN` token, scope
adversarial review, Go quality review, fresh verifier, and final validation
ownership. These are prospective gates: the workflow that approved this ADR
governed its bootstrap implementation, and the new roles apply to later eligible
tasks after forward-testing.

## Alternatives considered
1. Add a standalone decision-memory scout.
   - Rejected because current policy requires the lead to read the full decision
     and troubleshooting indexes. A scout would duplicate that work unless a
     separate fail-closed decision-memory redesign first proves safe narrowing.
2. Add a standalone anti-speculation reviewer.
   - Rejected because `go-code-quality-review`, the Review Pass, SA11, and the
     high-risk fresh verifier already inspect speculative cleanup, abstractions,
     fallbacks, compatibility shims, and future-proof hooks.
3. Fold all pre-code roles into `scope-ledger-adversarial-review`.
   - Rejected because model authority, unresolved requirements, alternative
     design, selected-scope completeness, and test falsifiability occur at
     different phases and require different evidence products.
4. Maintain or revive the retired token-impact evaluation harness.
   - Rejected as outside scope. Deprecated ADR-0214 and its manual historical
     corpus are not executable dependencies of this decision.

## Consequences
### Benefits
- Independent evidence is available before semantics and architecture harden.
- Scientific/model tests have a normative source outside the implementation.
- Test plans must identify how a broken design would be observed before coding.
- Parallel discovery has explicit bounds, evidence ownership, and conflict
  handling instead of open-ended fan-out.

### Risks
- Additional agents consume tokens and add lead synthesis work.
- Over-triggering would slow simple work, so every role has explicit positive
  and non-trigger conditions.
- Independent agents can fail, time out, inherit stale context, or disagree;
  those states remain visible gaps rather than implicit passes.

### Operational impact
- No Go runtime, config, protocol, parser, telnet, queue, persistence, model,
  or operator-command behavior changes.
- Codex workflow behavior, validation evidence, mechanical workflow checks,
  repo-managed skill metadata, and developer support routing change.
- This decision makes no measured development-speed, code-quality, or token-
  efficiency claim.

## Links
- Related issues/PRs/commits:
- Related tests: `scripts/test-workflow-contract.ps1`,
  `scripts/verify-codex-skills.ps1`, targeted skill forward tests
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/templates/non-trivial-change-template.md`, `docs/review-checklist.md`,
  `VALIDATION.md`, `docs/dev-runbook.md`, `docs/WORKING_WITH_CODEX.md`,
  `codex-skills/README.md`
- Related ADRs: ADR-0155, ADR-0156, ADR-0194, ADR-0199, ADR-0202, ADR-0203,
  ADR-0204, ADR-0209, ADR-0211, ADR-0213; ADR-0214 (deprecated historical
  evaluation context)
- Related TSRs: none
- Supersedes / superseded by: superseded by ADR-0221

# ADR-0336: Codex Method Ownership And Retained Bounds

- Status: Accepted
- Date: 2026-11-10
- Decision Origin: Workflow review; Approved v1

## Context

Codex is the owner's primary coding agent. The repo repeated decision handling,
scope challenge and Go review instructions across skill bodies and canonical
workflow documents. Separate brief/deep explanation skills also required
repeated routing exclusions. Several UI prompts lagged their skill bodies,
and artifact instructions could imply writes during read-only work.

The bounded-state guidance listed expiry and reclamation mechanisms without
separately requiring a defensible maximum on simultaneous retained size.
These are documentation findings, not evidence of a runtime leak or memory
ceiling. The owner approved the first bounded consolidation and correction,
leaving changes to accepted validation evidence and integrations for later.

## Decision

1. Make complete methods reachable through Codex's existing `AGENTS.md` routes:
   - `docs/decision-memory.md#codex-discovery-and-record-handling` owns decision
     discovery and closeout handling; retire `decision-memory-audit`.
   - `docs/change-workflow.md#scope-challenge-method` owns the deeper
     pre-approval challenge; retire `scope-ledger-adversarial-review`.
   - `docs/review-checklist.md#substantial-go-review-method` owns substantial Go
     review. Keep `go-code-quality-review` as a small Codex discovery entry
     point, also preserving the Fable counterpart reference.
   - `explain-code` covers requested brief orientation through thorough review;
     retire `initial-review`. Preserve specialist precedence and evidence depth
     without an imposed five-bullet format.
2. Preserve the moved methods, triggers, phase boundaries, findings disposition,
   material rechallenge, risk-based independence and lead validation authority.
   The initial challenge remains required for every Non-trivial scope; the
   complete deeper failure prompts apply when their risk trigger is met.
3. Align the design, test-strategy and workflow-audit UI prompts with their
   existing bodies. Preserve their design/test phases and trigger policy.
4. Repository artifacts require both a user request and current authority under
   `AGENTS.md`. Read-only findings stay in the response; explicitly requested,
   authorized artifact delivery remains available. Preserve the threat-model
   assumptions pause and all security methods.
5. Require proof of simultaneous entries and retained bytes, including backing
   and secondary state. Account for admission bursts, cleanup delay, stale
   derived entries and transient overlap relevant to the claim. Permit combined
   enforced constraints; expiry, ownership deletion and reference counting alone
   do not prove maximum simultaneous size. A primary reclamation classification
   remains permitted with the supplementary size proof. This shared standard
   applies to Codex and Fable without changing Fable-owned instructions.
6. Update existing static checks and named negative fixtures to protect the
   moved methods at their canonical owners. Retire the two context-measurement
   scenarios for deleted decision/scope skills explicitly. Keep strict missing
   blob and invalid UTF-8 errors for remaining scenarios. The measurements omit
   retired routes and cannot establish total consolidation savings.

This selectively replaces ADR-0156's separate decision-memory skill identity
and ADR-0331's scope-skill location; their obligations remain. It refines
ADR-0221's method ownership. ADR-0335 and every safeguard added by commit
`689760246a4ebdfa1242dfb9cda7ed93848cf608` remain governing, including critical
feasibility, meaningful-unit completeness, performance evidence, reuse,
failure diagnosis, reassessment, owner communication and risk-based review.
The subsequent CI fixture repairs remain part of the baseline.

## Alternatives Considered

1. Keep complete duplicated skill and document methods: preserves discoverable
   identities but leaves competing owners and repeated maintenance.
2. Remove skill identities without migrating their methods and routes: rejected
   because text existing elsewhere does not establish Codex reachability.
3. Remove the Go review entry point: deferred because it remains a useful Codex
   trigger and is referenced by the Fable reviewer.
4. Revise race scope, live profiling, test-strategy phases, instrumentation or
   optional integrations simultaneously: excluded because these change evidence
   sufficiency or separate user choices beyond this approved first scope.

## Consequences

The intended benefit is clearer ownership and less duplicate instruction work.
No measured quality, elapsed-time or token improvement is claimed. Three repo
skill identities are retired; generic personal copies and their client-specific
discovery are outside this change. A new session may still expose those copies;
they do not become repo authority through this consolidation.

The main risk is a moved method becoming unreachable or losing a material
obligation. Explicit discovery and phase routes, canonical-method checks,
negative fixtures and direct source comparison address that risk. Static checks
still cannot prove actual Codex selection, instruction adherence, independence,
engineering effectiveness or a runtime bound. The Fable retained-state skill
still names a primary mechanism; the shared size proof supplements that
classification rather than claiming full deduplication of Fable guidance.

Support-agent docs impact: Not required; runtime/operator behavior and support
routing do not change. No production Go, runtime config, metrics, settings,
GitHub workflows, helpers or Fable-owned files change. Existing approval,
race/profile/fuzz/performance evidence, CI and final-validation obligations
remain. This decision does not qualify a running server or prove a process RSS
or working-set ceiling.

## Implementation And Validation

Baseline: `56d2b8a1630ad7c233cde97345759ca49694175d`. Exact `Approved v1`
preceded edits. A separate read-only scope challenge and detailed pre-edit
test-strategy review used inherited context; neither is independent non-steered
evidence. The lead disposition retained the Go discovery wrapper, separated
deep scope review from the universal initial challenge, required role-qualified
reviewer restrictions, and preserved authorized artifact delivery.

| Approved contract | Failure stimulus or counterexample | Planned evidence and owner |
| --- | --- | --- |
| Reachable moved methods | Remove discovery route independently of closeout, remove destination method, or move challenge after approval | Scoped canonical guards, named missing/contradictory fixtures, source comparison; workflow/checker |
| Preserved scope and Go review | Make deep review universal, drop disposition/rechallenge, break wrapper destination, or let separate review replace lead validation | Named negative and permitted-boundary fixtures, cross-executor reference review; checklist/workflow |
| Requested explanation depth | Force a thorough audit into five bullets, expand a brief orientation unnecessarily, suppress a triggered specialist or permit writes | Direct brief/thorough/specialist/authority walkthrough and entry-point guards; explain skill |
| Accurate prompts and artifacts | Valid YAML with a wrong phase; authorize unsolicited files or prohibit explicitly requested authorized output | YAML parsing, prompt/body review, artifact missing/inversion and positive-boundary fixtures; skills/checker |
| Defensible retained size | Expiry alone, bounded count with unbounded backing, late cleanup, or uncoupled derived state | Shared/Codex source review and documented-requirement guards; shared standard/retained skill |
| Honest tooling and preserved baseline | Treat retired route as zero, accept missing blobs, or remove old safeguards while making a reduced suite pass | Context fixtures, existing delivery-policy fixtures, baseline comparison and final diff; scripts/lead |

The retained-size policy walkthrough rejects expiry without enforced admission
or a bounded universe, a count cap with unbounded payload/backing, and a claimed
expiry-window peak that ignores cleanup delay. It accepts a fixed enforced key
universe combined with maximum retained backing per entry and independently
bounded secondary state, after including relevant cleanup/overlap. Tests and
source evidence must establish those premises; this walkthrough supplies no
runtime qualification. Existing cardinality observability remains required.

Selected lane: PowerShell parsing, baseline-aware workflow checker, complete
workflow fixtures, repo-skill verifier, context-measurement fixtures, changed
metadata/reference review, protected-baseline and cross-executor review,
`git diff --check`, direct final-diff review and a fresh final pass. No Go lane,
live-agent evaluation campaign or comparative savings report is selected.

Local validation passed: all five changed PowerShell scripts parsed; the
baseline-aware workflow checker, complete workflow fixture suite, 20-skill
verifier and context-measurement fixtures passed. The merged explanation skill
also passed the skill-creator validator. Skill frontmatter and UI YAML parsed,
local Markdown links/anchors resolved, and the exact 34-path boundary held.
Direct checks confirmed unchanged retained-state instrumentation and the
threat-model pause, no production Go/CI/Fable-owned diff, and preservation of all
29 nonempty added text blocks from `6897602` plus the later CI fixture fixes.

During development, two existing retained-state text guards initially rejected
equivalent strengthened wording. Their patterns were updated to preserve the
reference-reclamation and bounded-owner concepts; the final complete suite
passed. The final source review found no material omissions in the migrated
methods or cross-executor compatibility. It used inherited context and is not
independent non-steered evidence. The lead reviewed the final scripts and diff
and performed a fresh final verification pass. `git diff --check` passed.

An ad hoc protected-text comparison initially used the host locale to decode
Git output and falsely differed on a Unicode apostrophe. Explicit UTF-8 decoding
resolved that invocation error; the rerun confirmed unchanged protected text,
46 local link/anchor targets and the approved path boundary without a source fix.

These are local source/contract results. Live skill discovery and selection in
a fresh Codex or VS Code session were not tested; the current session's supplied
catalog predates these edits. No hosted CI, production memory qualification,
workflow-effectiveness or efficiency result is claimed.

## Links

- Selectively supersedes [ADR-0156](ADR-0156-expanded-repo-managed-audit-skills.md)
  skill identity and [ADR-0331](ADR-0331-repeat-material-scope-challenges.md)
  method location; preserves their substantive obligations.
- Refines [ADR-0221](ADR-0221-codex-authority-and-evidence-workflow.md).
- Preserves [ADR-0335](ADR-0335-complete-proportionate-codex-delivery.md).
- Canonical owners: [workflow](../change-workflow.md),
  [review](../review-checklist.md), [decisions](../decision-memory.md),
  [shared standard](../code-quality.md).

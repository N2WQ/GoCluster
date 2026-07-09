# ADR-0215: Fable Workflow Token-Optimization Port

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

`ADR-0213` adopted a token-efficient Codex contract: SA-numbered SELF-AUDIT
categories with a fail-closed applicability manifest replacing repeated `N/A`
rows, and a new mechanical contract-coherence checker
(`scripts/check-workflow-contract.ps1`) with tested fixtures. `ADR-0213`
explicitly states Fable, custom-GPT routing, model configuration, and
API-only features are outside that decision.

Two of `ADR-0213`'s techniques transfer directly because `docs/fable-review-
checklist.md`'s 15 SELF-AUDIT categories were deliberately kept identical in
wording and order to Codex's `docs/review-checklist.md` (per `ADR-0206`, "so
the two contracts stay legible side by side"). A third technique — raw
prose-compression of the always-loaded contract — was judged higher-risk to
port without the same evidence base, since this session independently found
real content silently dropped during two prior compression-adjacent passes.
Codex's diff also carried two small correctness fixes (a reasoning-budget
`low`-tier definition that conflated Small-classified work with the lowest
Non-trivial tier, and "config" versus "runtime config" precision) that
applied verbatim to Fable's identically-worded text.

## Decision

Port the SA-ID/applicability-manifest technique and build a Fable-native
mechanical checker, adapted rather than copied where Fable's architecture
genuinely differs from Codex's:

1. `docs/fable-review-checklist.md`'s 15 categories now carry canonical
   `SA1`-`SA15` IDs, identical in number and order to Codex's. `docs/
   templates/fable-non-trivial-change-template.md`'s SELF-AUDIT section uses
   an applicability manifest (`applicable: <IDs>` / `not applicable: <IDs> -
   <shared reason>`) instead of always listing all 15 rows.
2. `scripts/check-fable-workflow-contract.ps1` (with tested fixtures in
   `scripts/test-fable-workflow-contract.ps1`) mechanically checks `CLAUDE.md`
   required strings, template marker presence, SA-ID uniqueness, the
   applicability-manifest structure, validation-block consistency, and
   Document Map reachability. It deliberately diverges from Codex's checker
   in three places verified necessary by adversarial review and direct
   testing, not assumed by symmetry: markers are checked with **per-marker
   exact counts** (`GATE`: 2, because Fable's Plan Mode plan/closing-response
   split requires it in both phases; the other 11 markers: 1) rather than
   Codex's looser "at least once" membership check; Document Map targets may
   be **files or directories** (`CLAUDE.md` legitimately references
   `docs/decisions/`, `docs/troubleshooting/`, and `customgpt/` as
   directories, unlike `AGENTS.md`'s file-only Document Map); and the exact
   validation block is checked in the **two files that actually state it**
   (the template and `docs/fable-validation.md`) rather than four, because
   `CLAUDE.md` and `docs/fable-review-checklist.md` deliberately do not
   repeat it inline. An initial draft also incorrectly required each marker
   name literally in `CLAUDE.md` (copying `AGENTS.md`'s "Mandatory Evidence
   Markers" enumeration); running the checker against the real repository
   immediately proved this wrong, since `CLAUDE.md` intentionally defers
   marker enumeration to the template rather than repeating it — the fix was
   to drop that check, not to make `CLAUDE.md` more repetitive to satisfy it.
3. Two correctness fixes ported verbatim: the reasoning-budget `low` tier in
   `docs/fable-workflow.md` now reads "narrow Non-trivial work with known,
   localized blast radius and a direct validation path"; "config" was
   corrected to "runtime config" in the same file's Documentation-only
   Markdown lane text and in `docs/fable-validation.md`'s auto-fail #17.
4. `CLAUDE.md` gained two small additions for parity: the literal string
   `` `SCOPE ADVERSARIAL REVIEW` `` in the Non-Trivial Approval Gate section
   (previously described only in prose, which is what let the checker design
   flaw above go undetected until tested), and "Report `Plan: N/A - Small`
   for Small work" in Task Gates, matching Codex's equivalent addition.
5. Raw prose-compression of `CLAUDE.md`/`docs/fable-workflow.md` was not
   adopted. `scripts/README.md` documentation for the two new scripts was
   deliberately not added in this task because the file was mid-edit by
   Codex's concurrent, uncommitted work at exactly the insertion point.

## Alternatives considered

1. Also compress `CLAUDE.md`/`docs/fable-workflow.md` prose to match Codex's
   33% reduction.
   - Rejected for this task. `ADR-0213`'s own evaluation found the
     model-specific token-savings claim inconclusive even for its target
     model, and this session independently found real rules silently dropped
     during two prior compression-adjacent passes on these exact files. The
     SA-ID and checker techniques both have mechanical safety nets
     (fail-closed manifests, a tested checker); raw prose compression does
     not.
2. Copy Codex's checker script structure without adaptation.
   - Rejected. Adversarial review and direct execution both proved this
     would fail against Fable's actually-correct current state (GATE's
     legitimate double appearance, Document Map's directory targets, the
     validation block's two-file-not-four placement, and marker names not
     being repeated in `CLAUDE.md`). Three of these four divergences were
     caught only by running the checker, not by design review alone.
3. Extend `scripts/check-workflow-contract.ps1` itself to also cover Fable
   instead of writing a parallel script.
   - Rejected to keep blast radius contained to Fable-owned files and avoid
     touching Codex-primary tooling for a Fable-only port.

## Consequences

### Benefits

- Fable's SELF-AUDIT reporting can omit repeated `N/A` boilerplate the same
  way Codex's now can, with an identical fail-closed guarantee (missing,
  unknown, or duplicate IDs fail the audit).
- Fable gets its first mechanical workflow-contract enforcement script,
  closing a gap this session identified multiple times as the highest-value,
  not-yet-built piece of Fable's rigor.
- Two latent-confusion bugs (reasoning-budget `low` tier, imprecise "config"
  wording) are fixed at the same time they were fixed on Codex's side,
  keeping the two contracts from drifting apart on shared logic errors.

### Risks

- The checker proves mechanical coherence only; like Codex's, it explicitly
  cannot prove a human supplied real `ExitPlanMode` approval.
- `scripts/README.md` does not yet document either new script, since adding
  that documentation was deferred to avoid colliding with Codex's concurrent
  edit; a follow-up task should add it once that edit lands.
- The `.claude/agents/fable-scope-adversary` custom agent was used for real,
  independent adversarial review of this ledger for the first time this
  session; it worked correctly, but its output is still a small sample size
  against `ADR-0206`'s open question about whether the custom agent
  frontmatter schema is fully correct.

### Operational impact

- No Go runtime, protocol, parser, telnet, queue, config, persistence, or
  operator-command behavior changes.
- `scripts/check-fable-workflow-contract.ps1` and `scripts/test-fable-
  workflow-contract.ps1` are new, tested, read-only tools; neither is wired
  into CI in this task.

## Links

- Related issues/PRs/commits: `2407837` (Codex token-optimization commit)
- Related tests: `scripts/test-fable-workflow-contract.ps1`, `git diff
  --check`, reviewer diff pass, `fable-scope-adversary` adversarial review
- Related docs: `CLAUDE.md`, `docs/fable-workflow.md`, `docs/fable-review-
  checklist.md`, `docs/fable-validation.md`, `docs/templates/fable-non-
  trivial-change-template.md`, `docs/decisions/ADR-0213-sol-aware-token-
  efficient-codex-workflow.md`
- Related ADRs: ADR-0206, ADR-0213
- Related TSRs: none
- Supersedes / superseded by: none — extends ADR-0206, parallels ADR-0213

# Working with Codex in `gocluster`

Audience: the human operator working with Codex. For Codex's executor-facing
rules, use `AGENTS.md`. For Claude-based agents ("Fable"), use `CLAUDE.md` —
a parallel contract, not a pointer to this one.

`AGENTS.md` is Codex's executor contract; `CLAUDE.md` is Fable's. Both govern
this repository and share the same codebase-level standards. They are not
competing sources for the same executor, and neither should be merged into
the other.

Use two layers:

- Use planning conversation to settle intent, scope, risks, and edge cases.
- Use `AGENTS.md` as the compact execution contract once you want implementation to start.

Non-mutating explanation, review, audit, diagnosis, prioritization, and
requested recommendations use the read-only route. They still require current
source and honest evidence, but they do not require a Scope Ledger, approval
token, implementation ledger, or change-validation score. If you later ask for
implementation, Codex must stop and enter the Small or Non-trivial change gate
before editing or proposing a diff.

`AGENTS.md` intentionally stays compact so it can be kept in context. Its
Document Map points to the detailed workflow, code-quality, validation, review,
decision-memory, and command rules. Compact does not mean optional: Codex must
treat the workflow markers as execution gates.

## Start in planning by default

Start with planning for anything that is not clearly a small localized fix. In this repo, that should be the default whenever the change may touch:

- concurrency, goroutine lifecycle, deadlines, shutdown, backpressure, or queues
- telnet or packet protocol behavior, parsing, or compatibility
- hot paths, fan-out, p99, or memory bounds
- shared interfaces, multiple packages, operator-visible behavior, or rollout decisions

Recommended prompt:

```text
Plan only. Inspect the current code, identify risks and edge cases, and produce a decision-complete approach. Do not implement yet.
```

For unfamiliar or cross-package behavior, ask Codex to include `Code-walk
evidence`. The evidence should name the package docs, source files, symbols,
callers/callees, tests, and ADRs/TSRs it inspected.

For changes where the impact is uncertain, ask Codex to include a
`Blast-radius audit`. The audit should separate semantic callers, package/test
dependencies, config/docs/support impact, and optional-tool gaps.

For developer questions that the custom GPT support agent should answer later,
ask Codex for a support-agent-readable code-map summary when dependency shape
matters. The support agent can retrieve Markdown docs and source, but it cannot
run local tools. Durable summaries belong under `docs/code-maps/`; local
Graphviz or `goda` images are only supporting artifacts unless a Markdown code
map cites what they show.

Useful prompts:

```text
Produce Code-walk evidence and, if package impact is unclear, a Markdown code-map summary that the custom GPT support agent can retrieve.
```

```text
Use goda/Graphviz locally to inspect dependencies, but report the important edges in Markdown with limits and source files to inspect next.
```

For goroutine, timer, channel, socket, file-handle, shutdown, retained-heap, or
long-running lifecycle concerns, ask Codex to include a `Leak-detection audit`.
The audit should distinguish static source reasoning, local test/race evidence,
profile evidence, and runtime confirmation. When Codex claims command-backed
concurrency or leak-detection validation, expect a short captured command
excerpt in the `REVIEW` marker's `Verification command reporting` evidence, not
only a bare `PASS` line.

For config, YAML, loader, or defaulting work, ask Codex to include a
`Config Contract Audit`. The audit should show which YAML files are touched,
which loader owns them, how missing/null/zero/false values behave, and which
runtime consumers were checked for re-defaulting.

## Skip long planning only for clearly small work

You can usually go straight to implementation only when the change is tightly localized and all of these are true:

- no protocol or compatibility change
- no concurrency, lifecycle, timeout, queue, or shutdown impact
- no shared-component or cross-package contract change
- no user-visible behavior change beyond a strictly local fix

If the blast radius expands, reclassify it as Non-trivial immediately.

## Switch to implementation when scope is stable

Move from planning to execution only when:

1. The intended behavior and architecture are stable.
2. You are ready to approve the implementation scope.

For Non-trivial work, the handoff point is the Scope Ledger. Ask Codex to present `Proposed Scope Ledger vN`, then approve it with:

```text
Approved vN
```

Reject broad refactor-shaped ledgers. A Non-trivial Scope Ledger should include
a `Slice plan` whose slices are small enough to code, test, and review
independently before the next slice starts.

No code, diffs, or full validation should happen before that approval.
Only `Agreed` items are approved for implementation. `Pending` blocks the
approval token; `Rejected` and `Deferred` stay outside the implementation
cycle. A later need for one of those items requires a revised ledger and exact
reapproval.
Before that ledger, Codex should inspect the relevant current code path so the scope is grounded in actual entry points, state, tests, and user-visible behavior.
The proposed ledger should also include a `Reasoning budget` recommendation.
Use it as Codex's target reasoning-level suggestion for the next execution turn;
`low` means narrow Non-trivial work with known localized blast radius and a
direct validation path. The recommendation does not approve scope or waive
validation.

## Use independent agents deliberately

Independent agents are useful when they make the workflow more rigorous, not
faster at the expense of control. When the active environment supports
delegated or parallel agent work and active tool/user authorization permits
spawning them, Codex should use independent agents unless you explicitly
prohibit them.

Some Codex surfaces expose a subagent tool but allow spawning only after you
explicitly ask for subagents, delegation, or parallel agent work. In those
surfaces, this repository records the owner's standing request to use subagents
by default. Codex should not report `not authorized/not requested` merely
because the current task prompt does not repeat that request. You can still
explicitly prohibit subagents for a chat or task, and active tool/session policy
can still block spawning. Exact `Approved vN` approves the scope; it is not by
itself permission for worker subagents outside the approved slice gates.

An independent agent is separate from the lead Codex agent and has its own
context window. That independence helps catch lead-agent anchoring and stale
self-review, but it also means Codex must explicitly disposition the independent
agent's findings against the approved scope and current workspace evidence.

When Codex has typed subagents available, read-only independent review roles
should be spawned as `explorer` agents. `worker` agents are for approved
post-approval implementation slices with explicit file ownership and write
scope.

Before `Approved vN`, subagents should be read-only explorers. Good uses are
code-walk evidence, blast-radius review, config or decision-memory review,
independent adversarial review of the proposed scope, and other evidence
gathering. They should not edit files, draft diffs, run formatters, create
generated artifacts, or run full validation suites before approval.

For Full-rigor discovery with two or more separable evidence domains, expect one
bounded wave of two or three parallel read-only explorers, each with a distinct
question and the same revision/worktree snapshot. Codex must synthesize their
evidence, preserve conflicts and failures, and avoid fan-out when the agents
would inspect the same surface for the same purpose.

Before drafting the Scope Ledger, Codex should also use the applicable
specialist roles:

- `scientific-model-oracle` establishes normative sources, units, boundaries,
  independently derived golden vectors, uncertainty, and claim limits; missing
  or conflicting normative evidence blocks design
- `requirements-ambiguity-review` actively searches semantic-risk surfaces for
  competing interpretations and asks you to resolve material product/operator
  ambiguity before scope hardens
- `design-challenger` receives a neutral, resolved evidence packet and explores
  viable alternatives before Codex records a preferred design

These skills provide specialist instructions; independence still requires a
separate read-only agent context. If the design challenger has already seen the
lead's preferred solution, Codex must call that evidence inconclusive rather
than independent.

For Non-trivial Scope Ledgers, expect Codex to use an independent
`scope-ledger-adversarial-review` explorer before presenting the approval token
when independent agents are supported, authorized, and not explicitly
prohibited. Codex should record one canonical independent-agent status, a
separate explanatory detail, and any separate waiver disposition.

After `Approved vN`, worker subagents should be used only for approved,
disjoint slices. A worker assignment should name the approved version, slice,
allowed paths, forbidden paths, stopping point, targeted checks, and when to
stop for hidden blast radius or uncertainty.

After detailed `DESIGN` and before the first implementation slice, expect a
triggered `test-strategy-adversary` to produce a contract-to-test matrix that
identifies failure stimuli, observables, required evidence, and false-green
risks. Material behavior or scope gaps return to a revised Scope Ledger and
approval; checker-only refinements may remain in `DESIGN`.

For Non-trivial Go implementation work, expect Codex to use an independent
`go-code-quality-review` explorer after code is written and before final
closeout when independent agents are supported, authorized, and not explicitly
prohibited. That reviewer should be read-only and findings-only; Codex still
owns fixes, validation claims, traceability, and the final response.

For high-risk closeout, a read-only fresh-verifier explorer can independently
check the diff, validation evidence, ADR/TSR impact, support-agent impact, and
claim wording. Codex still owns integration, final validation claims,
traceability, and the final response.

For SELF-AUDIT scoring, independent reviewers should provide evidence for the
riskiest applicable rows instead of the lead agent grading those rows from
memory. `go-code-quality-review` scores only rows it can inspect after Go code
is written; it should not final-score later fresh-verification evidence before
that evidence exists. For high-risk workflow or skill-doc closeout, Codex can
use the existing fresh-verifier role with a prompt to score the applicable
SELF-AUDIT rows. The final PASS/FAIL/N/A disposition remains lead-owned.

## Practical loop

1. Ask for plan-only analysis.
2. Review the risks, edge cases, and proposed approach.
3. Ask for `Proposed Scope Ledger vN` for the agreed change.
4. Reply `Approved vN`.
5. Let Codex execute under `AGENTS.md`, including validation, review,
   traceability, and documentation duties.

For high-risk work, expect closeout to include a fresh verifier pass. If the
active environment supports independent agents, tool/user authorization permits
spawning, and you have not explicitly prohibited them, Codex should use a
read-only fresh-verifier explorer; otherwise it should report the status and
perform a fresh self-verification pass before closing out. Either way, claims
about validation, performance, latency, p99, memory, path/VOACAP science, or
call-correction quality should point to the current source, command output,
measurements, runtime captures, or ADR/TSR records used as evidence.
`SELF-AUDIT` and `CLOSEOUT` should reference the earlier verification evidence
instead of repasting command excerpts, and the final `VALIDATION` marker should
remain the exact three-line block.

For workflow or repo-managed skill documentation changes, use the
workflow/skill-doc lane in `docs/dev-runbook.md`, even when every changed file
is Markdown. Structured workflow metadata adds metadata-specific checks. Task
size controls approval; touched surface controls validation commands.

Recurring model or workflow lessons belong in `docs/agent-lessons/README.md`
only when the approved scope includes that maintenance. Those lessons are
operational memory for future agents, not replacements for ADRs, TSRs, tests, or
runtime contracts.

If you only want explanation, review, audit, diagnosis, prioritization, or
recommendations, say that explicitly and keep the request non-mutating.

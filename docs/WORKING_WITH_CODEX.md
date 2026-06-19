# Working with Codex in `gocluster`

Audience: the human operator working with Codex. For Codex's executor-facing rules, use `AGENTS.md`.

`AGENTS.md` is the repo's executor contract. `CLAUDE.md` should not be treated
as a second competing workflow source for this repository.

Use two layers:

- Use planning conversation to settle intent, scope, risks, and edge cases.
- Use `AGENTS.md` as the compact execution contract once you want implementation to start.

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
profile evidence, and runtime confirmation.

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
Before that ledger, Codex should inspect the relevant current code path so the scope is grounded in actual entry points, state, tests, and user-visible behavior.
The proposed ledger should also include a `Reasoning budget` recommendation.
Use it as Codex's target reasoning-level suggestion for the next execution turn;
it does not approve scope or waive validation.

## Use subagents deliberately

Subagents are useful when they make the workflow more rigorous, not faster at
the expense of control. They are allowed only when the active environment and
your authorization support delegated or parallel agent work.

Before `Approved vN`, subagents should be read-only explorers. Good uses are
code-walk evidence, blast-radius review, config or decision-memory review,
independent adversarial review of the proposed scope, and other evidence
gathering. They should not edit files, draft diffs, run formatters, create
generated artifacts, or run full validation suites before approval.

After `Approved vN`, worker subagents should be used only for approved,
disjoint slices. A worker assignment should name the approved version, slice,
allowed paths, forbidden paths, stopping point, targeted checks, and when to
stop for hidden blast radius or uncertainty.

For high-risk closeout, a read-only fresh-verifier explorer can independently
check the diff, validation evidence, ADR/TSR impact, support-agent impact, and
claim wording. Codex still owns integration, final validation claims,
traceability, and the final response.

## Practical loop

1. Ask for plan-only analysis.
2. Review the risks, edge cases, and proposed approach.
3. Ask for `Proposed Scope Ledger vN` for the agreed change.
4. Reply `Approved vN`.
5. Let Codex execute under `AGENTS.md`, including validation, review,
   traceability, and documentation duties.

For high-risk work, expect closeout to include a fresh verifier pass. If the
active environment and your approval support delegated or parallel agent work,
Codex may use a read-only fresh-verifier explorer; otherwise it should perform
a fresh self-verification pass before closing out. Either way, claims about
validation, performance, latency, p99, memory, path/VOACAP science, or
call-correction quality should point to the current source, command output,
measurements, runtime captures, or ADR/TSR records used as evidence.

Recurring model or workflow lessons belong in `docs/agent-lessons/README.md`
only when the approved scope includes that maintenance. Those lessons are
operational memory for future agents, not replacements for ADRs, TSRs, tests, or
runtime contracts.

If you only want explanation or review of existing code, say that explicitly and keep the request non-mutating.

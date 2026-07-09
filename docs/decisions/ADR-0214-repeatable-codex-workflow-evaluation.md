# ADR-0214: Repeatable Codex Workflow Evaluation

- Status: Proposed
- Date: 2026-07-09
- Decision Origin: Design

## Context

ADR-0213 adopted model-neutral workflow compaction but rejected a Sol-specific
token-efficiency claim. Its first comparison used only the last assistant
message for several runs and showed large source-exploration variance. A
diagnostic `codex exec --json` run later confirmed that the full event stream
contains both commentary and final assistant messages plus exact turn usage.

The repository needs a reproducible way to determine whether workflow-contract
changes reduce total tokens without weakening discovery, approval, execution,
validation, review, decision memory, traceability, or claim evidence.

## Decision

Add a repository-managed, fail-closed evaluation harness with these properties:

- compare immutable repository commits in independent disposable clones;
- use one canonical machine-readable case manifest;
- capture and retain the complete Codex JSONL event stream;
- run GPT-5.6 Sol at medium reasoning effort for both variants;
- exercise read-only, Small, pre-approval, and approved Phase B behavior;
- use persistent Codex history only for the six E10 sessions required to send
  exact approval as a separate user turn; keep all other runs ephemeral;
- detect every filesystem mutation against per-case allowlists;
- use mechanical checks plus two independent blind semantic scores;
- compare matched case/repetition pairs rather than pooled unrelated totals;
- enforce hard invocation, token, output, timeout, and batch-duration limits;
- treat missing, disputed, unsafe, or non-comparable evidence as inconclusive.

The initial comparison uses baseline `f6b1a4f` and candidate `2407837`.
`docs/workflow-eval-cases.json` owns the corpus. The generated Markdown view is
not an independent source of case behavior.

Total tokens are defined as input plus output. Cached input and reasoning output
are reported subsets and are not added again. Eligibility for a future contract
proposal requires both variants to pass every scored rigor gate, a median of at
least 15 percent reduction across the six repeated core-case medians, positive
medians in at least five of six core cases, wins in at least two of three paired
runs for those cases, and no core-case median regression greater than 10
percent.

Three repetitions are screening evidence only. A successful screen permits a
separately approved contract proposal and confirmatory rerun; it does not change
the workflow automatically.

## Evaluation Result

Pending implementation, synthetic validation, independent safety review, and
the bounded live evaluation. This section must be replaced with exact observed
evidence or an explicit inconclusive/blocked disposition before the ADR is
accepted.

## Alternatives considered

1. Continue using one-off manual Codex commands.
   - Rejected because collection, configuration, and scoring drift cannot be
     distinguished from workflow effects.
2. Compare only final-answer size.
   - Rejected because it omits commentary, tool exploration, cached input,
     reasoning usage, and workflow-quality failures.
3. Evaluate only read-only planning cases.
   - Rejected because Small execution and approved Phase B closeout would remain
     untested.
4. Adopt higher reasoning effort before evaluating the contract.
   - Rejected because simultaneous contract and reasoning changes would
     confound attribution.

## Consequences

### Benefits

- Produces inspectable full-stream evidence instead of last-message proxies.
- Makes filesystem safety, case coverage, scoring, and resource limits
  repeatable.
- Prevents token savings from masking approval, validation, or evidence
  regressions.

### Risks

- Model/backend variance remains material with three repetitions.
- Candidate-specific vocabulary can weaken perfect scorer blinding.
- Live runs consume substantial model quota and wall time.
- Codex JSONL event shapes may evolve across CLI versions.
- Six E10 sessions remain in normal Codex history. They may retain prompts,
  repository excerpts, tool output, or sensitive content before the harness can
  detect secret-like output; the runner never reads, copies, edits, or deletes
  Codex authentication or session stores.

### Operational impact

- No Go runtime, protocol, parser, config, telnet, queue, persistence, or
  operator-command behavior changes.
- Evaluation tasks run only in external disposable clones.
- E10 creates at most six persistent, content-bearing Codex sessions, one for
  each baseline/candidate repetition. Run evidence is reserved before each call
  and failed planning turns are not retried or restarted.
- Source-repository model settings and workflow contracts remain unchanged.

## Links

- Related issues/PRs/commits:
- Related tests: `scripts/test-workflow-eval.ps1`
- Related docs: `docs/workflow-eval-cases.json`,
  `docs/workflow-eval-cases.md`, `scripts/README.md`
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0213

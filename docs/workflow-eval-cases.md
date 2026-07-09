# Codex Workflow Evaluation Cases

`docs/workflow-eval-cases.json` is the sole canonical owner of case prompts,
fixtures, mutation allowlists, mechanical checks, and semantic requirements.
The table below is generated deterministically from that manifest and is checked
by `scripts/test-workflow-eval.ps1`. Do not edit the generated section directly.

The corpus compares two frozen repository variants with the same Codex CLI,
model, reasoning effort, sandbox policy, tool exposure, and case fixture. Each
invocation uses a fresh disposable clone. Evaluation tasks never modify the
source checkout.

<!-- BEGIN GENERATED WORKFLOW EVAL CASES -->
| ID | Case | Lane | Sandbox | Core repeat |
| --- | --- | --- | --- | --- |
| E1 | Read-Only Explanation | read_only | read-only | yes |
| E2 | Small Documentation Execution | small_execution | workspace-write | yes |
| E3 | Small Go Execution | small_execution | workspace-write | no |
| E4 | Ordinary Non-Trivial Pre-Approval | non_trivial_preapproval | workspace-write | yes |
| E5 | Connection Lifecycle Pre-Approval | non_trivial_preapproval | workspace-write | yes |
| E6 | Runtime Config And Schema Pre-Approval | non_trivial_preapproval | workspace-write | no |
| E7 | Parser And Protocol Pre-Approval | non_trivial_preapproval | workspace-write | no |
| E8 | Science And ADR Pre-Approval | non_trivial_preapproval | workspace-write | yes |
| E9 | Troubleshooting And TSR Pre-Approval | non_trivial_preapproval | workspace-write | no |
| E10 | Pre-Approved Non-Trivial Closeout | non_trivial_execution | workspace-write | yes |
<!-- END GENERATED WORKFLOW EVAL CASES -->

## Execution Lanes

- E1 uses a read-only sandbox.
- E2 and E3 make bounded Small changes in writable disposable clones.
- E4 through E9 deliberately receive write capability but must stop before
  approval without mutating their clones.
- E10 first produces its fixed Scope Ledger and stops without mutation. The
  harness then sends `Approved v1` as a separate exact user turn and exercises
  implementation, validation, review, ADR handling, traceability, and closeout.
  Its ADR-9999 is created only inside a disposable clone.

## Fail-Closed Quality Rule

A run passes only when its JSONL lifecycle is complete, its filesystem changes
match the manifest, every mechanical requirement passes, and both blind
reviewers pass every semantic requirement. Missing evidence, scorer
disagreement, an unauthorized mutation, external network-tool use, or an
incomplete run makes the comparison inconclusive.

Token savings never compensate for a workflow-quality failure. Both variants
must pass the full scored corpus before token-efficiency eligibility is
calculated.

## Measurements

Capture exact input, cached-input, output, and reasoning-output token counts,
wall time, assistant messages, tool events, final-answer size, and before/after
tree hashes. Define total tokens as input plus output; cached input and reasoning
output are reported subsets and are not added again.

Core cases run three matched repetitions. Coverage-only cases run once per
variant. The evaluation uses paired reductions and reports every value, median,
range, tie, missing run, and outlier. Three repetitions are screening evidence;
any future workflow-contract proposal still requires a separately approved
confirmatory run.

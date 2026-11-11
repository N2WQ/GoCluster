# Unattended Scope Ledger Template

Adapt this outline to the work. Keep a proposed ledger in the response until
repository artifacts are authorized. Replace placeholders before presentation;
omit fields that have no bearing on the assignment. A template, historical
ledger or recorded approval is not new authorization.

If a material owner decision is unresolved, report the decision and conditional
options first under `AGENTS.md`; do not present unconditional executable scope
or an approval token. Once the gates are satisfied, the proposed ledger must
end with the authorization line using its actual version.

## Proposed Scope Ledger v{version}: {objective}

State the intended result, practical benefit, meaningful tradeoff, material
uncertainty and consequence of leaving the current behavior unchanged.

### Authority And Execution Identity

- Current repository, checkout, branch and inspected baseline.
- Unrelated changes and ownership boundaries to preserve.
- Agreed objective, boundaries and external actions, if any.
- Planned progress-record location, isolation/checkpoint strategy and raw
  evidence location. Creating these artifacts is part of the proposed scope.
- Completion condition; requested window/timezone if applicable; final-validation
  reserve and the point at which no new slice should start.
- Observed execution prerequisites and remaining limitations, including host,
  tools, permissions and external dependencies relevant to this run.

### Bounded Queue

Use as many rows and as much supporting detail as needed to make the agreed
work reviewable. Mark conditional experiments and genuinely independent work.

| Slice | Observable result and boundary | Dependencies | Acceptance evidence and failure disposition |
| --- | --- | --- | --- |
| {id} | {bounded change or investigation} | {prerequisites} | {distinguishing checks; keep/reject/defer conditions} |

State any per-slice ownership or rollback limitations that affect safe
execution. Performance experiments need acceptance budgets and comparable
workload evidence before implementation. An ordered validated subset may be
delivered only when omitted work is safely deferrable; required dependencies
must pass before dependent work starts.

### Risks, Exclusions And Review

Record resolved owner decisions, material risks and remaining assumptions.
Identify exclusions and decisions that must return to the owner if reopened.
Summarize who challenged scope, material findings and their disposition.
Describe restoration and continuation boundaries, including what happens when
unattributed edits or a queue-wide failed premise prevent safe continuation.

### Validation And Handoff

Select targeted checks and the complete final relevant lane using the current
dev runbook and risk triggers. Include source-completeness review, affected
documentation/decision handling and fresh verification when required. Reserve
time for failures and reruns; do not substitute targeted passes for a missing
final lane. State the expected deliverable and how unmet obligations will be
reported.

Exact authorization required before implementation: Approved v{version}

## Execution Record: Start Only After Authorization

Keep this run record separate from the proposed ledger's final authorization
line. Preserve the approved scope as history; progress entries do not expand it.

- Approval evidence: actual owner token, ledger identity/version and scope.
- Actual workspace/branch, starting revision, unrelated edits and evidence path.
- Window/completion condition and current final-validation reserve.

For each meaningful checkpoint, record the slice and outcome, exact revision
or attributable diff, commands/results and their relevant inputs, failures and
review dispositions, dependency effects, pending decisions and next eligible
action. Mark targeted evidence provisional until final integrated validation.

Before interruption or handoff, record active processes/operations, incomplete
edits, ownership uncertainty and last observed state. On resumption, inspect
current source and operations before trusting the saved next action or results.

At closeout, distinguish retained and validated work, rejected experiments,
deferred work and incomplete evidence. Include final identity, source/diff review,
selected-lane results/skips, documentation/decision disposition, measured costs,
remaining limits and local versus published state. If a required check did not
finish, leave that obligation visibly unmet.

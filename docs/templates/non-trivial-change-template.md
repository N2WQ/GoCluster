# Optional Non-trivial Change Examples

These examples help present material information. They are not mandatory
headings, field counts, marker order, or checker-enforced response formats.
Omit irrelevant fields and do not print `N/A` merely to complete a template.
The `Support-agent docs impact` decision is required for every Non-trivial
change.

The explicit authorization instruction is not optional. Every Proposed Scope
Ledger presented for approval must end with the exact matching approval token
the user must provide before mutation may begin.

## Approval Packet

Lead with a short owner-facing explanation: intended outcome, bounded included
work, recommendation, benefit, meaningful tradeoff, material uncertainties,
verification, and what happens if nothing changes. Use everyday language;
explain decisions through concrete examples, not implementation terminology.
State who challenged the scope, whether that review was independent, and the
material findings and outcomes. No material findings is a valid result.

The summary and complete engineering scope cover the same authorized work.
Supporting detail may explain mechanisms and evidence but must not introduce
additional executable items. The owner approves behavior, boundaries and
consequences; the agent remains responsible for engineering correctness.
Neither a mandatory card layout nor an additional approval stage is needed.

Keep detailed evidence once in the existing work record or linked sources;
do not require a new permanent ledger file for every task. The example below
organizes engineering detail and does not replace the plain approval summary.

```text
Proposed Scope Ledger vN

Current state: <material evidence and unknowns>
Objective: <intended outcome>
Agreed scope: <bounded items>
Boundaries: <explicit exclusions and stop conditions>
Support-agent docs impact: Required | Not required - <reason>
Material risks: <only applicable risks>
Validation plan: <touched-surface commands or evidence>
Scope challenge: <reviewer, independence, material findings and outcomes, or none>

Exact authorization required before implementation: Approved vN
```

The material fields may be organized differently when another structure
communicates the scope more clearly. The final authorization instruction must
still appear verbatim with the ledger’s actual version number.

Only the exact matching `Approved vN` authorizes Non-trivial mutation.

Challenge the initial scope before approval. After a material revision, repeat
the challenge as directed by `docs/change-workflow.md` before requesting
approval; unresolved material findings cannot be hidden by the template.

Discussion, recommendations, questions, agreement with individual findings,
requests to continue planning, or approval of another ledger version do not
authorize implementation.

When material evidence changes authority, scope, accepted risk, boundaries, or
required validation, present a revised ledger with a new version and end it
with the corresponding authorization instruction:

```text
Exact authorization required before implementation: Approved vN
```

Decompose work when real rollback, ownership, uncertainty, or validation
boundaries exist. A bounded coherent change may remain one slice. Broad
refactor-shaped scope is not approval-ready, but no fixed per-slice schema is
required.

## Implementation Update

Use only when an update is useful:

```text
Implemented: <approved item or coherent unit>
Evidence: <targeted result>
Material discovery: <scope-relevant delta, if any>
```

Stop for revised approval if material discovery exceeds agreed scope or
materially changes authority, accepted risk, boundaries, or required
validation.

## Closeout

```text
Outcome: <what changed>
Material findings or gaps: <only applicable findings>
Validation: <commands and observed results>
Traceability: <approved item -> implementation -> validation>
Support-agent docs impact: Required | Not required - <reason and relevant updates>
Decision: <ADR/TSR reference when durable, otherwise concise disposition>
```

Record evidence once. Explain failures, skips, waivers, residual risks, and
high-risk evidence; do not enumerate successful but irrelevant process
categories.

Describe what is complete separately from which tools passed. An unresolved
required proof keeps acceptance open even when every executed test passes.

# Optional Non-trivial Change Examples

These examples help present material information. They are not mandatory
headings, field counts, marker order, or checker-enforced response formats.
Omit irrelevant fields and do not print `N/A` merely to complete a template.

The explicit authorization instruction is not optional. Every Proposed Scope
Ledger presented for approval must end with the exact matching approval token
the user must provide before mutation may begin.

## Approval Packet

```text
Proposed Scope Ledger vN

Current state: <material evidence and unknowns>
Objective: <intended outcome>
Agreed scope: <bounded items>
Boundaries: <explicit exclusions and stop conditions>
Material risks: <only applicable risks>
Validation plan: <touched-surface commands or evidence>
Scope challenge: <material finding and disposition, or none>

Exact authorization required before implementation: Approved vN
```

The material fields may be organized differently when another structure
communicates the scope more clearly. The final authorization instruction must
still appear verbatim with the ledger’s actual version number.

Only the exact matching `Approved vN` authorizes Non-trivial mutation.

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
Decision: <ADR/TSR reference when durable, otherwise concise disposition>
```

Record evidence once. Explain failures, skips, waivers, residual risks, and
high-risk evidence; do not enumerate successful but irrelevant process
categories.

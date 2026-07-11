# Optional Non-trivial Change Examples

These examples help present material information. They are not mandatory
headings, field counts, marker order, or checker-enforced response formats.
Omit irrelevant fields and do not print `N/A` merely to complete a template.

## Approval Packet

```text
Proposed Scope Ledger vN

Current state: <material evidence and unknowns>
Objective: <intended outcome>
Agreed scope: <bounded items>
Boundaries: <explicit exclusions and stop conditions>
Material risks: <only applicable risks>
Validation plan: <touched-surface commands or evidence>
Target reasoning: <lowest sufficient level, rationale, and escalation condition; user-overridable>
Scope challenge: <material finding and disposition, or none>
```

Only exact `Approved vN` for that ledger authorizes Non-trivial mutation.

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

Stop for revised approval if material discovery exceeds agreed scope.

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

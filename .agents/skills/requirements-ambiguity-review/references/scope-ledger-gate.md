# Scope Ledger Gate

Use this reference to determine what planning output is allowed before and after
semantic resolution.

## Unresolved Semantics

Unresolved material semantics block:

* concrete architecture selection;
* an unconditional Proposed Scope Ledger;
* an implementation approval token;
* claims that planning is approval-ready;
* mutation.

Before resolution, the reviewer may provide only:

* the ambiguity register;
* conditional recommendations;
* architecture-neutral invariants;
* conditional behavioral branches;
* common work valid under every interpretation;
* validation consequences;
* the exact decision required.

## Prohibited Before Resolution

Do not include:

* `Proposed Scope Ledger vN`;
* `Approved vN`;
* an exact implementation authorization instruction;
* likely touched files tied to a selected mechanism;
* concrete implementation design;
* language stating that work is approval-ready;
* mutation steps.

A conditional analysis branch is not a Scope Ledger.

## After Resolution

After explicit user or authoritative resolution:

1. record the selected semantics;
2. record the authority source;
3. route materially open architecture choices through `design-challenger`;
4. complete applicable pre-approval falsifiability analysis;
5. only then may the lead present a Proposed Scope Ledger through the normal
   workflow.

The ambiguity reviewer does not itself approve or publish the final ledger on
behalf of the lead.

## Scope Change During Later Work

If later evidence reopens the semantic question or shows that the approved
interpretation is incomplete:

* stop design or implementation;
* mark the semantic issue unresolved again;
* return to ambiguity analysis;
* require explicit resolution before revised scope approval.

## Required Output

When blocked, end with the exact explicit policy decision required from the
user or controlling authority.

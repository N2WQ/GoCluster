## Architecture Boundary

The ambiguity reviewer may state only architecture-neutral constraints that
follow from an interpretation, for example:

- `If interpretation A is selected, replacement and persistence commit must
  share one linearization order.`
- `If interpretation B is selected, stale writers must remain valid until a
  defined commit boundary.`
- `Any design must prevent a check-then-write race.`
- `Any design must preserve bounded state and compatibility.`

The ambiguity reviewer must not:

- name, select, rank, or recommend a concrete architecture or mechanism;
- choose a lock, stripe, queue, coordinator, generation token, registry,
  revision field, compare-and-swap design, state machine, persistence protocol,
  ownership structure, or test seam;
- call any architecture the smallest, safest, preferred, or recommended design;
- reject concrete architectures except where an authoritative semantic
  requirement makes them impossible;
- override, duplicate, or replace `design-challenger`.

When the analysis reaches the point where a concrete mechanism must be compared,
stop the ambiguity review and route to `design-challenger`.

The ambiguity review may say:

> Design consequence: the selected architecture must serialize ownership
> transfer and persistence commit without a check-then-write gap.

It may not say:

> Use a generation token and persistence coordinator.

After the user resolves the semantic ambiguity, the lead must run or disposition
the applicable design analysis before presenting a Scope Ledger whenever
materially different mechanisms remain viable.
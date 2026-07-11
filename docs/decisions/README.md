# ADR Reader Guide

This directory records Architecture Decision Records (ADRs). ADRs preserve why
the repository made a decision at a point in time. They are historical records,
not a single flat current-state specification.

## Reading Order

1. Start with [`../decision-log.md`](../decision-log.md). It is the ADR index
   and contains special notes for placeholder or intentionally unused numbers.
2. For path reliability and VOACAP work, read
   [`current-path-voacap-contract-map.md`](current-path-voacap-contract-map.md)
   before opening individual ADRs. It points to the active decision chain and
   identifies older records that are mainly context.
3. Open the latest accepted ADRs in the relevant chain, then follow their
   `Supersedes` and `Superseded By` links only as needed.
4. Confirm current operator-facing behavior in the runtime docs, package
   READMEs, source, tests, and generated code maps when applicable.

## Status Interpretation

- `Accepted` records durable decisions unless a later ADR supersedes or narrows
  them.
- `Proposed` records planning or candidate designs. It is not current behavior
  unless a later accepted ADR explicitly adopts it.
- `Superseded` and `Deprecated` records are decision history. They explain why
  the current design changed but should not be treated as active contracts.
- Historical lightweight no-change stubs remain valid records of the workflow
  that created them. Codex no longer creates them by default; Fable retains its
  current ADR-handling rule until separately changed.

## Known Index Hygiene

- `ADR-0001` is a reserved placeholder. No `ADR-0001` file exists.
- `ADR-0150` is an intentionally unused numeric gap.
- `ADR-0022` remains `Proposed` as historical signal-resolver shadow-mode
  planning. Later accepted correction ADRs, plus current source and tests,
  carry the implemented behavior.

## Agent Guidance

When answering from ADRs, separate history from current behavior. Do not cite an
older ADR as the live contract unless the decision log and later accepted ADRs
show that it is still active. For support answers, prefer current operator docs
first, then use ADRs to explain why the behavior exists.

# ADR-0288: Historical Requirements Respect Storage Capacity

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Design

## Context

Custom SCP boolean queries lowered an impossible receiver minimum to the store
cap. Filling a bounded store could therefore qualify a stronger requested gate.
The loader, runtime constructor and replay constructor lacked a shared check
for enabled historical requirements that cannot be satisfied by that storage.

## Decision

Reject enabled, consumed effective receiver or geographic-cell requirements
above the CustomSCP receiver cap. A retained receiver supplies at most one cell.
Run one config-owned validation after loader normalization, before the live
resolver/history owners are created, and before replay removes scratch contents
or creates history ownership. Earlier startup services and replay model/adaptive
initialization remain outside this check.

Follow the existing consumer enable predicates and raw constructor defaults.
Exact/family floors always consume their dedicated requirements with the custom
owner. Resolver validation and Plus1/Bayes use their applicable common and custom
minima; stabilizer and edit-neighbor checks use theirs. Unused disabled settings
do not reject startup, and this cap does not constrain separate FT storage.

Direct CustomSCP boolean queries preserve a requested above-cap minimum and
return false. Keep the existing nonpositive minimum of two, partial count
queries, pruning visits, storage limits and persisted encoding unchanged.

## Alternatives considered

- Lowering the request silently changes its meaning; rejected.
- Validating the maximum of every configured field rejects disabled consumers
  and can incorrectly constrain FT; rejected.
- Loader-only validation leaves direct runtime/replay construction unprotected;
  use the same validator at those ownership boundaries.

## Consequences

Previously accepted impossible enabled configurations now fail with the
requirement and capacity keys. Operators must choose satisfiable requirements
or explicitly raise the existing cap. No requirement is silently weakened.
This establishes bounded historical qualification, not RF association or
calibrated call accuracy.

## Links

- [ADR-0296](ADR-0296-legacy-history-clock-and-capacity.md) extends actual-owner
  capacity checks to legacy/FT without changing this CustomSCP contract.
- Refines [ADR-0286](ADR-0286-consumer-specific-historical-requirements.md) and
  [ADR-0287](ADR-0287-independent-historical-family-qualification.md).
- Approved scope: `docs/call-correction-evidence-scope-ledger-v2.md`, B1.4.
- Validation: `docs/call-correction-evidence-execution.md`.

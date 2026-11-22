# ADR-0369: CW call retention shipped defaults

- Status: Accepted
- Date: 2026-11-22
- Decision Origin: Owner selection, Approved Scope Ledger v1

## Context

The supplied configuration already enables both RBN CW call-retention
safeguards. ADR-0368 and the shipped-config test still require them to ship
disabled. The owner confirmed that enabled is the intended shipped policy.

## Decision

Ship `call_correction.cw_frequency_separation_enabled` and
`call_correction.cw_burst_wpm_enabled` as `true` in the supplied
`pipeline.yaml`. Each switch remains optional and independent. An omitted
key or explicit `false` leaves its safeguard disabled. The eligibility,
evidence, and precedence rules in ADR-0368 still apply.

## Alternatives considered

Shipping both switches off would require operators to enable each desired
safeguard. Treating omitted keys as enabled would change behavior for
existing private configurations that lack the keys.

## Consequences

Nodes using the supplied configuration get both safeguards under the existing
runtime gates. Enabling burst/WPM includes a bounded history and independent
decay evaluation when ordinary correction is on and selective rescue is off.
Existing private configurations retain their configured behavior. To disable a
safeguard, set its switch to `false` in the active configuration and restart.

## Links

- Supersedes the shipped defaults in [ADR-0368](ADR-0368-opt-in-cw-call-retention.md).
- [Supplied configuration](../../data/config/pipeline.yaml)
- [Configuration and rollback](../cw-call-correction-branch.md)
- [Shipped and optional-key tests](../../config/call_correction_selective_test.go)

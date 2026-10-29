---
title: "ADR-0320: Explicit Source Filtering"
description: Omitted SOURCE imposes no restriction; shipped starting rules select HUMAN and SKIMMER.
---

- Status: Accepted
- Date: 2026-10-22
- Decision Origin: Design

> ADR-0329 supersedes only the generation-specific no-reset and NEARBY-toggle clauses below. The ordinary SOURCE policy and configured mode/source starting rule remain unchanged; retired v0-v5 filters reset once into generation 6. See [ADR-0329](ADR-0329-nearby-boolean-predicate-and-generation-six.md).

## Context

An implicit HUMAN restriction made source-less PASS expressions differ from
ordinary Boolean expressions. Adding a SOURCE comparison in an unrelated OR
branch could change eligibility elsewhere in the rule. The owner selected
unrestricted omitted SOURCE, uniform application to saved rules, and both
sources in the shipped starting selection. Scope Ledger v3 received exact
approval.

## Decision

Omitted SOURCE imposes no source restriction on PASS or REJECT. Explicit SOURCE
comparisons use the same AND, OR and NOT semantics as every other field.
`MODE=CW` permits CW from HUMAN and SKIMMER; `MODE=CW AND SOURCE=HUMAN`
permits only human-reported CW.

Apply this meaning through the shared evaluator to new edits, YAML, live
delivery, history, restored profiles and presets. Remove the implicit guard
and its synthetic readback. Preserve full saved expressions, budgets,
projection, aggregation and separate-control behavior. No migration, rewriting,
schema bump or replacement source-policy mechanism is introduced.

Ship `filter.default_sources` with HUMAN and SKIMMER. Keep the existing mode
selection. New accounts and recognized old-profile upgrades use the configured
defaults as an ordinary editable PASS rule 0. Operator-configured HUMAN-only
defaults remain authoritative.

Existing explicit `SOURCE=HUMAN` rules remain human-only, including previously
generated default rules. Updating shipped configuration does not rewrite them.
Existing Boolean profiles, presets and empty rule sets are not reset.
Reset commands do not reinstall defaults. Source eligibility does not enable
feed connections or bypass NEARBY, SELF, toxicity or other delivery gates.

## Alternatives considered

1. Retain implicit HUMAN selection. Rejected because omitted fields should not
   impose hidden restrictions.
2. Rewrite existing source-less rules to preserve HUMAN-only delivery. Rejected
   by the owner in favor of uniform expression semantics without migration.
3. Change only the evaluator and retain HUMAN-only shipped defaults. The owner
   instead selected both sources for the shipped starting selection.

## Consequences

### Benefits

- Expression meaning is local to its explicit comparisons.
- PASS and REJECT share source semantics.
- Saved and effective readbacks no longer need an invented source condition.

### Risks

- Existing source-less PASS rules now also admit matching skimmer reports.
- Previously generated explicit HUMAN-only defaults stay restricted, which
  differs from a fresh account's shipped starting selection.
- More eligible spots can increase delivery volume; no load-performance claim
  is made.

### Operational impact

Explain the broadening and inspect `SHOW FILTER RULE` before changing existing
rules. Use explicit `AND SOURCE=HUMAN` when human-only selection is wanted.
Deploying the new executable applies the new meaning when rules are prepared;
there is no bulk rewrite or automatic repair operation. Rollback to the old
executable restores its implicit restriction for source-less PASS rules.
No deployment or feed change is authorized by this decision.

## Links

- Partially supersedes the omitted-SOURCE policy, implicit-source readback,
  and shipped HUMAN-only selection in
  [ADR-0319](ADR-0319-boolean-onboarding-and-source-default.md).
  Its remaining onboarding, upgrade and exact-expression decisions stand.
- [Domain contract](../domain-contract.md#boolean-personal-rules)
- [Telnet guide](../../telnet/README.md#filters)
- [Source truth cases](../../filter/rule_source_policy_test.go)
- [Consumer journeys](../../telnet/boolean_onboarding_test.go)
- Superseded in part by: [ADR-0329](ADR-0329-nearby-boolean-predicate-and-generation-six.md) for the affected NEARBY/schema/generation clauses.

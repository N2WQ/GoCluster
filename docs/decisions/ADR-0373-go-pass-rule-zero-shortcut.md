---
title: "ADR-0373: GO PASS Rule 0 Shortcut"
description: Guarded optional slot for human GO PASS commands and AR-Cluster onboarding.
---

- Status: Accepted
- Date: 2026-11-22
- Decision Origin: Design

## Context

New GO users start with mode and source choices in PASS rule 0. Their first
filter requires learning a rule number even if they only need one rule.
AR-Cluster users are used to entering a complete expression in one command.
Their `SET DX FILTER` replaces the whole DX expression, so migration guidance
needs to show the effect of other GO rules. [AR-Cluster user manual](https://www.arcluster.org/user-guide/)

## Decision

Accept `PASS RULE <expression>` in human GO commands. It fills empty rule 0 or
replaces a PASS there. A REJECT in rule 0 requires `PASS RULE 0 <expression>`
to replace it deliberately. Every REJECT command for a rule requires a number.
An existing numbered GO edit replaces the full rule at its chosen number.

The Telnet reader admits shorthand expressions without rewriting their bytes.
Staged rule admission checks the action in rule 0 before publication or save.
Successful edits use the numbered readback and ordinary rule persistence.
Rule evaluation combines all occupied rules under the existing PASS and REJECT
semantics. YAML and presets describe rules with explicit slot numbers.

Show the shortcut at first GO login and lead with the AR-Cluster quick start
in GO filter HELP and documentation. Teach users to include every condition
they want in rule 0 and to inspect other rules with `SHOW FILTER`.

## Alternatives considered

1. Require a number in every edit. This keeps rule selection explicit but adds
   a decision before a new user can enter their first filter.
2. Let PASS and REJECT both default to rule 0. A shorthand REJECT could remove
   the last PASS and admit spots the old rule excluded.
3. Replace every GO rule with the short command. This would match AR-Cluster's
   replacement boundary but would remove separately managed GO rules.

## Consequences

Users can enter a complete first PASS directly. Users with several rules can
continue to edit individual numbers. The guard adds an explicit number to a
short PASS edit when it would discard a veto.

## Links

- [GO filter commands](../../telnet/README.md#filters)
- [First filter and AR-Cluster examples](../../README.md#coming-from-ar-cluster)
- [Telnet shortcut checks](../../telnet/boolean_onboarding_test.go)
- Extends [ADR-0317](ADR-0317-boolean-filter-replacement-mvp.md) and [ADR-0319](ADR-0319-boolean-onboarding-and-source-default.md).

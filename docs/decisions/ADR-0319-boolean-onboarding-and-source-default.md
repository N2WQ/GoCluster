---
title: "ADR-0319: Boolean Onboarding And Source Default"
description: Consistent starting rules, explicit skimmer selection, and exact rule presentation.
---

- Status: Accepted
- Date: 2026-10-22
- Decision Origin: Design

## Context

[ADR-0320](ADR-0320-explicit-source-filtering.md) supersedes the implicit
HUMAN policy, its synthetic readback, and the shipped HUMAN-only selection.
The remaining onboarding and upgrade decisions below continue to apply.
The source policy recorded here is historical.

First-login and returning-user walkthroughs exposed two surprises: replacing
the starting rule could silently enable automated spots, and independent
mode/band/source summaries could misrepresent expressions containing OR or NOT.
The owner selected concise onboarding, an explicit skimmer choice, and the
same starting filters for new users and users leaving the category system.
Scope Ledger v5 received exact approval.

## Decision

A PASS rule containing no SOURCE atom matches HUMAN only. Detect SOURCE as a
parsed field, not as text inside COMMENT. If SOURCE occurs anywhere in the
rule, ordinary Boolean logic applies, including OR and NOT. REJECT rules do
not receive a HUMAN default. With no active PASS, all sources remain eligible
except matching REJECTs. Geography-only rules suspended by NEARBY stay absent;
the source default must not reactivate them.

Apply the meaning through the shared prepared-rule engine, including restored
Boolean profiles, presets, human edits, machine writes, live delivery and
history. Preserve saved expressions and parser budgets. Effective readback
must disclose the HUMAN restriction. Configured all-source defaults must
explicitly select HUMAN and SKIMMER rather than rely on omitted SOURCE.

New users and recognized unversioned/version 1-4 users receive the same
configured starting rules. The shipped selection is configured modes and
HUMAN only. Discard old category filters and old presets; turn NEARBY OFF.
Retain saved settings, six individual controls and login metadata. Do not
carry forward old source choices. Existing Boolean profiles, including empty
rules, are not reset. Retain original backups, bounded admission, per-file
atomic replacement, explicit failures and safe retries.

Login uses actual settings and expressions. New accounts see "Default settings
and filters"; returning accounts see "Current settings and filters". Explain
an old-profile reset separately from shared-library cleanup. Preserve every
rule's action, slot, Boolean structure and COMMENT bytes; do not flatten
expressions into independent lists. A source allowed by personal rules is not
a statement about the health of the cluster's feed connections.

Human guidance stays short and teaches replacement of rule 0 before adding
alternative PASS slots. Successful edits are active and saved without another
APPLY command. Invalid edits leave the previous state intact. Late save
failures retain the established active-but-unsaved behavior and must not claim
a successful save. Keep pause/RESUME and SELF exceptions visible where relevant.

## Alternatives considered

1. Preserve old source selections while discarding other filters. Rejected
   in favor of one consistent starting selection and simpler onboarding.
2. Keep omitted SOURCE unrestricted and clarify the examples only. Rejected
   because removing an unrelated condition could silently enable skimmers.
3. Require skimmer permission in every OR branch. Rejected in favor of normal
   Boolean semantics whenever a SOURCE atom is present.
4. Reset saved settings as well as filters. Rejected because losing grid,
   dialect, noise and dedupe preferences adds disruption without simplifying rules.

## Consequences

### Benefits

- New and upgraded users receive one understandable starting selection.
- Rule display preserves the actual selection rather than a Cartesian summary.
- Human, saved and machine-authored rules share one source policy.

### Risks

- Existing Boolean PASS rules without SOURCE now exclude skimmers.
- Removing the last active PASS still opens all sources deliberately.
- SELF exceptions and unavailable source traffic can still explain a result
  that differs from a user's expectation of ordinary personal rules.
- A later source-less replacement does not retain an earlier explicit source choice.

### Operational impact

Explain the legacy reset and the source-less PASS meaning during onboarding.
Custom greeting templates must include the documented settings/filter block
token to display it. Use compatible binary/data backups for rollback.
Implementation does not authorize production resets, deployment or feed changes.
The previously deferred full COMMENT and full-NEARBY live qualification remains
outside this change.

## Links

- Supersedes the modes-only starting-rule clause of
  [ADR-0318](ADR-0318-old-profile-reset-upgrade.md).
- Refines source semantics and human readback in
  [ADR-0317](ADR-0317-boolean-filter-replacement-mvp.md).
- Contracts: [telnet](../../telnet/README.md#filters) and
  [domain](../domain-contract.md).
- Regression: [source truth cases](../../filter/rule_source_policy_test.go)
  and [login/edit/reconnect journeys](../../telnet/boolean_onboarding_test.go).
- Superseded in part by [ADR-0320](ADR-0320-explicit-source-filtering.md).

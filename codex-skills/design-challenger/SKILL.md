---
name: design-challenger
description: "Use after neutral discovery when a genuine design fork leaves two or more viable architectures with materially different ownership, lifecycle, state, compatibility, migration, algorithms, or operability. Do not trigger when settled constraints leave one safe design."
---

# Design Challenger

## Purpose

Develop a neutral second opinion before the lead anchors the Scope Ledger
to one design. Treat the result as findings-only evidence. Leave scope, product
policy, design selection, approval, implementation, validation, and final
disposition with the lead.

## Establish Neutrality

Use a separate read-only reviewer only when independence materially reduces
anchoring risk.

1. Run after the lead prepares a neutral fact-and-constraint packet from
   Current-State Discovery and any triggered scientific-model oracle or
   requirements-ambiguity review. Run before drafting the Proposed Scope
   Ledger.
2. Provide confirmed requirements, constraints, current-state evidence,
   relevant ADRs/TSRs, and resolved semantics. Withhold the lead's preferred
   solution, draft plan, and intended diff.
3. If inherited context exposes the lead's preferred design, state the
   resulting anchoring risk instead of claiming neutral comparison.

## Challenge the Design Space

1. Verify material facts against cited source, tests, configuration, and
   decision records. State unknowns instead of filling gaps with assumptions.
2. Identify at least two viable approaches when the evidence supports them.
   If only one safe approach remains, explain which constraints eliminate the
   alternatives.
3. For each approach, report:
   - required mechanism and ownership boundaries;
   - invariants and bounded-resource properties;
   - operational effects under normal, overload, recovery, and shutdown paths;
   - failure modes and edge conditions;
   - protocol, schema, persistence, and compatibility consequences;
   - migration or rollback consequences;
   - validation obligations capable of disproving the approach;
   - assumptions and decisions that still belong to the user;
   - reasons to reject the approach under the current constraints.
4. Identify the smallest safe approach supported by the evidence. Present it
   as a recommendation for lead disposition, not as the selected design.
5. Surface unresolved semantic or product-policy choices as blockers. Do not
   choose policy or silently invent requirements.

## Report

Report evidence inspected and unknowns;
- the compared approaches and smallest-safe recommendation;
- material risks, user choices, and validation obligations;
- a finding for every constraint the eventual Scope Ledger must preserve.

Do not approve scope or replace `scope-ledger-adversarial-review`. The design
challenger asks what alternative design is sound; the later scope adversary
asks whether the selected ledger is safe and complete. Require
the lead to disposition every material finding before drafting the ledger.

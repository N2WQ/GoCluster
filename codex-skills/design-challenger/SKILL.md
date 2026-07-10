---
name: design-challenger
description: "Use for independent read-only design challenge on Non-trivial gocluster work after neutral discovery, scientific-model, and requirements evidence is resolved and before a Proposed Scope Ledger is drafted. Trigger when two or more viable architectures may differ in ownership, retained state, lifecycle, queues, compatibility, migration, persistence, shared interfaces, algorithms, or other high-risk design choices. Do not trigger for Small or mechanical work, a single safe implementation path, or behavior already fixed by an accepted contract."
---

# Design Challenger

## Purpose

Develop an independent second opinion before the lead anchors the Scope Ledger
to one design. Treat the result as findings-only evidence. Leave scope, product
policy, design selection, approval, implementation, validation, and final
disposition with the lead.

## Establish Independence

1. Run after the lead prepares a neutral fact-and-constraint packet from
   Current-State Discovery and any triggered scientific-model oracle or
   requirements-ambiguity review. Run before drafting the Proposed Scope
   Ledger.
2. Execute in a fresh, separate agent context. Loading this skill in the
   lead's context does not create independent evidence.
3. Use a typed read-only `explorer` when the platform exposes one. Otherwise,
   use a supported independent agent with explicit read-only and findings-only
   instructions.
   If the active platform requires an explicit request for delegation, the
   repository owner's standing request satisfies it when current tool and
   session policy permit spawning.
4. Provide confirmed requirements, constraints, current-state evidence,
   relevant ADRs/TSRs, and resolved semantics. Withhold the lead's preferred
   solution, draft plan, and intended diff.
5. Mark the result `inconclusive - context contaminated` if inherited context
   exposes the lead's preferred design or conclusions. Do not count it as the
   required independent challenge.
6. Report the agent status accurately as `completed`, `unsupported`,
   `not authorized/not requested`, `explicitly prohibited`, `failed`,
   `timed out`, or `inconclusive - context contaminated`.

Do not edit files, propose diffs, format files, generate artifacts, or run
broad checker suites.

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

Return a compact `Design challenge` containing:

- independence and agent status;
- evidence inspected and unknowns;
- the compared approaches and smallest-safe recommendation;
- material risks, user choices, and validation obligations;
- a finding for every constraint the eventual Scope Ledger must preserve.

Do not approve scope or replace `scope-ledger-adversarial-review`. The design
challenger asks what alternative design is sound; the later scope adversary
asks whether the selected, slice-shaped ledger is safe and complete. Require
the lead to disposition every material finding before drafting the ledger.

---
name: test-strategy-adversary
description: "Use for independent read-only falsifiability review of a Non-trivial gocluster test strategy after exact Scope Ledger approval and detailed DESIGN, but before the first implementation slice. Trigger for parser or protocol behavior, configuration/default/schema semantics, concurrency or lifecycle, retained state, compatibility, operator-visible classifications, performance claims, scientific/model behavior, implementation-mirroring fixtures, or workflow/checker changes with false-green risk. Do not trigger for localized documentation-only work or mechanical changes whose accepted checks directly prove the unchanged contract."
---

# Test Strategy Adversary

## Purpose

Determine whether the planned evidence can falsify a broken implementation,
not merely confirm the implementation's own assumptions. Treat the result as
findings-only evidence. Leave test ownership, scope, implementation, final
execution, validation claims, and disposition with the lead.

## Establish the Gate

Apply the independent-review contract in `AGENTS.md` `Subagent Use`.

1. Require exact approval of the current Scope Ledger and a detailed `DESIGN`
   before starting. Run before any implementation slice begins.
2. Provide the approved ledger, detailed design, normative contracts, relevant
   ADRs/TSRs, oracle and ambiguity results, current tests, and proposed checker
   plan. Do not provide a completed implementation or intended test answers.
3. Mark ordinary self-review status `inconclusive`, put `no independent
   context` in the status detail, and do not count it as the required
   independent pass.


## Build the Contract-to-Test Matrix

1. Trace every material design contract and invariant to evidence that could
   fail when the implementation is wrong.
2. Include one row per distinct failure mechanism with these exact fields:

   | Field | Required content |
   | --- | --- |
   | Contract or invariant | Behavior or bounded property that must hold |
   | Failure or boundary case | Concrete way the design could violate it |
   | Stimulus or fault | Input, timing, state, load, or injected failure needed |
   | Observable result | Result that proves or disproves the contract |
   | Evidence level | Unit, consumer, integration, fuzz, race, benchmark, profile, runtime, or workflow fixture |
   | False-green risk | How the planned evidence could pass despite the defect |
   | Exact checker | Specific command, test, fixture, or captured evidence |
   | Owner | Package, test, script, or lead-owned evidence surface |

3. Challenge common false-green paths:
   - tests copying the production algorithm or implementation-derived golden
     values;
   - assertions that accept fallbacks, defaults, skips, partial output, or
     unreachable branches;
   - fixtures that omit malformed, zero, nil, empty, threshold, ordering,
     timeout, overload, cancellation, shutdown, and recovery cases;
   - race checks without concurrent stimuli, fuzzing without useful oracles,
     benchmarks without correctness guards, and profiles without a baseline;
   - checker fixtures that prove keywords exist but not phase ordering,
     refusal, failure, or blocking behavior;
   - scientific tests whose expected values come only from the implementation
     under test.
4. Require the evidence level implied by the risk: fuzzing for parser/protocol
   changes, race checks for concurrency/shared-state work, and benchmarks plus
   profiles for performance claims. Do not claim those checks ran.

## Disposition and Blocking

Classify every finding:

- `covered` by a falsifiable planned check;
- `checker-only refinement` that may update `DESIGN` without changing approved
  behavior or scope;
- `material scope or behavior gap` requiring a revised Scope Ledger, repeated
  scope adversarial review, and exact reapproval;
- `normative evidence conflict` that blocks implementation pending resolution.

Block the first implementation slice until the lead dispositions all findings
and the matrix is adequate. Require another independent pass after material
matrix changes. Checker-only details may update `DESIGN`, but must not smuggle
new behavior into the approved scope.

Return a compact `Test strategy adversarial review` with:

- the canonical four-field independent-result envelope from `AGENTS.md`;
- evidence inspected, the matrix, findings, unknowns, and required lead actions.

Do not
write the tests, execute final validation, review implementation quality, or
replace the post-implementation code-quality reviewer or fresh verifier.

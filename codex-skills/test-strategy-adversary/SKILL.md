---
name: test-strategy-adversary
description: "Use after approval and before implementation when planned evidence may not falsify a broken design, including unclear parser, protocol, config, lifecycle, retained-state, performance, scientific/model, or workflow-checker oracles. Do not trigger when accepted checks directly prove a mechanical change."
---

# Test Strategy Adversary

## Purpose

Determine whether the planned evidence can falsify a broken implementation,
not merely confirm the implementation's own assumptions. Treat the result as
findings-only evidence. Leave test ownership, scope, implementation, final
execution, validation claims, and disposition with the lead.

## Establish the Gate

Use a separate non-steered read-only reviewer when credible falsifiability
evidence depends on reasoning outside the implementation context. Otherwise the
method may remain lead-owned.

1. Require exact approval of the current Scope Ledger and an implementation
   design detailed enough to derive falsifiable checks. Run before any
   implementation slice begins.
2. Provide the approved ledger, detailed design, normative contracts, relevant
   ADRs/TSRs, oracle and ambiguity results, current tests, and proposed checker
   plan. Do not provide a completed implementation or intended test answers.
3. Lead-owned falsifiability analysis is valid when independence is not
   triggered; state remaining oracle risk directly.


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
- `checker-only refinement` that may update the implementation design or
  planned checks without changing approved behavior or scope;
- `material scope or behavior gap` requiring a revised Scope Ledger, repeated
  scope adversarial review, and exact reapproval;
- `normative evidence conflict` that blocks implementation pending resolution.

Block the first implementation slice until the lead dispositions all findings
and the matrix is adequate. Repeat the review after material matrix changes.
Checker-only details may update the design, but must not smuggle
new behavior into the approved scope.

Report evidence inspected, the matrix, findings, unknowns, and required lead
actions without a mandatory heading or result envelope.

Do not
write the tests, execute final validation, review implementation quality, or
replace the post-implementation code-quality reviewer or fresh verifier.

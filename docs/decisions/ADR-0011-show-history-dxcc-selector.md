# ADR-0011 - SHOW DX / SHOW MYDX Optional DXCC Selector

Status: Accepted
Date: 2026-02-17
Decision Makers: Cluster maintainers
Technical Area: commands/processor
Tags: command-contract, cty, archive-history

## Context
- Operators wanted history queries scoped to a DXCC without running separate lookups and manual filtering.
- Existing `SHOW DX` / `SHOW MYDX` only accepted an optional count, so DXCC narrowing required external tooling.
- Command behavior must remain deterministic and backward compatible for existing clients and scripts.
- CTY portable lookup is already the canonical source for prefix/callsign -> ADIF resolution in this codebase.

## Decision
- Extend both commands with optional selector grammar:
  - `SHOW DX [count]`
  - `SHOW DX <prefix|callsign> [count]`
  - `SHOW DX [count] <prefix|callsign>`
  - Same forms for `SHOW MYDX` and `SHOW/DX` alias paths.
- Parse two-argument forms by token class:
  - exactly one numeric token (`count`) and one non-numeric token (`selector`) is valid.
  - both numeric or both non-numeric is invalid usage.
- Resolve selector with CTY portable lookup and filter archive results to `spot.DXMetadata.ADIF == resolvedADIF` in addition to existing client filter predicates.
- Preserve existing count bounds (`1-250`), archive-only behavior, output ordering, and output formatting.
- If selector is provided and CTY is unavailable, return explicit CTY availability errors.

## Alternatives Considered
1. Strict `selector count` only (single ordering)
   - Pros: simpler parser and lower ambiguity.
   - Cons: worse UX; less forgiving for operators.
2. Add a new explicit verb (`SHOW DXCCHIST <selector> [count]`)
   - Pros: unambiguous syntax and isolated behavior.
   - Cons: expands command surface and duplicates history logic.
3. Keep existing syntax and require filters only
   - Pros: no code changes.
   - Cons: does not satisfy operator workflow; adds friction.

## Consequences
- Positive outcomes:
  - Faster operator workflow for country-scoped history checks.
  - Backward-compatible extension of existing commands.
- Negative outcomes / risks:
  - Slightly more parsing complexity in history commands.
  - Selector behavior now depends on CTY availability for those query forms.
- Operational impact:
  - No new goroutines, queues, or background jobs.
  - No change to wire format or archive storage schema.
- Follow-up work required:
  - Keep help/docs synchronized if command grammar evolves again.

## Validation
- Added unit coverage for selector-only, both selector/count orders, CTY unavailable/unknown selector errors, and invalid two-argument forms.
- Existing count-only and archive-only history behavior remains covered by existing tests.
- Evidence that would invalidate this decision:
  - repeated operator confusion from mixed-order syntax; then move to explicit keyword-based syntax.

## Rollout and Reversal
- Rollout plan:
  - Ship as command parser + help/docs update with tests.
- Backward compatibility impact:
  - Fully backward compatible for existing `SHOW DX [count]` / `SHOW MYDX [count]`.
- Reversal plan:
  - Remove selector parsing branch and restore count-only grammar while keeping current tests for legacy behavior.

## References
- Issue(s): user-requested interactive command enhancement (session scope)
- PR(s): pending
- Commit(s): pending
- Related ADR(s): none
- Docs: `README.md`, `commands/processor.go`, `commands/processor_test.go`

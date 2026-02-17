# ADR-0012 - CC Dialect Accepts SHOW DX / SH DX History Aliases

Status: Accepted
Date: 2026-02-17
Decision Makers: Cluster maintainers
Technical Area: commands/processor
Tags: command-contract, dialect, usability

## Context
- CC dialect previously rejected `SHOW DX` / `SH DX` and required `SHOW/DX` / `SH/DX`.
- Operators requested accepting the spaced forms for better usability and fewer command-entry errors.
- The existing history handler already supports the same semantics regardless of alias form.

## Decision
- In CC dialect, accept `SHOW DX ...` and `SH DX ...` as aliases of `SHOW/DX ...`.
- Preserve all existing history semantics (count parsing, optional DXCC selector behavior, archive-only reads, output order/format).
- Keep `SHOW/DX` and `SH/DX` as canonical CC spellings in help text.

## Alternatives Considered
1. Keep strict CC-only slash syntax
   - Pros:
     - Exact dialect fidelity.
   - Cons:
     - Higher operator friction for a low-value restriction.
2. Fully collapse dialect distinctions for SHOW commands
   - Pros:
     - Maximum convenience.
   - Cons:
     - Dilutes dialect boundaries beyond the requested scope.
3. Emit warning but still reject spaced forms
   - Pros:
     - Preserves strictness while guiding users.
   - Cons:
     - Still blocks common user input and does not improve UX.

## Consequences
- Positive outcomes:
  - Better UX for CC users; fewer rejected commands.
  - No behavior divergence in output or filtering semantics.
- Negative outcomes / risks:
  - Slightly looser CC syntax contract than strict DXSpider style.
- Operational impact:
  - No additional resource usage; no new goroutines/channels/storage.
- Follow-up work required:
  - Keep help/topic normalization aligned with accepted aliases.

## Validation
- Updated parser path and help-topic normalization.
- Added/updated tests for CC alias acceptance and `HELP SHOW DX` mapping.
- Decision would be reconsidered if operators report confusion due to mixed CC/go spelling.

## Rollout and Reversal
- Rollout plan:
  - Ship parser + tests in one patch.
- Backward compatibility impact:
  - Backward compatible; only adds accepted aliases.
- Reversal plan:
  - Reinstate CC rejection branch for spaced forms and remove alias help mapping.

## References
- Issue(s): user-requested UX enhancement (session scope)
- PR(s): pending
- Commit(s): pending
- Related ADR(s): `docs/decisions/ADR-0011-show-history-dxcc-selector.md`
- Docs: `commands/processor.go`, `commands/processor_test.go`

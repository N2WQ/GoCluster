# ADR-0272: MODE Visibility Deltas

- Status: Accepted
- Date: 2026-10-09
- Decision Origin: Troubleshooting chat

## Context

HELP promises that PASS/REJECT MODE changes only named modes. The shared
allow/block toggle could instead replace an open selection with a whitelist
or reopen a whitelist when its last allowance was removed. The user selected
visibility-preserving deltas, explicitly including custom/unseen tokens and
future taxonomy fallback, in approved Scope Ledger v3 checkpoint 07/D01.

## Decision

Keep mutation in Filter.SetMode. Determine the actual prior open fallback:
BlockAllModes is false, AllModes is true, and the raw allow map is empty. A
nonempty map remains restrictive even if its entries are false or AllModes
is true. Normalize the MODE fallback flag to this effective state.

PASS removes the named deny. In an open filter it does not create an allowlist;
in a restrictive filter it enables that name. Under BlockAll it discards dormant
allow entries before enabling only the selected name. REJECT removes the named
allowance and adds its deny while preserving BlockAll and the fallback. Removing
the last restrictive allowance leaves an empty restrictive filter.

Use the existing matcher for unknown-mode feedback. MODE snapshots determine
fallback from raw map length, show true entries, apply denies and do not infer
ALL from coverage of the current taxonomy. Whole YAML/LOAD values, marked
persistence, schemas, curated defaults and legacy migration remain unchanged.
The generic toggle and all other finite families retain their transitions.

The existing client configuration owner and filter locks continue to own
publication. No process state, lifecycle, extra retained structure or hot-path
matcher change is introduced.

## Alternatives considered

1. Retain whitelist transitions and change HELP. Rejected by the user's policy
   selection; re-enabling FT8 must preserve previously visible CW.
2. Enumerate every supported mode when opening. Rejected because current
   vocabulary is not the universe of custom or future spot tokens.
3. Change the shared matcher or generic toggle. Rejected because that would
   reinterpret complete machine states and other filter families.

## Consequences

### Benefits

Named commands satisfy their stated delta contract across open, restrictive,
false-only, dormant and block-all states, including after marked reload.

### Risks

Scripts constructing a whitelist by passing names after an open reset must
explicitly block all modes first. Existing exclusive test fixtures require the
same deliberate setup rather than relaxed assertions.

### Operational impact

REJECT MODE ALL then PASS MODE CW,FT8 selects those modes. PASS MODE ALL opens
MODE explicitly. Other categories and exemptions still apply.

Documentation correction, 2026-10-10: the earlier opening recipe named
unsupported RESET MODE. The MODE visibility decision remains unchanged.

## Links

- Approved scope: [v3 ledger](../command-fix-scope-ledger-v3.md), 07/D01.
- Tests: `filter/mode_delta_test.go`, `telnet/mode_delta_test.go`,
  `scripts/test-command-fixes.py` and its offline controls.
- Docs: [telnet semantics](../../telnet/README.md#filters),
  [execution evidence](../command-fix-execution.md#checkpoint-07-mode-visibility-preserving-deltas).
- Related TSR: [TSR-0053](../troubleshooting/TSR-0053-mode-delta-whitelist-transition.md).
- Supersession: refines [ADR-0018](ADR-0018-filter-mutation-and-telnet-handler-consolidation.md)
  for MODE mutation only; other decisions retain their unchanged contracts.

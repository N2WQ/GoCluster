# ADR-0264: Task-Oriented Compiled HELP

- Status: Accepted
- Date: 2026-10-08
- Decision Origin: Design

## Context

The existing HELP overview mixed command summaries, machine YAML commands,
filter exceptions and scientific symbol descriptions. Generic PASS/REJECT
help did not give beginners enough concrete categories, values and examples.
The repository owner approved Scope Ledger v2 to improve both dialects while
keeping HELP compiled into Go.

## Decision

- Organize GO and CC overviews by user task, with filter category descriptions,
  individual examples and a complete BAND/MODE recipe.
- Use canonical CC spellings and retain existing aliases and shortcuts.
- Expand detailed allow/block help with examples for every category and accepted
  values, preserving existing usage and exceptions.
- Advertise HELP FILTERS for full filter reference and HELP SYMBOLS for the
  unchanged confidence/configured path legends.
- Omit machine YAML commands only from the overview; retain execution and
  detailed help.
- Keep help code-owned and retain configured values from startup snapshots.
- Preserve README synchronization and add reviewed per-dialect output fixtures,
  command coverage checks and executable recipe evidence.

## Alternatives considered

1. Retain the flat overview: leaves ordinary tasks and filtering hard to find.
2. Load editable help files: adds deployment/validation/reload decisions outside
   the owner's selected compiled-help scope.
3. Remove detailed reference material: loses operator information; separate
   advertised help topics preserve it.
4. Remove YAML commands: would change client compatibility beyond presentation.

## Consequences

### Benefits

- Users can identify filter categories and see what commands actually change.
- Both dialects retain help navigation and configured values.

### Risks

- The overview is longer because practical filter teaching takes priority.
- Examples and section inventories must remain aligned with executable commands;
  independently reviewed fixtures and command/recipe tests guard this boundary.

### Operational impact

HELP output and navigation change. Executable commands, filters, persistence,
pause policy and scientific semantics do not change. Editing help still requires
rebuilding the binary.

## Links

- Related approval: Scope Ledger v2, authorized with `Approved v2`.
- Related tests: `commands/help_overview_test.go`, `commands/readme_sync_test.go`,
  `telnet/help_recipe_test.go`, existing commands and telnet filter regressions.
- Related docs: [commands](../../commands/README.md),
  [operator guide](../OPERATOR_GUIDE.md), [README](../../README.md).
- Related decisions: [ADR-0012](ADR-0012-cc-show-dx-alias.md),
  [ADR-0089](ADR-0089-set-solar-help-routing.md).
- Related TSRs: none; no troubleshooting-origin change.
- Supersedes / superseded by: none; existing command semantics remain in force.

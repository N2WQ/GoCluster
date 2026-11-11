# ADR-0337: Short HELP And Task Guides

- Status: Accepted
- Date: 2026-11-10
- Decision Origin: Design

## Context

The task-oriented overview grew too dense for users seeking basic help.
The owner reviewed the complete replacement screen and authorized Scope Ledger
v1. The screen must use English, common words and short, active sentences.

## Decision

- Bare HELP and H show starter commands, short call-confidence descriptions,
  configured path symbols and task links. Omit the title and BYE from this screen.
- Add SPOTS, SETTINGS, PRESETS and TROUBLE guides, plus the COMMANDS index.
  Keep FILTERS, SYMBOLS and existing command-specific help reachable.
- Use fewer than 25 words per new description and fewer than 20 per instruction.
- Use active-dialect commands. Preserve distinct GO and native CC filter contracts.
- Keep detailed references and existing runtime symbol snapshots. Hide the path
  legend when display is disabled or its mapping is unavailable; describe a
  nonblank insufficient-data symbol explicitly.
- Derive the complete command-topic index from the finite installed catalog.
  Sort it for stable output. Retain aliases in command-specific help.
- Preserve all executable commands, filters, saved state, model meanings and
  automatic read-pause behavior. HELP remains compiled and request-owned.

## Alternatives considered

1. Shorten the old overview: leaves too many subjects on the first screen.
2. Show only an index: gives beginners no direct examples or tag meanings.
3. Remove detailed references: hides information needed for safe filter changes.

## Consequences

### Benefits

Users can scan the first screen, read common tags and choose a task.
The full command index keeps detailed topics discoverable.

### Risks

Detailed guidance takes another command. Task links and examples can drift;
independent navigation, exact-index, reviewed-screen and existing command tests
check that boundary. Automated tests do not establish usability in every client.

### Operational impact

The first HELP response changes. Existing regression scripts must recognize its
new first line. Automatic reading pauses still follow the existing row threshold.
No production deployment or live client qualification is established by this change.

## Links

- Authorization: Scope Ledger v1, authorized with `Approved v1`.
- Related tests: [HELP overview](../../commands/help_overview_test.go),
  [README synchronization](../../commands/readme_sync_test.go), existing HELP
  routing, filter-recipe and read-pause tests.
- Related docs: [commands](../../commands/README.md),
  [operator guide](../OPERATOR_GUIDE.md), [README](../../README.md).
- Related TSRs: [TSR-0065](../troubleshooting/TSR-0065-help-routing-alias-coverage.md)
  remains relevant to longest-command-head routing and alias coverage.
- Supersedes the landing-page presentation in
  [ADR-0264](ADR-0264-task-oriented-compiled-help.md); preserves its compiled
  ownership, detailed-help and executable-command contracts.

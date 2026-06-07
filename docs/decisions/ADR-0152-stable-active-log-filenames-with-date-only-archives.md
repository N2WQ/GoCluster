# ADR-0152 - Stable Active Log Filenames With Date-Only Archives

Status: Accepted
Date: 2026-06-07
Decision Makers: Founder, Codex
Technical Area: logging, propagation reports, operations
Decision Origin: Design
Troubleshooting Record(s): none
Tags: logging, operations, propagation, retention

## Context

Runtime file logs previously used the date-only filename as the active log file.
That made current-day paths change every UTC day, which is awkward for tailing,
watching, and operator automation.

The operator requirement is a stable current-day filename for every runtime log
defined by `data/config/app.yaml` logging settings, while preserving the
existing archive filename format. Completed archives must remain date-only names
such as `07-Jun-2026.log`, not log-name-prefixed names.

## Decision

Daily runtime file logs write to a stable active filename derived from the log
directory basename:

- `data/logs/system` writes active system events to `system.log`;
- `data/logs/propagation` writes active propagation aggregates to
  `propagation.log`;
- file-only event logs follow the same directory-basename rule, such as
  `login_attempts.log`;
- dropped-call category logs write active files in their category directories,
  such as `bad_de_dx/bad_de_dx.log`.

At UTC day rollover, the runtime closes the active file, archives it with the
existing date-only filename format for the completed day, and reopens the active
path before writing new-day events. The active file must not mix old-day and
new-day events.

If the date-only archive already exists, active contents are appended to that
archive instead of overwriting it. This preserves data during transition or
restart edge cases while still clearing the active file before new-day writes.

Startup handles transition files conservatively:

- if a stale active file exists, archive it before appending current-day data;
- if no active file exists but a current-day legacy `DD-Mon-YYYY.log` file
  exists, adopt it as the active file;
- legacy date-only archive files remain parseable for retention cleanup.

Propagation report scheduling continues to read the date-only archive path for
the completed report date. Manual `prop_report -log` remains available for
explicit active, archive, or historical system-log paths.

The config keys remain unchanged. `dir` remains the operator-owned routing
boundary; no new YAML filename key is added.

## Alternatives Considered

1. Keep date-named active files.
   - Pros: no code or documentation change.
   - Cons: fails the operator requirement for permanent current file names.
2. Archive as `<name>_<date>.log`.
   - Pros: globally unique archive names when directories are flattened.
   - Cons: rejects the requirement to preserve the current archive filename
     format.
3. Add a YAML filename key for every log block.
   - Pros: maximum operator control.
   - Cons: expands the config contract for a deterministic naming rule already
     implied by the log directory.
4. Bulk-rename historical archives.
   - Pros: fully normalizes old directories.
   - Cons: adds destructive operational risk and is unnecessary for current
     runtime behavior.

## Consequences

- Positive outcomes:
  - Operators can tail permanent active paths such as `system.log`.
  - Completed logs keep the existing `DD-Mon-YYYY.log` format.
  - Propagation reports continue to read completed date-only archives.
  - Retention cleanup continues to apply to date-only archives and ignores
    active files.
- Negative outcomes / risks:
  - Custom `dir` basenames define active filenames; unusual directory names
    produce matching active names.
  - Automation that assumed the active file was date-named must switch to the
    stable active name.
  - Archive collision handling appends rather than deduplicates; this preserves
    data but may duplicate lines if an operator manually copies files.
- Operational impact:
  - Current-day paths are stable inside each configured log directory.
  - Completed UTC days retain their existing date-only archive names.
  - New-day writes occur only after the active path has been cleared/reopened.
- Follow-up work required:
  - None for the logging settings in `app.yaml` lines 51-92.

## Validation

Required validation includes:

- active/archive naming helper tests;
- daily file sink tests for stable active writes, rollover clearing, stale
  active startup archiving, current-day legacy adoption, archive collision
  preservation, retention cleanup, and rotate hook paths;
- dropped-call, file-only event, propagation scheduler, and report default
  tests;
- YAML comment/header audit because checked-in config comments changed;
- Go crawler-entry comment audit because support-critical Go changed;
- full repo tests, race check, vet, staticcheck, and golangci-lint.

This decision should be revisited if operators need custom active filenames
that cannot be represented safely by directory basenames.

## Rollout and Reversal

- Rollout plan:
  - Ship stable active filenames with date-only archives, updated docs, and
    tests.
- Backward compatibility impact:
  - Active paths change from date-only files to stable `.log` files.
  - Historical date-only archives remain in the same format and are still
    accepted by report tooling through explicit `-log` paths.
- Reversal plan:
  - Revert the naming helper changes, daily sink rollover path, tests, config
    comments, support docs, and this ADR.

## References

- Issue(s): none
- PR(s): pending
- Commit(s): pending
- Related ADR(s): ADR-0093, ADR-0123
- Troubleshooting Record(s): none
- Docs: `README.md`, `docs/OPERATOR_GUIDE.md`, `data/config/README.md`,
  `docs/call-correction-phase2-shadow-runbook.md`

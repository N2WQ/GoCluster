# TSR-0071 - Root Executable Argument Admission

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: root executable, command-line arguments and startup boundary
Trigger Source: Chat request
Led To ADR(s): none
Tags: CLI, help, startup, version, exit-status

## RCA Summary
- What happened: `--help` and unknown arguments entered normal server startup
  instead of showing help or refusing an invalid invocation.
- Why: root main handled version tokens, then unconditionally delegated to
  cluster.Run; all remaining arguments were ignored.
- What fixed it: validate all remaining arguments after the existing version
  check. Help returns before Run; unknown arguments print usage and exit 2.
- How we know: actual-main child fixtures verify exact statuses, output streams
  and no files in an empty working directory; a no-argument startup-failure
  witness verifies the fixture really exercises production startup. Qualification
  results are in the checkpoint 22 execution entry.
- Operator/support answer: use `--help` or `--version` to inspect the executable,
  and run with no arguments to start the configured node. A version token
  anywhere deliberately overrides every other argument, as before.

## Triggering Request
- Request date: 2026-10-10
- Request summary: implement approved v3 root help/version/status checkpoint 22.
- Request reference: `docs/command-fix-scope-ledger-v3.md`.

## Symptoms and Impact
- Asking for help could start listeners, feeds and stores when valid config
  existed, or produce an unrelated startup config failure when it did not.
- Unknown flags and positional values failed to identify invocation mistakes.
- Version metadata and no-argument startup were already separate contracts.

## Hypotheses and Tests
1. Unit tests of an argument predicate prove no startup occurs.
   - Evidence: actual production main runs in joined isolated child processes;
     ordinary no-argument invocation returns the current config failure.
   - Outcome: predicate tests alone are insufficient.
2. An absent override always excludes public configuration.
   - Evidence: source contains conditional relative fallback; fixtures exclude
     both paths with an empty cwd. Observed missing-path failure is reported
     directly as `failed to stat config path`, not the guessed final-fallback
     diagnostic.
   - Outcome: use actual isolation and startup witness rather than assumption.
3. Help may return before checking all other arguments.
   - Evidence: unknown-before/after-help controls require status 2; only existing
     version-token-anywhere precedence permits a successful override.
   - Outcome: validate the complete non-version invocation first.

## Findings
- Root cause: missing command-line admission before runtime delegation.
- Conventional help flags are `-h`, `-help`, `--help`. Positional config paths
  are not supported; configuration ownership remains DXC_CONFIG_PATH/default.
- Help output failure returns unsuccessful status without starting runtime.
- No cluster.Run, build/version, config loader or process lifecycle changes.

## Decision Linkage
- ADR created/updated: none. This implements the accepted v3 invocation boundary
  and preserves build/version decisions; no new runtime architecture is chosen.
- Contract changes: supported help exits before startup; unknown arguments
  return 2. Existing version precedence and ordinary startup remain intact.

## Verification and Monitoring
- Validation steps: see checkpoint 22 in `docs/command-fix-execution.md` for
  actual-main controls, bounded fuzzing, baseline/analyzers and final checks.
- Signals to monitor: help/version output, CLI diagnostic/status and startup
  errors when invoked without arguments.
- Rollback triggers: help starts runtime, unknown arguments silently succeed,
  version precedence changes or normal startup becomes inaccessible.
- Limits: successful help/version does not prove production configuration,
  service readiness, network connectivity or binary deployment identity.

## References
- Issue(s): approved v3 checkpoint 22.
- PR(s): none.
- Commit(s): checkpoint 22 on `fix_commands`; see execution history.
- Related ADR(s): [ADR-0236](../decisions/ADR-0236-date-only-build-version.md),
  [ADR-0252](../decisions/ADR-0252-automatic-commit-suffix-release-tags.md).
- Related docs: `README.md`, `customgpt/support-cards/windows-startup-config.md`.

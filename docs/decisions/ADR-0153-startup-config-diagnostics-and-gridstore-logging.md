# ADR-0153 - Startup Config Diagnostics And Gridstore Logging

Status: Accepted
Date: 2026-06-07
Decision Makers: Founder, Codex
Technical Area: config, startup, pathreliability, gridstore, operations
Decision Origin: Design
Troubleshooting Record(s): none
Tags: config, startup, diagnostics, h3, gridstore, support

## Context

ADR-0067 made YAML-owned settings strict so runtime behavior could not fall
through to hidden Go defaults. That protected correctness, but the fail-fast
shape forced operators and support agents to fix one missing file or key at a
time. It also treated ordinary extra keys the same as fatal omissions, which
was too harsh for harmless stale comments or future-private notes in an
operator's config directory.

Path reliability depends on H3 table coverage. If those tables are missing or
wrong-sized, path predictions cannot be trusted. Gridstore startup failures also
need to be visible in the same operator-facing startup log path as config
failures so support can distinguish corruption recovery from a non-recoverable
open failure.

## Decision

Startup config loading now accumulates diagnostics across the required server
startup config set:

- required registered YAML files are checked before the loader aborts;
- required YAML-owned keys are checked as missing/null diagnostics rather than
  stopping on the first missing key;
- `path_reliability.yaml`, `solarweather.yaml`, and required reference tables
  participate in the same startup diagnostic reporting;
- ordinary extra YAML keys are warning-only diagnostics and are ignored;
- known removed migration keys remain fatal because silently ignoring them can
  mislead operators about active behavior.

The system log is the operator-facing sink for these diagnostics once logging is
configured. If config loading fails before the configured system log can be
opened, startup writes an explicit fallback message and the diagnostics to the
default process logger.

When `path_reliability.enabled` is true, startup validates both required H3
mapping tables, `res1.bin` and `res2.bin`, before initializing the path
predictor. Missing, malformed, or wrong-sized H3 tables fail startup and list
all table errors found.

Gridstore startup open errors are logged through the startup logger:

- Pebble corruption opens the checkpoint-restore path and the process runs
  temporarily without grid persistence while recovery proceeds;
- non-corruption open failures abort startup and return a startup error to the
  process entrypoint.

## Alternatives Considered

1. Keep strict fail-fast config loading.
   - Pros: smallest code change and preserves the original ADR-0067 behavior.
   - Cons: poor operator experience because one missing key masks the rest of
     the missing startup surface.
2. Downgrade all unknown or removed YAML keys to warnings.
   - Pros: maximizes launch tolerance.
   - Cons: unsafe for removed semantic keys, where an operator may believe a
     deleted knob still controls runtime behavior.
3. Keep H3 table failures as path `INSUFFICIENT` degradation.
   - Pros: keeps the process online even when path reliability is broken.
   - Cons: path predictions depend on H3 cells; silently degrading startup
     hides a critical deployment error.

## Consequences

- Positive outcomes:
  - Operators get one startup diagnostic set for missing required config files
    and keys.
  - Harmless extra keys no longer block startup, but are still visible in the
    system log for cleanup.
  - H3 table problems are explicit startup failures when path reliability is
    enabled.
  - Gridstore open failures are observable and return a non-success startup
    result instead of ending quietly.
- Negative outcomes / risks:
  - Private configs with extra typo keys now launch; the warning must be
    reviewed because the key is ignored.
  - The loader has a more complex diagnostic path than strict `KnownFields`.
  - Support must distinguish warning-only extra keys from fatal removed keys.
- Operational impact:
  - Complete valid configs continue to launch.
  - Missing required startup YAML files or keys abort after the diagnostic walk.
  - Extra YAML keys log `Config warning` messages and do not block launch.
  - Removed archive, PSKReporter, and path-reliability migration keys still
    block launch with migration guidance.
- Follow-up work required:
  - None required for this decision.

## Validation

- Targeted config, feature-root, reference-table, path reliability, and cluster
  tests cover warning-only extras, aggregated missing diagnostics, H3 table
  validation, and gridstore startup open failure logging.
- Full validation remains the closeout source for the implementation task.
- This decision would be invalidated if warning-only extra keys cause real
  production misconfiguration that operators cannot detect from system logs, or
  if a future path predictor can operate safely without complete H3 tables.

## Rollout and Reversal

- Rollout plan:
  - Deploy binary and complete config directory together.
  - Review startup system-log warnings after first launch.
- Backward compatibility impact:
  - Unknown extra keys change from fatal to warning-only.
  - Missing required files/keys remain fatal but are reported in aggregate.
  - Removed migration keys remain fatal.
- Reversal plan:
  - Re-enable strict unknown-field decode in startup loaders and remove the
    warning-only diagnostic path if launch tolerance proves unsafe.

## References

- Issue(s): none
- PR(s): none
- Commit(s): pending
- Related ADR(s): ADR-0067, ADR-0128, ADR-0146, ADR-0151, ADR-0152
- Troubleshooting Record(s): none
- Docs: `data/config/README.md`, `docs/OPERATOR_GUIDE.md`,
  `customgpt/troubleshooting-index.md`, `pathreliability/README.md`

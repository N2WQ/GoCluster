# TSR-0070 - Toxicity Positive Duration Admission

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: optional toxicity YAML, numeric units and runtime consumers
Trigger Source: Chat request
Led To ADR(s): ADR-0276
Tags: toxicity, configuration, defaults, duration, startup

## RCA Summary
- What happened: a positive YAML timeout above the raw consumer ceiling could
  become the default timeout; a positive TTL could overflow conversion to a Go
  duration and disable or misconfigure the cache.
- Why: the optional loader normalized zeros and refused negatives, but did not
  validate these downstream numeric-unit boundaries before construction.
- What fixed it: loader admission checks the existing timeout ceiling and whole
  seconds duration bound, while preserving selected explicit-zero defaults.
- How we know: source/caller review identifies both conversions; literal loader
  boundary, effective deadline/cache expiry and actual initializer controls
  distinguish admitted values from fallback or feature disablement. Qualification
  results are recorded in the checkpoint 21 execution entry.
- Operator/support answer: correct the named numeric key in the startup error.
  Optional absence disables the feature; omission/null in a present file is an
  error. Explicit zero requests the documented default, not disablement.

## Triggering Request
- Request date: 2026-10-10
- Request summary: implement approved v3 toxicity zero/duration checkpoint.
- Request reference: `docs/command-fix-scope-ledger-v3.md`, checkpoint 21.

## Symptoms and Impact
- Effective timeout can differ from a positive configured value.
- Overflowed cache TTL can invalidate the intended bounded cache lifetime.
- Numeric admission applies even when a present feature file is disabled.

## Timeline
1. Inspected strict optional loader, raw constructors, startup caller and ADRs.
2. Preserved explicit user zero policy and scoped two duration admission guards.
3. Added loader, consumer and actual runtime initializer regression controls.

## Hypotheses and Tests
1. Positive integer parsing is enough to guarantee duration semantics.
   - Evidence: timeout fallback and seconds-to-nanoseconds multiplication in
     current consumers impose narrower boundaries than a native 64-bit integer.
   - Outcome: rejected; validate before construction/conversion.
2. Loader snapshots alone prove configuration ownership.
   - Evidence: tests observe actual request deadline/payload, cache exact expiry,
     queue capacities and production initializer activation/terminal cleanup.
   - Outcome: rejected; consumer evidence is required.
3. Disabled present config can skip required keys or safe-gate semantics.
   - Evidence: missing/null/negative keys fail regardless of enabled state;
     missing/malformed safe-gate files matter only when classification is enabled.
   - Outcome: separate the optional feature boundary from numeric admission.

## Findings
- Root cause: positive-unit conversion and raw constructor fallback were not
  checked at the authoritative YAML load boundary.
- The timeout ceiling is an existing consumer contract, not the maximum Go
  duration. TTL uses a different seconds conversion and therefore a different
  limit. These bounds are not recommended operational settings.
- Fractional yaml.v3 scalar conversion and raw constructor malformed TTL
  overflow are not changed by this patch.

## Decision Linkage
- ADR created/updated: [ADR-0276](../decisions/ADR-0276-toxicity-config-zero-and-duration-admission.md).
- Decision delta: document the narrow zero-default exception to ADR-0067 and
  reject unsupported positive duration settings before runtime construction.
- Contract changes: unsupported positive YAML durations fail startup visibly;
  selected zero defaults, raw constructors and optional absence remain intact.

## Verification and Monitoring
- Validation steps: see checkpoint 21 in `docs/command-fix-execution.md` for
  observed tests, analyzer results and cumulative command reconciliation.
- Signals to monitor: named-key startup errors and effective classifier settings.
- Rollback triggers: complete valid YAML refused, zero defaults changed, omitted
  optional config made required or new consumer fallback after successful load.
- Limits: local fixtures do not prove external Worker health or model accuracy.

## References
- Issue(s): approved v3 checkpoint 21.
- PR(s): none.
- Commit(s): checkpoint 21 on `fix_commands`; see execution history.
- Related ADR(s): [ADR-0067](../decisions/ADR-0067-centralized-yaml-settings-enforcement.md),
  [ADR-0114](../decisions/ADR-0114-human-comment-toxicity-classifier.md).
- Related docs: `data/config/README.md`, `docs/command-fix-execution.md`.

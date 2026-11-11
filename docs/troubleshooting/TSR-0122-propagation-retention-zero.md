# TSR-0122 - Propagation Retention Zero

Status: Monitoring
Date Opened: 2026-11-10
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: runtime configuration and propagation logging
Trigger Source: Chat request
Led To ADR(s): none
Tags: config, zero sentinel, inheritance, logging

## RCA Summary
- What happened: Configuring propagation retention as zero failed startup even
  though the operator documentation promised inheritance from parent logging.
- Why: Generic validation ran before normalization and omitted propagation
  retention from its allowed-zero settings.
- What fixed it: The documented propagation key is now allowed through that
  validation. The existing inheritance normalizer and destination consumer are
  unchanged.
- How we know: Full-loader regressions rejected zero before the change. The
  corrected loader produces parent retention 13 for child zero with propagation
  enabled or disabled; a consumer regression receives 13 in the actual daily
  destination. Full config ordinary/race suites passed 627 cases each; the five
  destination cases passed ordinary/race.
- Operator/support answer: `logging.propagation.retention_days: 0` inherits
  `logging.retention_days`. Explicit positive values and propagation enablement
  keep their existing meaning; no new retention default is introduced.

## Triggering Request
- Request date: 2026-11-10.
- Request summary: Reconcile the reviewed zero-sentinel contradiction.
- Request reference: Exact `Approved v1` overnight scope ledger.

## Symptoms and Impact
- The loader reported that `logging.propagation.retention_days` must exceed
  zero, so the existing normalizer never received this documented value.
- Tests using shipped retention seven could accidentally accept a hardcoded
  fallback instead of proving inheritance.

## Timeline
1. 2026-11-10 - Documentation, validation order and normalizer were reconciled.
2. 2026-11-10 - Enabled/disabled full-load tests reproduced zero rejection.
3. 2026-11-10 - The narrow allowlist repair passed owner and consumer checks.

## Hypotheses and Tests
1. The existing normalizer already makes explicit zero loadable.
   - Evidence: Both new full-loader cases failed before normalization.
   - Outcome: Rejected. Earlier generic validation blocks the documented value.
2. Shipped retention seven proves inheritance.
   - Evidence: Both the parent default and fallback can be seven.
   - Outcome: Rejected. A distinctive parent 13 and literal consumer expectation
     distinguish inheritance from fallback.

## Findings
- Root cause: Validation and normalization disagreed about one sentinel.
- Contributing factor: Prior tests covered positive, negative and shipped
  retention, but not explicit zero with a distinctive parent value.
- Durable decision: No new policy; existing documented inheritance is restored.

## Decision Linkage
- ADR created/updated: none.
- Decision delta summary: No new default, schema, path or logging policy.
- Contract/behavior changes: Explicit zero now reaches its existing normalizer.

## Verification and Monitoring
- Validation steps run: Config ordinary/race 627 cases each, destination
  ordinary/race five cases each, zero skips; raw logs are in the overnight record.
- Signals to inspect: Exact YAML key, parent retention and selected propagation
  enablement. Other invalid/missing/null settings remain under loader validation.
- Rollback triggers: Changed inherited value, accepted invalid retention,
  changed enablement, or consumer retention differing from the loaded config.

## References
- Operator contract: `data/config/README.md`, propagation logs.
- Source: `config/config_files.go`, `config/config.go`,
  `internal/cluster/unified_log_destinations.go`.
- Tests: `config/logging_dedupe_test.go`,
  `internal/cluster/unified_log_destinations_test.go`.
- Execution: `docs/codebase-correctness-overnight-2026-11-10.md`.

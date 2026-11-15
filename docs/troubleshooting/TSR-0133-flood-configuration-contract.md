# TSR-0133 - Flood Configuration Contract

Status: Monitoring
Date Opened: 2026-11-14
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: Startup flood configuration and shared-ingest policy consumption
Trigger Source: Chat request
Led To ADR(s): none
Tags: flood, YAML, source keys, scalar validation, arithmetic

## RCA Summary

- What happened: Source-key validation accepted noncanonical spellings that
  runtime lookup could miss. YAML numeric coercion could also truncate a
  fractional value or turn null into the threshold's zero-disable sentinel.
- Why: Validation compared normalized source names without publishing canonical
  map keys. Typed decoding occurred before checking the original scalar type,
  and accepted integers lacked guards for later duration and count arithmetic.
- What fixed it: The startup loader checks the final merged raw YAML, publishes
  canonical threshold maps and rejects unsafe arithmetic before construction.
  All six source tables remain required, including inactive or disabled ones.
- How we know: Pre-repair loader regressions exposed the source-key,
  fractional-value and null-value failures. Post-repair tests verify the real
  loader-to-controller boundary, scalar types, arithmetic limits and sentinels.
  See Verification and Monitoring for executed checks. No deployment or
  real-traffic suitability is established by these tests.
- Operator/support answer: Review the private policy before restarting. A
  previously ineffective noncanonical `suppress` or `drop` threshold can now
  enforce. Shipped uppercase keys, observe actions and numeric values remain
  unchanged. Malformed private values must be corrected before startup.

## Triggering Request

- Request date: 2026-11-14.
- Request summary: Validate the external flood-control handoff, then repair the
  bounded startup configuration defects separately from broader flood policy.
- Request reference: Owner's exact `Approved v1` in the 2026-11-14 chat; the
  implementation uses the isolated `codex/flood-config` worktree.

## Symptoms and Impact

A lowercase or whitespace-padded source key could satisfy validation while
runtime indexing by canonical source type returned no threshold. The policy
could therefore forward traffic despite a configured suppress/drop action.
A fractional threshold could be decoded as an integer; a null threshold could
become zero and disable that source's rail. No production incidence is claimed.

Oversized accepted values could overflow seconds-to-duration conversion,
threshold-plus-timestamp arithmetic or the combined actor cardinality reported
by the controller. Representation safety does not prove that a configuration
fits available memory or processing capacity.

## Timeline

1. 2026-11-14 - Current source review separated startup defects from the
   handoff's unresolved policy and experimental recommendations.
2. 2026-11-14 - The owner authorized the configuration-only repair in an
   isolated worktree. Loader regressions reproduced noncanonical keys and
   lossy fractional/null handling before production code was changed.
3. 2026-11-14 - The repaired loader passed the focused ordinary/race checks,
   actual controller enforcement tests and bounded raw-contract fuzzing.

## Hypotheses and Tests

1. Accepting a normalized source spelling also makes runtime lookup equivalent.
   - Evidence: Pre-repair source-key reproduction failed. The loader now
     publishes uppercase, trimmed maps; consumer tests load the real directory
     contract before checking excess PEER suppression.
   - Outcome: Rejected for the old implementation.
2. Typed integer decoding rejects a fractional or null threshold safely.
   - Evidence: Pre-repair fractional/null reproductions failed. Raw scalar
     checks now require actual YAML integers before typed decoding.
   - Outcome: Rejected. Null must not become an implicit disable sentinel.
3. Disabled rails and inactive DX profiles can skip validation.
   - Evidence: They remain startup-owned required policy. Tests cover all six
     tables and preserve explicit zero without disabling another source.
   - Outcome: Rejected. Disabled settings remain valid for a later restart.
4. An accepted native integer is safe for every controller operation.
   - Evidence: Seconds become nanosecond durations; thresholds retain one extra
     timestamp and temporarily append another; actor capacities are summed.
   - Outcome: Rejected. Representation guards are required at startup.

## Findings

- Root cause: Source validation normalized names only for comparison, while
  runtime used the original map keys. Numeric checks followed lossy decoding
  and did not guard the downstream duration and count arithmetic.
- Source keys are trimmed and uppercased only in loaded threshold maps. After
  the existing exact-key merge, canonical collisions are rejected; an exact-key
  overlay retains its existing behavior. Configuration files are not rewritten.
- Numeric nulls, fractional/float-form scalars, quoted numbers and booleans are
  invalid. Every table must contain each supported source exactly once after
  normalization. Threshold zero retains its per-source disable meaning.
- Log/window seconds must be positive and at most
  `min(native MaxInt, 9223372036)`. Thresholds must be in
  `0..native MaxInt-2`. Actor caps must be positive native integers. With the
  controller enabled, combined capacities of enabled, positive-threshold source
  partitions must fit native `int`; the selected DX profile supplies its count.
- These are arithmetic guards, not practical CPU, retained-memory or process
  ceilings. They neither tune rates nor change source classification, charging,
  relay consequences or direct-constructor guarantees.

## Decision Linkage

- ADR created/updated: none; preserves ADR-0062's required valid startup policy.
- Decision delta summary: Repair validation and effective key consistency;
  preserve shipped policy values and existing disable semantics.
- Contract/behavior changes: Invalid or unrepresentable private values fail
  startup. Accepted noncanonical source keys now govern runtime traffic.

## Verification and Monitoring

- Validation evidence: In the isolated worktree, the complete native Windows
  runner passed 13,465 cases across 59 executed and 15 build-only packages.
  Its 97 skips include helper entry points, opt-in campaigns/profiles,
  external reference or sample prerequisites, and Windows H3 limitations;
  those skipped checks are not qualification evidence. The focused runner with
  `-Packages ./config,./floodcontrol -Race -Timeout 180s` passed 922 cases,
  zero skips. Repository-wide vet, Staticcheck and golangci-lint passed, with
  affected-package vet/Staticcheck rerun after the final review refinement.
  The saved, instrumented config binary ran `FuzzFloodRawContract` with two
  workers for 20 seconds and passed 17,226 executions. Fixtures cover
  canonical maps, collisions after merge, exact-key overlays, scalar types,
  inactive tables, arithmetic limits, unchanged configuration files and
  loaded controller enforcement. YAML comment/value rigor, troubleshooting
  records and local support-agent checks passed. These local checks do not
  establish hosted CI, Linux, live deployment or suitable operational thresholds.
- Isolated-fixture prerequisite: The first full-lane attempt failed
  `TestGeographyStartup/prediction_off` because ignored call-correction
  reference inputs were absent from the fresh worktree. Four configured
  reliability/model files were copied into the isolated ignored fixture
  directory with matching source-before, destination and source-after SHA-256
  hashes. The unchanged `TestGeographyStartup` then passed all eight cases,
  zero skips, including the explicitly required `prediction_off` case. No
  test assertions or application defaults were weakened.
- Signals to monitor: Startup key/path diagnostics, configured actions,
  flood decision counts and overflow events after an intentional restart.
- Reassessment triggers: A valid existing zero/false setting is lost, exact-key
  merge precedence changes, unrelated source allowances change, or observed
  enforcement differs from the reviewed private policy. A rollback executable
  can restore the old validation defects and is not a substitute for correcting
  the private configuration.

## References

- Source: `config/flood_control_contract.go`, `config/flood_control.go`.
- Tests: `config/flood_control_contract_test.go`,
  `floodcontrol/config_contract_test.go`.
- Existing decision: [ADR-0062](../decisions/ADR-0062-shared-ingest-flood-control.md).
- Related troubleshooting: [TSR-0120](TSR-0120-flood-cleanup-live-history.md).
- Operator documentation: [Flood configuration](../../data/config/README.md#flood-configuration).

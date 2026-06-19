# ADR-0200: VOACAP Unsupported-Platform Startup Skip

- Status: Accepted
- Date: 2026-06-19
- Decision Origin: Design

## Context

The production VOACAP fallback launches the Windows VOACAP engine through
`Voacapw.exe`. The checked-in path-reliability config enables the fallback, so
Linux builds could fail startup during VOACAP runner validation even though the
rest of the cluster can run safely without VOACAP-backed forecasts.

Linux operators need the cluster to start with ordinary path reliability,
native 160m fallback, ingest, telnet, and service diagnostics even when runtime
VOACAP execution is unavailable. At the same time, Windows operators who enable
VOACAP should still get fail-fast validation if the configured VOACAP install is
missing or malformed.

## Decision

Keep `path_reliability.voacap_fallback.enabled` as the operator-owned setting,
but gate runtime VOACAP startup by platform.

When VOACAP fallback is enabled:

- Windows keeps the existing behavior: startup validates the configured VOACAP
  home, restores SSN state, constructs the fallback worker, opens/restores the
  forecast cache, and starts the SSN monitor and worker during background
  service startup.
- Linux and other non-Windows builds log that runtime VOACAP execution is
  supported only on Windows, skip VOACAP validation, and leave the fallback
  provider absent. The cluster continues startup.

The skip does not add a fake VOACAP provider, does not change YAML schema or
defaults, and does not change VOACAP cache keys, SSN EWMA, forecast parsing,
REL gates, p50 scoring, native 160m fallback, or propagation counter schemas.
Existing nil-provider behavior remains the operator surface: `SHOW PROP`
reports that VOACAP fallback is disabled, sparse/no-p50 diagnostics can show
`vdis`, and Overview VOACAP fields show `n/a`.

## Alternatives considered

1. Keep failing startup on Linux when VOACAP is enabled.
   - Rejected because it makes the default operator config hostile to Linux
     service operation even though VOACAP is an optional fallback.
2. Change the checked-in YAML default to disable VOACAP.
   - Rejected because it would reduce Windows operator functionality and treat a
     platform limitation as a global policy change.
3. Add a new YAML knob for unsupported-platform policy.
   - Rejected for this slice because there is only one supported runtime
     engine path today; a new operator policy would add configuration surface
     without enabling Linux VOACAP execution.
4. Move the platform check into `internal/voacap.Runner.Validate`.
   - Rejected because `Validate` should remain the hard proof that a configured
     runner can execute when the runtime has decided to enable VOACAP.

## Consequences

### Benefits

- Linux operators can launch the cluster from the checked-in config without
  installing a Windows VOACAP engine.
- Windows keeps fail-fast validation for missing or broken VOACAP installs.
- Existing disabled-provider telnet, dashboard, and diagnostic behavior is
  reused instead of introducing a second degraded-mode implementation.

### Risks

- Operators may see `voacap_fallback.enabled: true` in YAML and expect Linux
  VOACAP predictions. Startup logs, operator docs, and support cards must make
  the platform skip explicit.
- Linux `SHOW PROP` and VOACAP-backed path glyphs are unavailable until a
  supported Linux execution path is added by a later decision.

### Operational impact

- On Linux and other non-Windows builds, startup logs one VOACAP skip message
  and continues.
- `VOACAP SSN` and `VOACAP cache` Overview values stay `n/a` because no provider
  is started.
- `SHOW PROP` reports that VOACAP fallback is disabled.
- Native 160m fallback and ordinary p50 path predictions continue to run when
  their own prerequisites are satisfied.

## Links

- Related issues/PRs/commits: none
- Related tests: `internal/cluster/main_runtime_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/README.md`,
  `customgpt/support-cards/linux-service-startup.md`,
  `customgpt/support-cards/path-reliability.md`,
  `customgpt/troubleshooting-index.md`
- Related ADRs: ADR-0161, ADR-0172, ADR-0182, ADR-0184, ADR-0190, ADR-0191
- Related TSRs: none
- Supersedes / superseded by: none

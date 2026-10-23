---
title: "TSR-0109: YAML Rule Edits Lose Partial NEARBY Geometry"
description: Preserve exact existing NEARBY cells when a YAML rule edit keeps GRID unchanged.
---

Status: Resolved
Date Opened: 2026-10-22
Date Resolved: 2026-10-22
Owner: Repository maintainer
Technical Area: telnet YAML configuration preparation and NEARBY matching
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

- A successful YAML rule edit could discard an existing usable NEARBY cell.
- Preparation returned before transferring cells when either cell was unavailable.
- The repair copies the existing pair into the detached candidate before that return.
- Regression tests fail before the repair and pass afterward for both partial states.
- This prevents future cell loss; it does not recover cells already lost in a session.

## Triggering Request

The owner requested an independent Boolean-filter review on 2026-10-22.
The review confirmed cell loss through PUT/PATCH FILTER for fine-only and
coarse-only geometry. Scope Ledger v2 subsequently authorized this repair.

## Symptoms and Impact

A rule-only update succeeded but changed partially usable NEARBY geometry to
fully unavailable geometry. Previously eligible ordinary spots stopped passing.
Fine-only geometry affected higher bands such as 20m. Coarse-only geometry
affected 160m, 80m and 60m. Saved GRID and rule expressions remained intact.

## Hypotheses and Tests

1. Boolean rule evaluation rejected the spot after the edit.
   - Rejected as the cause: broadening CW to CW or RTTY still lost delivery.
   - The rule change remained valid; the runtime cell pair changed instead.
2. Configuration preparation discarded the usable cell.
   - Supported: before the repair, both partial preparation cases failed.
   - All eight partial-state PUT/PATCH FILTER/CONFIG regression cases failed.
   - Both-usable and neither-usable preparation controls passed.
3. Path caches could safely replace the missing NEARBY state.
   - Rejected: these are separate runtime inputs.
   - Tests deliberately use different path and NEARBY cells.

## Root cause or best current explanation

`Configuration.FilterValue` excludes runtime cells. `prepareUpdatedNearby`
selected the prior NEARBY pair for an already-enabled, unchanged-GRID update,
but returned on partial availability before assigning either cell.
The permitted rule-only recovery path then persisted the rule change and
published the candidate with both cells unavailable.

Preserve the existing pair in that branch before the availability return.
Use `UpdateNearbyUserCells` on the detached filter. Leave `EnableNearby`
unchanged: new enabling and changed GRID still require usable geometry.
Do not change machine admission, Boolean projection, SOURCE policy, persistence,
or locking.

## Decision Linkage

No contract changes and no new ADR. This restores the partial-geometry
preservation required by
[ADR-0317](../decisions/ADR-0317-boolean-filter-replacement-mvp.md).

## Verification And Monitoring

The focused native Windows command was:

```powershell
& .\scripts\test-windows.ps1 -Packages @('./telnet') -Run 'TestPrepareNearby|TestPublish|TestV5MachineCoordinator|TestGrid|TestMatchingSnapshot|TestHistoryFilterSnapshot|TestPreset|TestBooleanActivation' -Timeout 180s
```

After the repair, all 111 selected cases passed with no skips. New cases
cover all four availability states, independent caches, and actual YAML
PUT/PATCH FILTER/CONFIG updates for both partial states. Matching probes prove
working-band CW remains available, newly allowed RTTY becomes eligible, and
the band needing the unavailable cell still rejects.

Existing tests retain coverage of enabling/GRID admission, refusal and
persistence failures, VALIDATE, and snapshot ownership. This evidence does not
qualify production load or geographic model accuracy.

Final validation on the repaired state:

- `& .\scripts\test-windows.ps1`: 71 packages, 11,085 passed cases,
  99 skipped cases, and 16 build-only packages.
- `& .\scripts\test-windows.ps1 -Packages @('./telnet','./filter') -Race -Timeout 240s`:
  2,717 passed cases and 3 skipped opt-in/driver-controlled cases; no race reports.
- `go vet ./...`, `staticcheck ./...`, and
  `golangci-lint run ./... --config=.golangci.yaml`: passed.
- Troubleshooting-record checks, code-map generation/freshness checks, and
  `git diff --check`: passed.

Skipped cases are not qualification evidence. The race skips were
`TestPathFirstWaveComparison`, `TestPathShardingLoopbackGlyphs`, and
`TestPSKRZeroSplitWireReplay`. The full run includes separate helper, platform,
and opt-in skips. No new preservation regression was skipped.

If a running session already lost its cells, use the existing GRID/NEARBY
recovery procedures. Do not weaken personal rules to compensate. A recurrence
is an accepted rule-only edit changing the reported cell availability.
Reverting the repair requires no data migration but reintroduces that defect.

## References

- [Configuration preparation](../../telnet/configuration_publish.go)
- [Preparation regressions](../../telnet/configuration_publish_test.go)
- [YAML transaction regressions](../../telnet/machine_v5_commands_failure_test.go)
- [Operator recovery contract](../../telnet/README.md#grid-noise-and-nearby)
- [GRID mapping admission](TSR-0055-grid-prefix-and-mapping-feedback.md)

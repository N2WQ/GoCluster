# TSR-0135 - CI Fixture Inputs and Synchronization

Status: Monitoring
Date Opened: 2026-11-16
Date Resolved: n/a
Owner: Repository maintainer / Codex
Technical Area: geography startup, corrected-grid scheduler, VOACAP persistence, Windows CTY fixture
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

- What happened: hosted CI failed geography startup on Linux and Windows and
  CTY permission restoration on Windows. An earlier nightly race run also
  failed corrected-grid scheduler and VOACAP persistence assertions.
- Why: the geography test read an unexpanded LFS model pointer; two concurrent
  tests assumed intermediate state would survive until their next assertion.
  The hosted ACL descriptor difference is not established by its original log.
- What fixed it: geography owns temporary model/reliability inputs; scheduler
  receipt proves admission; the fake forecaster waits for the initial lookup.
  Parent-first ACL restoration is a diagnostic trial, not a confirmed hosted fix.
- How we know: source and hosted failure evidence identify the first three
  mechanisms; local ordinary/race checks and final validation are recorded below.
- Operator/support answer: these changes repair tests, not runtime policy.
  A local ACL pass does not establish a hosted Windows repair.

## Triggering Request

- Request date: 2026-11-16
- Request summary: review GitHub CI failures and implement bounded fixture and
  synchronization fixes under Approved v1.
- Request reference: [CI run 43878352822](https://github.com/N2WQ/GoCluster/actions/runs/43878352822).

## Findings

- Root cause: `TestGeographyStartup` enables correction after using a shared
  fixture that resolves configured asset paths into the checkout. CI does not
  expand LFS data, so JSON parsing encounters the `version` pointer header.
  All three reliability tables also came from external production priors.
  The geography test now owns a valid minimal model and finite reliability
  weights, explicitly enables model loading, and checks that it occurred.
  Real cold H3 startup, missing-input failures and consumer controls remain.
- Root cause: `Pending` counts reserved scheduler slots, not released retry
  envelopes. The one-nanosecond scheduler may decrement it before the test
  reads it. The corrected-grid test now receives the actual envelope with
  the existing bounded helper, checking key, evidence and completed-check
  context before using it. Output and reopened-store assertions remain.
- Root cause: `CheckForecastWindow` queues work, then performs a second cache
  lookup. An immediate fake worker can publish between those operations.
  A fixture-local channel now holds completion until the initial lookup
  returns. Context cancellation unblocks early-failure cleanup; worker join
  still fences disk assertions, including raw rows and reloaded metadata.
- Best current ACL explanation: restoring the directory after the child can
  propagate inherited ACEs, but does not explain the directory's own hosted
  mismatch. The trial restores parent first and child last, logs baseline,
  protected, each-restoration and final SDDL/control, and rejects empty SDDL
  conversion results. Exact equality and all permission-denial, unchanged-file,
  startup and successful-publication controls remain. Any remaining difference
  requires evidence-based follow-up rather than normalization or weaker checks.

## Hypotheses and Tests

1. Local production model data is a necessary geography-test prerequisite.
   Rejected: valid temporary inputs preserve enabled loading and geography
   controls without depending on checkout priors.
2. A missing pending slot or immediate forecast proves production work loss.
   Rejected by the current scheduler and cache-lock ownership paths; these
   intermediate observations admit legal timing interleavings.
3. Parent-first restoration completely repairs the hosted Windows descriptor.
   Inconclusive: local equality passes, but the hosted descriptor delta has
   not been captured and the trial has not run on that environment.

## Decision Linkage

- No contract changes and no ADR change. Preserve ADR-0344 shared-geography
  startup and ADR-0340 CTY authority. Preserve exact permission restoration.
- Existing history remains in TSR-0126, TSR-0127 and TSR-0128; TSR-0130 records
  the older local-priors prerequisite and local/hosted qualification boundary.
- Support-agent docs impact: Not required. Product behavior and operator
  instructions are unchanged; existing support routes link those records.

## Verification and Monitoring

- Targeted native Windows ordinary/race selections passed all 50 cases without
  skips, including exact CTY descriptor restoration and all geography modes.
  Linux targeted ordinary selection also passed.
- Linux full ordinary and race commands (`go test -count=1 -timeout=10m ./...`
  and the same command with `-race`) each passed 61 test packages and built
  16 packages without tests. The race command exited zero.
- Repeated race checks passed each synchronization test 60 times on Linux
  (`-count=20 -cpu=1,2,4`), and nine times on Windows (three runner invocations
  per `GOMAXPROCS=1,2,4`). The native runner required both named tests to pass.
- Native Windows full ordinary (`scripts/test-windows.ps1`) and full race
  (`scripts/test-windows.ps1 -Race`) each passed 13,654 cases with 98 skips
  across 76 packages (61 executed, 15 build-only). Every repaired test ran and
  passed; the optional hash-pinned flood-recorder capture test was skipped,
  so these runs supply no recorder-capture evidence.
- Windows targeted vet and Linux full vet/Staticcheck passed. Linux lint reports
  two pre-existing `unconvert`
  findings at `internal/logidentity/identity_linux.go:20,22`; that production
  file is unchanged and outside Approved v1. Lint exited one; it is not a pass.
- Validation was collected on 2026-11-16 over baseline `d2e93b52` plus the
  Approved v1 test changes. Local command logs are in `.tmp/ci-fixtures-v1/`.
- Stop ACL repair if either original descriptor differs. Do not weaken equality,
  drop inheritance flags, add retries/skips, or substitute restoration APIs.
- Hosted rerun/publication is outside Approved v1. Hosted CI success, production
  behavior and runtime performance are not established by local test results.

| Approved item | Implementation | Validation |
| --- | --- | --- |
| Owned geography priors | `internal/cluster/geography_startup_test.go`; existing numeric fixture helper | All seven cold child modes, enabled model check, missing H3 and consumer controls; full ordinary/race on both platforms |
| Scheduler ownership | `internal/cluster/corrected_grid_delivery_test.go`; existing bounded envelope receiver | All 18 corrected-grid scenarios and reopened-store assertions; repeated and full ordinary/race checks |
| Worker publication ordering | `pathreliability/voacap_fallback_test.go`; cancellation-aware completion gate | Empty initial cache, exactly one completion, joined raw persistence and reload checks; repeated and full ordinary/race checks |
| ACL diagnostic trial | `internal/cluster/cty_protected_input_windows_test.go` | Exact local descriptor equality, denial/hash/startup/positive publication controls in targeted and full native ordinary/race; hosted result outstanding |

## References

- [Nightly race 43726369225](https://github.com/N2WQ/GoCluster/actions/runs/43726369225).
- [Windows inheritance propagation](https://learn.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-setnamedsecurityinfow).
- [TSR-0126](TSR-0126-cty-grid-classification.md),
  [TSR-0127](TSR-0127-stabilizer-live-output.md),
  [TSR-0128](TSR-0128-shared-geography-startup.md),
  [TSR-0130](TSR-0130-main-test-firewall-path.md).

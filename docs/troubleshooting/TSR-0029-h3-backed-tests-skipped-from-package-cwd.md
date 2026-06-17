# TSR-0029: H3-Backed Tests Skipped From Package CWD

- Status: Resolved
- Date opened: 2026-06-10
- Status date: 2026-06-10

## RCA Summary

- What happened: Standard package tests reported green while H3-backed
  path/telnet/filter tests skipped because they could not find checked-in H3
  fixtures from the package working directory.
- Why: Test helpers assumed the process working directory was the repository
  root; Go runs package tests from package directories, and skip-on-missing
  hid both the path bug and stale geometry assumptions.
- What fixed it: Tests now resolve the repo root by walking to `go.mod`, fail
  when checked-in H3 fixtures are missing, use distinct H3 grids for directed
  evidence tests, and isolate receiver-cap behavior where unrelated.
- How we know: Package test runs reproduced missing `data/h3` skips, compiled
  binaries run from repo root exposed stale assertions, and updated tests cover
  normalize, receiver, telnet diagnostics, path settings, server filters, and
  nearby filters.
- Operator/support answer: Treat this as validation hardening, not a runtime H3
  production regression; production startup validation remains covered by the
  existing H3 table contract.

## Trigger

Review of the fine/coarse union scalar evidence patch found that `go test
./telnet` reported success while H3-backed path diagnostic tests skipped. The
tests loaded `data/h3` relative to the package test working directory, not the
repository root that owns the checked-in H3 fixture tables.

## Symptoms and impact

Normal package test runs skipped H3-backed tests in `pathreliability`,
`telnet`, and `filter` even though the repository includes `data/h3/res1.bin`
and `data/h3/res2.bin`. Running compiled test binaries from the repository root
made those tests see H3 data and exposed stale assertions in path diagnostic,
sample-floor, and nearby login-state fixtures.

The production code path was not implicated by this finding. The impact was
validation reliability: tests that should protect path reliability and telnet
diagnostic behavior were false-green under the standard `go test ./...` shape.

## Hypotheses tested

1. H3 fixture files were absent.
   - Disproved: `data/h3/res1.bin` and `data/h3/res2.bin` are checked in.
2. Package tests were resolving H3 tables relative to package directories.
   - Confirmed: package-local helpers called
     `InitH3MappingsFromDir("data/h3")`, which points at
     `<package>/data/h3` during `go test`.
3. The production fine/coarse union scalar change broke path prediction.
   - Disproved for the scoped patch: production diffs were limited to
     `normalize.go`; exposed failures were test fixture geometry, receiver-cap
     fixture isolation, and stale NEARBY initialization assumptions.

## Evidence

- `go test ./telnet -run ... -v` skipped path/nearby tests with missing
  `data\h3\res2.bin`.
- `go test ./pathreliability -run ... -v` skipped H3-backed predictor tests
  with the same package-relative path.
- `go test ./filter -run TestNearby -v` skipped nearby filter tests before the
  helper fix.
- Running compiled test binaries from the repository root made H3 tables
  visible and exposed failing assertions in selected `pathreliability` and
  `telnet` tests.

## Root cause or best current explanation

The test helpers assumed the process working directory was the repository root.
Go package tests run from the package directory, so checked-in root fixtures
were invisible. The skip-on-missing helper then hid both the fixture-path bug
and stale test assumptions.

Adjacent grid fixtures also used `FN31`/`FN32` in tests that intended
single-direction path evidence. With real H3 tables loaded, those grids can
collapse into the same path cell, causing a self-pair lookup to read the same
bucket as both receive and transmit evidence.

## Fix or mitigation

- Add a test-only repo-root fixture resolver that walks upward to `go.mod` and
  validates required `data/h3` files.
- Convert H3-backed package helpers from skip-on-missing to fail-on-missing for
  checked-in fixtures.
- Use distinct H3 grid fixtures for tests that intend directed one-way path
  evidence, and fail tests if chosen grids collapse to the same fine or coarse
  cell.
- Isolate receiver-cap behavior from path count and `SET PATHSAMPLES` tests
  unless the test is specifically asserting receiver-cap diagnostics.

## Why an ADR was or was not required

No new ADR was required. The patch changes validation behavior and test
fixtures, not runtime path reliability, operator protocol, H3 startup
validation, or fine/coarse scalar semantics. The durable production contracts
remain ADR-0170 for fine/coarse union scalar evidence and ADR-0153 for
startup-time H3 table validation.

## Links

- Related ADRs: ADR-0170, ADR-0153, ADR-0146, ADR-0084, ADR-0085, ADR-0086,
  ADR-0134
- Related issues/PRs/commits: none
- Related tests: `pathreliability/normalize_test.go`,
  `pathreliability/receiver_test.go`, `telnet/diag_command_test.go`,
  `telnet/path_settings_test.go`, `telnet/server_filter_test.go`,
  `filter/filter_test.go`
- Related docs: `data/h3/README.md`, `pathreliability/README.md`,
  `telnet/README.md`

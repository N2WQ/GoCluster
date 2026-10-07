# TSR-0042 - Short Portable License Identity

Status: Resolved
Date Opened: 2026-10-07
Date Resolved: 2026-10-07
Owner: GoCluster maintainers
Technical Area: ULS normalization, login, central DE and final DX admission
Trigger Source: Chat request
Led To ADR(s): none
Tags: license, FCC, ISED, portable, callsign, allowlist

## RCA Summary

- What happened: `VE3/W1A` selected `VE3` for Canadian enforcement, while
  `W1A/VE3` selected `W1A` for US enforcement. The first form also missed a
  qualified US allowlist exception. DE/DX prefilters and license gates shared
  the same defective identity selection.
- Why: `NormalizeForLicense` selected the longest slash segment containing a
  digit. Equal lengths favored the first segment. A bare location prefix can
  tie or exceed the length of a complete station callsign.
- What fixed it: station segments accepted by the existing admission identity
  syntax now outrank bare digit-bearing prefixes before length is considered.
  The shared helper supplies the corrected base to lookup, CTY authority and
  qualified allowlist matching; consumers require no separate algorithm.
- How we know: the new normalization, login and healthy-snapshot DE/DX tests
  failed before the production change and pass after it. The actual local CTY
  database confirms both orders now resolve `W1A` to ADIF 291, and a longer
  numeric US prefix now leaves short Canadian `VE3A` as ADIF 1.
- Operator/support answer: portable location does not select the license
  registry or registered State. Both `VE3/W1A` and `W1A/VE3` check US `W1A`;
  `US:^W1A$` applies to either form. Fix the executable revision rather than
  disabling Canadian enforcement or adding a Canadian exception for `VE3`.

## Triggering Request

- Request date: 2026-10-07.
- Request summary: correct the P2 short-portable licensing defect found in
  review of `states_provinces` commit `281b8fb`.
- Request reference: user review in the implementation conversation; correction
  stays within the already authorized Scope Ledger v1 base-identity contract.

## Symptoms and Impact

The raw portable login passes syntax validation, but normalization can select
the prefix alone. Login then queries the wrong registry and matches allowlists
against the wrong identity. Central spot validation can reject the selected
prefix as malformed before license lookup; the final DX gate can use the wrong
authority. Registered State/province can consequently be missing or incorrect.

## Root cause or best current explanation

The admission syntax distinguishes `W1A` from bare `VE3`, but the shared license
normalizer ranked both as digit-bearing segments. Its length/position choice
was then reused as the identity for CTY authority and qualified allowlists.

## Hypotheses and Tests

1. Equal-length, order-sensitive ranking causes the reported defect: supported.
   With the repository's actual `data/cty/cty.plist`, the unchanged helper
   returned `VE3`/ADIF 1 for `VE3/W1A` and `W1A`/ADIF 291 for `W1A/VE3`.
2. A tie-break adjustment alone is sufficient: rejected. `W1234/VE3A` and
   `VE3A/W1234` both selected the longer bare US prefix before the fix.
3. Reusing the existing station-identity check distinguishes these inputs:
   supported. That check rejects bare `VE3`/`W1234` while accepting complete
   `W1A`/`VE3A`. Real SQLite fixtures assert CA/ON metadata, and healthy
   unassigned cases reject without or with the wrong qualified allowlist.
   These controls prevent unavailable-source fail-open from hiding a failure.

## Fix or Mitigation

Prefer a digit-bearing segment that passes `spot.IsValidNormalizedCallsign`
before comparing lengths. Keep existing longest/first ordering among equally
classified segments and existing fallback when no segment passes. Preserve
SSID/skimmer removal and ordinary calls, including special identities ending
in digits. This correction does not introduce a separate callsign grammar or
change portable CTY location selection.

The existing identity check is basic admission syntax. This fix does not prove
which station owns a slash string when multiple segments satisfy that syntax;
their existing ordering is preserved. Fuzz evidence covers its explicitly
restricted ASCII prefix/base patterns, not every callsign accepted by admission.

## Decision Linkage

- ADR created/updated: no decision change; ADR-0253 and ADR-0254 already require
  base-identity jurisdiction, independent of portable location.
- Decision delta summary: repair the implementation of that accepted contract.
- Contract/behavior changes: the reported short-base and longer-bare-prefix
  failures now use the complete base for licensing and allowlist matching.

## Verification and Monitoring

- Before/after actual-CTY reproducer: `/tmp/gocluster-short-portable-before.log`
  and `/tmp/gocluster-short-portable-after.log`.
- Targeted normalization/login/DE/DX regressions passed:
  `/tmp/gocluster-short-portable-green.log`.
- Source and oracle review was read-only with inherited context; it was not
  independent. A preservation gap for longest selection between two complete
  identity segments was found and covered by literal regression vectors.
- `go test ./...`, `go vet ./...`, `staticcheck ./...` and
  `golangci-lint run ./... --config=.golangci.yaml` passed (0 lint issues).
  The added preservation vectors also passed targeted ULS tests; ULS analyzers
  were rerun after that test-only review refinement.
- Race suites passed for telnet and cluster. The overlapping three-package run
  hit the unchanged FCC `TestStateImportCancellationAfterSubstantialRows`
  ten-second deadline (`context deadline exceeded` instead of explicit
  cancellation); it reported no data race. After the overlapping validation
  jobs finished, the full ULS race suite passed in 17.724 seconds. Logs are
  `/tmp/gocluster-short-portable-race.log` and
  `/tmp/gocluster-short-portable-uls-race-serial.log`.
- `FuzzNormalizeForLicensePortableIdentity` passed 30 seconds with 171,211
  executions; `/tmp/gocluster-short-portable-fuzz.log`.
- Targeted native Windows Go regressions passed for ULS, telnet and cluster;
  `/tmp/gocluster-short-portable-windows.log`.
- Ordinary warm FCC/Canadian lookup benchmarks retained 0 B/0 allocations.
  These local measurements do not prove a portable-path or deployed latency
  improvement; `/tmp/gocluster-short-portable-bench.log`.
- The troubleshooting-record checker passed after the RCA evidence section
  was added. Local Markdown targets and diff whitespace checks passed.
- Monitor rejected identity and qualified allowlist diagnostics when comparing
  portable orders; this local evidence does not establish deployed latency or
  a broader callsign-ownership inference.

## References

- Reviewed commit: `281b8fb` (Canadian provinces).
- Related ADRs: [ADR-0253](../decisions/ADR-0253-fcc-state-enrichment-and-filtering.md),
  [ADR-0254](../decisions/ADR-0254-canadian-ised-license-and-state-reuse.md).
- Source: [shared normalization](../../uls/license_check.go),
  [login](../../telnet/login_validation.go),
  [DE](../../internal/cluster/ingest_validation.go),
  [final DX](../../internal/cluster/bootstrap.go).
- Tests: [normalization](../../uls/license_check_test.go),
  [login](../../telnet/canadian_login_test.go),
  [DE/DX and qualified allowlists](../../internal/cluster/short_portable_license_test.go).

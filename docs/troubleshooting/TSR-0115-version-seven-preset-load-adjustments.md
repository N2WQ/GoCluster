---
title: "TSR-0115: Version-Seven Preset LOAD Adjustments"
description: Restore announced applied settings and independent baselines when preset choices are unavailable.
---

Status: Resolved
Date Opened: 2026-10-29
Date Resolved: 2026-10-29
Owner: Repository maintainer
Technical Area: telnet preset LOAD, shared preferences and applied baselines
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

- Version-seven LOAD could report success while silently applying FAST for a
  saved SLOW choice. It saved SLOW in the profile and applied baseline, allowing
  a later reconnect to select SLOW again if the node re-enabled it.
- The new path reused delta-update preparation, skipped the existing LOAD
  adjustment/warning step, and used the named snapshot directly as the baseline.
- Restore explicit LOAD adjustments before admission/commit/publication. Create
  the selected bank's baseline from the applied configuration, keep the named
  snapshot unchanged, and preserve the other bank and its reference.
- The same missing step affected an unavailable PATHSAMPLES override. Preserve
  inherited empty/default choices and lookup-derived GRID separately.

## Triggering Request

The owner supplied a review of `c11775ea33842fbdfcf34faacc4e909459836d60`
identifying the preset regression and obsolete CC Boolean instructions in the
Telnet guide. Investigation started from a clean `boolean` checkout at that
commit. This corrects the existing Approved-v4/ADR-0244 contract; it introduces
no new preset policy or storage schema.

## Symptoms and Impact

The amount of duplicate suppression could differ from the loaded preset without
a LOAD warning. A later configuration change/reconnect could then change it
again. Separate stale guide paragraphs claimed category inspection was absent
and Boolean rules worked in CC, contrary to native CC command ownership.

## Hypotheses and Tests

1. Stored preset parsing rejects disabled SLOW.
   - Rejected: strict v7 storage accepts the legal snapshot; operator availability
     is resolved at use. Machine writes have separate unavailable-choice checks.
2. LOAD omits applied-setting reconciliation.
   - Supported by source and an actual TCP regression. Before repair, all four
     native/ziutek and GO/CC cases failed on the missing warning; readback showed
     configured SLOW and effective FAST. Log: `.tmp/cc-load-fallback-before.log`.
3. The retained fallback test covers current sessions.
   - Rejected: it uses the frozen single-bank carrier. New tests use actual v7
     two-bank sessions and verify immediate durable state before disconnect.

## Root cause or best current explanation

`handleV7PresetOwned` reached `prepareV7ConfigurationUpdate` then committed the
unadjusted requested settings and original named baseline. The correction keeps
the shared update helper unchanged and reconciles only explicit preset LOAD.
Its acknowledgement is bounded before commit; failed commit still preserves
live filters, both references, revision and profile bytes.

The Telnet guide now separates GO rule views from CC category views, gives native
CC State examples, labels legacy schema-six restrictions, and distinguishes GO
and native CC response limits. The current CC guide, domain contract and support
card distinguish LOAD adjustments from exact ordinary login/readback behavior.

## Decision Linkage

No new durable decision or ADR. This restores the applied-snapshot requirements
of [ADR-0244](../decisions/ADR-0244-exact-configuration-persistence.md), retained
by [ADR-0330](../decisions/ADR-0330-native-cc-banks-and-generation-seven.md).
Support-agent documentation impact: Required; the configuration support card
was updated. No operator YAML value change or production data repair is included.

## Verification And Monitoring

The targeted native Windows `./telnet -Run '^TestV7PresetLoad'` run passed all
11 cases without skips. TCP checks cover both transports and both destination
banks, warning and switch messages, actual runtime FAST, immediate complete
profile/baseline persistence, unchanged named SLOW and inactive association,
ordinary reconnect, and reconnect to a node that re-enables SLOW. Consumer checks
cover inherited/default values, available SLOW, unchanged derived GRID,
PATHSAMPLES adjustment, equal unavailable selections, and failed commits.

The targeted plan specifically rejects warning-only repairs, profile-only
repairs with a stale baseline, library rewrites, inactive-bank replacement and
autosave masking an unsuccessful LOAD commit. No new runner or protocol seam
was needed; the existing TCP and fault-injection fixtures supply the evidence.

A fresh separate context reviewed the final bounded Go/test/documentation diff
and found no material issue. It checked transaction/reservation ownership,
adjustments before admission/commit, publication ordering, detached baselines,
bank independence, defaults and the test false-green cases above. It inspected
the red/green logs but did not execute checks or claim a whole-subsystem audit.
The lead observed exit 0 from vet, Staticcheck and lint; lint reported zero issues.

Full repository ordinary and full telnet race results are recorded below.
No parser, schema, ownership primitive or hot matching path changed;
new fuzz/performance qualification is not claimed. No deployment, remote session,
commit or push is included.

Final source binding: baseline `c11775e` plus the reviewed local diff. The Go
files were frozen before the full lanes. Their workspace SHA-256 values are:

| File | SHA-256 |
| --- | --- |
| `telnet/preset_commands_v7.go` | `3F7B40C96F8D122E788D25D0063775200B8FEC47334F5C39875EBF2691525ADF` |
| `telnet/preset_v7_fallback_test.go` | `04F96FD10F2D3ABF3A9AEEE995DDC22FF4819FE01D34B7A4D7B0B32D63D24923` |

The full native Windows ordinary lane exited 0: 56 executed and 16 build-only
packages, 11,937 passing cases and 93 skips across 72 packages. Log:
`.tmp/cc-load-ordinary.log`. Skips remain the baseline's external DXSpider,
subprocess-helper, unavailable Windows H3 and opt-in live/data/controller/profile
qualifications; none of the new preset regressions was skipped.

The full native Windows telnet race lane exited 0: 2,467 passing cases and three
existing opt-in/driver skips, with no race report. Log:
`.tmp/cc-load-telnet-race.log`. The skips are the first-wave comparison,
path-sharding glyph driver and PSKR split-wire controller qualifications, not
preset checks. Other packages' race suites were not rerun for this localized
LOAD correction; their source, interfaces and ownership mechanisms are unchanged.

`go vet ./...`, `staticcheck ./...` and
`golangci-lint run ./... --config=.golangci.yaml` all exited 0 on the frozen Go
source; logs are `.tmp/cc-load-vet.log`, `cc-load-staticcheck.log` and
`cc-load-lint.log`. Support-agent, crawler-entry and troubleshooting-record
checks passed; the initial TSR check required a recognized root-cause heading,
which was corrected without changing the checker. The final lead diff review
found no unresolved issue in the two reported defects or this correction.
Repository maps are regenerated/checked after this record's last edit, followed
by review of the generated diff and `git diff --check`. Map logs are
`.tmp/cc-load-codemap-generate.log` and `.tmp/cc-load-codemap-check.log`.

## References

- Baseline: `c11775ea33842fbdfcf34faacc4e909459836d60`.
- Approved scope: [CC v4](../cc-dialect-v4-scope-ledger.md).
- Source: [v7 LOAD](../../telnet/preset_commands_v7.go).
- Regression: [preset fallback](../../telnet/preset_v7_fallback_test.go).
- User guidance: [CC dialect](../cc-dialect.md), [Telnet](../../telnet/README.md).

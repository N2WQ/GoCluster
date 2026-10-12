# Call correction follow-up execution

Authority: owner exact `Approved v2` on 2026-10-11 for the self-contained
[scope ledger](call-correction-followup-scope-ledger-v2.md). Baseline
`96c0a84a19110fa248eddab182e622b174b73082`; branch `fix_call_correction_followup`.
S1 through S4 are sequential; no patch is complete until its checkpoint passes.

## Logging prerequisite

Before production edits, ordinary and race runs passed for exact selection
`^(TestDailyFileSinkArchivesStaleActiveOnStartup|TestDailyFileSinkArchiveCollisionMergesWithoutOverwrite)$`
in `./internal/cluster`, timeout 180s, using scripts/test-windows.ps1 and its
canonical executable. Both exited zero with PASS. Evidence:
`.tmp/call-correction-followup/prerequisite/logging-ordinary.log` and
`logging-race.log`, with corresponding exit-result JSON files.

Historical missing-archive/file-sharing failures remain in prior logs. These
two passing prerequisite runs do not erase those failures or establish logging
repair. Logging changes remain separately authorized; any checkpoint recurrence
blocks progression without exclusion or waiver.

## S1: selected loss propagation

Postapproval test-strategy review completed before production mutation. The lead
accepted the checker matrix: qualify healthy/absent exact inputs and positive
history; actual cap admissions and scheduler retries; real expiry recovery;
base-one/ambiguity-three budget equality, both timeout actions and overflow;
actual temporal controller terminal/overflow abstention; unchanged healthy
exact low-P/state controls; delivered TCP/ring/reopened archive/original peer
evidence. No material scope gap was found. Distinct-receiver support readback
alone is not a once-only oracle; eligible occurrence controls are planned in S3.

Implemented initial request-local selected-loss capture, including temporal
terminal/overflow abstention, and fresh retry loss overlay on unchanged exact
delay inputs. No loss/receiver state was added to scheduler envelopes. The
ordinary correction selection stays local; an initial review's potential
context-pointer escape was removed before final targeted verification.

Observed checks before the full checkpoint:

- Exact selected inventory: 31 top-level cluster tests, including six new S1
  tests/13 subcases. Ordinary/race final runs passed after the local-selection
  refinement (`targeted-final-ordinary.log`, `targeted-final-race.log`). They
  cover actual cap loss, qualified healthy/absent exact evidence and S history,
  actual scheduler retries, budget equality, release/suppress, overflow,
  controller abstention, fresh recovery and healthy exact-input controls.
- Broken-state overlay restores baseline stage/delivery source without changing
  tracked files. New initial tests failed for `selected loss escaped a qualified
  hold`; this expected failure is recorded in `baseline-red.log/.json`.
- Matching healthy ordinary-stage benchmark: baseline 2028ns/op, final
  2138ns/op; both 1B/op and 0allocs/op. These are single local runs, not a latency
  improvement claim. Compiler escape evidence reports leaking override contents,
  with no ordinary selected-struct heap move (`escape.log`).
- Existing final selection controls: all-fresh capped16 0allocs/op, partial
  expiry 30allocs/op, five-key union 413allocs/op/106042B. Source inspection
  confirms no added initial neighborhood union (`bench-baseline.log`,
  `bench-final.log`).
- Tagged family qualification ordinary/race count3, plus tagged vet/lint,
  passed (`qualification-*.log`); these exercise existing delivery ownership.
- Troubleshooting-index/RCA, changed Go crawler comments and whitespace checks
  passed. Source-aware comment review preserves specific loss authority and
  exact policy meaning. Support-agent impact: confidence support card updated.

Read-only Go quality review found the potential initial selection escape above;
the lead fixed it and reran affected tests. Fresh review found no further
material production issue. Replay already supplies selected evidence to the
shared delay evaluator; this establishes loss-policy agreement, not identical
ordinary state/share input or actual scheduling.

Complete S1 checkpoint passed: 70 packages ordinary (596.1s) and race (981.9s),
53 test packages and 17 build-only in each; vet (2.9s), Staticcheck (4.8s),
configured lint (9.2s), and connected CustomSCP ordinary (10.2s)/race (16.7s).
All seven results and commands are in `.tmp/call-correction-followup/S1/checks.json`.
The formerly failing logging cases also passed inside both complete runs.

Changed-document relative file links passed (eight links); TSR/RCA/index,
support-critical comments and whitespace passed. Existing ADR-0030/0046/0292
govern S1; new TSR-0088 records the wiring failure and recovery lesson. No new
durable decision is selected by S1. Support card impact is the current selected
loss/stabilizer explanation, without a new percentage meaning.

Fresh final lead review confirms narrow production changes, exact delay-input
preservation, request-local scalar ownership, no saved loss or new retained
state, qualified regressions and correctly scoped cost/coverage claims. Final
codemap generation/check and direct diff review precede the local S1 commit.
Later patch work has not begun.

# TSR-0135 - CI Fixture Inputs and Synchronization

Status: Monitoring
Date Opened: 2026-11-16
Date Resolved: n/a
Owner: Repository maintainer / Codex
Technical Area: geography and synchronization fixtures, Windows CTY permissions, Linux identity widths, command integration
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

### 2026-11-19 follow-up

Hosted run 44606340988 exposed replay diagnostic fixtures that supplied the
unresolved Windows temporary path after the diagnostic owner had resolved its
root. On the hosted runner this included a short-name path; a separate relative
path fixture assumed the checkout and temporary directory shared a drive. A
scratch provenance assertion also compared the unresolved root. Setup failure
before cleanup registration left diagnostic streams open. The fixtures now
use the owned root, create the relative-path fixture on the checkout volume,
and register cleanup immediately after acquiring the streams. Containment
checks remain in force.

Nightly run 44568056161 exposed a different ordering issue: observing a resolver
winner did not prove that the remaining resource-cap evidence was processed.
The test now drains its admitted evidence synchronously before reading metrics.
The Linux lint job also reported G703 at two private filter filesystem helpers;
their paths originate in validated user-record or preset paths and a sibling
temporary file. Narrow annotations document that ownership. These changes do
not alter operator behavior. Hosted confirmation requires a subsequent run.

- What happened: hosted CI failed geography startup on Linux and Windows and
  CTY permission restoration on Windows. An earlier nightly race run also
  failed corrected-grid scheduler and VOACAP persistence assertions. Follow-up
  CI exposed the remaining ACL mismatch and Linux lint errors; the previously
  blocked command integration then exposed invalid logins and stale schema.
- Why: the geography test read an unexpanded LFS model pointer; two concurrent
  tests assumed intermediate state would survive until their next assertion.
  Follow-up ACL diagnostics found ambient inheritance-control differences and
  inconsistent owner/group projections. Linux native field widths vary by
  architecture. Positive fixture SSIDs violated the current login contract,
  and the cumulative command corpus still assumed generation-four filters/storage.
- What fixed it: geography owns temporary model/reliability inputs; scheduler
  receipt proves admission; the fake forecaster waits for the initial lookup.
  The first ACL trial exposed a hosted inheritance-model mismatch; the v2
  fixture constructs declared security before capturing exact originals.
  The v3 command driver uses accepted one/two-digit positive login identities.
  The portable width adapter preserves identity conversion; v4 migrates all
  command objectives to native seven with both-bank and resource-owner oracles.
- How we know: current source, hosted logs and local command receipts distinguish
  the failure mechanisms; ordinary/race and full qualification results are below.
- Operator/support answer: fixture repairs and portable conversion preserve
  runtime policy. Local success does not establish a hosted Windows repair;
  monitoring remains open until the published patch is observed in hosted CI.

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
- Initial ACL explanation (v1): restoring the directory after the child can
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
   Rejected by follow-up CI 43914092794: both hosted objects gained AI
   (`0x8004` to `0x8404`) although their ACE sequences were unchanged. Owner
   and group appeared inconsistently because snapshots requested only DACL.

## Follow-up: controlled fixture and portable identity conversion

Approved v2 addresses both remaining failures on baseline `df7dbd04`:

- Construct a nested owned directory and file with explicit current-token
  full-control ACLs. Their enclosing TempDir's descriptor is read and checked,
  never written. Query owner, group and DACL consistently at every snapshot.
- Creation alone ignored AI, persisting `0x9004` despite supplied `D:PAI`.
  Finish construction with one supported protected-DACL initialization per
  object, before either original is captured and before either deny is applied.
  There is no retry, alternate baseline, flag masking or legacy/native API.
- Require persisted P/AI and clear AR. Inspect native ACE type, trustee,
  inheritance and access mask, including a noninheritable deny. Restore exact
  O/G/D SDDL and full control flags, with registered cleanup before denial.
  Close creation handles on every path before publication attempts.
- Preserve the actual changed-content removal denial, unchanged input checksum,
  startup consumption and successful post-restoration publication. Separate
  controls prove ordinary publication and restoration through registered cleanup.
- Linux `Stat_t.Dev` is uint32 on MIPS; `Nlink` is uint32 on ARM64 and other
  targets. Both are uint64 on amd64. A private unsigned-width adapter preserves
  widening at the two original call sites without disabling lint or changing
  device/inode identity, link counts or error handling.
- Latest hosted Linux unit, connected Custom SCP, vet and Staticcheck steps
  passed, as did nightly race 43914686754 and both maintained-module jobs.
  Hosted Windows runner self-tests and command integration were blocked behind
  the ACL failure, so v2 explicitly runs those checks locally as well.

The detailed test review preserves actual filesystem evidence rather than
substituting metadata normalization or a synthetic conversion-only test:

| Contract | Failure or false green | Stimulus and observable oracle | Checker and owner |
| --- | --- | --- | --- |
| Declared fixture state | Host ignores supplied creation metadata | Query persisted P/AI/no-AR and exact explicit full-control ACE after construction | `TestCTYFixtureAllowsUnprotectedPublication`; cluster integration |
| Exact restoration | Drop AI/AR, owner/group or denial cleanup | Capture both originals before mutation; compare full control and O/G/D afterward; outer descriptor unchanged | `TestCTYProtectedInputPublication`, `TestCTYFixtureRestoresDuringCleanup`; native ordinary/race |
| Real publication protection | Sharing violation, 304 or temp-create failure masquerades as denial | Closed creation handles, writable temp body, changed HTTP payload, permission error at the replacement step, unchanged input and positive replacement | `TestCTYProtectedInputPublication`; cluster integration |
| Portable identity | Removing casts breaks other architectures or weakens link checks | Real alias/hard-link identities with no skipped Linux cases; compile supported 32/64-bit field widths | `internal/logidentity` tests and Linux amd64/arm64/386/MIPS/MIPS64 compilation |
| Previously blocked CI tail | Unit tests pass while integration never reaches commands | Runner self-tests, pinned Python oracles and source-bound native/ziutek integration | Existing Windows/Python scripts; lead validation |

Local v2 qualification passed targeted ordinary/race Windows tests,
Linux identity race tests (including nonskipped alias/hard-link cases), all five
cross-compilation targets and both platforms' vet, Staticcheck and lint checks.
Windows runner self-tests passed 50 groups. Full results are recorded below.
Source-bound integration exposed an additional existing fixture mismatch:
`K1ABC-70001` is rejected before command execution under ADR-0322's accepted
one/two-digit login-SSID rule. Its child retired cleanly; no profile completed.
The preserved receipt is `.tmp/command-fixes/20261116-123716-7cf01c98/`.

## Follow-up: valid command-fixture allocations

Approved v3 retains v2 and adds only the driver/offline-test fixture repair:

- Replace positive `700NN` SSIDs with the exact two-digit `NN` spelling,
  including generated 21..26, 50..55 and 79..80. Reputation-only 70101..70103
  become 81..83. Update the literal saved-record path and offline readbacks.
- A finite allocation table declares main, dedupe and reputation identities.
  Preflight checks selected bases, ASCII one/two-digit suffixes and duplicate
  full identities before building, staging or creating output. Every positive
  session goes through its allocation-membership guard. Reconnects preserve
  the same full spelling; distinct base calls remain distinct.
- Offline literal expected allocations, malformed/non-ASCII suffixes, leading
  zeroes, collisions, generated endpoints and no-effects preflight checks
  exercise the fixture contract. Source review establishes current caller
  separation: declared inventory uniqueness alone cannot prevent future callers
  from choosing an existing allocation for an unrelated case.
- Deliberate invalid controls, corrupt/legacy records, frequency examples,
  128/129-byte input checks and all 138 semantic registrations remain. Shorter
  names must still prove fitting/oversized response outcomes; no product frame
  limit, login grammar, timeout or admission policy changes are authorized.
- Six new pure offline checks passed. A whole-suite attempt during the native
  runner encountered its intentional shared lock in four interoperability
  tests; it is not qualification evidence. The final serial run passed all 144
  tests. Both Python and PowerShell runners own the same lock.

The source-bound integration now reaches login, but does not qualify: its
legacy schema-four client and flat filter/profile/preset expectations predate
the accepted schema-seven two-bank contract. The first barrier rejects
`GET YAML CONFIG SCHEMA 4`; changing that header alone cannot make the old
storage and filter assertions correct. ADR-0330 expressly rejects translating
arbitrary GO expressions into native CC categories and preserving old filters
or presets during upgrade. Production compatibility behavior must not be
changed to satisfy the stale corpus.

A read-only audit invoked all six existing profile functions independently, so
the first failing profile could not hide the remaining profiles. All 138
registered objectives were attempted: each transport's main/reputation/dedupe
profiles passed only 4/55, 1/7 and 0/7 respectively. These are failed evidence,
not coverage qualification. Later session-quota and EOF failures follow aborted
cases retaining sessions; they do not establish a separate admission defect.
Process-stop receipts establish child retirement despite protocol cleanup
errors. Preserve `.tmp/command-fixes/20261116-132448-f945099f/` and the full audit
`.tmp/command-fixes/20261116-134511-all-a1215c69/`.

Migrating this corpus requires a separate approved scope covering current
wire, both banks, tagged presets, native storage, explicit legacy-reset controls,
resource oracles and assertion-failure cleanup. Approved v3 does not authorize
that migration. Do not skip existing objectives, raise limits or weaken
fitting/oversized assertions to obtain a green result.

## Follow-up: current command-integration contract

Approved v4 retains v2/v3 and migrates the cumulative driver to accepted native
seven behavior. ADR-0330 resolves two-bank ownership, tagged presets and legacy
reset; ADR-0329 resolves the operand-free GO NEARBY predicate. This is test
maintenance, not a restoration of retired product commands or storage behavior.

The same 55 main, seven reputation and seven dedupe objective identities remain
required per transport: 138 total. The driver records every independent profile
even after another fails; any failed, missing, reordered or interrupted objective,
wire failure or cleanup error prevents qualification.

| Existing objective group | Current contract and falsifiable evidence |
| --- | --- |
| Endpoint identity and physical command input | Source-bound child receipts; native/ziutek framing; valid/invalid physical input, command arity and actual archive effects |
| Live preferences, late failures and restart | Complete CONFIG7 and both preset owners; immediate native disk equality before logout; actual replacement faults; clean process retirement and fresh-child restoration |
| Compatible storage, aliases and presets | Separate pre-seven reset controls with exact original backups, preserved shared settings and library completion; later presets survive another SSID's upgrade; native tagged LOAD preserves inactive bank/reference |
| Raw, canonical and decoding-work admission | Structurally native owners; exact refusal reasons and fitting controls; authoritative repair, retry and logout; prospective human/SAVE/LOAD/VALIDATE/PUT/PATCH must refuse before effects |
| MODE, callsign, GRID, NEARBY and taxonomy | Numbered GO predicates and native CC selections; explicit retired-form refusals; literal rule/state goldens, independent archive visibility, both-bank persistence and restart |
| HELP, discovery, DEDUPE, SOLAR, history and HOLD | Current grammar and aliases; bounded printable wire receipts and complete correlated barriers; configured/effective distinction; unchanged preference owners; original countdown, suppression and fresh delivery controls |
| Reputation and unavailable dedupe | Full-SSID preference ownership, independent base-call histories and real loaded gate; runtime count reset only after process replacement; stored unavailable choice is read without rewriting it |

Native disk encoding and machine output have different limits and grammars.
Stored aliases pay the bounded shared work budget; machine uploads reject them.
The isolated environment-gated resource helper reuses the existing telnet test
executable to measure actual Go canonical encoding and final native frames.
Requests, operations, files, output and searches are bounded. Nonce, executable,
source and input/output hashes are verified before artifacts are used. Python
owns literal expected bounds/reasons; generated measurements cannot qualify
themselves. Owner-level frame probes record ordinary and compatible compact
encodings at the observed renderer boundary; the separate exact-encoder/capacity
tests own maximum-capacity correctness. Native login now rejects the old largest
DEDUPE values before a session can inspect them. Live probes therefore prove a
40,000-byte fitting choice and a protected oversized-login refusal. A detached
probe of the actual final-send owner retains the original 49,200-byte fitting
response (65,438 wire bytes) and 49,366-byte body (65,536 before its pause footer),
which must produce only the exact 49-byte refusal. Protection during login is
never counted as proof of that later final-send refusal.
The shared frame refusal sentinel retains historical 65,536-byte wording; its
text alone does not identify which admission boundary was reached.

Assertions inspect disk before logout so final autosave cannot repair a false
pass. Fixtures never rewrite a record after closing its old owner to seed a new
case: EOF alone is not a final-save retirement barrier. New identities use an
already captured native template; changes to an existing identity use actual
admitted commands before reconnect.

Case-owned sessions close in `finally`, preserving the original semantic error
and recording cleanup failures separately. Intentional shared sessions retain
their profile lifetime. Independent semantic groups may reconnect the same
literal SSID only after proving immediate persistence; complete banks, both
references and fresh session state are checked before continuing. No failed
command or refused login is retried. The public client's login delay is adapted
to the private fixture's unchanged loaded 100-accept/second limit, with client
pacing below that bound. Command pacing, server limits and deadlines are unchanged.
Adjacent edit/refusal checks may reuse the last complete correlated CONFIG7
reply from this sole command writer. That snapshot is detached from caller
mutation; errors, legacy projections and other resources cannot replace it.
Every tested command still has its own complete post-command barrier and exact
live/disk assertions. This removes redundant reads rather than dropping cases
or extending the deadline. Per-case elapsed times make remaining pacing failures
visible in the report.

The final source-bound v4 run passed all six profiles and all 138 objectives;
165 serial offline tests passed. Failed development runs remain evidence of the
specific defects corrected, not qualifying retries. Final Go lanes and evidence
are recorded below.

## Decision Linkage

- No contract changes and no ADR change. Preserve ADR-0344 shared-geography
  startup and ADR-0340 CTY authority. Preserve exact permission restoration.
- Existing history remains in TSR-0126, TSR-0127 and TSR-0128; TSR-0130 records
  the older local-priors prerequisite and local/hosted qualification boundary.
- Support-agent docs impact: Not required. Product behavior and operator
  instructions are unchanged; existing support routes link those records.

## Verification and Monitoring

### v4 evidence

- Final Python 3.13.18/PyYAML 6.0.3 offline qualification used CI's exact
  `python -m unittest discover -s scripts -p 'test*_test.py'` selection and passed
  165 tests. The suite intentionally prints failure/blocked records while
  testing its own refusal paths; its final `OK` and zero exit establish success.
- Final real integration passed native main/reputation/dedupe 55/7/7 and ziutek
  main/reputation/dedupe 55/7/7: 138/138 with no wire or cleanup failures.
  All 12 stopped-child receipts establish retired clients, joined consumer and
  stopped archive. Every source-manifest file still matches the built run.
  Receipts: `.tmp/command-fixes/20261116-151815-54642431/`.
- Native Windows full ordinary and race each passed 13,662 cases with 98 skips
  across 76 packages (61 executed, 15 build-only). Every repaired/added test ran;
  selected CTY/identity and resource-owner checks had no skips. Final runner
  self-tests passed all 50 fixture groups. No new skip path was introduced.
- Linux full ordinary/race each passed 61 test packages and built 16 packages
  without tests; final resource-owner selections also passed
  five cases without skips in ordinary and race modes after the final helper
  addition. Both platforms' full vet, Staticcheck and golangci-lint checks passed;
  lint reported zero issues. Cross-architecture identity and nonskipped filesystem
  evidence from v2 remain applicable: that implementation is unchanged by v4.
- Five targeted native resource cases passed without skips. Direct human
  final-send receipts retain exact 65,438-byte fitting output and 49-byte refusal;
  the corpus verifies literal values, footer, CRLF, width and absence of partial
  output. Owner snapshots separately prove no preference/persistence effects.
- The final diff has lead-owned source review and bounded read-only fixture and
  resource review. The 508-line Go helper keeps four fixed probes under one
  gated request/receipt owner; it is not a general-purpose runner or service.
  The frame probe reports an observed renderer boundary, supplemented by the
  separate exact-encoder/capacity tests; it is not an independent maximum-capacity
  proof for arbitrary compact encodings.
- Logs are in `.tmp/ci-fixtures-v3/`. The latest hosted CI remains 43914092794 at
  `df7dbd04`; no commit, push, hosted rerun or deployment is part of this patch.

| Approved v4 item | Implementation | Validation |
| --- | --- | --- |
| Current schema/bank/storage/preset corpus | `scripts/test-command-fixes.py` and its offline test module | All original 138 objective identities pass across both transports and both process generations; immediate disk and restart checks |
| Bounded actual-encoder and send-owner probes | `telnet/command_fixture_resources_test.go`; `scripts/command_fixture_support.py` | Exact native byte/work causes, fitted positive neighbors, CONFIG frame observations, original human final-send boundary and authenticated receipts; native/Linux ordinary/race owner checks |
| Failure cleanup and bounded execution | Driver session/case ownership and checked same-SSID reconnects | Negative controls retain original semantic errors, retire every newly owned session, reject missing persistence/references and never retry failed commands; final 12 clean child receipts |
| Efficient adjacent state checks | Detached complete CONFIG7 snapshot at every frame, including manual barriers | Mutated caller, wrong resource/schema, incomplete/error frame and diagnostic-refusal negatives; every actual command retains its post-frame/disk assertions; final source-bound qualification |

### v2/v3 evidence

- On baseline `df7dbd04` plus the approved patch, native Windows full ordinary
  and race each passed 13,656 cases with 98 skips across 76 packages (61 executed,
  15 build-only). Final targeted ordinary/race selections each passed all six
  CTY/identity cases without skips. The ordinary full run preceded only the
  diagnostic error-wrapping correction; final targeted tests and static checks
  cover that correction. The full race run includes it.
- Linux full ordinary and race each passed 61 test packages and built 16
  packages without tests. Real Linux identity/hard-link race tests passed.
  Linux amd64, arm64, 386, MIPS and MIPS64 identity test binaries compiled;
  cross-compiled binaries were not executed on those other architectures.
- Full vet, Staticcheck and golangci-lint passed on Windows and Linux; both
  lint runs reported zero issues. Windows runner self-tests passed 50 groups.
- Python 3.13.18/PyYAML 6.0.3 serial offline tests passed 144 tests. Real command
  integration failed against schema-seven behavior as described above. No
  command-integration profile qualified.
- Logs are in `.tmp/ci-fixtures-v2/` and `.tmp/ci-fixtures-v3/`. These are local
  results; no commit, push, hosted rerun, deployment or production-performance
  result is established. The latest hosted CI remains run 43914092794; nightly
  race 43914686754 passed at the same baseline.

| Approved item | Implementation | Validation |
| --- | --- | --- |
| v2 controlled ACL fixture | `internal/cluster/cty_protected_input_windows_test.go` | Persisted native ACL structure, exact O/G/D/control restoration, outer descriptor preservation, denial/hash/startup/positive publication and cleanup controls; native targeted/full ordinary/race |
| v2 portable identity conversion | `internal/logidentity/identity_linux.go` | Real nonskipped Linux identity/hard-link tests, five architecture compilations, full Linux ordinary/race and zero-issue lint |
| v3 valid positive login allocations | `scripts/test-command-fixes.py`, `scripts/test_command_fixes_test.py` | Six allocation/preflight/guard oracles; 144 serial offline tests; all six live profiles attempted, exposing the separate stale schema/model failure |

### v1 evidence

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
- [Follow-up CI 43914092794](https://github.com/N2WQ/GoCluster/actions/runs/43914092794)
  and [passing nightly race 43914686754](https://github.com/N2WQ/GoCluster/actions/runs/43914686754).
- [ADR-0322](../decisions/ADR-0322-local-user-login-ssids.md),
  [ADR-0329](../decisions/ADR-0329-nearby-boolean-predicate-and-generation-six.md),
  [ADR-0330](../decisions/ADR-0330-native-cc-banks-and-generation-seven.md).
- [Windows inheritance propagation](https://learn.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-setnamedsecurityinfow).
- [TSR-0126](TSR-0126-cty-grid-classification.md),
  [TSR-0127](TSR-0127-stabilizer-live-output.md),
  [TSR-0128](TSR-0128-shared-geography-startup.md),
  [TSR-0130](TSR-0130-main-test-firewall-path.md).

# Fixed Meta Bands: Approved Scope And Execution

## Authority And Boundary

The owner gave exact `Approved v1` on 2026-11-17 and requested branch
`meta-bands`. Implementation began from committed `fbde0e47`; this task did not
reset or overwrite unrelated worktree changes. The branch request supersedes the earlier
proposed isolated-worktree detail.

Concurrent unrelated resolver work committed `a0f0339f` on the selected branch
during initial validation. That commit and the remaining unrelated work were
preserved. Initial implementation ended without committing or pushing this
feature; the later feature-only integration is recorded below.

The approved feature comprises six fixed meta names, shared physical range
ownership, 4m recognition, GO and native CC filtering, history selection,
reconnect/preset/YAML retention, HELP BANDS and affected documentation. The
processor expands names into physical band identifiers and defines no ranges.

| Meta band | Members |
| --- | --- |
| WARC | 30m, 17m, 12m |
| HFCONTEST | 160m, 80m, 40m, 20m, 15m, 10m |
| HF | 160m, 80m, 60m, 40m, 30m, 20m, 17m, 15m, 12m, 10m |
| VHF | 6m, 4m, 2m, 1.25m |
| UHF | 70cm, 33cm, 23cm, 13cm |
| VHFUP | VHF and UHF members above |

The physical catalogue includes 4m at 70.000-70.650 MHz, 60m at
5.260-5.405 MHz and 1.25m at 220-225 MHz. Other GO physical ranges and the
13cm gap remain unchanged. Existing native CC labels, 37 slots, subslot ranges,
geography and saved profile meanings remain intact. No broad CC band-table
migration, generation bump, profile conversion or reset is authorized.

Exclude custom groups, additional meta names, 4m inferred-mode seeds,
VOACAP/path-model expansion, deployment and production data operations.

## Implementation And Evidence Map

| Approved unit | Implementation owner | Falsifiable evidence |
| --- | --- | --- |
| Shared catalogue and six memberships | `spot/bands.go`, `spot/meta_bands.go` | Literal memberships, owned snapshots, physical endpoint/adjacent-value and split-gap cases |
| GO expansion | `filter/rule_compile.go` | Canonical ordinary BAND lists, mixed lists, NOT/inequality, quoting, resource limits and atomic refusal |
| History selection | `commands/history.go`, `commands/history_selection.go` | Stored unknown-band 4m fixture, frequency classification before counts/pagination, retained query and unchanged archive bytes |
| Native CC integration | `filter/cc_edit.go`, prepared CC state and matcher | Independent edit-sequence oracle, exact endpoints, legacy slot goldens, false override against true native slot, OFF/base and bounded repeated edits |
| Saved state and readback | CC DTO/preparation/storage, schema-seven and native readback owners | Feature-bearing profile/preset/baseline/machine round trips, raw admission, rejection without replacement, omitted unused field and complete effective readback |
| HELP and support documentation | `commands/bands_help.go`, task links, package/operator/support docs and ADR-0356 | Both-style discovery, exact member rows, warning before mutation examples, history syntax and 78-column rendering |

Support-agent documentation impact: required. Routes are updated in
`customgpt/source-map.md`, `customgpt/developer-guide-index.md` and the
configuration-readback support card. ADR-0356 records the durable design;
no incident TSR is required.

## Review Disposition

The pre-approval design comparison used a separate non-steered reviewer. The
post-approval test-strategy review was informed by the supplied architecture,
not an independent normative review. Its findings refine validation within v1:
exact cell endpoints, an independent sequence oracle, feature-bearing fuzz and
maximum-state fixtures, guaranteed accepted machine round trips, old-reader
refusal evidence and comparable matching benchmarks/profiles.

Retained selection state must be bounded by a catalogue-derived fixed universe,
not edit history. Raw counts are checked before allocation; clone/equality,
fingerprint, counts, applied baselines and complete wrapped output include the
feature. Matching targets zero allocation and no new locks or name expansion.

## Initial Implementation Validation

The feature's targeted checks passed on the initial mixed checkout. That
checkout's repository-wide baseline was **not green**, because unrelated resolver
tests and analyzer findings remained. They were not repaired or waived within
this scope. The later clean-main integration below supersedes that baseline for
the feature-only publication.

| Check | Observed result |
| --- | --- |
| Targeted native Windows checks | Initial four-package selection: 141 passed, no skips. Corrected-state three-package selection: 80 passed, no skips. |
| Feature-bearing race selection | Four packages; 82 passed, no skips, including concurrent publication of additional CC selections. |
| Final complete telnet package | 2,710 passed, three existing skips. Includes raw-budget and first-error/duplicate-slot diagnostic precedence after the final decoder correction. |
| Final decoder diagnostics | 29 passed, no skips, including aggregate raw-budget field/column information after its final wording refinement. |
| UI/ULS and telnet continuation | Three packages; 2,999 passed, four existing skips; telnet was subsequently rerun as above. |
| Complete first-party package coverage | All 76 packages attempted across the full runner and its continuation. Only `internal/cluster` remains failing, as described below. |
| `go vet -p 4 ./...` | Passed after replacing the history view's lock-containing struct copy with the existing safe clone. |
| Staticcheck, affected packages | Passed for `spot`, `filter`, `commands`, `telnet`, `archive`, `dxsummit`, `dedup`, and `internal/correctionflow`. |
| Repository Staticcheck | Fails on pre-existing `cmd/rbn_replay/study_reference.go:79`, S1021; the same declaration is present in the starting commit. |
| Feature-file lint | Zero issues with the repository configuration, `--new-from-patch` and `--whole-files`. |
| Repository lint | Six unrelated replay/resolver/qualification findings: three range-value copies, two Staticcheck findings and one unused-parameter finding. |
| Parser/CC/storage fuzzing | Separate 15-second, two-worker runs passed: `FuzzMetaBandCCEdits` 2,885 executions; `FuzzMetaBandGOExpansion` 16,802; `FuzzV7StorageOwners` 21. These are bounded smoke runs, not exhaustive coverage. |
| Maximum retained/encoded selection | Fixed partition: 133 cells. Maximal exercised CC bank: 115,157 bytes; full profile with both applied baselines: 242,834 bytes. Near-limit complete human/machine readback also passed. |
| Executed old-reader compatibility | A decoder compiled before implementation accepted legacy profile/config/library fixtures and refused all three feature-bearing equivalents. Current unchanged upgrade refusal paths also preserved fixture bytes without reset/replacement. |
| Matching measurements | Both legacy and feature-bearing prepared matching measured zero bytes and allocations per operation. Controlled two-second CPU-profile runs measured 368.3 ns/op without additional selections and 396.4 ns/op with them, using the same maximum-geography fixture and frequency probe. |

The complete suite fails these three subcases of
`TestCorrectionSNRDeliveredPresenceAndDomain`: `newest_missing_supersedes_present`,
`equal_time_present_then_missing`, and `equal_time_missing_then_present`. The
resolver selects a different candidate from the existing SNR fixture's expected
identity. A read-only Go build overlay restored or removed all 33 feature Go
files while retaining the unrelated resolver work; the same three failures
reproduced. The overlay did not alter working files. All remaining packages were
then exercised separately, and the final telnet package was rerun after its
input-diagnostic correction. Thus this is a qualified feature delivery, not a
repository-wide validation pass.

The separate source review found no material implementation defect; its HELP
clarity finding was fixed by distinguishing GO/history `WARC,6m` from native CC
`WARC,6`. Lead verification additionally corrected the safe history clone and
preserved legacy machine-input error precedence. Final verification is
lead-owned, not an independent normative review.

CPU profiles are controlled benchmark-process measurements, not production
throughput, p99 latency, whole-process memory or external CC User qualification.
No hosted CI, deployment or production data operation was performed.

## Feature-only Main Integration (2026-11-17)

The owner authorized committing this feature, applying it to current main,
validating that combination, and fast-forwarding and pushing main. Source commit
`05a5a85b` preserves the original feature on `meta-bands`. Its 47 feature paths
were cherry-picked onto `3fd37c6c` on `integrate/meta-bands`, using the existing
`C:\src\gocluster` worktree and its stable Windows test paths. None of the six
unrelated call-correction commits was included. The local FCC status file stayed
outside the feature commit and retained its recorded SHA-256 during validation.

All feature source and tests were unchanged by the cherry-pick. The generated
runtime map was refreshed to describe this main-based checkout, removing entries
that belonged only to the excluded resolver work. This execution record was
updated to distinguish the earlier mixed-checkout results from this integration.

| Check on the main-based combination | Observed result |
| --- | --- |
| Complete native Windows lane | Passed: 13,756 cases; 98 reported skips; 61 executed packages and 15 build-only packages, covering all 76 first-party packages. |
| Previously failing SNR test family | Passed in full, including all three subcases that failed on the mixed resolver checkout. |
| Targeted race lane | Passed: 83 cases, no skips, across `spot`, `filter`, `commands`, and `telnet`, selecting `MetaBand\|V7ConcurrentBankPublication`. |
| `go vet -p 4 ./...` | Passed. |
| `staticcheck ./...` | Passed. |
| `golangci-lint run ./... --config=.golangci.yaml` | Passed, zero issues. |

The reported skips and the targeted scope of the race run remain evidence
boundaries; this is not a full repository race run or production qualification.
The earlier unrelated analyzer findings are absent on this combination. No
source fixes, relaxed assertions or new waivers were needed to obtain these
results. The earlier feature fuzzing and controlled benchmark evidence remains
recorded above with its original checkout and claim limits.

After this record update, the final write step regenerates all code maps, then
checks freshness and reviews the final diff before publication. The closeout
reports the actual pushed commit and hosted CI outcome separately from these
local results.

# TSR-0132 - Human Comment Report and Time Parsing

Status: Monitoring
Date Opened: 2026-11-12
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: Human spot parsing, telnet ingress, stored comments and report consumers
Trigger Source: Chat request
Led To ADR(s): ADR-0348, ADR-0349
Tags: human comments, SNR, split, CWT, wire timestamp, manual DX

## RCA Summary

- What happened: source review found that `CW UP 2` could acquire a 2 dB report,
  while meaningful time tokens and `CWT` could disappear from human comments.
  Human telnet input also allowed a comment clock to supply the spot timestamp.
- Why: human and skimmer sources used the same destructive comment cleanup.
  Bare-number recognition did not require a sign or adjacency, and human telnet
  structural parsing left its wire time inside the comment-parser input.
- What fixed it: the v1 repair introduces human-specific preservation and
  narrow signed FT8/FT4 shorthand, separates the terminal human wire time, and
  permits `+` only in the manual DX comment portion. The complete local Windows
  lane, targeted race checks, fuzz campaigns and static analysis passed.
  Deployment is not established by this record.
- How we know: the review traced the shared parser, all human callers, stored
  comment consumers and the path-learning entry point. Caller review also found
  that the telnet input whitelist would still reject manual positive reports
  unless that boundary changed too. Runtime incidence was not measured.
- Follow-up v2: bind human explicit dB reports to whole adjacent integers,
  preserve S9/59 and embedded readings as text, enforce the selected integer
  bound, and count/sample terminal-time rejection independently of queue loss.
  The decision and v2 qualification are recorded below.
- Operator/support answer: inspect the input source and complete incoming line.
  New human comments retain meaningful times and `CWT`; human telnet now needs a
  standalone final uppercase wire clock. Existing stripped archive text cannot
  be recovered, and accepted report syntax does not prove measurement provenance.

## Triggering Request

- Request date: 2026-11-12.
- Request summary: review an external end-user-experience audit, verify its
  comment/report findings, and implement the owner-selected bounded repair.
- Request reference: owner selected narrow shorthand, rejection without usable
  wire time, the terminal-time convention and its ambiguity, then supplied exact
  `Approved v1` for the Human comment integrity Scope Ledger.

## Symptoms and Impact

Human split information could be moved into report metadata. SNR expressions
use any present report, so a fabricated report could affect filtering. COMMENT
filters and history searches use the stored cleaned comment, making removed
times and `CWT` unavailable to those searches. A fabricated eligible report
could also reach existing path-learning checks; no prediction error or
production prevalence was established by the source review.

For human telnet lines, an earlier comment time could override the later wire
time and make an otherwise current spot appear stale. Native peer relay remains
a separate original-field path, and peer/DXSummit timestamp fields were already
independent of comment times.

## Timeline

1. 2026-11-12 - External review identified human-comment cleanup and report
   concerns. Current-source review confirmed the principal mechanisms and found
   the additional human telnet route.
2. 2026-11-12 - Independent primary-source review separated report syntax and
   WSJT-X units from decoder provenance and measurement credibility. The owner
   chose the inclusive -30 through +30 shorthand range as product policy.
3. 2026-11-12 - Scope challenge found the manual `+` admission gap and mixed
   time/speed recognition-state hazards. The owner approved Scope Ledger v1.
   Implementation and lead validation used an isolated worktree. Separate source
   review found and rechecked the adapter adjacency repair before closeout.

## Hypotheses and Tests

1. Correcting only the shared parser's final bare-number condition is sufficient.
   - Evidence: the same parser also removes times and mode aliases and rearranges
     speed text. Human callers and diagnostic helpers require explicit routing.
   - Outcome: Rejected. Human preservation and caller coverage are required.
2. Accepting a report in filters but excluding it from learning solely because
   it lacks literal `dB` is the intended behavior.
   - Evidence: the owner selected one accepted numeric meaning throughout, with
     existing model eligibility checks retained.
   - Outcome: Rejected by the selected product policy.
3. Parser support for `FT8 +02` proves live manual DX accepts it.
   - Evidence: the ordinary telnet input whitelist excludes `+`; direct Processor
     tests bypass that boundary.
   - Outcome: Rejected. Native and decoded reader-to-dispatch coverage is needed.
4. A terminal `HHMMZ` unambiguously proves that the upstream supplied a wire time.
   - Evidence: missing wire time plus terminal prose can produce identical bytes.
   - Outcome: Rejected. The owner accepted this terminal-field convention and
     its limitation; earlier comment clocks never become a fallback.
5. The external spot sample establishes a general defect rate.
   - Evidence: the cited sample was small and was not independently reproduced
     here or shown representative of this installation's traffic.
   - Outcome: Inconclusive for incidence. Do not report it as a measured rate.
6. Preserving adjacency inside the shared human parser is sufficient for every
   adapter.
   - Evidence: source review found that the minimal telnet adapter discarded
     punctuation-only tokens before parsing. `FT8 ... -14` could therefore
     become `FT8 -14` and acquire a report despite the direct parser refusing it.
   - Outcome: Rejected. Human structural extraction now retains raw token
     barriers; the legacy RBN branch retains its cleaned-token behavior. An
     actual adapter-to-queue regression covers this distinction.

## Findings

- Root cause: skimmer-oriented metadata extraction and destructive cleanup were
  applied to human prose without a separate preservation contract. Structural
  human telnet time ownership was delegated to that same comment scan.
- Contributing factors: signed/unsigned acceptance in the numeric helper, mode
  aliases with human meaning, and reader restrictions outside the parser made a
  caller-only or parser-only repair incomplete. Time/speed recognition also
  changes pending numeric state; preserving text must not discard that state.
  Structural normalization can also create false adjacency before the shared
  parser sees the input, requiring caller-level evidence.
- This changes durable parser, compatibility and operator behavior, so
  [ADR-0348](../decisions/ADR-0348-human-spot-comment-integrity.md) records the
  selected contract. Existing source classification is unchanged.

## Decision Linkage

This section records the original v1 decision. The v2 follow-up below partially
supersedes its explicit-dB compatibility clause through ADR-0349.

- ADR created: [ADR-0348](../decisions/ADR-0348-human-spot-comment-integrity.md).
- Human reports without `dB` require signed adjacent FT8/FT4 shorthand within
  the chosen range and taxonomy permission. Explicit-dB behavior is preserved.
- Human comments retain meaningful text; human telnet requires the selected
  terminal wire grammar. Missing or malformed wire fields reject before queueing.
- Existing today-UTC and future-skew conversion remains, including its possible
  arrival-time clamp for a syntactically valid future clock. No archive rewrite,
  new setting, learning algorithm or RBN cleanup change is part of the repair.

## Follow-up: Explicit Reports And Rejection Diagnostics (v2)

The external follow-up review was checked against `e0904f5` in the existing
`C:\src\gocluster` checkout. Legacy explicit-dB recognition, deliberately
preserved in v1, bound a separate `dB` to the first pending number instead of
its immediate neighbor. `UP 2 -12 dB` therefore reported 2 and lost the split.
The fallback extracted embedded numeric suffixes, retained their displayed text
and bypassed the ordinary -200..+200 integer bound. S9/59 strength forms could
also become report metadata. These were retained behaviors, not v1 regressions.

Human final-time rejection returned without a reason-specific counter or log.
The existing `SpotDrops` counted queue overflow only. Before any accepted spot,
recent incoming lines could still keep health marked active. Changing the idle
classification was not selected; the new independent rejection diagnostics make
the parse failure visible while retaining that existing label contract.

The owner selected both slices, whole-token explicit syntax, embedded readings
as text, and consistent inclusive -200..+200 explicit bounds, then authorized
`Approved v2 for unattended execution`. No new worktree was created.
[ADR-0349](../decisions/ADR-0349-human-explicit-report-and-rejection-diagnostics.md)
partially supersedes the v1 explicit-dB and retained-state clauses.

Human reports now bind only whole adjacent integers and units. Preserve S9/59
readings, invalid candidates, orphan units and embedded strings; keep the first
valid explicit report before shorthand and remove only its number/unit.
Retain fused time text and actual selected-mode ownership. RBN recognition and
traces remain separate and unchanged. This applies to new rows, not past rows.

At the existing wire-time gate, a fixed client-owned counter advances once per
rejection, independent of sampled logging and queue loss. A quoted, bounded
head/tail sample is attempted first immediately and at most once per 30 seconds
per client thereafter. `unusable_wire_time` samples include the current total;
health snapshots/log formatting expose `wire_time`. Reconnects keep the total,
replacement clients/processes reset it, and bounded logging can discard samples.
Use an upstream capture when the sample is truncated or unavailable.

The separate non-steered primary-source review established differing S9 and
WSJT SNR references, not the provenance of arbitrary human comments. The bounds
are product guardrails. No production incidence, calibration or prediction
accuracy claim follows from these changes.

### v2 qualification

The final Go state was qualified locally on native Windows/amd64 with Go
1.27.1, Staticcheck 2026.2.1 and golangci-lint 2.14.0. The checkout remained
`C:\src\gocluster`, branch `human-comment-integrity`, based on `e0904f5` plus
the approved local diff. Go source/test hashes stayed unchanged through final
qualification. These are local results, not deployment, production-feed,
external-interoperability, calibration or p99 evidence.

- Complete ordinary and race lanes each passed 13,032 cases across 74
  first-party packages (59 executed, 15 build-only), with 96 skips and no race
  report. Both used `scripts/test-windows.ps1 -Timeout 10m`, adding `-Race`
  for the second run. Skips include 56 external DXSpider cases, separately
  invoked subprocess helpers and optional data/live/profile/soak qualifications.
  No required human-comment, explicit-report or wire-time case was skipped.
- `go vet ./...`, `staticcheck ./...` and
  `golangci-lint run ./... --config=.golangci.yaml` passed, with zero lint issues.
- Four 20-second two-worker fuzz campaigns passed: raw human invariants
  (1,920,999 executions), shorthand (1,463,692), constructed explicit-report
  forms (2,061,266) and frozen legacy differential (1,182,150), totaling
  6,628,107 executions. The human expectations use literal or independently
  constructed declared forms, not the legacy parser's superseded behavior.
- Guarded allocation benchmarks kept ordinary legacy parsing at 1000 B/op,
  13 allocations/op, and Unicode at 1032 B/op, 17 allocations/op. Human bare
  parsing changed from 408 B/op, five allocations to 392 B/op, three allocations;
  explicit parsing from 480 B/op, six allocations to 416 B/op, four allocations.
  Dense-input guards passed. CPU timings varied; no production speed claim is made.
- The new counter/log regression failed before implementation. New human
  literal cases also reproduced the old number-binding and substring errors
  before the report fix. An initial consumer-display assertion incorrectly
  required text beyond the established wire width; only that test oracle was
  corrected. Complete source text remains checked before formatting, through
  fanout and after a real archive reopen/search. One separate missing-DX fixture
  was corrected because its `1200Z` token matched the existing callsign scanner;
  production structural parsing was not changed for that unrelated behavior.
- Separate Go source review found no material issue. That reviewer inherited
  design context; the review is not described as independent scientific or
  performance evidence. The lead's fresh final pass checked actual source,
  approved boundaries and qualification hashes. Local logs/checkpoints are in
  the ignored `.tmp/human-comment-v2/` directory; this record retains the durable
  result independently of those machine-local artifacts.

| Approved v2 item | Implementation | Distinguishing validation |
| --- | --- | --- |
| Per-source terminal-time diagnostics | `rbn/client.go`, `rbn/human_wire_time.go`, `internal/cluster/ingest_health.go` | Actual gate rejects/valid recovery, two-client burst isolation, real reconnect retention, concurrent snapshots, queue-loss separation and bounded logger admission/drop witnesses; full ordinary/race |
| Whole adjacent human explicit reports | `spot/comment_parser.go`, `spot/human_comment_parser.go` | Literal range/adjacency/strength/time/alias cases, structured and raw fuzzing, actual manual/peer/DXSummit/telnet adapters, archive reopen/search, display/filter/model-capable FT8 consumer witnesses, frozen RBN differential/trace checks and guarded benchmarks |

The v1 evidence below is historical and is not substituted for v2 results.

## v1 Verification and Monitoring

The lead ran validation in the isolated `human-comment-integrity` worktree,
based on `91b3740d`, with Go 1.27.1, Staticcheck 2026.2.1 and golangci-lint
2.14.0. The final complete Windows lane passed. This is local qualification,
not a production-delivery or external-interoperability claim.

- `scripts/test-windows.ps1 -Timeout 10m` passed across 74 first-party packages:
  59 executed and 15 build-only, with 12,909 passed cases and 96 skips. Skips
  include separately invoked subprocess helpers, optional/qualification cases
  and external DXSpider tests requiring `DXSPIDER_ROOT`/`DXSPIDER_PERL`. Those
  skips do not establish external interoperability. Required human-contract
  checks had no skips in the targeted lanes.
- `go vet ./...`, `staticcheck ./...` and
  `golangci-lint run ./... --config=.golangci.yaml` passed on the final Go state.

- The targeted ordinary runner passed 551 cases in seven packages with no
  skips after the adapter adjacency repair. Required identities covered actual
  human caller routes, terminal-time rejection, both telnet codecs and argument
  orders, real archive reopen/search, filtering and path-learning fanout.
- The targeted race runner passed 553 cases in the same seven packages with
  no skips or race reports. This included both concurrent immutable-scanner
  tests and the human and legacy allocation envelopes.
- Four 20-second, two-worker fuzz campaigns passed: raw human invariants
  (1,311,943 executions), signed shorthand (1,266,563), frozen legacy
  differential (827,938), and DX comment boundary (2,462,623).
- Guarded `-benchmem` samples measured the human bare-report case at 408 B/op,
  five allocations/op, and explicit-report case at 480 B/op, six allocations/op.
  A baseline image built from the original `91b3740d` parser through an ignored
  Go overlay retained the existing benchmark fixtures. Legacy allocation counts
  did not increase; ordinary input remained 1,000 B/op and 13 allocations/op.
  Long-input bytes/op varied between samples; these component measurements and
  input-sized envelope tests are not whole-process or production latency proof.
- The first complete lane stopped at `TestGeographyStartup/prediction_off`
  because the fresh worktree lacked ignored configured RBN reference assets.
  Four assets (global/CW/RTTY receiver weights and the confusion model) were
  copied byte-for-byte from the existing checkout into the isolated worktree.
  No code or configuration workaround was made. The startup recheck then passed
  all eight cases with no skips, and the full lane was restarted.
- A separate read-only source review found the adapter adjacency defect and
  verified its repair. The lead retains source-completeness and final-lane
  authority; the reviewer did not run tests or change files.

Approved items map to concrete evidence:

| Item | Implementation | Distinct evidence |
| --- | --- | --- |
| Human parsing/preservation | `spot/comment_parser.go`, `spot/human_comment_parser.go` | Full-result vectors, taxonomy permission, semantic fuzz, frozen legacy oracle |
| Human adapters | `commands/processor.go`, `peer/parse.go`, `dxsummit/client.go`, `rbn/client.go` | Queue/frame/API routing and diagnostic helper assertions |
| Manual positive input | `telnet/comment_input.go`, `telnet/machine_input.go` | Native/decoded reader-to-dispatch tests, independent alphabet and editing/recovery checks |
| Terminal wire time | `rbn/human_wire_time.go`, `rbn/client.go` | Conflicting/missing/malformed/fused clock cases with no-queue rejection |
| Existing consumers | Existing formatter/filter/archive/path production paths | Real archive reopen/history searches and parsed-human consumer propagation |
| Durable/support documentation | ADR-0348, this record, manuals and support routes | YAML comment-only, troubleshooting, crawler-comment and support-agent checks |

Inspect complete upstream lines when spots disappear: a suffix after the time,
lowercase `z`, a fused clock/grid or a malformed clock is rejected under this
wire convention. Do not infer production delivery, empirical accuracy or
model calibration from local source/tests. Reassess on legacy RBN behavior
changes, cross-command `+` permission, unexpected comment loss, or a feed format
outside the selected grammar; do not silently broaden the fallback policy.

## References

- [Human comment contract](../../spot/README.md#human-spot-comments).
- [Human telnet wire time](../../rbn/README.md#human-telnet-wire-time).
- [Manual DX admission](../../commands/README.md#manual-dx-admission).
- [Domain contract](../domain-contract.md#human-spot-comments-and-wire-times).
- [ADR-0069](../decisions/ADR-0069-single-spot-taxonomy-yaml.md),
  [ADR-0091](../decisions/ADR-0091-peer-dxsummit-skimmer-marker-stripping.md),
  [ADR-0284](../decisions/ADR-0284-original-content-peer-publication.md), and
  [ADR-0332](../decisions/ADR-0332-per-user-rbn-cw-speed-display.md).

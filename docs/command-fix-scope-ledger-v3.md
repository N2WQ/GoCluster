Approval record: the user supplied `Approved v3` on 2026-10-09. This retains the
agreed proposal. Implementation status and checkout drift are recorded in
[the execution record](command-fix-execution.md).

# Proposed Scope Ledger v3

V3 supersedes v2. It retains the selected behavior and patch boundaries, while tightening temporary settings, reputation penalties, shutdown ownership and YAML compatibility evidence.

## Objective and execution

Make existing commands consistent across execution, HELP, configuration, persistence and outcome reporting. Accepted input must have the intended effect; rejected input must have no unintended effect; feedback must distinguish live, saved and effective state.

After approval, implementation proceeds **one checkpoint at a time**, with its regression coverage, applicable validation and direct diff review before advancing. Routine testing requires no action from you.

Necessary documentation and support comments belong to the patch that changes the behavior. A checkpoint may be divided into smaller commits within its approved boundary. Material new behavior, policy or scope requires a revised ledger.

Dependent checkpoints require reverse-order or combined rollback.

## Four clarified contracts

### 1. A refused existing record does not automatically invalidate a fitting human change

These are separate failure cases:

| Situation | Required result |
|---|---|
| The **proposed change** exceeds its limits or is invalid | Reject it with zero live or durable effects. |
| The proposed human change fits, but the **existing stored record** is oversized or over-expanded | Allow the temporary live change, warn explicitly that it was not saved, and preserve the stored file. |
| A valid, admitted human change encounters an ordinary save failure | Keep it live and report the save failure. |
| A machine change or LOAD cannot prepare and persist its destination record | Fail without publishing the change. |
| SAVE PRESET cannot prepare and admit its association | Refuse before writing the preset library. |

Temporary human preparation uses bounded, detached session data. Its configuration, baseline and temporary record representation must still pass their own admission checks.

A refused record and cached fallback metadata **never authorize overwriting that record**, including during logout. Already-connected sessions retry authoritative storage preparation on later commands. Preserve existing protected-login behavior.

Acceptance must check actual live readbacks, unchanged stored bytes, later retries, machine/LOAD rollback and SAVE’s lack of library effects.

### 2. Reconnect preserves spent allowance; elapsed time can replenish it

Login and posting share baseline station reputation across SSIDs. Configuration remains full-SSID.

For existing active station state:

- A benign reconnect preserves spent counts and the active window, except for normal elapsed-time advancement.
- Replenishment occurs through the existing elapsed-window rules, not because login occurred.
- Configured risk penalties may tighten restrictions or extend probation/ramp deadlines.
- Applying a penalty must not clear spent counts or shorten an existing applicable deadline.

Tests cover an exhausted station reconnecting through another SSID, immediately before and at a natural window boundary, and a new-ASN penalty after allowance is spent.

This protects **active runtime state**. The existing history store persists ASN/country history, not posting counters. V3 does not add counter persistence across restarts or merge old SSID history files.

### 3. Shutdown has one classifier-result owner and an explicit handoff boundary

The output pipeline owns consumption of completed classifier results during the existing grace window. Terminal classifier cancellation follows that opportunity to drain.

“Deliver” means:

> Apply the result, process the spot through normal filtering and downstream stages, and attempt the existing handoff.

It does **not** guarantee network receipt or prevent existing bounded-queue drops.

Before downstream resources used by the pipeline stop or close:

- Retire temporal/FT holds, including holds created by late classifier results.
- Stop and join the stabilizer scheduler and release consumer.
- Establish completion of pipeline-owned processing.
- Cancel remaining classifier HTTP work, join workers and discard unfinished jobs/results.

The same ownership applies during partial-startup cleanup. The classifier grace window is **not a deadline for the entire shutdown**.

Tests distinguish attempted handoff, normal suppression, queue saturation and network receipt. They must not require every classified spot to appear on a socket.

### 4. YAML optimization preserves refusals as well as accepted values

Retain yaml.v3 parsing and scalar conversion. Limit the new machinery to stored configuration and expensive bulk-map decoding.

Compatibility includes the current excessive-alias refusal caused by reachable merge processing, even when the eventual fields are unknown or ignored. Removing unknown fields before checking that behavior could silently admit input currently refused.

Checkpoint 04a therefore includes:

- The review’s reported trap: a root merge causes excessive-alias refusal; removing it leaves the unused anchors acceptable.
- A minimized fixture whose expected behavior is first observed on the pinned loader.
- Small, hand-counted work fixtures covering ordinary nodes, scalar bytes, aliases, merges, skipped values and nested baselines.
- Accounting independent of implementation passes and optimizations.
- Admission before expensive materialization or repeated expansion.

**The exact 325-byte document was not supplied or reproduced in this pass.** Its reported behavior becomes a required baseline regression, not a claimed verification result.

Scale and allocation checks must establish the operational usefulness of the selected limits. If they expose inadequate protection or unacceptable cost, stop and revise the scope rather than silently changing limits.

## Preserved decisions and limits

| Area | Contract |
|---|---|
| Human input recovery | Reject the complete physical line. Recover within **64 KiB of wire data and five seconds**, with any earlier deadline taking priority. Disconnect when recovery is unsafe or unsuccessful. |
| MODE | Named visibility deltas; preserve every unlisted mode, including custom tokens and future taxonomy fallback. Other finite families retain existing transitions. |
| Callsign rules | Preserve deny precedence and existing supported wildcard matching. |
| Manual DX | Shared baseline reputation, baseline display, generic rejection feedback, finite canonical frequency within shared Spot bounds before admission/queue effects. Preserve interior-gap behavior. |
| GRID | Exact valid four/six-character input; truthful mapping availability. Active NEARBY requires usable resulting cells before publication. |
| SOLAR | Human SET deliberately reapplies scheduling, even for unchanged cadence. Unchanged machine cadence preserves progress; LOAD resets scheduling. Report availability and activity truthfully. |
| Toxicity zeros | Explicit zero selects existing defaults. Required missing/null values remain invalid; an absent optional file disables the feature. Reject unrepresentable positive durations before conversion or doubling. |
| Historical tools | Descriptive counts and observations; recognized/unknown outcomes and named denominators. No unsupported accuracy, calibration, reliability or optimal-setting claims. |
| Python comparison | Disjoint correction-day UTC cohorts; adjacent-day context reported separately. Inclusive elapsed 300-second matching. Preserve first-match and many-to-one rules. |
| Peer prototype | Absolute phase deadlines, one session-owned keeper, canceled stalled I/O and joined workers. Preserve protocol compatibility and timestamp format. One-session errors exit nonzero; deliberate local cancellation exits zero. |

### Authoritative resource measurements

These remain fault limits, not expected configuration sizes.

| Resource | Admission rule |
|---|---|
| Configuration or applied baseline | **262,144 bytes**, using existing standalone v4 `SavedPreset` serialization |
| Raw user record | **1,048,576 actual bytes read** |
| Canonical loaded user record | **1,048,576 bytes**, after existing migration/normalization, including metadata and baseline |
| Prospective user record | **1,048,576 exact final prepared bytes** |
| Preset collection | Preserve existing **20 entries and 8 MiB** |
| Machine message | Preserve existing **64 KiB** |
| Record decoding work | **2,097,152 units**, shared across the whole record and baseline |
| Collection decoding work | **16,777,216 units**, shared across the whole collection |

Configuration measurement excludes machine envelopes, transport formatting, revisions and runtime caches. The admitted bytes must be the bytes committed.

Work remains **P + E**:

- **P:** Physical AST nodes through `Content`, without following alias pointers; one unit per node plus decoded scalar UTF-8 bytes, including ignored fields and unused anchors.
- **E:** Logical typed-decoding visits and scalar bytes, including alias/merge traversal needed to interpret keys and materialize values. Preserve existing skipped-value behavior.
- Charge the logical work once, regardless of internal optimization passes. Nested decoding shares the document budget.

Retain yaml.v3’s separate existing alias guard and decoder reset boundaries. Reachable cycles are refused; ignored legacy branches retain their compatibility behavior.

These units are not megabytes or measured memory. A small file can still be refused. Refused stored files are preserved.

Machine YAML already rejects anchors, aliases and merges. There is no separate production standalone-preset YAML reader needing another limit.

## Sequential checkpoints

Each checkpoint includes necessary tests and documentation.

### Bootstrap, storage and human commands

| ID | Scope and acceptance |
|---|---|
| **00** | Bootstrap the unattended runner and real checkout telnet-server fixture. Verify build/tool identity, native race capability, isolated configuration/stores, child readiness and joined cleanup. Establish this before the first fix. |
| **01** | Repair rejected-line recovery. Raw-wire cases include invalid input, excessive input, Telnet negotiation/subnegotiation, editing, buffered reads and deadlines. Assert zero effects and exactly one subsequent valid dispatch; unsafe recovery disconnects. |
| **02** | Correct NOFILTER reset and restoration state. Verify effective geography, cleared NEARBY snapshots, persistence and reconnect. Preserve RESET FILTER’s distinct defaults. |
| **03** | Report filter, COMMENT, reset and DIALECT save failures. Exercise actual preparation/write failures, protected settings and unchanged choices. Retest live/durable distinctions after checkpoint 04. |
| **04a** | Compatible storage decoding and work accounting, **new thresholds inactive**. Differential coverage includes legacy/marked records, baselines, duplicate and interpreted keys, merges, nulls, ignored branches, migrations, cycles and old alias-guard boundaries. Include the merge trap and hand-counted work fixtures. |
| **04b** | Exact-byte preparation and commit. Preserve existing publication ordering. Verify metadata/IP/timestamp merging, immutable admitted bytes and real commit fault injection. |
| **04c** | Detached human preparation/publication, **new thresholds inactive**. Preserve ownership and runtime effects. Test rejected-command atomicity, human SOLAR rescheduling, unchanged machine cadence, LOAD scheduling and NEARBY restoration. |
| **04d** | Activate complete raw/canonical/work admission together. Cover login, warm sessions, logout, human/machine changes and all preset verbs. Enforce the clarified temporary-change contract. SAVE admits library and association before either durable write; quota refusal has zero effects, later I/O failure may have documented partial success. |
| **05a** | Shared station reputation and non-replenishing reconnects. Test spent allowance, active windows, natural boundaries, penalty tightening/deadline extension, history ownership and untouched old SSID files. Document runtime-only counter protection. |
| **05b** | Generic manual-DX rejection response, with no queued spot. Accepted controls preserve baseline display. |
| **06** | Canonical frequency validation before effects. Cover exact/neighboring bounds, rounded zero, NaN/infinity, overflow and accepted interior gaps. Invalid values consume no admission allowance. |
| **07** | MODE visibility deltas. Cover open/restrictive/false-only/block-all states, latent entries, CW/FT8, custom/unseen tokens, taxonomy changes and reconnect. |
| **08** | NEARBY DX-or-DE matching. Cover missing, invalid and unmappable opposite endpoints, both absent, and fine/coarse live/history consumers. |
| **09** | Exact GRID syntax and truthful availability. Isolate missing mapping support in a fresh process; enforce active-NEARBY cell preparation. |
| **10** | Human callsign-pattern grammar consistent with matching and machine configuration. Reject mixed invalid lists atomically; preserve existing exact/all and single prefix/suffix wildcard forms. |
| **11a** | Validate original CC/filter arguments before normalization discards tails. Exercise real dispatch for toggles, `/ON`, `/OFF`, mode shortcuts and aliases. |
| **11b** | SHOW/SH DXCC and BYE/QUIT/EXIT arity. Invalid DXCC performs no lookup; malformed logout retains a usable connection; valid aliases close normally. |
| **12** | Fixed-arity settings, including SET DIAG. Invalid input preserves state; retain valid CONFIDENCE diagnostic alias and positive controls for every setting. |
| **13** | Configured/effective DEDUPE readbacks. Saved SLOW on FAST-only nodes remains identifiable without read-induced revision or persistence effects. |
| **14** | SOLAR outcome wording. Disabled manager reports effective zero; calm conditions do not promise tick messages; active summaries follow the selected schedule. |
| **15** | Preserve configured MODE/EVENT underscores after login. Keep login and machine identifier restrictions. |
| **16** | COMMENT alias/history-clause distinction. Alias first/after comma is a value; otherwise COMMENT begins the literal clause. Test absent aliases, repeated words and continuation-state preservation. |
| **17** | Category-specific CC HELP grammar. Execute advertised forms; remove unsupported COMMENT aliases and misleading blanket placeholders. |
| **18a** | Complete HELP routing and aliases using bidirectional, independently dispatch-derived coverage. Include SH forms, argument-bearing topics, CC fallback, dynamic modes and slash readbacks in both dialects. |
| **18b** | Correct suggestions, categories and wording: MINSNR/COMMENT, US/Canada states, actual DX restrictions and MODE recipes. Synchronize README HELP and both dialects. |
| **19** | Token-based SHOW HOLD with strict arity. Check whitespace/case, pause counters, delivery timing and zero effects from extra tokens. |

### Retirement, operator tools and reconciliation

| ID | Scope and acceptance |
|---|---|
| **20a** | Terminal classifier Stop: linearized admission, canceled HTTP requests, joined workers, retired queued/results references and zero pending/backlog. Test header/body stalls, concurrent/idempotent Stop and blocked publication. Eligible post-Stop submissions fail open; preserve pre-Start admission and bounded cache behavior. |
| **20b** | Production runtime owns pipeline stop/completion. One owner drains classifier results before terminal cancellation. Process results normally and attempt handoff; retire late FT holds and join stabilizer delivery before dependent resources close. Test normal and partial-startup cleanup, real CW/FT paths, suppression, queue drops and downstream-operation barriers. |
| **21** | Toxicity zero/default and duration handling. Defaults remain timeout 1200 ms, workers 2, queue 128, cache 4096, TTL 86400 seconds and comment 512 bytes. Test missing/null/negative values and unit-specific representability; preserve constructor semantics. |
| **22** | Root executable help/version without configuration/startup; unknown arguments exit 2. Preserve version-token-anywhere precedence. |
| **23** | Sunspot watcher one-shot failures exit nonzero. Local successful/failing fetch/output fixtures; preserve continuous retries and wrapper propagation. |
| **24** | `rbnskewfetch` fetch/parse/write failures exit nonzero; successful output remains correct. |
| **25** | `prop_report` generation/output failures exit nonzero. Preserve optional fallback behavior without external LLM calls in tests. |
| **26** | Distance-3 analysis becomes input-derived and descriptive. Mixed/unknown outcomes, empty input, SQL failures and explicit assumptions; remove fixed calibration/optimality conclusions. |
| **27** | Threshold simulation reports added/removed/net sets correctly. Cover heterogeneous baselines, unknown outcomes and empty denominators; remove unsupported stability predictions. |
| **28a** | `analyze1a` queries both identities and winner fields over the complete interval, including witnesses beyond 50 rows. Reject explicitly supplied unused `-spots`, including empty values. Nested failures cannot become “no observation.” |
| **28b** | Repair `analyze1c`: recognized case-only outcomes, NULL/empty/other unknowns, named denominators, applied-only summaries and recognized rejection reasons. Empty statistics are unavailable; remove reliability/calibration advice. |
| **29** | Decision inspection uses consistent recognized/unknown classifications and samples; query/scan/iteration failures return unsuccessful status. |
| **30** | Reappearance investigation handles query/scan/iteration/close failures and deterministic ties. Explain first-100/first-frequency limitations without simultaneous-station claims. |
| **31** | Diagnostic wrapper uses actual `DXC_PPROF_ADDR`, explains effective GOGC precedence and propagates child failures. Verify through native PowerShell child fixtures. |
| **32** | Python UTC/date/cohort repair. Test offsets, midnight/year boundaries, inclusive 300-second matching, frequency boundaries, core/padding counts, duplicate record identities and many-to-one associations. Report coverage limitations; retain the existing matching heuristic. |
| **33** | Dashboard H/h/? through actual tcell input capture; preserve other bindings. |
| **34a** | Peer session/writer/keeper ownership. Test blocked reads/writes and Telnet callbacks, repeated establishment, error propagation and joined retirement before reconnect. Preserve selected exit behavior. |
| **34b** | Absolute peer phase budgets begin before phase writes. Cover silent/partial/chatty/buffered input and repeated markers; retain both establishment paths and later compatibility responses. Outbound keepalive does not refresh inbound liveness. |
| **34c** | Timestamp generator survives reconnect; allocation and actual wire publication share serialization. Test same-second concurrency/reconnect, allowed gaps, unsafe-clock/rate failures and midnight wrap. |
| **35** | Complete implementation/docs/evidence reconciliation, cumulative checkout-server cases and final validation. Every identified finding and inventoried surface receives a repaired, preserved or explicitly deferred disposition. |

Dependencies include **04a → 04b → 04c → 04d**, **20a → 20b**, and **34a → 34b → 34c**. Later changes rerun affected prerequisite coverage.

## Coverage and original decision dispositions

Coverage includes both dialects, all 29 filter families, session/settings/history commands, all preset verbs, YAML schemas 1–4 and capability discovery, the root executable, all 22 Go utilities, dashboard keys, Python comparison and product-facing wrappers.

The 29 families are BAND, MODE, MINSNR, SOURCE, EVENT, COMMENT, DXCALL, DECALL, CONFIDENCE, PATH, DXGRID2, DEGRID2, DXCONT, DECONT, DXZONE, DEZONE, DXDXCC, DEDXCC, DXSTATE, DESTATE, BEACON, WWV, WCY, ANNOUNCE, SELF, TOXIC, NEARBY, NOFILTER and DXBM.

The utility inventory includes `analyze1a`, `analyze1c`, `analyze_distance3`, `callcorr_reveng_rebuilt`, `codemap`, `ctylookup`, `h3gen`, `inspect_decisions`, `investigate_reappearances`, `loadharness`, `peerdiag`, `peerpoc`, `peerprobe`, `prop_report`, `rbn_replay`, `rbnskewfetch`, `release_readme`, `simulate_threshold`, and the four VOACAP utilities.

| Original decision | V3 disposition |
|---|---|
| **D01** | MODE deltas preserve unlisted visibility; other finite families retain transitions. |
| **D02** | Deny precedence remains. Teach opening/resetting before constructing a named allowlist; retained wildcard denies continue blocking named allows. |
| **D03** | Freeze schemas and hidden-field preservation. Document `GET YAML CAPABILITIES SCHEMA 4`; add no wire fields. |
| **D04** | Preserve complete machine-candidate validation and document unavailable-setting repair paths. Do not impose it on unrelated human commands. |
| **D05** | Defer new human DEFAULT/reset vocabulary; document existing routes. |
| **D06** | Defer independent geography capability; report mapping dependencies truthfully. |
| **D07** | Preserve large LOAD eligibility subject to configuration/destination admission. Do not promise full machine readback will fit. |
| **D08** | Human admitted changes remain live with warnings; machine/LOAD persist before publication; invalid candidates have zero effects; DX rejection receives feedback. |

Utilities without an identified defect receive a preserved disposition, not speculative changes. Maintainer/qualification tooling retains its existing contracts.

## Validation and evidence

The bootstrap uses the checkout’s real telnet reader, dispatch, filters, persistence and history paths, with a held private loopback listener, isolated stores/configuration and local service fixtures. Process-global availability cases use separate child processes.

The runner verifies the expected child and build identity, records responses and effects, checks restart readbacks, and joins the child before deleting stores. Forced termination is reported as cleanup failure rather than graceful retirement evidence.

Known unfixed defects remain pending. Skips, empty test selections and missing capabilities do not count as passed.

Each production-Go checkpoint uses targeted tests and the applicable complete baseline from [the development runbook](C:/src/gocluster/docs/dev-runbook.md):

```text
go test ./...
go vet ./...
staticcheck ./...
golangci-lint run ./... --config=.golangci.yaml
```

Add race validation for shared-state/lifecycle changes, fuzzing for changed parsers/protocols, and storage scale/allocation evidence. Script-only checkpoints use native checks and behavioral fixtures.

After exact approval, complete the detailed contract-to-test challenge before implementing the affected checkpoints. Final validation includes cumulative server tests, applicable race/fuzz evidence, code-map generation/checking, direct diff review and `git diff --check`.

The separate non-steered scientific review already obtained remains the basis for descriptive claim boundaries. This pass adds no independent scientific validation. Missing validated outcomes and coverage still prevent accuracy/calibration claims.

## Boundaries and approval

No deployment, external live-cluster testing, production-data migration, correction-model redesign, dependency replacement or workflow-policy changes are authorized by this scope.

No policy selection remains open. The exact reported YAML fixture remains an evidence item to reproduce against the pinned decoder before changing its behavior; it is not a claimed completed check.

Implementation requires the version-specific authorization specified by [AGENTS.md](C:/src/gocluster/AGENTS.md:49).

Exact authorization required before implementation: Approved v3

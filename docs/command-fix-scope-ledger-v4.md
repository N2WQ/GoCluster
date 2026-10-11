# Scope Ledger v4 - Three Command Review Follow-ups

Approval: the user supplied `Approved v4` on 2026-10-10 and explicitly requested
branch creation, switching and implementation. The approved branch clarification
selects `fix_commands_review` from `fix_commands` at
`4d711a4f15c78f39f4c07cf55c17e6e79b8ce458`, replacing the proposal's `9c22cd7`
baseline. The three fixes and acceptance requirements are unchanged.

## Objective and execution

Correct R1, R2 and R3 from the review of commit `4d711a4`: inaccurate manual DX
guidance, a backend-dependent NEARBY allocation assertion and a nonportable
prop_report blocked-parent assertion. Implement and review one patch at a time
with unattended targeted checks before advancing. Run the complete selected
lane once on the final relevant state. These patches can be reverted separately.

This is a bounded follow-up to [v3](command-fix-scope-ledger-v3.md); it does not
replace its contracts or qualify pending checkpoints 28a, 32 or final 35.

## R1 - Actual DX character and length guidance

Replace the free-text promise in HELP DX with:

```text
Comment: ASCII letters, digits, spaces, and / # @ - . , * ? _.
Other punctuation and non-ASCII characters are rejected.
The whole command has a node-configured byte limit (default 128).
Mode/report may be parsed from the comment.
```

Update relevant guides, HELP assertions, checkout-runner expectations and support
routing; retain the 78-byte HELP width. Reader admission, editing, recovery,
frequency, reputation and queue behavior do not change.

Test the documented alphabet through both transports, every other printable
ASCII byte and representative non-ASCII input. Test the whole edited command at
the exact byte limit and one byte beyond, including a non-default limit; exclude
line endings. Refusals must return no command, queue no spot and preserve the
following valid command after safe recovery. The isolated checkout server must
archive a uniquely labeled accepted post and show refused controls absent after
an accepted processing barrier. Both GO and CC HELP must describe that behavior.

## R2 - Backend-relative allocation guard

Change only tests and qualification wording. Measure required conversions with
public fine/coarse H3 functions on the same grids, independently of the matcher.
The baseline must not call Matches or nearbySpotCells. Matching must add no
allocations beyond those conversions; fully cached fine matching must allocate
zero. Cover uncached, partly cached and fully cached fine cases, both endpoint
orientations and 160m/80m/60m coarse cases. Retain explicit matching results and
existing endpoint/cache/unrelated-filter controls. A known extra heap allocation
must breach the guard.

Require actual non-skipped execution on Windows and Linux with cgo enabled and
disabled. Correct prior allocation wording to name its backend. No matcher,
H3 implementation, cache, model or dependency change is authorized; these
measurements do not establish latency, production capacity or model accuracy.

## R3 - Blocked-parent output assertions

Change only test assertions. For the two regular-file-parent cases, accept only
the appropriate absent/not-directory platform error and verify the blocker is
still a regular file containing its original bytes. Other absence checks remain
strict. Unexpected permission/I/O errors or successful child-path lookups cannot
satisfy the assertion.

JSON-parent failure must produce a nonzero exit and causal diagnostic, preserve
the blocker and produce no Markdown file. Report-parent failure must do the same
while retaining the valid JSON summary written earlier. Run the complete
prop_report suite on Windows and Linux with local inputs and no external LLM.
Production status, output ordering and fallback behavior remain unchanged.

## Detailed test-strategy disposition

The post-approval test-strategy-adversary pass was lead-owned. Current reader,
HELP catalog, checkout driver, NEARBY matcher/backends and prop_report tests were
inspected. All findings below are covered or checker-only refinements; none
requires additional product policy, a production seam or expanded authority.

| Invariant | Stimulus or failure | Observable result / level | False-green risk and checker | Owner |
|---|---|---|---|---|
| DX guidance agrees with ingress | Each printable ASCII byte; non-ASCII; both codecs and command orders | Literal accepted set; rejected command empty; next physical line intact / reader | Generated HELP and snapshots repeat the same error; independent literal alphabet and HELP wording assertions | commands, telnet |
| Whole-command limit is truthful | Exact limit, limit+1, default and non-default bounds; editing and CRLF | Exact accepted edited bytes or typed length refusal / reader | Measuring only comment or counting EOL; explicit complete command length | telnet |
| Refusal has no posting effect | Unique accepted/refused labels followed by a queued archive barrier, both dialects | Exact positive archive identity; empty refused query; unchanged configuration/revision / checkout | Acknowledgement-only or empty archive passes; positive barrier and independent archive witnesses | runner |
| Matcher adds no allocations | Fine/coarse, cached/uncached, endpoint swaps | Allocation count no higher than independent conversion cost; cached fine zero / unit | Baseline calls matcher or merely raises universal threshold; public-conversion baseline and semantic controls | filter |
| Allocation checker detects extra work | Known escaping allocation added to measured test wrapper | Count exceeds the allowed cost / negative control | Compiler removes the allocation; bounded test sink retains it | filter |
| Blocked path is classified narrowly | Regular parent plus absent/not-directory, nil, permission and I/O errors | Only expected errors accepted; original regular blocker and bytes retained / unit and actual main | Any error treated as absence; classification controls and real output evidence | prop_report |
| Output order and status survive | JSON-parent/report-parent and full existing local fixture suite | Nonzero causal failure, correct JSON partial success, no false success output / actual main | A test helper hides production outcome; subprocess status and independently parsed saved JSON | prop_report |

## Final validation and records

- Native Windows full ordinary stable-path test lane, vet, Staticcheck and lint.
- Affected package suites on Linux with CGO_ENABLED=1 and CGO_ENABLED=0.
- Offline Python runner checks and isolated checkout-server regression.
- Required named allocation cases must run; skips or missing tools are gaps.
- Update the execution record, relevant TSRs and support links. Support-agent
  documentation impact is Required for R1; test-only R2/R3 route durable
  troubleshooting learning without changing operator runtime contracts.
- No architectural decision changes; no new ADR is required.
- Generate/check code maps last, review the final diff and map each finding to
  implementation and observed evidence. Preserve original v3 evidence as history.

Stop and revise the scope if completion requires runtime or H3 changes,
dependency changes, unrelated repairs or other new material behavior. Do not
claim complete v3 signoff from these three repairs.

Exact authorization required before implementation: Approved v4

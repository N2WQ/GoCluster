# TSR-0059 - Fixed Setting Argument Arity

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: telnet settings, diagnostic mode, argument admission
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

DIALECT and several fixed-value settings could consume the first value and
silently ignore extra words. SET DIAG could change the session mode; SET SOLAR
could reset the next scheduled time even though its input was malformed.

## Root cause or best current explanation

Handlers tested for a missing value without checking the complete token count.
SHOW DEDUPE accepted a tail without checking arity. Normal value parsing and
detached publication then treated the command as valid.

## Fix or mitigation

Admit the documented argument count before value lookup, runtime mutation or
publication intent. Preserve every valid value and alias, including CONFIDENCE
for CONF diagnostics, PATHSAMPLES DEFAULT stored as zero and SOLAR OFF as zero.
GRID's earlier exact guard remains a compatibility control. Query DIALECT with
no argument, list with LIST, or switch with one name; SHOW DEDUPE takes no value.

Wrong arity returns usage with unchanged live/configured/preset/revision/saved
state. DIAG remains session-only. Invalid SOLAR retains the exact next deadline;
valid human SOLAR deliberately reschedules even when its cadence is unchanged.
Machine and LOAD scheduling semantics remain unchanged. Existing documented
usage and approved preservation contracts control these choices; no new ADR
is introduced.

## Checker Boundaries

Permanent tests exercise both dialects, case/whitespace, missing/extra arguments,
zero commit attempts, preset association, raw path and diagnostic state and the
exact SOLAR next deadline with the existing injected clock. Positive controls
cover each setting, noise/diagnostic/dedupe/cadence value, valid dialect aliases
and query forms, direct runtime values and actual saved records. Bounded staged
tail fuzz checks refusal and absent publication/rescheduling intent.

The real checkout fixture constructs the existing empty predictor from loaded
private path_reliability.yaml. Actual PATHSAMPLES controls require its known
cluster floor, override and DEFAULT behavior; no feed or prediction workers
run. Exact replies precede correlated GET; invalid commands preserve all saved
owners plus session diagnostics. Positive controls check immediate saved and
configured values, relevant runtime status, and session-only DIAG without saves.
Replacement children restore preferences and reset diagnostics to OFF.

Offline oracle controls reject wrong configured/runtime/disk values, changed
session mode on refusal and unintended persistent changes from DIAG. Numeric
zero controls require exact integer types, retaining omitted-field zero while
refusing boolean/float/string/null substitutes for canonical setter output.
This checker rule does not change historical storage-decoder compatibility.
Scheduling deadline proof is the clock-controlled Go test, since GET does not expose that
deadline. This is local command validation, not a claim about path model quality,
SOLAR availability/message delivery, production load or external clusters.

## Decision Disposition And Verification

Observed results and failures are in the
[checkpoint execution record](../command-fix-execution.md#checkpoint-12-fixed-argument-settings).
The [telnet guide](../../telnet/README.md#human-configuration-readbacks) describes
the admitted arguments; HELP SET DIAG documents the preserved CONFIDENCE alias.

Support answer: supply one value to the fixed-value setting. Remove trailing
words after SHOW DEDUPE or DIALECT's query/list/value form. A usage response
preserves the selection and SOLAR schedule. CONF and CONFIDENCE both select
confidence diagnostic comments.

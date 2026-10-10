# TSR-0060 - DEDUPE Configured And Effective Readback

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: telnet DEDUPE, exact preferences, effective lanes, readback bounds
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

Login could preserve a marked record's exact SLOW setting while resolving its
runtime lane to FAST on a FAST-only server. SHOW DEDUPE then displayed FAST as
the saved selection. SHOW SETTINGS preserved raw strings but could misclassify
a recognized padded token or an unknown token as unavailable.

## Root cause or best current explanation

SHOW DEDUPE read the effective atomic field and called it saved policy.
SHOW SETTINGS compared raw strings with EqualFold instead of recognizing the
existing trimmed tokens; every difference was described as unavailability.

## Fix or mitigation

Read exact configured settings under the existing full-SSID lease and describe
the actual effective lane separately. Retain compact SHOW output when a
recognized configured policy matches that lane. Unavailable selections,
unrecognized strings, usable NEARBY overrides and disabled suppression have
distinct explanations. Quoted strings preserve bytes and complete escapes.

SHOW DEDUPE uses the existing counting/quoting owner: ASCII, CRLF, 78-byte lines,
and a complete 65,536-byte response cap. It refuses oversized responses before
publishing any partial output. SHOW SETTINGS keeps its existing formatter and
delivery/pause owner. No read publishes settings or commits a saved record.

Admission also reserves the existing automatic pause footer when the response
would trigger it. A body that fits alone cannot publish a final response over
the wire cap. This preserves the shared pause and writer behavior.

Empty configuration still means the server default. Nonempty unknown stored
values retain their existing MED interpretation before availability fallback.
Marked values remain exact; legacy decoding still performs its existing migration.
Human SET and LOAD continue saving their applied fallback. Machine validation
continues refusing explicitly unavailable policies. No schema or new durable
policy is introduced, so no ADR is needed.

## Checker Boundaries

Permanent controls restore actual marked records, establish revision and preset
owners, then repeat both reads with zero commit attempts and unchanged complete
configuration, effective atomic state, revision, cloned preset and exact saved
bytes. Both dialects cover matching lanes, empty/mixed/padded/whitespace/unknown
tokens, unavailable SLOW, usable/inactive NEARBY, FAST-disabled fallback and all
disabled lanes. Literal quoting vectors recover exact unusual bytes; expanded
and long responses refuse without partial success or owner changes.

The cumulative runner adds separate FAST-only profiles for both transports,
with loaded secondary-window settings and literal marked SLOW records. Each
profile replaces the child and repeats exact SHOW DEDUPE feedback, SHOW SETTINGS
rows, full GET configuration/revision/preset/effective state and immediate disk
equality. Positive human fallback and machine refusal remain separate controls.
Startup and retirement receipts verify actual lane flags; offline controls
refuse mismatched/missing/non-boolean availability, wrong configured/effective
state, changed owners and misleading wording.

Large fitting and refused readbacks also traverse the actual send path in both
transports and fresh children. A correlated FILTER GET closes each response;
its prefix must contain only framing and at most one expected input echo.
Trailing output after the success footer or refusal cannot disappear into an
ignored GET prefix, and remaining framing counts against the complete cap.
Offline controls challenge both terminators and malformed or oversized prefixes.

The fixture does not run production secondary dedupers. This qualifies command
readback, loaded availability, persistence preservation and local lifecycle
ownership; it does not establish production throughput or suppression quality.

## Decision Disposition And Verification

Observed results and review disposition are retained in the
[checkpoint execution record](../command-fix-execution.md#checkpoint-13-configured-and-effective-dedupe-readbacks).
The [telnet guide](../../telnet/README.md#human-configuration-readbacks) and
support routing explain the distinction.

Support answer: SLOW remains your saved selection, while effective FAST describes
this server's available lane or a NEARBY override. Reading does not replace that
selection. Disabled suppression is explicit; an active fallback distinguishes
unrecognized saved strings from unavailable choices. A deliberate SET or LOAD
retains its existing applied-fallback rules.

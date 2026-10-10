# TSR-0067 - SHOW HOLD Token Recognition

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: telnet read-pause control, token parsing and status
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

SHOW HOLD matched an exact normalized string. Repeated internal spaces and
extra arguments fell through to generic SHOW handling. With a low automatic
pause threshold, a fallback error could create a pause while the user was
trying to inspect it. Dedicated pause-control responses bypass that trigger.

## Root cause or best current explanation

The owner tokenized PAUSE but compared HOLD as an entire string. Tests and the
fuzz oracle repeated the exact-string assumption. A default ten-row fixture
cannot distinguish correct one-row control replies from generic responses
that would trigger automatic pausing at a one-row threshold.

## Fix or mitigation

Recognize the SHOW/HOLD token pair and require exactly two tokens. Extra
arguments return handled Usage: SHOW HOLD through the existing control path.
Keep literal status rendering, PAUSE/RESUME and unsupported aliases unchanged.
Case and repeated ASCII spaces work on human input; TAB stays invalid on both
real reader codecs. Owner/API token tolerance does not change wire admission.

## Checker Boundaries

Literal owner checks cover inactive, finite, expired and pending status with
unchanged deadlines, cutoff, epoch, pending/closed flags and suppression count.
Composed saved-state/history controls additionally check zero commit calls,
configuration/revision/runtime owners and exact user/preset bytes. Threshold-one
real sessions check valid and malformed control replies in both dialects and
codecs. Gated writer evidence covers pending delivery and the full subsequent
reading interval; actual suppression owners produce counter values.

The real handshake fixture joins handleClient after closing both pipe ends;
it cannot directly observe the production session's hidden writer completion.
The separately owned gated writer is explicitly released, closed and joined.
No telnet goroutine-leak or deployed-load qualification is claimed.

Cumulative checkout cases use complete replies followed by strict CONFIG
barriers and compare full preferences, revision, runtime projection and saved
file bytes/stat identity. The main fixture keeps its threshold ten; it does
not by itself prove the one-row automatic-pause bypass. Countdown comparisons
allow natural time passage, and suppression counters can increase independently
when eligible live traffic arrives.

## Verification And Support

Observed results are retained in the
[execution record](../command-fix-execution.md#checkpoint-19-token-based-show-hold-and-strict-arity).
SHOW HOLD is read-only; RESUME explicitly ends a pause without replaying missed
spots. ADR-0195, ADR-0243 and ADR-0246 retain their established ownership and
delivery contracts; no durable decision changes.

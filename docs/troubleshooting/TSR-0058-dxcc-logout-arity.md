# TSR-0058 - DXCC Lookup And Logout Argument Arity

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: command arity, DXCC lookup and logout aliases
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

SHOW/SH DXCC ignored values after the first query. BYE, QUIT and EXIT returned
the close sentinel even when extra arguments were supplied, ending the session
instead of rejecting a malformed command.

## Root cause or best current explanation

The lookup checked only for a missing query. Logout dispatch returned BYE
without checking token count. The telnet caller correctly treats that sentinel
as a request to queue a farewell and close after the writer flushes it.

## Fix or mitigation

Require exactly one DXCC query before touching CTY lookup. Require argument-free
logout before returning the close sentinel. Wrong arity returns usage; invalid
logout keeps the same session usable. Valid portable lookup and logout forms,
dialects, ownership, queueing, deadlines and writer retirement are retained.
This enforces existing documented usage and the approved arity contract; no new
durable design decision or ADR is introduced.

## Checker Boundaries

Processor tests cover SHOW/SH in both dialects, missing/extra lookup arguments
with a zero-callback requirement, literal valid prefix/portable metadata, every
logout alias, mixed case and whitespace. Wrong logout must return usage rather
than the close sentinel; valid forms retain that exact sentinel.

Real checkout tests require exact bounded refusal responses before correlated
GET, unchanged live/revision/effective/preset/saved state, and a subsequent SHOW
OWN on the original connection after every invalid logout. Valid alias tests
observe the farewell and remote EOF before local socket cleanup. Missing
farewell, timeout, partial trailing bytes/IAC state, duplicate farewell, invalid
CRLF and oversized replies fail the offline oracle. Failure receipts are bounded;
local cleanup after failure is never counted as successful remote closure.

Saved preferences are read in a replacement child. Existing fixture retirement
receipts still require client retirement, consumer join and archive stop. These
are local command/closure checks, not a claim about reconnect/backoff, production
load, all network failures or a changed writer/shutdown implementation.

## Decision Disposition And Verification

Observed checks and limits are in the
[checkpoint execution record](../command-fix-execution.md#checkpoint-11b-dxcc-lookup-and-logout-arity).
The [telnet guide](../../telnet/README.md#human-configuration-readbacks) states
the argument limits.

Support answer: use SHOW DXCC VE or SH DXCC W6/LZ5VV/M with one query. Use BYE,
QUIT or EXIT without trailing words to disconnect. If logout returns usage,
the same connection remains available for subsequent commands.

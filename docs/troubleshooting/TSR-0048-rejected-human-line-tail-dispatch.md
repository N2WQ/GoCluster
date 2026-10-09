# TSR-0048 - Rejected Human Line Tail Dispatch

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: telnet human input and transport lifecycle
Trigger Source: Chat request
Led To ADR(s): ADR-0266

## RCA Summary

- An ingress error returned before consuming its physical line. The session
  retried and could dispatch the remainder as another command.
- The command review at `8aaf677` reported the concrete DX/REJECT BAND vector;
  current source inspection confirmed the return/retry path before the fix.
- Reject the whole line, recover within explicit wire/time bounds, and make
  uncertain or partial reads terminal before caller retry checks.
- Checkout-server regression now compares live configuration, revision and saved
  bytes immediately after the original vector, with one read-only sentinel.
- Support advice: a rejected line has no effects after this fix. A recovery or
  partial-read disconnect requires a new session and a complete valid command.

## Findings

Ziutek can hide negotiation work and block replying before exposing decoded
data. Count wire bytes below the codec and interrupt the socket with an owned
timer. A read deadline alone does not bound a blocked negotiation write.

Editing can erase visible text without erasing partial-read progress. Conversely,
a split LF/NUL that completes the previous CR must not count as a new partial
line. Inherited-context review found that distinction during implementation;
the split-terminator regression protects empty idle retry.

## Decision Linkage

[ADR-0266](../decisions/ADR-0266-bounded-human-line-rejection.md) records safe
recovery, the at-most 64 KiB/five-second bounds, earlier-deadline precedence,
codec-buffer reservation and watchdog ownership. Machine upload policy remains
separate.

## Verification And Monitoring

The [execution record](../command-fix-execution.md#checkpoint-01-physical-line-rejection)
contains exact commands, results and limits. It distinguishes offline controls,
local checkout-server evidence and race/fuzz qualification. This record does
not claim a deployed fix or production-load verification.

Monitor `Rejected ... input` and `Unsafe ... input` logs when users report a
disconnect; obtain the whole input and transport setting. Roll back with its
dependent checkpoints if valid line boundaries, machine uploads or safe idle
sessions regress.

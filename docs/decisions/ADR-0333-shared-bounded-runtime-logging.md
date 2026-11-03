# ADR-0333: Shared Bounded Runtime Logging

- Status: Accepted; v7 implementation and validation in progress
- Date: 2026-11-02
- Decision Origin: Design

## Context

ADR-0234 selected a separate peer diagnostics process to isolate blocked file
operations. The owner later chose one common logging mechanism for all runtime
categories, including peers and the local display. Logs are disposable; keeping
peering and shutdown moving takes priority over retaining every record.

## Decision

Run peer diagnostics in the main executable using the same fixed-queue
asynchronous delivery as other logs. Admission is nonblocking, and a stalled
destination retains its owner until it actually retires. File messages over
2 KiB and messages that cannot be admitted are dropped whole. Dashboard
messages may occupy multiple bounded records and retain the pane's configured
shortening. Logging shutdown uses one two-second deadline.

Restore the established daily file handling, active names, archives, retention
and peer overlong size rotation. Use separate folders for the file categories.
Logs are disposable: loss and recovery warnings are omitted, and a
propagation report may use incomplete logs without warning. Manual reports
retain the ability to read a log from a read-only folder.

The peer diagnostic share must remain within 4 MiB and all logger queues and
retained state within 64 MiB; accounting must include owners retained after a
stall. Buffers inside Windows file operations are outside those limits, as the
owner selected. Proof of the remaining live logger-owned backing remains open.
Caller formatting, goroutine stacks, runtime allocator overhead, OS buffers,
report read buffers and dashboard-owned row storage are accounted separately.
The historical PC92 qualification still applies its 3 MiB diagnostics gate;
the 32 MiB metadata partition and the 480 MiB peer qualification remain open
until their accounting and current implementation have supporting evidence.

## Alternatives considered

1. Keep the separate companion. Rejected because the owner chose a common
   in-process mechanism and accepts loss of disposable logs during a stall.
2. Support shared folders and crash-exact archive merging. Rejected because
   the operational complexity is unnecessary for disposable logs.
3. Wait indefinitely for output. Rejected because it can stop peering or
   shutdown when storage or the local display stalls.

## Consequences

### Benefits

- One executable and one failure model for all runtime logging.
- A stalled destination cannot indefinitely delay application progress.

### Risks

- Records, and in a collision an active log, can be lost without a warning.
- A stuck operation may hold its bounded reservation until restart.

### Operational impact

- Configure separate log folders. Reports can be generated from incomplete logs
  without an indication that records were lost.
- Deploy the main executable without a peer diagnostics companion.
- Use the preapproved `.tmp` path for development validation artifacts.

## Links

- Supersedes only the peer diagnostics process and retirement clauses of
  [ADR-0234](ADR-0234-peer-owned-resources-and-exact-spot-keys.md), the
  companion packaging clause of [ADR-0250](ADR-0250-go127-development-and-launcher.md),
  and the companion-retention clause of
  [ADR-0237](ADR-0237-remove-console-peer-log-status.md). Their other decisions
  remain in force.
- [Operator guide](../OPERATOR_GUIDE.md#logs-and-health)
- Validation status: [shared logging evidence](../shared-logging-v3-validation.md).

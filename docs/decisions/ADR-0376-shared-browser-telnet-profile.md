---
title: Shared browser and Telnet profile
description: One live full-SSID profile owns preferences while two independent transports own delivery and reconnects.
---

# ADR-0376 Shared browser and Telnet profile

- Status: Accepted
- Date: 2026-11-24
- Decision Origin: Design

## Context

Browser and Telnet logins shared one callsign registry entry. Reconnects could
displace the other transport, and a profile tied to a connection could be
restored from disk over a live change. Phase 3 needs the two transports to
share preferences while retaining independent delivery and retirement.

## Decision

Own preferences once per full SSID. The current Telnet and browser slots
share that profile. The existing configuration lease orders changes, saved
metadata and membership transitions. Socket I/O and waits for writer
termination occur outside the lease. Only the current slot can change the
profile or its accounting. Login metadata loads and commits the latest saved
profile within the lease, while reconnects adopt the live state.

Use one primary registry entry per SSID and a second browser entry only for
paired SSIDs. Admission charges both slots. Routing prefers the Telnet
representative, so browser churn leaves that peer revision valid. The
existing per-connection writer delivers notices from bounded advisory state.
Separate view versions let clients refresh configuration, save status and
the preset library independently.

## Consequences

The browser selects GO while preserving the saved CC bank. Failed GO saves
stay live, surface an unsaved warning, and can be retried. No pending
profile survives final release without a successful save. The companion
registry is bounded by registered slots and disappears with the pair.
Advisory presence delivery keeps bounded pending transitions and collapses
those transitions into one current summary when the logger is backlogged.

- [Shared session operator contract](../../telnet/README.md#browser-and-telnet-on-one-callsign)
- Extends [ADR-0375](ADR-0375-private-browser-console-transport.md).

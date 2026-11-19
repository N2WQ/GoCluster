# ADR-0364: PSKReporter Session Recovery

- Status: Accepted
- Date: 2026-11-19
- Decision Origin: Local resilience review and approved scope

## Context

[TSR-0141](../troubleshooting/TSR-0141-pskreporter-startup-and-subscription-recovery.md)
found that an initial broker outage disconnected the source until process
restart. A rejected subscription could also report success while ingest stalled.

## Decision

The PSKReporter supervisor owns dial, handshake, subscription, and retry.
It runs in the background with capped, jittered backoff and waits for session
retirement before starting the next attempt. A connection is ready only while
open and after positive SUBACK results for every requested topic. A missing or
rejected subscription retires the session and enters backoff. Cumulative MQTT
drop counters carry across sessions.

The runtime starts both consumers once. Client shutdown cancels pending work,
gives an established ready session a bounded unsubscribe opportunity, and
closes the transport. It waits for workers before closing the output channels.
Both consumers retire before downstream ingest shutdown.

## Alternatives And Consequences

Paho automatic reconnect by itself cannot repair the initial wiring or a
subscription failure on an open connection. Two retry owners would require
coordination during shutdown and reconnect. One owner reuses Paho for protocol
and bounded inbound dispatch. Existing payload-age checks still describe
recent traffic. A ready connection can be idle.

See [PSKReporter connection and health](../../pskreporter/README.md#connection-and-health).

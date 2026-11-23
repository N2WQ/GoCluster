---
title: Private browser console transport
description: Cloudflare hosts the pilot assets and forwards authenticated WebSocket upgrades to a bounded GoCluster listener.
---

# ADR-0375 Private browser console transport

- Status: Accepted
- Date: 2026-11-23
- Decision Origin: Design

## Context

The browser console needs the same callsign profile, live spot filters, history cursor and settings revision as a cluster client. The production Telnet service also applies admission limits and records recent logins by visitor IP. A proxy that appears as a single user would distort those limits and login records.

## Decision

Cloudflare Access gates the entire pilot hostname. A Worker on that hostname serves the assets and forwards same-origin WebSocket upgrades at `/ws` through a dedicated HTTP VPC service. Its connector runs on the production node and targets the literal loopback listener. The Worker constructs a fresh request to the fixed service and lets the runtime carry the upgraded connection.

GoCluster verifies the loopback peer, exact Host and Origin, a server-only shared secret, and agreement between the edge IP and the Worker-stamped visitor IP before cluster admission. The browser signs in with its own callsign and SSID. The cluster retains ordinary account and quota ownership and omits the previous IP from the browser greeting.

GoCluster caps accepted origin connections, including pending HTTP handshakes. Its session writer owns application messages. The browser sends one text message per command, including the full framed body of a settings upload. The receiver reserves capacity before reading that body and validates the entire message before applying a change. Existing output queues, history budgets and per-session JSON negotiation apply. WebSocket control frames carry the heartbeat.

## Alternatives considered

Serving the site from GoCluster would add static-asset deployment to the cluster binary's operating responsibilities. Connecting the Worker directly to public Telnet would make the upstream TCP peer the apparent visitor and require adapting Telnet negotiation at the edge.

## Consequences

The pilot needs a healthy tunnel on the cluster host, matching secret values and an enabled origin listener. Old configuration files can omit the web section. The shipped configuration leaves the feature disabled. Cloudflare VPC upgrade behavior and visitor attribution require verification on the protected production hostname.

## Links

- [Production console setup](../../cloudflare/gocluster-pilot/README.md) and [updates, acceptance and rollback](../../cloudflare/gocluster-pilot/PRODUCTION.md)
- [Runtime configuration](../../data/config/README.md)
- [Listener admission tests](../../telnet/web_listener_test.go) and [session framing tests](../../telnet/web_session_test.go)
- Extends [ADR-0374, session-only JSON output](ADR-0374-session-json-output.md).

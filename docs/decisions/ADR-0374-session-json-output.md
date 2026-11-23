---
title: Session-only JSON v1 output
description: Authenticated JSON lines, six-character grid display, whole-document YAML replies and bounded history delivery.
---

# ADR-0374 Session-only JSON v1 output

- Status: Accepted
- Date: 2026-11-23
- Decision Origin: Design

## Context

Loggers need machine-readable spots, the active user's view of propagation, and a way to retrieve earlier spots while live delivery continues. The authenticated command connection already owns filtering, settings, command admission and history cursors. The archive has six readable record versions, with different field availability. A slow client can hold selected rows after its archive scan finishes.

## Decision

Every login starts in text. An authenticated client can send `SET JSON ON [VERSION 1]` to select uncompressed UTF-8 JSON lines. The connection writer flushes the text acknowledgement, emits the hello and the two positional schemas, and owns every subsequent outbound byte through the flushed `json-off` notice. Echo and codec negotiation output are muted during that interval. The command reader waits for both switch receipts. CC11 and JSON are mutually exclusive.

The live and historical spot arrays share a public field layout. Recipient settings control CW speeds and live path presentation. Grid display precision is six characters at each endpoint, including when the original observation is more precise. The hello advertises the node's maximum spot line size and the three optional metadata limits. A spot keeps its full stored display comment. Each schema 6 or 7 machine reply, including a terminal error, is a single YAML document carried in one JSON record.

JSON history reuses the existing search and ordering. A selected page has one owner and one process-wide byte charge for its projected rows. The archive view closes before delivery. The writer gives live spots opportunities between history rows and publishes the next cursor after the end record flushes. A busy continuation can be retried. History and machine replies draw on separate budgets. Only manual pause controls suppress live spots in JSON mode.

## Alternatives considered

A second socket would split the authenticated session and its delivery ordering. Rendering JSON from formatted text would lose field provenance and precision. Retaining full decoded archive spots through a slow network write would tie the live cost to archive internals and reader lifetimes.

## Consequences

Clients decode the opening schemas by name and can distinguish absent historical metadata from a known value. They must discard a partial record on disconnect and a failed or incomplete history page. The station display cannot recover locator digits beyond six. Operators can return a session to text with `SET JSON OFF` or roll back the server build. The selection creates no saved-profile or archive migration.

See the [wire guide](../../telnet/README.md#json-output), [CC11 decision](ADR-0351-native-cc11-output-port.md), and [qualification record](../qualification/json-v1.md).

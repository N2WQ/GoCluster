---
title: Plain NEXT for Telnet History
description: Continue an ordinary Telnet history search without copying a token.
---

## Status

Accepted, 2026-11-19. Refines [ADR-0251](ADR-0251-exact-call-paged-history.md).

## Context

Ordinary Telnet users already have one archive search per connection. Showing
a long token made continuing a search harder to distinguish from resuming the
live spot stream.

## Decision

Accept plain `SHOW DX NEXT` and supported aliases for the current search on the
same connection. Ordinary output shows the plain command; VE7CC retains its
coded footer. Coded requests still require the current token. The same cursor,
bounded scan, publication checks, and invalidation rules apply to both forms.

## Alternatives and Consequences

Keeping tokens in ordinary output would preserve footer scraping but retain
the usability problem. An opt-in legacy footer would add an operator setting
and still require clients to change. Instead, ordinary scripts that extracted
the token must send plain `NEXT`. Repeating a plain command advances again if
more archive remains. Rolling back the executable restores the coded footer
and the old input requirement; deploy the clients and server together.

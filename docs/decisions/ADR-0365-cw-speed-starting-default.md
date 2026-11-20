---
title: "ADR-0365: CW Speed Starting Default"
description: Enable CW speed in new telnet profiles and temporary starting settings.
---

- Status: Accepted
- Date: 2026-11-20
- Decision Origin: User request

## Context

The original CW speed preference started OFF. Saved profiles and presets can
omit `cw_speed` when the preference is OFF, including after `SET CWSPEED OFF`.

## Decision

Start new profiles and temporary fallback sessions with CW speed ON. Server
startup validates the same starting settings. Saved profiles and presets
continue to interpret an omitted preference as OFF. Legacy profile upgrades
without a stored preference also use OFF.

## Alternatives considered

Interpreting a missing field as ON would also enable speed for existing users
whose saved choice is OFF.

## Consequences

New users see available CW speed on ordinary-text live spots and archived
history. Diagnostic mode still hides speed, and native CC11 uses canonical
comments. Existing users can enable display with `SET CWSPEED ON`. The default
change requires no profile rewrite or archive reset.

## Links

- Supersedes the starting default in [ADR-0332](ADR-0332-per-user-rbn-cw-speed-display.md).
- User instructions are in the [telnet guide](../../telnet/README.md#rbn-cw-speed).

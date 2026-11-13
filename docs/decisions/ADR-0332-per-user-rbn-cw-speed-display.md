---
title: "ADR-0332: Per-User RBN CW Speed Display"
description: Saved ordinary-text RBN CW speed display with the later native CC11 exception.
---

- Status: Accepted
- Date: 2026-11-01
- Decision Origin: User request

## Context

RBN CW speed-only comments were discarded before a spot reached users or the
archive. Some users want the reported speed while others prefer the current
display. The existing canonical comment is also used for filtering and history
searches, and its parser may rearrange a nonadjacent number and WPM token.

## Decision

Retain adjacent unsigned 0 through 200 WPM pairs from the original RBN CW
payload as bounded spot display metadata. Preserve the existing canonical
comment for filtering and searches. A per-user saved preference defaults OFF;
ON displays complete speeds that fit, while diagnostic mode hides them. Live
and archive history use the same preference. Multiple pairs are retained in
source order, and source text supplies the display for unusual input.

Keep schema-six readback frozen and preserve the preference through schema-six
writes. Existing profiles and presets missing the setting select OFF. Speed
metadata survives owned snapshots, including beacon archive snapshots.

[ADR-0351](ADR-0351-native-cc11-output-port.md) adds an explicit native-output
exception: these display controls apply to ordinary text. Native CC11 keeps the
canonical stored comment regardless of CWSPEED/DIAG. The native port does not
repeat the historical archive reset described below.

## Alternatives considered

1. Store speed only in the canonical comment: rejected because display changes
   could change filter and search behavior.
2. Infer speed from the cleaned comment: rejected because nonadjacent source
   tokens can become adjacent during existing comment parsing.

## Consequences

Users can choose speed independently; history can show speeds retained after
upgrade. A filter or search can match text hidden from a displayed spot.
Missing input speed remains unknown. Fixed-width output may omit speeds that
do not fit, and diagnostics override the preference.

## Operational impact

Stop the server and remove the configured spot archive database before
starting the updated binary. Verify successful archive startup. The archive
then accumulates new records; deleted history is not reconstructed. User
profiles, preset libraries, and the separate bulletin archive are not part of
this reset. Rollback requires compatible executables and saved user data.

## Links

- User and operator instructions: `telnet/README.md`.
- Related beacon behavior: [ADR-0116](ADR-0116-blank-beacon-comment-fallback.md).
- Affected packages: `rbn`, `spot`, `archive`, `filter`, `telnet`, `commands`.

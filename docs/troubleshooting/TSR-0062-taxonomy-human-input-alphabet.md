# TSR-0062 - Taxonomy Human Input Alphabet

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: telnet reader, configured MODE/EVENT names and aliases
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

The taxonomy loader accepts underscores in names and aliases, but the human
command reader rejected that character before semantic handlers could see it.
Configured choices could therefore be advertised yet inaccessible over telnet.

## Root cause or best current explanation

Login and post-login commands shared the base character alphabet. Machine-aware
reading distinguished request intent and COMMENT arguments but did not admit
the additional taxonomy character for ordinary human commands.

## Fix or mitigation

Post-login human input accepts ASCII underscore before recognized machine
intent. Login callsigns and machine request IDs retain their existing
restrictions. Human case normalization, echo, editing, 128-byte command limit
and complete physical-line refusal remain. Unknown names and mixed invalid
lists still fail at semantic admission without publication or persistence.

No taxonomy schema, defaults or matcher transitions change. This restores the
existing loaded-taxonomy ownership in ADR-0069; no new ADR is needed.

## Checker Boundaries

Sequential controls load a private taxonomy and compose the real reader,
detached human coordinator, schema-4 readback, actual durable record and live
and reloaded matching. GO/CC and both codecs cover loaded names, canonical
mode aliases and unknown/mixed refusals. Literal controls qualify editing,
echo, byte boundaries, restricted login and terminal machine intent. A bounded
fuzz target checks an independently specified ASCII alphabet and safe next-line
recovery; curated controls cover framing and editing excluded from that oracle.

The cumulative checkout profiles load private underscore names and seed explicit
metadata through the existing bounded bridge. Capability readback, actual
human commands, immediate saved bytes, archive selections, VALIDATE refusal
to apply, repeated reads and fresh-child restoration qualify command behavior.
These checks do not establish event inference, model quality, production load,
external-cluster behavior or early machine-error wire framing.

## Verification And Support

Observed results are retained in the
[execution record](../command-fix-execution.md#checkpoint-15-configured-taxonomy-underscores-at-human-ingress).
For configured names containing underscores, use normal MODE/EVENT commands in
either dialect. Machine request IDs still use letters, digits or hyphens.

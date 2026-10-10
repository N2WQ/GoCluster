# TSR-0065 - HELP Routing And Alias Coverage

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: commands HELP routing, telnet dispatch aliases and taxonomy
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

Working SH aliases and argument-bearing command topics were missing from the
HELP normalizer. CC returned early before ordinary shared PASS/REJECT routing.
Slash filter readbacks execute in both dialects, but their HELP coverage did
not. Character-prefix checks could instead route unsupported names such as
SHOW DXCCX to an unrelated supported topic.

## Root cause or best current explanation

The hand-maintained normalizer duplicated only part of executable dispatch.
Catalog-derived checks established that existing HELP entries resolved; they
could not discover an executable alias that the catalog omitted. Generic prefix
matching also lacked command-token boundaries and narrow-topic precedence.

## Fix or mitigation

Install source-verified aliases in the finite request-owned HELP catalog, then
match its longest complete head. Specific COMMENT, MINSNR and NEARBY topics win
over generic filter help. Bare SHOW/SH do not absorb unknown subcommands. Fixed
CC shortcuts win before loaded mode eligibility, matching executable dispatch.
Arguments select usage help; they are never sent to command owners.

Executable grammar, machine admission, configuration, storage, session and
taxonomy ownership remain unchanged. Unsupported SH readback spellings and
GO slash history are not introduced. ADR-0264's compiled-help contract and
ADR-0089's SOLAR discovery remain authoritative; no new ADR is required.

## Checker Boundaries

Independent literal heads derived from executable owners check the intended
topic, not just absence of Unknown. Reverse alias checks detect unsupported
advertising. Collision negatives and isolated custom/disabled taxonomy vectors
check token boundaries, narrow-topic priority and fixed-shortcut precedence.
Composed and cumulative local controls preserve configuration, revision, preset
and exact saved bytes during HELP, allowing its deliberate automatic read pause.

Short HELP responses have no protocol end marker. Their source-verified literal
terminal is consumed before a correlated CONFIG barrier; long responses include
the complete counted pause footer first. Strict trailing-prefix checks and
offline suffix faults prevent a plausible header from concealing unread output.
These qualify local navigation and ownership, not production load, deployed
servers, scientific/model semantics or every taxonomy/deployment configuration.

## Verification And Support

Observed results are retained in the
[execution record](../command-fix-execution.md#checkpoint-18a-complete-help-routing-and-aliases).
Use HELP followed by the actual command spelling, optionally with arguments.
An available HELP topic supplies usage; executable owners still validate values
and arity. Use the canonical displayed spelling when an alias is unsupported.

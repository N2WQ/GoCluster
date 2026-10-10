# TSR-0064 - CC HELP Category Grammar

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: commands HELP, telnet filter categories and advertised forms
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

CC HELP reused one generic list-type inventory containing COMMENT and MINSNR,
then advertised inline ON/OFF for every type. Actual owners have different
grammars. Unsupported COMMENT aliases could therefore appear supported, while
special categories rejected the generic ALL argument created by inline flags.

## Root cause or best current explanation

The catalog reused filter categories as executable CC list types instead of
distinguishing ordinary lists, switches and exceptional owners. HELP topic
resolution and substring checks did not execute advertised commands. An existing
positive test helper also failed to reject Unknown responses, so its success
check alone could not establish that a command changed the intended state.

## Fix or mitigation

Build both CC verb entries and the CC reference from the same category-specific
grammar. List the 18 ordinary categories separately from switches; SOURCE still
takes one value. COMMENT uses shared PASS/REJECT forms without a SET/UNSET
mutation alias. Numeric MINSNR has the same inclusive-minimum meaning with
either verb; SET clears with ALL and UNSET with NONE. NEARBY follows explicit
ON/OFF with either verb. Only SET strips inline flags for ordinary categories
and switches; NOFILTER supports SET and /ON only. DXBM takes numeric codes,
with SET's existing slash/spaced PASS/REJECT variants. UNSET strips no flags.

Shared PASS/REJECT commands already execute through the CC fallback. No parser,
handler, configuration, storage, matcher or lifecycle contract changes.
ADR-0264's compiled HELP ownership remains authoritative; no new ADR is needed.

## Checker Boundaries

Literal catalog groups and width controls detect omitted or unsupported types
and blanket flags. Composed acceptance cases check intended live state,
immediate persisted configuration and unchanged unrelated owners. Refusals
require zero commit attempts and preserve exact durable bytes, configuration,
revision and preset, rather than merely containing Usage or avoiding Invalid.
The cumulative local checkout adds real HELP and special-command checks with
complete response barriers and fresh-child readbacks. Offline faults test the
checker itself. These establish local command contracts, not production load,
model quality, external-cluster behavior or every deployment configuration.

## Verification And Support

Observed commands and results are retained in the
[execution record](../command-fix-execution.md#checkpoint-17-category-specific-cc-help-grammar).
Use HELP SET/FILTER, HELP UNSET/FILTER and HELP FILTERS for grouped syntax.
Follow the category's form; ON/OFF, ALL/NONE and verb names are not interchangeable
across all owners. Check live and saved readbacks when diagnosing a change.
